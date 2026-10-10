//! Tachyon transaction aggregation for mined block templates.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use rand_10::rngs::ThreadRng;
use tokio::time::timeout;
use tower::{BoxError, Service, ServiceExt};
use zakura_chain::{
    block::{self, Block},
    parameters::Network,
    tachyon,
    transaction::{Transaction, UnminedTx, VerifiedUnminedTx, WtxId},
};
use zakura_node_services::mempool;
use zakura_state::{ReadRequest, ReadResponse, TachyonMiningData};
use zcash_tachyon::stamp::StampState as _;
use zcash_tachyon::{Bundle, EpochIndex, PointerStamp, ProofStamp, TachyonBundle};

const STATE_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const AGGREGATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Aggregate the selected autonome Tachyon transactions without changing their effecting data.
///
/// A Tachyon transaction is omitted unless its anchor and tachygrams are valid for the exact
/// candidate tip. Proving failures leave the remaining safe transactions autonome, so block
/// template generation remains available while aggregation is temporarily unavailable.
pub async fn aggregate_transactions<S, M>(
    network: Network,
    candidate_height: block::Height,
    tip_hash: block::Hash,
    mut read_state: S,
    mempool: M,
    transactions: Vec<VerifiedUnminedTx>,
) -> Vec<VerifiedUnminedTx>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Send + Clone + 'static,
    S::Future: Send + 'static,
    M: Service<mempool::Request, Response = mempool::Response, Error = BoxError>
        + Send
        + Clone
        + 'static,
    M::Future: Send + 'static,
{
    if !transactions
        .iter()
        .any(|tx| tx.transaction.transaction().has_tachyon_shielded_data())
    {
        return transactions;
    }

    let cached_aggregates = match timeout(
        STATE_QUERY_TIMEOUT,
        mempool
            .clone()
            .oneshot(mempool::Request::TachyonAggregates(tip_hash)),
    )
    .await
    {
        Ok(Ok(mempool::Response::TachyonAggregates(aggregates))) => aggregates,
        _ => Vec::new(),
    };
    let candidates = transactions
        .iter()
        .filter_map(|tx| autonome_bundle(tx.transaction.transaction()))
        .chain(
            cached_aggregates
                .iter()
                .filter_map(|tx| proof_bundle(tx.transaction())),
        )
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return without_tachyon_transactions(transactions);
    }

    let anchors = candidates
        .iter()
        .map(|bundle| tachyon::Anchor::from(bundle.stamp.anchor))
        .collect::<HashSet<_>>();
    let tachygrams = candidates
        .into_iter()
        .flat_map(|bundle| bundle.stamp.tachygrams.iter().copied())
        .map(tachyon::Tachygram::from)
        .collect::<HashSet<_>>();

    let request = ReadRequest::TachyonMiningData {
        anchors,
        tachygrams,
        tip_hash,
        candidate_height,
    };
    let state_response = timeout(STATE_QUERY_TIMEOUT, async move {
        read_state.ready().await?.call(request).await
    })
    .await;

    let mining_data = match state_response {
        Ok(Ok(ReadResponse::TachyonMiningData(Some(mining_data)))) => mining_data,
        Ok(Ok(ReadResponse::TachyonMiningData(None))) => {
            return without_tachyon_transactions(transactions);
        }
        Ok(Ok(_)) => unreachable!("TachyonMiningData requests have matching responses"),
        Ok(Err(error)) => {
            tracing::warn!(?error, "could not load Tachyon mining data");
            return without_tachyon_transactions(transactions);
        }
        Err(_) => {
            tracing::warn!("timed out loading Tachyon mining data");
            return without_tachyon_transactions(transactions);
        }
    };

    let mut transactions =
        valid_for_candidate(&network, candidate_height, transactions, &mining_data);
    apply_cached_aggregates(
        &network,
        candidate_height,
        &mut transactions,
        &mining_data,
        cached_aggregates,
    );
    if transactions
        .iter()
        .filter_map(|tx| proof_bundle(tx.transaction.transaction()))
        .count()
        < 2
    {
        return transactions;
    }

    let fallback_transactions = transactions.clone();
    static PROVER: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
    let Ok(permit) = PROVER
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
        .try_acquire_owned()
    else {
        return fallback_transactions;
    };
    let original_bytes: usize = transactions.iter().map(|tx| tx.transaction.size()).sum();
    let aggregation = tokio::task::spawn_blocking(move || {
        // Remain charged after a caller's timeout until the non-cancellable proof work ends.
        let _permit = permit;
        aggregate_with_data(&network, transactions, mining_data)
    });

    let transactions = match timeout(AGGREGATION_TIMEOUT, aggregation).await {
        Ok(Ok(Ok(transactions))) => {
            if transactions
                .iter()
                .map(|tx| tx.transaction.size())
                .sum::<usize>()
                <= original_bytes
            {
                transactions
            } else {
                fallback_transactions
            }
        }
        Ok(Ok(Err(error))) => {
            tracing::warn!(%error, "could not aggregate Tachyon transactions");
            fallback_transactions
        }
        Ok(Err(error)) => {
            tracing::warn!(?error, "Tachyon aggregation task failed");
            fallback_transactions
        }
        Err(_) => {
            tracing::warn!("timed out aggregating Tachyon transactions");
            fallback_transactions
        }
    };
    let publish = transactions
        .iter()
        .filter(|tx| !tx.transaction.tachyon_dependencies().is_empty())
        .map(|tx| mempool::Gossip::Tx(tx.transaction.clone()))
        .collect::<Vec<_>>();
    if !publish.is_empty() {
        // Admission still performs full verification and reserves the serving lease before gossip.
        let _ = timeout(
            STATE_QUERY_TIMEOUT,
            mempool.oneshot(mempool::Request::Queue(publish)),
        )
        .await;
    }
    transactions
}

fn apply_cached_aggregates(
    network: &Network,
    height: block::Height,
    transactions: &mut [VerifiedUnminedTx],
    mining_data: &TachyonMiningData,
    mut candidates: Vec<UnminedTx>,
) {
    use zakura_chain::tachyon::aggregation::check_package;
    // Prefer broader coverage; exact-ID checks below prevent overlap or double selection.
    candidates.sort_by_key(|tx| std::cmp::Reverse(tx.tachyon_dependencies().len()));
    for candidate in candidates {
        if check_package(&candidate).is_err() {
            continue;
        }
        let bundle =
            proof_bundle(candidate.transaction()).expect("package check requires proof stamp");
        let Some(&anchor_height) = mining_data
            .anchor_heights
            .get(&tachyon::Anchor::from(bundle.stamp.anchor))
        else {
            continue;
        };
        if !tachyon::within_scan_window(network, anchor_height, height)
            || bundle.stamp.tachygrams.iter().any(|gram| {
                mining_data
                    .revealed_tachygrams
                    .contains(&tachyon::Tachygram::from(*gram))
            })
        {
            continue;
        }
        let indices: Option<Vec<_>> = candidate
            .tachyon_dependencies()
            .iter()
            .map(|original| {
                transactions
                    .iter()
                    .position(|tx| tx.transaction.id() == original.id())
            })
            .collect();
        let Some(indices) = indices else {
            continue;
        };
        let selected: HashSet<_> = indices.iter().copied().collect();
        let grams: BTreeSet<_> = bundle.stamp.tachygrams.iter().copied().collect();
        if transactions
            .iter()
            .enumerate()
            .filter(|(index, _)| !selected.contains(index))
            .filter_map(|(_, tx)| proof_bundle(tx.transaction.transaction()))
            .any(|other| {
                other
                    .stamp
                    .tachygrams
                    .iter()
                    .any(|gram| grams.contains(gram))
            })
        {
            continue;
        }
        let Some(carrier) = indices.iter().copied().find(|index| {
            transactions[*index].transaction.id().mined_id() == candidate.id().mined_id()
        }) else {
            continue;
        };
        let Ok(pointer) =
            PointerStamp::try_from(WtxId::from(candidate.transaction().as_ref()).as_bytes())
        else {
            continue;
        };
        let mut replacements = Vec::new();
        for &index in &indices {
            let mut tx = transactions[index].clone();
            if index == carrier {
                tx.transaction = candidate.clone();
            } else {
                let original = autonome_bundle(tx.transaction.transaction())
                    .expect("exact original IDs refer to autonomes");
                let adjunct = original.clone().strip(pointer);
                replace_bundle(&mut tx, TachyonBundle::Adjunct(adjunct));
            }
            replacements.push((index, tx));
        }
        let old_bytes: usize = indices
            .iter()
            .map(|index| transactions[*index].transaction.size())
            .sum();
        let new_bytes: usize = replacements
            .iter()
            .map(|(_, tx)| tx.transaction.size())
            .sum();
        if new_bytes > old_bytes {
            continue;
        }
        for (index, tx) in replacements {
            transactions[index] = tx;
        }
    }
}

fn valid_for_candidate(
    network: &Network,
    candidate_height: block::Height,
    transactions: Vec<VerifiedUnminedTx>,
    mining_data: &TachyonMiningData,
) -> Vec<VerifiedUnminedTx> {
    let mut seen_descriptors = BTreeSet::new();
    let mut seen_tachygrams = BTreeSet::new();
    let mut omitted_ids = HashSet::new();
    let mut valid = Vec::with_capacity(transactions.len());

    // ZIP-317 selection orders parents before descendants, so recording every omitted mined ID
    // removes its dependent transactions transitively in this single pass.
    for transaction in transactions {
        let mined_id = transaction.transaction.id().mined_id();
        if transaction
            .transaction
            .transaction()
            .spent_outpoints()
            .any(|outpoint| omitted_ids.contains(&outpoint.hash))
        {
            omitted_ids.insert(mined_id);
            continue;
        }

        let Some(data) = transaction
            .transaction
            .transaction()
            .tachyon_shielded_data()
        else {
            valid.push(transaction);
            continue;
        };
        let TachyonBundle::Proven(bundle) = &data.0 else {
            omitted_ids.insert(mined_id);
            continue;
        };

        if !bundle.is_autonome() || bundle.verify_coverage(&[]).is_err() {
            omitted_ids.insert(mined_id);
            continue;
        }

        let anchor = tachyon::Anchor::from(bundle.stamp.anchor);
        let Some(&anchor_height) = mining_data.anchor_heights.get(&anchor) else {
            omitted_ids.insert(mined_id);
            continue;
        };
        if !tachyon::within_scan_window(network, anchor_height, candidate_height) {
            omitted_ids.insert(mined_id);
            continue;
        }

        let descriptors = bundle
            .actions
            .iter()
            .map(|action| action.descriptor())
            .collect::<BTreeSet<_>>();
        if descriptors
            .iter()
            .any(|item| seen_descriptors.contains(item))
        {
            omitted_ids.insert(mined_id);
            continue;
        }

        let tachygrams = bundle
            .stamp
            .tachygrams
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if tachygrams.iter().any(|item| {
            seen_tachygrams.contains(item)
                || mining_data
                    .revealed_tachygrams
                    .contains(&tachyon::Tachygram::from(*item))
        }) {
            omitted_ids.insert(mined_id);
            continue;
        }

        seen_descriptors.extend(descriptors);
        seen_tachygrams.extend(tachygrams);
        valid.push(transaction);
    }

    valid
}

fn without_tachyon_transactions(transactions: Vec<VerifiedUnminedTx>) -> Vec<VerifiedUnminedTx> {
    let mut omitted_ids = HashSet::new();
    let mut valid = Vec::with_capacity(transactions.len());

    // Preserve dependency closure when a Tachyon parent is omitted.
    for transaction in transactions {
        let mined_id = transaction.transaction.id().mined_id();
        let depends_on_omitted = transaction
            .transaction
            .transaction()
            .spent_outpoints()
            .any(|outpoint| omitted_ids.contains(&outpoint.hash));

        if transaction
            .transaction
            .transaction()
            .has_tachyon_shielded_data()
            || depends_on_omitted
        {
            omitted_ids.insert(mined_id);
        } else {
            valid.push(transaction);
        }
    }

    valid
}

fn aggregate_with_data(
    network: &Network,
    mut transactions: Vec<VerifiedUnminedTx>,
    mining_data: TachyonMiningData,
) -> Result<Vec<VerifiedUnminedTx>, String> {
    let mut epoch_groups = BTreeMap::<u32, Vec<(usize, block::Height)>>::new();

    for (index, transaction) in transactions.iter().enumerate() {
        let Some(bundle) = proof_bundle(transaction.transaction.transaction()) else {
            continue;
        };
        let anchor = tachyon::Anchor::from(bundle.stamp.anchor);
        let Some(&height) = mining_data.anchor_heights.get(&anchor) else {
            continue;
        };
        let Some(epoch) = tachyon::epoch(network, height) else {
            continue;
        };

        epoch_groups.entry(epoch).or_default().push((index, height));
    }

    let mut rng = rand_10::rng();
    for (epoch, group) in epoch_groups {
        if group.len() < 2 {
            continue;
        }

        aggregate_epoch_group(
            &mut rng,
            &mut transactions,
            &mining_data.blocks,
            EpochIndex::new(epoch),
            &group,
        )?;
    }

    Ok(transactions)
}

fn aggregate_epoch_group(
    rng: &mut ThreadRng,
    transactions: &mut [VerifiedUnminedTx],
    blocks: &BTreeMap<block::Height, Arc<Block>>,
    epoch: EpochIndex,
    group: &[(usize, block::Height)],
) -> Result<(), String> {
    let originals: Vec<_> = group
        .iter()
        .flat_map(|(index, _)| {
            let tx = &transactions[*index].transaction;
            if tx.tachyon_dependencies().is_empty() {
                vec![tx.clone()]
            } else {
                tx.tachyon_dependencies().to_vec()
            }
        })
        .collect();
    let targets: HashSet<_> = group
        .iter()
        .map(|(index, _)| {
            WtxId::from(transactions[*index].transaction.transaction().as_ref()).as_bytes()
        })
        .collect();
    let covered_indices: Vec<_> = transactions
        .iter()
        .enumerate()
        .filter_map(
            |(index, tx)| match &tx.transaction.transaction().tachyon_shielded_data()?.0 {
                TachyonBundle::Adjunct(bundle)
                    if targets.contains(&bundle.stamp.stamp_digest()) =>
                {
                    Some(index)
                }
                _ => None,
            },
        )
        .collect();
    let target_height = group
        .iter()
        .map(|(_index, height)| *height)
        .max()
        .expect("non-empty aggregation groups have a target height");
    let target_anchor = group
        .iter()
        .find_map(|(index, height)| {
            (*height == target_height)
                .then(|| proof_bundle(transactions[*index].transaction.transaction()))
                .flatten()
                .map(|bundle| bundle.stamp.anchor)
        })
        .expect("group transactions have proof stamps");

    let mut lifted_stamps = Vec::with_capacity(group.len());

    for &(index, start_height) in group {
        let bundle = proof_bundle(transactions[index].transaction.transaction())
            .expect("group transactions have proof stamps");
        let target = WtxId::from(transactions[index].transaction.transaction().as_ref()).as_bytes();
        let covered: Vec<_> = covered_indices
            .iter()
            .filter_map(|index| {
                match &transactions[*index]
                    .transaction
                    .transaction()
                    .tachyon_shielded_data()?
                    .0
                {
                    TachyonBundle::Adjunct(bundle) if bundle.stamp.stamp_digest() == target => {
                        Some(bundle.as_dyn())
                    }
                    _ => None,
                }
            })
            .collect();
        let descriptors = bundle
            .actions
            .iter()
            .map(|action| action.descriptor())
            .chain(
                covered
                    .iter()
                    .flat_map(|bundle| bundle.actions.iter().map(|action| action.descriptor())),
            )
            .collect::<BTreeSet<_>>();
        let mut lifted_bundle = bundle.clone();

        if lifted_bundle.stamp.anchor != target_anchor {
            let mut next_bundles = Vec::new();
            for height_value in (start_height.0 + 1)..=target_height.0 {
                let height = block::Height(height_value);
                let block = blocks
                    .get(&height)
                    .ok_or_else(|| format!("missing block at {height:?} for Tachyon lift"))?;
                next_bundles.extend(
                    block
                        .transactions
                        .iter()
                        .filter_map(|transaction| proof_bundle(transaction)),
                );
            }

            lifted_bundle = lifted_bundle
                .lift(rng, &covered, (epoch, &next_bundles))
                .map_err(|error| format!("could not lift Tachyon proof stamp: {error}"))?;
        }

        lifted_stamps.push((lifted_bundle.stamp, descriptors));
    }

    let mut lifted_stamps = lifted_stamps.into_iter();
    let (mut merged_stamp, mut merged_descriptors) = lifted_stamps
        .next()
        .expect("non-empty aggregation groups have a first stamp");
    for (stamp, descriptors) in lifted_stamps {
        let combined_descriptors = merged_descriptors
            .union(&descriptors)
            .copied()
            .collect::<BTreeSet<_>>();
        merged_stamp = ProofStamp::merge(
            rng,
            (merged_stamp, merged_descriptors),
            (stamp, descriptors),
        )
        .map_err(|error| format!("could not merge Tachyon proof stamps: {error}"))?;
        merged_descriptors = combined_descriptors;
    }

    let aggregate_index = group[0].0;
    let mut aggregate_bundle =
        proof_bundle(transactions[aggregate_index].transaction.transaction())
            .expect("group transactions have proof stamps")
            .clone();
    aggregate_bundle.stamp = merged_stamp;
    replace_bundle(
        &mut transactions[aggregate_index],
        TachyonBundle::Proven(aggregate_bundle),
    );

    let aggregate_wtxid = WtxId::from(
        transactions[aggregate_index]
            .transaction
            .transaction()
            .as_ref(),
    )
    .as_bytes();
    let pointer = PointerStamp::try_from(aggregate_wtxid)
        .map_err(|error| format!("invalid Tachyon aggregate wtxid: {error}"))?;

    for &(index, _height) in &group[1..] {
        let adjunct = proof_bundle(transactions[index].transaction.transaction())
            .expect("group transactions have proof stamps")
            .clone()
            .strip(pointer);
        replace_bundle(&mut transactions[index], TachyonBundle::Adjunct(adjunct));
    }
    for index in covered_indices {
        let Some(data) = transactions[index]
            .transaction
            .transaction()
            .tachyon_shielded_data()
        else {
            continue;
        };
        if let TachyonBundle::Adjunct(bundle) = &data.0 {
            let mut bundle = bundle.clone();
            bundle.stamp = pointer;
            replace_bundle(&mut transactions[index], TachyonBundle::Adjunct(bundle));
        }
    }

    let package = transactions[aggregate_index]
        .transaction
        .clone()
        .with_tachyon_dependencies(originals);
    if zakura_chain::tachyon::aggregation::check_package(&package).is_ok() {
        transactions[aggregate_index].transaction = package;
    }

    Ok(())
}

fn autonome_bundle(transaction: &Transaction) -> Option<&Bundle<ProofStamp>> {
    let bundle = proof_bundle(transaction)?;
    bundle.is_autonome().then_some(bundle)
}

fn proof_bundle(transaction: &Transaction) -> Option<&Bundle<ProofStamp>> {
    let data = transaction.tachyon_shielded_data()?;
    let TachyonBundle::Proven(bundle) = &data.0 else {
        return None;
    };
    Some(bundle)
}

fn replace_bundle(transaction: &mut VerifiedUnminedTx, bundle: TachyonBundle) {
    let mut mined_transaction = transaction.transaction.transaction().as_ref().clone();
    let Transaction::V7 {
        tachyon_shielded_data,
        ..
    } = &mut mined_transaction
    else {
        unreachable!("Tachyon bundles only appear in V7 transactions");
    };
    *tachyon_shielded_data = Some(bundle.into());
    transaction.transaction = UnminedTx::from(Arc::new(mined_transaction));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use zakura_chain::{
        amount::Amount,
        block::{Block, Height},
        parameters::{testnet::ConfiguredActivationHeights, NetworkUpgrade},
        serialization::ZcashDeserialize,
        transaction::LockTime,
        transparent,
    };
    use zcash_tachyon::{
        entropy::ActionEntropy,
        keys::private,
        note::{CommitmentTrapdoor, Note},
        nullifier,
        stamp::StampState as _,
        value, Anchor, TachygramSetPoly,
    };

    use super::*;

    fn relay_fixture() -> (
        Network,
        Vec<VerifiedUnminedTx>,
        UnminedTx,
        TachyonMiningData,
    ) {
        let network = nutachyon_network();
        let anchor = Anchor::read(&[0; 32][..]).unwrap();
        let originals = vec![
            verified_transaction(anchor),
            verified_transaction(anchor),
            verified_transaction(anchor),
        ];
        let mining_data = TachyonMiningData {
            tip_anchor: tachyon::Anchor::from(anchor),
            anchor_heights: HashMap::from([(tachyon::Anchor::from(anchor), Height(10))]),
            blocks: BTreeMap::new(),
            revealed_tachygrams: HashSet::new(),
        };
        let mut merged = originals.clone();
        aggregate_epoch_group(
            &mut rand_10::rng(),
            &mut merged,
            &BTreeMap::new(),
            EpochIndex::new(0),
            &[(0, Height(10)), (1, Height(10))],
        )
        .unwrap();
        let aggregate = merged[0].transaction.clone();
        zakura_chain::tachyon::aggregation::check_package(&aggregate).unwrap();
        (network, originals, aggregate, mining_data)
    }

    #[test]
    fn relay_aggregate_requires_every_exact_original_before_mining() {
        let (network, original, aggregate, data) = relay_fixture();
        let mut missing = vec![original[0].clone(), original[2].clone()];
        apply_cached_aggregates(
            &network,
            Height(11),
            &mut missing,
            &data,
            vec![aggregate.clone()],
        );
        assert_eq!(missing, vec![original[0].clone(), original[2].clone()]);
        let mut selected = original.clone();
        apply_cached_aggregates(
            &network,
            Height(11),
            &mut selected,
            &data,
            vec![aggregate.clone()],
        );
        assert_eq!(selected[0].transaction.id(), aggregate.id());
        assert!(matches!(
            &selected[1]
                .transaction
                .transaction()
                .tachyon_shielded_data()
                .unwrap()
                .0,
            TachyonBundle::Adjunct(_)
        ));
        assert_eq!(selected[2], original[2]);
        assert_eq!(
            selected
                .iter()
                .map(|tx| tx.transaction.id().mined_id())
                .collect::<Vec<_>>(),
            original
                .iter()
                .map(|tx| tx.transaction.id().mined_id())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn relay_aggregate_can_be_merged_again_and_flattens_manifest() {
        let (network, originals, aggregate, data) = relay_fixture();
        let ids = originals
            .iter()
            .map(|tx| tx.transaction.id().mined_id())
            .collect::<Vec<_>>();
        let mut selected = originals.clone();
        apply_cached_aggregates(&network, Height(11), &mut selected, &data, vec![aggregate]);
        let merged = aggregate_with_data(&network, selected, data).unwrap();
        assert_aggregated(&merged, &ids);
        assert_eq!(merged[0].transaction.tachyon_dependencies().len(), 3);
        zakura_chain::tachyon::aggregation::check_package(&merged[0].transaction).unwrap();
    }

    #[test]
    fn relay_package_rejects_missing_recursive_and_changed_carrier() {
        let (_, originals, aggregate, _) = relay_fixture();
        use zakura_chain::tachyon::aggregation::check_package;
        assert!(
            check_package(&aggregate.clone().with_tachyon_dependencies(vec![
                originals[1].transaction.clone(),
                originals[2].transaction.clone()
            ]))
            .is_err()
        );
        assert!(check_package(
            &aggregate.clone().with_tachyon_dependencies(vec![
                aggregate.clone(),
                originals[1].transaction.clone()
            ])
        )
        .is_err());
        let mut changed = aggregate.transaction().as_ref().clone();
        let Transaction::V7 {
            tachyon_shielded_data: Some(data),
            ..
        } = &mut changed
        else {
            unreachable!()
        };
        let TachyonBundle::Proven(bundle) = &mut data.0 else {
            unreachable!()
        };
        bundle.binding_sig = zcash_tachyon::bundle::Signature::read(&[9; 64][..]).unwrap();
        assert!(check_package(
            &UnminedTx::from(changed)
                .with_tachyon_dependencies(aggregate.tachyon_dependencies().to_vec())
        )
        .is_err());
    }

    #[test]
    fn relay_aggregate_with_expired_anchor_is_not_mined() {
        let (network, originals, aggregate, mut data) = relay_fixture();
        data.anchor_heights.clear();
        let mut selected = originals.clone();
        apply_cached_aggregates(&network, Height(11), &mut selected, &data, vec![aggregate]);
        assert_eq!(selected, originals);
    }

    #[test]
    fn same_anchor_autonome_transactions_are_aggregated() {
        let network = nutachyon_network();
        let anchor = Anchor::read(&[0u8; 32][..]).expect("zero anchor reads");
        let original = vec![verified_transaction(anchor), verified_transaction(anchor)];
        let original_ids = original
            .iter()
            .map(|tx| tx.transaction.id().mined_id())
            .collect::<Vec<_>>();
        let mining_data = TachyonMiningData {
            tip_anchor: tachyon::Anchor::from(anchor),
            anchor_heights: HashMap::from([(tachyon::Anchor::from(anchor), Height(10))]),
            blocks: BTreeMap::new(),
            revealed_tachygrams: HashSet::new(),
        };

        let original = valid_for_candidate(&network, Height(11), original, &mining_data);
        let aggregated = aggregate_with_data(&network, original, mining_data)
            .expect("same-anchor stamps can be merged");

        assert_aggregated(&aggregated, &original_ids);
    }

    #[test]
    fn older_autonome_stamp_is_lifted_before_aggregation() {
        let network = nutachyon_network();
        let start_anchor = Anchor::read(&[0u8; 32][..]).expect("zero anchor reads");
        let intervening_transaction = verified_transaction(start_anchor)
            .transaction
            .transaction()
            .clone();
        let intervening_bundle = proof_bundle(&intervening_transaction)
            .expect("intervening transaction has a proof-stamped bundle");
        let target_anchor = start_anchor
            .next_stamp(EpochIndex::new(0), &intervening_bundle.stamp.tachygram_set)
            .expect("intervening stamp advances the anchor");
        let original = vec![
            verified_transaction(start_anchor),
            verified_transaction(target_anchor),
        ];
        let original_ids = original
            .iter()
            .map(|tx| tx.transaction.id().mined_id())
            .collect::<Vec<_>>();

        let mut intervening_block =
            Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
                .expect("hardcoded genesis block deserializes");
        intervening_block.transactions = vec![intervening_transaction];
        let mining_data = TachyonMiningData {
            tip_anchor: tachyon::Anchor::from(target_anchor),
            anchor_heights: HashMap::from([
                (tachyon::Anchor::from(start_anchor), Height(10)),
                (tachyon::Anchor::from(target_anchor), Height(11)),
            ]),
            blocks: BTreeMap::from([(Height(11), Arc::new(intervening_block))]),
            revealed_tachygrams: HashSet::new(),
        };

        let original = valid_for_candidate(&network, Height(12), original, &mining_data);
        let aggregated = aggregate_with_data(&network, original, mining_data)
            .expect("the older stamp can be lifted and merged");

        assert_aggregated(&aggregated, &original_ids);
        let TachyonBundle::Proven(aggregate) = &aggregated[0]
            .transaction
            .transaction()
            .tachyon_shielded_data()
            .expect("aggregate has Tachyon data")
            .0
        else {
            panic!("the first transaction should carry the aggregate proof");
        };
        assert_eq!(aggregate.stamp.anchor, target_anchor);
    }

    #[test]
    fn stale_anchor_transactions_are_omitted() {
        let network = nutachyon_network();
        let anchor = Anchor::read(&[0u8; 32][..]).expect("zero anchor reads");
        let parent = verified_transaction(anchor);
        let child = verified_dependent_transaction(parent.transaction.id().mined_id());
        let transactions = vec![parent, child];
        let mining_data = TachyonMiningData {
            tip_anchor: tachyon::Anchor::from(anchor),
            anchor_heights: HashMap::from([(tachyon::Anchor::from(anchor), Height(10))]),
            blocks: BTreeMap::new(),
            revealed_tachygrams: HashSet::new(),
        };
        let candidate_height = Height(10 + 2 * tachyon::EPOCH_LENGTH);

        assert!(
            valid_for_candidate(&network, candidate_height, transactions, &mining_data).is_empty(),
            "stale-anchor transactions and their descendants must not enter a block template",
        );
    }

    #[test]
    fn already_revealed_tachygram_transactions_are_omitted() {
        let network = nutachyon_network();
        let anchor = Anchor::read(&[0u8; 32][..]).expect("zero anchor reads");
        let transaction = verified_transaction(anchor);
        let revealed = autonome_bundle(transaction.transaction.transaction())
            .expect("test transaction is autonome")
            .stamp
            .tachygrams
            .iter()
            .next()
            .copied()
            .expect("test stamp has tachygrams");
        let mining_data = TachyonMiningData {
            tip_anchor: tachyon::Anchor::from(anchor),
            anchor_heights: HashMap::from([(tachyon::Anchor::from(anchor), Height(10))]),
            blocks: BTreeMap::new(),
            revealed_tachygrams: HashSet::from([tachyon::Tachygram::from(revealed)]),
        };

        assert!(
            valid_for_candidate(&network, Height(11), vec![transaction], &mining_data).is_empty(),
            "a tachygram already revealed in the scan window must not enter a block template",
        );
    }

    fn nutachyon_network() -> Network {
        Network::new_regtest(
            ConfiguredActivationHeights {
                canopy: Some(1),
                nu5: Some(2),
                nu6: Some(3),
                nu6_1: Some(4),
                nu6_2: Some(5),
                nu6_3: Some(6),
                nu7: Some(8),
                nu_tachyon: Some(10),
                ..Default::default()
            }
            .into(),
        )
    }

    fn assert_aggregated(
        aggregated: &[VerifiedUnminedTx],
        original_ids: &[zakura_chain::transaction::Hash],
    ) {
        assert_eq!(
            aggregated
                .iter()
                .map(|tx| tx.transaction.id().mined_id())
                .collect::<Vec<_>>(),
            original_ids,
            "aggregation must not change mined transaction IDs",
        );

        let TachyonBundle::Proven(aggregate) = &aggregated[0]
            .transaction
            .transaction()
            .tachyon_shielded_data()
            .expect("aggregate has Tachyon data")
            .0
        else {
            panic!("the first transaction should carry the aggregate proof");
        };
        assert!(aggregate.is_aggregate());
        let adjuncts: Vec<_> = aggregated[1..]
            .iter()
            .flat_map(|tx| {
                let TachyonBundle::Adjunct(adjunct) = &tx
                    .transaction
                    .transaction()
                    .tachyon_shielded_data()
                    .expect("adjunct has Tachyon data")
                    .0
                else {
                    panic!("every remaining transaction should point to the aggregate");
                };
                assert_eq!(
                    adjunct.stamp.stamp_digest(),
                    WtxId::from(aggregated[0].transaction.transaction().as_ref()).as_bytes()
                );
                adjunct.descriptors()
            })
            .collect();
        assert!(aggregate.verify_coverage(&adjuncts).is_ok());
    }

    fn verified_transaction(anchor: Anchor) -> VerifiedUnminedTx {
        let bundle = autonome_bundle_at(anchor);
        let transaction = Transaction::V7 {
            network_upgrade: NetworkUpgrade::NuTachyon,
            lock_time: LockTime::min_lock_time_timestamp(),
            expiry_height: Height(0),
            zip233_amount: Amount::zero(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
            tachyon_shielded_data: Some(TachyonBundle::Proven(bundle).into()),
        };

        VerifiedUnminedTx::new(
            UnminedTx::from(transaction),
            Amount::try_from(100_000u64).expect("fee is in range"),
            0,
            0,
            Arc::new(Vec::new()),
        )
        .expect("test transaction pays the conventional fee")
    }

    fn verified_dependent_transaction(
        parent: zakura_chain::transaction::Hash,
    ) -> VerifiedUnminedTx {
        let transaction = Transaction::V7 {
            network_upgrade: NetworkUpgrade::NuTachyon,
            lock_time: LockTime::min_lock_time_timestamp(),
            expiry_height: Height(0),
            zip233_amount: Amount::zero(),
            inputs: vec![transparent::Input::PrevOut {
                outpoint: transparent::OutPoint {
                    hash: parent,
                    index: 0,
                },
                unlock_script: transparent::Script::new(&[]),
                sequence: 0,
            }],
            outputs: vec![transparent::Output::new(
                Amount::zero(),
                transparent::Script::new(&[]),
            )],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
            tachyon_shielded_data: None,
        };

        VerifiedUnminedTx::new(
            UnminedTx::from(transaction),
            Amount::try_from(100_000u64).expect("fee is in range"),
            0,
            0,
            Arc::new(Vec::new()),
        )
        .expect("test transaction pays the conventional fee")
    }

    fn autonome_bundle_at(anchor: Anchor) -> Bundle<ProofStamp> {
        let mut rng = rand_10::rng();
        let spending_key = private::SpendingKey::random(&mut rng);
        let note = Note {
            pk: spending_key.derive_payment_key(),
            value: value::Positive::try_from(100u64).expect("positive value"),
            psi: nullifier::Trapdoor::random(&mut rng),
            rcm: CommitmentTrapdoor::random(&mut rng),
        };
        let padding_note = Note {
            pk: spending_key.derive_payment_key(),
            value: value::Positive::try_from(1u64).expect("positive value"),
            psi: nullifier::Trapdoor::random(&mut rng),
            rcm: CommitmentTrapdoor::random(&mut rng),
        };
        let tachygrams =
            BTreeSet::from([note.commitment().into(), padding_note.commitment().into()]);
        let ask = spending_key.derive_auth_private();
        let theta = ActionEntropy::random(&mut rng);
        let rcv = value::Trapdoor::random(&mut rng);
        let spend = zcash_tachyon::action::Plan::spend(note, theta, rcv, |alpha| {
            ask.derive_action_private(&alpha).derive_action_public()
        });
        let plan = zcash_tachyon::bundle::Plan::new(vec![spend], vec![]);
        let bundle = plan
            .sign(&mut rng, &[0u8; 32], &ask)
            .expect("test bundle signs");
        let coverage = action_descriptor_digest(&bundle.actions);
        let tachygram_set = tachygrams
            .iter()
            .copied()
            .collect::<TachygramSetPoly>()
            .commit();

        bundle.stamp(ProofStamp {
            coverage,
            anchor,
            tachygram_set,
            tachygrams,
            proof: Box::new(ragu::Proof::trivial()),
        })
    }

    fn action_descriptor_digest(actions: &[zcash_tachyon::Action]) -> [u8; 32] {
        let mut descriptors: Vec<[u8; 64]> =
            actions.iter().map(|action| action.descriptor()).collect();
        descriptors.sort_unstable();

        let mut state = blake2b_simd::Params::new()
            .hash_length(32)
            .personal(b"Tachyon-Actions")
            .to_state();
        for descriptor in descriptors {
            state.update(&descriptor);
        }
        state
            .finalize()
            .as_bytes()
            .try_into()
            .expect("hash length is 32")
    }
}
