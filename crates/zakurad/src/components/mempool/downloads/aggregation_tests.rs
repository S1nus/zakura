//! Dependency exchange tests. Deliberately invalid empty bundles exercise the
//! downloader boundary; the verifier must reject them, never bypass validation.

use super::*;
use futures::StreamExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tower::{service_fn, util::BoxCloneService};
use zakura_chain::{
    amount::Amount,
    parameters::NetworkUpgrade,
    transaction::{aggregation::Manifest, LockTime, Transaction, UnminedTx, WtxId},
};

fn fixture(height: u32, aggregate: bool) -> UnminedTx {
    use zcash_tachyon::{
        bundle::Signature, Anchor, Bundle, ProofStamp, TachygramSetPoly, TachyonBundle,
    };
    let coverage = if aggregate {
        [1; 32]
    } else {
        blake2b_simd::Params::new()
            .hash_length(32)
            .personal(b"Tachyon-Actions")
            .hash(&[])
            .as_bytes()
            .try_into()
            .unwrap()
    };
    let bundle = Bundle {
        value_balance: 0i64.try_into().unwrap(),
        actions: Vec::new(),
        binding_sig: Signature::read(&[0; 64][..]).unwrap(),
        memo: Vec::new(),
        stamp: ProofStamp {
            coverage,
            anchor: Anchor::read(&[0; 32][..]).unwrap(),
            tachygram_set: std::iter::empty().collect::<TachygramSetPoly>().commit(),
            tachygrams: Default::default(),
            proof: Box::new(ragu::Proof::trivial()),
        },
    };
    Transaction::V7 {
        network_upgrade: NetworkUpgrade::NuTachyon,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(height),
        zip233_amount: Amount::zero(),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data: Some(TachyonBundle::Proven(bundle).into()),
    }
    .into()
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Scenario {
    Disabled,
    Unavailable,
    WrongOriginal,
    VerifyFailure,
    Timeout,
}

#[tokio::test(start_paused = true)]
async fn aggregate_downloads_bind_originals_and_do_not_poison_rejection_cache() {
    for scenario in [
        Scenario::Disabled,
        Scenario::Unavailable,
        Scenario::WrongOriginal,
        Scenario::VerifyFailure,
        Scenario::Timeout,
    ] {
        let aggregate = fixture(1, true);
        let id = aggregate.id();
        let original = fixture(2, false);
        let known = fixture(3, false);
        let mut originals = vec![
            WtxId::from(original.transaction().as_ref()),
            WtxId::from(known.transaction().as_ref()),
        ];
        originals.sort_by_key(WtxId::as_bytes);
        let manifest = Manifest {
            aggregate: WtxId::from(aggregate.transaction().as_ref()),
            originals,
        };
        let peer = PeerSocketAddr::from(([203, 0, 113, 7], 8233));
        let manifest_calls = Arc::new(AtomicUsize::new(0));
        let dependency_calls = Arc::new(AtomicUsize::new(0));
        let verifier_calls = Arc::new(AtomicUsize::new(0));
        let (manifest_count, dependency_count, verifier_count) = (
            manifest_calls.clone(),
            dependency_calls.clone(),
            verifier_calls.clone(),
        );
        let known_cache = Arc::new(Mutex::new(super::super::aggregation::Known::default()));
        known_cache.lock().unwrap().remember(known);
        let mut downloads = Downloads::new(
            BoxCloneService::new(service_fn(move |request| {
                let (mut manifest, original, aggregate) =
                    (manifest.clone(), original.clone(), aggregate.clone());
                let (manifest_count, dependency_count) =
                    (manifest_count.clone(), dependency_count.clone());
                async move {
                    match request {
                        zn::Request::TransactionsById(ids) => {
                            assert_eq!(ids, HashSet::from([id]));
                            Ok(zn::Response::Transactions(vec![
                                zn::InventoryResponse::Available((aggregate, Some(peer))),
                            ]))
                        }
                        zn::Request::AggregateDependencies { aggregate, source } => {
                            assert_eq!(aggregate, manifest.aggregate);
                            assert_eq!(source, Some(peer.into()));
                            manifest_count.fetch_add(1, Ordering::SeqCst);
                            if scenario == Scenario::Timeout {
                                return std::future::pending().await;
                            }
                            if scenario == Scenario::Unavailable {
                                manifest.originals.clear();
                            }
                            Ok(zn::Response::AggregateDependencies(manifest))
                        }
                        zn::Request::TransactionsByIdFrom { ids, source } => {
                            assert_eq!(source, peer.into());
                            assert_eq!(ids, HashSet::from([original.id()]));
                            dependency_count.fetch_add(1, Ordering::SeqCst);
                            let original = if scenario == Scenario::WrongOriginal {
                                // Same effects, wrong authorizing data: must never reach the verifier.
                                let mut wrong = original.transaction().as_ref().clone();
                                let Transaction::V7 {
                                    tachyon_shielded_data: Some(data),
                                    ..
                                } = &mut wrong
                                else {
                                    unreachable!()
                                };
                                let zcash_tachyon::TachyonBundle::Proven(bundle) = &mut data.0
                                else {
                                    unreachable!()
                                };
                                bundle.binding_sig =
                                    zcash_tachyon::bundle::Signature::read(&[7; 64][..]).unwrap();
                                let wrong = UnminedTx::from(wrong);
                                assert_eq!(wrong.id().mined_id(), original.id().mined_id());
                                assert_ne!(wrong.id(), original.id());
                                wrong
                            } else {
                                original
                            };
                            Ok(zn::Response::Transactions(vec![
                                zn::InventoryResponse::Available((original, Some(peer))),
                            ]))
                        }
                        _ => panic!("unexpected request: {request:?}"),
                    }
                }
            })),
            BoxCloneService::new(service_fn(move |request| {
                verifier_count.fetch_add(1, Ordering::SeqCst);
                let tx::Request::Mempool { transaction, .. } = request else {
                    panic!("expected mempool verification");
                };
                assert_eq!(transaction.tachyon_dependencies().len(), 2);
                async {
                    Err::<tx::Response, BoxError>("dependency consensus validation failed".into())
                }
            })),
            BoxCloneService::new(service_fn(|request| async move {
                match request {
                    zs::ReadRequest::Transaction(_) => Ok(zs::ReadResponse::Transaction(None)),
                    zs::ReadRequest::Tip => Ok(zs::ReadResponse::Tip(None)),
                    _ => panic!("unexpected state request"),
                }
            })),
            false,
            u64::MAX,
        )
        .with_aggregation(scenario != Scenario::Disabled, known_cache);
        downloads
            .download_if_needed_and_verify(Gossip::Id(id), None, None)
            .unwrap();
        let (_, error) = *tokio::time::timeout(Duration::from_secs(31), downloads.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error, TransactionDownloadVerifyError::DownloadFailed(_)),
            "{scenario:?}: {error:?}"
        );
        let mut storage = super::super::storage::Storage::new(&Default::default());
        storage.reject_if_needed(id, error);
        assert!(
            !storage.contains_rejected(&id),
            "dependency failure must remain retryable"
        );
        assert_eq!(
            verifier_calls.load(Ordering::SeqCst),
            usize::from(scenario == Scenario::VerifyFailure)
        );
        assert_eq!(
            manifest_calls.load(Ordering::SeqCst),
            usize::from(scenario != Scenario::Disabled)
        );
        assert_eq!(
            dependency_calls.load(Ordering::SeqCst),
            usize::from(matches!(
                scenario,
                Scenario::WrongOriginal | Scenario::VerifyFailure
            ))
        );
    }
}
