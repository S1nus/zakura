//! Bounded, flat dependency packages for experimental Tachyon aggregate relay.

use std::collections::HashSet;

use zcash_tachyon::{Bundle, ProofStamp, TachyonBundle};

use crate::transaction::{Transaction, UnminedTx};

/// Maximum number of original transactions, including the original carrier.
pub const MAX_ORIGINALS: usize = crate::transaction::aggregation::MAX_ORIGINALS;
/// Maximum serialized bytes in an aggregate and all its originals.
pub const MAX_PACKAGE_BYTES: usize = 4 * 1024 * 1024;

/// Returns a proof-stamped bundle, excluding absent and pointer-stamped bundles.
pub fn proof_bundle(transaction: &Transaction) -> Option<&Bundle<ProofStamp>> {
    match &transaction.tachyon_shielded_data()?.0 {
        TachyonBundle::Proven(bundle) => Some(bundle),
        _ => None,
    }
}

/// Checks a package's flatness, conflicts, carrier identity, coverage, and byte bounds.
///
/// This does not verify proofs, signatures, fees, or chain context.
pub fn check_package(transaction: &UnminedTx) -> Result<(), &'static str> {
    let originals = transaction.tachyon_dependencies();
    if !(2..=MAX_ORIGINALS).contains(&originals.len()) {
        return Err("aggregate manifest must contain 2..32 originals");
    }
    let aggregate =
        proof_bundle(transaction.transaction()).ok_or("aggregate has no proof stamp")?;
    if aggregate.is_autonome() {
        return Err("autonome transaction must not carry dependencies");
    }
    let mut bytes = transaction.size();
    let mut txids = HashSet::new();
    let mut outpoints = HashSet::new();
    let mut sprout = HashSet::new();
    let mut sapling = HashSet::new();
    let mut orchard = HashSet::new();
    let mut ironwood = HashSet::new();
    let mut carrier = false;
    let mut covered = Vec::new();
    for original in originals {
        bytes = bytes
            .checked_add(original.size())
            .ok_or("package size overflow")?;
        if bytes > MAX_PACKAGE_BYTES || !original.tachyon_dependencies().is_empty() {
            return Err("oversized or recursive aggregate package");
        }
        if !txids.insert(original.id().mined_id()) || original.id() == transaction.id() {
            return Err("duplicate or self-referential original");
        }
        let tx = original.transaction();
        let bundle = proof_bundle(tx).ok_or("original is not proof-stamped")?;
        if !bundle.is_autonome() {
            return Err("original is not autonome");
        }
        if tx.spent_outpoints().any(|item| !outpoints.insert(item))
            || tx.sprout_nullifiers().any(|item| !sprout.insert(item))
            || tx.sapling_nullifiers().any(|item| !sapling.insert(item))
            || tx.orchard_nullifiers().any(|item| !orchard.insert(item))
            || tx.ironwood_nullifiers().any(|item| !ironwood.insert(item))
        {
            return Err("original transactions have spend conflicts");
        }
        if original.id().mined_id() == transaction.id().mined_id() {
            let mut restamped = tx.as_ref().clone();
            let Transaction::V7 {
                tachyon_shielded_data,
                ..
            } = &mut restamped
            else {
                return Err("carrier is not V7");
            };
            *tachyon_shielded_data = transaction.transaction().tachyon_shielded_data().cloned();
            if restamped != *transaction.transaction().as_ref() {
                return Err("carrier differs outside its Tachyon bundle");
            }
            // Replacing the whole bundle above also requires equality of its non-stamp data.
            let pointer = zcash_tachyon::PointerStamp::try_from(
                crate::transaction::WtxId::from(transaction.transaction().as_ref()).as_bytes(),
            )
            .map_err(|_| "invalid aggregate pointer")?;
            if bundle.clone().strip(pointer) != aggregate.clone().strip(pointer) {
                return Err("carrier differs outside its proof stamp");
            }
            carrier = true;
        } else {
            covered.extend(bundle.descriptors());
        }
    }
    if !carrier {
        return Err("manifest is missing the carrier original");
    }
    aggregate
        .verify_coverage(&covered)
        .map_err(|_| "aggregate coverage mismatch")?;
    check_tachygrams(aggregate)
}

/// Reject duplicate public tachygrams before proof verification, including in-memory bundles.
pub fn check_tachygrams(bundle: &Bundle<ProofStamp>) -> Result<(), &'static str> {
    let mut seen = std::collections::BTreeSet::new();
    for gram in &bundle.stamp.tachygrams {
        if !seen.insert(*gram) {
            return Err("duplicate Tachygram in proof stamp");
        }
    }
    Ok(())
}
