//! Asynchronous verification of Tachyon proof stamps.

use crate::{block::tachyon::AggregateCoverage, error::BlockError};

use super::spawn_fifo;

/// Verify a relay stamp only after its complete, bounded covered-action set is available.
pub async fn verify_mempool_stamp(
    transaction: zakura_chain::transaction::UnminedTx,
) -> Result<(), crate::error::TransactionError> {
    use crate::error::TransactionError;
    use zakura_chain::tachyon::aggregation::{check_tachygrams, proof_bundle};
    if proof_bundle(transaction.transaction()).is_none() {
        return Ok(());
    }
    // Keep the permit in the worker, so cancellation cannot unbound detached proof work.
    static WORKERS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    let permit = WORKERS
        .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(4)))
        .clone()
        .acquire_owned()
        .await
        .map_err(|error| TransactionError::Other(error.to_string()))?;
    spawn_fifo(move || {
        let _permit = permit;
        let bundle =
            proof_bundle(transaction.transaction()).expect("proof bundle checked before spawning");
        check_tachygrams(bundle).map_err(|error| TransactionError::Other(error.into()))?;
        let covered: Vec<_> = transaction
            .tachyon_dependencies()
            .iter()
            .filter(|original| original.id().mined_id() != transaction.id().mined_id())
            .filter_map(|original| {
                proof_bundle(original.transaction()).map(|bundle| bundle.as_dyn())
            })
            .collect();
        bundle
            .verify_coverage(&covered)
            .map_err(|error| TransactionError::Other(error.to_string()))?;
        match bundle.verify_proof(&mut rand_10::rng(), &covered) {
            Ok(true) => Ok(()),
            Ok(false) => Err(TransactionError::Other(
                "invalid Tachyon proof stamp".into(),
            )),
            Err(error) => Err(TransactionError::Other(error.to_string())),
        }
    })
    .await
    .map_err(|error| TransactionError::Other(error.to_string()))?
}

/// Verifies a Tachyon aggregate's proof stamp against all covered actions.
pub async fn verify_proof_stamp(aggregate: AggregateCoverage) -> Result<(), BlockError> {
    spawn_fifo(move || {
        let adjunct_refs: Vec<_> = aggregate
            .adjuncts
            .iter()
            .map(|adjunct| adjunct.as_dyn())
            .collect();

        match aggregate
            .bundle
            .verify_proof(&mut rand_10::rng(), &adjunct_refs)
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(BlockError::TachyonProofInvalid(
                "proof stamp was disproved".to_string(),
            )),
            Err(error) => Err(BlockError::TachyonProofInvalid(error.to_string())),
        }
    })
    .await
    .map_err(|_| {
        BlockError::Other(
            "threadpool unexpectedly dropped response channel sender; is Zakura shutting down?"
                .to_string(),
        )
    })?
}
