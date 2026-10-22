//! Leased aggregate inventory and bounded pending-package accounting.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use zakura_chain::{
    block,
    transaction::{aggregation::Manifest, UnminedTx, UnminedTxId, WtxId},
};
use zakura_node_services::mempool::QueueSource;

const LEASE: Duration = Duration::from_secs(600);
const MAX_ENTRIES: usize = 64;
const MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
struct Entry {
    transaction: UnminedTx,
    tip: block::Hash,
    expires: Instant,
    bytes: usize,
}

/// Retention is independent of ordinary mempool eviction and chain-tip resets.
#[derive(Debug, Default)]
pub(super) struct Cache {
    pub known: Arc<Mutex<Known>>,
    entries: HashMap<UnminedTxId, Entry>,
    serving: HashMap<Option<QueueSource>, Instant>,
}

impl Cache {
    pub fn prune(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.expires > now);
        self.serving
            .retain(|_, when| now.duration_since(*when) < Duration::from_secs(1));
    }

    pub fn insert(&mut self, transaction: UnminedTx, tip: block::Hash) -> bool {
        self.prune();
        if zakura_chain::tachyon::aggregation::check_package(&transaction).is_err() {
            return false;
        }
        let bytes = transaction.size()
            + transaction
                .tachyon_dependencies()
                .iter()
                .map(UnminedTx::size)
                .sum::<usize>();
        let id = transaction.id();
        // A lease also promises the originals in manifests already handed to peers.
        // Do not replace those objects with different authorization forms mid-lease.
        if self.entries.get(&id).is_some_and(|entry| {
            entry.transaction.tachyon_dependencies() != transaction.tachyon_dependencies()
        }) {
            return false;
        }
        let old_bytes = self.entries.get(&id).map_or(0, |entry| entry.bytes);
        let used: usize = self.entries.values().map(|entry| entry.bytes).sum();
        if (!self.entries.contains_key(&id) && self.entries.len() >= MAX_ENTRIES)
            || used - old_bytes + bytes > MAX_BYTES
        {
            return false;
        }
        self.entries.insert(
            id,
            Entry {
                transaction,
                tip,
                bytes,
                expires: Instant::now() + LEASE,
            },
        );
        true
    }

    /// Find packages still usable at this exact tip without renewing their leases.
    pub fn active_ids(
        &mut self,
        tip: block::Hash,
        available: &HashSet<UnminedTxId>,
    ) -> HashSet<UnminedTxId> {
        self.prune();
        let mut ids = HashSet::new();
        for (id, entry) in &mut self.entries {
            if entry.tip == tip
                && entry
                    .transaction
                    .tachyon_dependencies()
                    .iter()
                    .all(|tx| available.contains(&tx.id()))
            {
                ids.insert(*id);
            }
        }
        ids
    }

    pub fn renew(&mut self, advertised: &HashSet<UnminedTxId>) {
        for id in advertised {
            if let Some(entry) = self.entries.get_mut(id) {
                entry.expires = Instant::now() + LEASE;
            }
        }
    }

    pub fn candidates(&self, tip: block::Hash) -> Vec<UnminedTx> {
        self.entries
            .values()
            .filter(|entry| entry.tip == tip && entry.expires > Instant::now())
            .map(|entry| entry.transaction.clone())
            .collect()
    }

    /// Serve exact immutable objects even if they are no longer admissible.
    pub fn transaction(&self, id: UnminedTxId) -> Option<UnminedTx> {
        for entry in self
            .entries
            .values()
            .filter(|entry| entry.expires > Instant::now())
        {
            if entry.transaction.id() == id {
                return Some(entry.transaction.transaction().clone().into());
            }
            if let Some(tx) = entry
                .transaction
                .tachyon_dependencies()
                .iter()
                .find(|tx| tx.id() == id)
            {
                return Some(tx.clone());
            }
        }
        None
    }

    pub fn manifest(&mut self, aggregate: WtxId, source: Option<QueueSource>) -> Manifest {
        self.prune();
        let mut result = Manifest {
            aggregate,
            originals: Vec::new(),
        };
        // The map contains at most eight one-second rate tokens, not an unbounded peer history.
        if self.serving.len() >= 8 || self.serving.contains_key(&source) {
            return result;
        }
        self.serving.insert(source, Instant::now());
        if let Some(entry) = self.entries.get(&UnminedTxId::Witnessed(aggregate)) {
            result.originals = entry
                .transaction
                .tachyon_dependencies()
                .iter()
                .map(|tx| WtxId::from(tx.transaction().as_ref()))
                .collect();
            result.originals.sort_by_key(WtxId::as_bytes);
        }
        result
    }
}

/// Small opportunistic exact-ID cache. Reused bytes are always reverified against the tip.
#[derive(Debug, Default)]
pub(super) struct Known {
    transactions: HashMap<UnminedTxId, UnminedTx>,
    order: std::collections::VecDeque<UnminedTxId>,
    bytes: usize,
}

impl Known {
    pub fn remember(&mut self, tx: UnminedTx) {
        if tx.size() > 4 * 1024 * 1024 || self.transactions.contains_key(&tx.id()) {
            return;
        }
        while self.transactions.len() >= 128 || self.bytes + tx.size() > 4 * 1024 * 1024 {
            let Some(id) = self.order.pop_front() else {
                break;
            };
            if let Some(old) = self.transactions.remove(&id) {
                self.bytes -= old.size();
            }
        }
        self.bytes += tx.size();
        self.order.push_back(tx.id());
        self.transactions.insert(tx.id(), tx);
    }
    pub fn get(&self, id: UnminedTxId) -> Option<UnminedTx> {
        self.transactions.get(&id).cloned()
    }
}

/// Synchronous bookkeeping only; this mutex is never held across an await.
#[derive(Debug, Default)]
pub(super) struct Pending {
    active: HashSet<Option<QueueSource>>,
    starts: Vec<Instant>,
}

#[derive(Debug)]
pub(super) struct Permit {
    pending: Arc<Mutex<Pending>>,
    source: Option<QueueSource>,
}

impl Pending {
    pub fn acquire(pending: &Arc<Mutex<Self>>, source: Option<QueueSource>) -> Option<Permit> {
        let mut state = pending
            .lock()
            .expect("pending accounting lock is never held during fallible work");
        let now = Instant::now();
        state
            .starts
            .retain(|when| now.duration_since(*when) < Duration::from_secs(1));
        if state.active.len() >= 4 || state.active.contains(&source) || state.starts.len() >= 2 {
            return None;
        }
        state.active.insert(source.clone());
        state.starts.push(now);
        Some(Permit {
            pending: pending.clone(),
            source,
        })
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.pending
            .lock()
            .expect("pending accounting lock is never held during fallible work")
            .active
            .remove(&self.source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        amount::Amount,
        parameters::NetworkUpgrade,
        transaction::{LockTime, Transaction},
    };

    fn tx(height: u32) -> UnminedTx {
        Transaction::V7 {
            network_upgrade: NetworkUpgrade::NuTachyon,
            lock_time: LockTime::unlocked(),
            expiry_height: block::Height(height),
            zip233_amount: Amount::zero(),
            inputs: vec![],
            outputs: vec![],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
            tachyon_shielded_data: None,
        }
        .into()
    }

    #[test]
    fn aggregate_retention_survives_tip_change_but_stops_advertising() {
        let mut cache = Cache::default();
        // Exercise retention independently of cryptographic/package validation.
        let original = tx(2);
        let aggregate = tx(1).with_tachyon_dependencies(vec![original.clone()]);
        let id = aggregate.id();
        let tip = block::Hash([1; 32]);
        cache.entries.insert(
            id,
            Entry {
                bytes: aggregate.size(),
                transaction: aggregate,
                tip,
                expires: Instant::now() + LEASE,
            },
        );
        let available = HashSet::from([original.id()]);
        assert!(cache.active_ids(tip, &available).contains(&id));
        assert!(cache
            .active_ids(block::Hash([2; 32]), &available)
            .is_empty());
        assert!(cache.active_ids(tip, &HashSet::new()).is_empty());
        assert!(cache.transaction(id).is_some());
        assert!(cache.transaction(original.id()).is_some());
        let before = cache.entries[&id].expires;
        let witnessed = WtxId::from(cache.entries[&id].transaction.transaction().as_ref());
        assert_eq!(cache.manifest(witnessed, None).originals.len(), 1);
        assert_eq!(
            cache.entries[&id].expires, before,
            "reads must not renew leases"
        );
        assert!(
            cache.manifest(witnessed, None).originals.is_empty(),
            "serving is rate limited"
        );
        cache.entries.get_mut(&id).unwrap().expires = Instant::now() - Duration::from_secs(1);
        assert!(cache.transaction(id).is_none());
        cache.prune();
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn aggregate_pending_limits_survive_fast_failures_and_release_on_drop() {
        let pending = Arc::new(Mutex::new(Pending::default()));
        let first = Pending::acquire(&pending, None).unwrap();
        assert!(Pending::acquire(&pending, None).is_none());
        drop(first);
        let second = Pending::acquire(&pending, None).unwrap();
        drop(second);
        assert!(
            Pending::acquire(&pending, None).is_none(),
            "fast failures still consume rate tokens"
        );
        assert!(pending.lock().unwrap().active.is_empty());
    }

    #[test]
    fn aggregate_pending_global_limit_is_independent_of_rate_window() {
        let pending = Arc::new(Mutex::new(Pending::default()));
        let mut permits = Vec::new();
        for index in 0..4 {
            // Advance only the rate window; unfinished work must still consume capacity.
            pending.lock().unwrap().starts.clear();
            permits.push(
                Pending::acquire(&pending, Some(QueueSource::Zakura(vec![index; 32]))).unwrap(),
            );
        }
        pending.lock().unwrap().starts.clear();
        assert!(Pending::acquire(&pending, None).is_none());
        permits.pop();
        assert!(Pending::acquire(&pending, None).is_some());
    }

    #[test]
    fn aggregate_known_cache_is_bounded_and_matches_exact_ids() {
        let mut known = Known::default();
        let original = tx(1);
        known.remember(original.clone());
        assert_eq!(known.get(original.id()), Some(original.clone()));
        let UnminedTxId::Witnessed(mut wrong) = original.id() else {
            unreachable!()
        };
        wrong.auth_digest = [7; 32].into();
        assert!(known.get(UnminedTxId::Witnessed(wrong)).is_none());
        for height in 2..200 {
            known.remember(tx(height));
        }
        assert_eq!(known.transactions.len(), 128);
        assert!(known.get(original.id()).is_none());
        assert!(known.bytes <= 4 * 1024 * 1024);
    }
}
