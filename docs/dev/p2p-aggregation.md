# Experimental Tachyon aggregate relay

This implementation targets the version-1 dependency exchange in
`zips/draft-tachyon-aggregation-protocol.md` on the ZIP repository's
`c/p2p-aggregation` branch. It is layered on Zakura PR #795, not a production
network upgrade. The inherited Tachyon dependency uses Ragu's **mock proof
system**; these builds are for development networks only.

## Enable

Build with `--cfg zcash_unstable="nutachyon"` and opt in in the node configuration:

```toml
[mempool]
enable_tachyon_aggregation = true
```

The default is `false`: ordinary nodes continue admitting and relaying only
autonome Tachyon transactions. Block validation still accepts consensus-valid
aggregates and adjuncts independently of this setting. NuTachyon activation and
the existing mining RPC configuration are still required on the development
network; this setting does not activate a consensus upgrade or start a miner.

## Behavior

- Aggregates are ordinary `MSG_WTX` inventory objects. `getaggdeps` / `aggdeps`
  resolve a flat, exact-`wtxid` manifest; missing originals are ordinary
  `getdata(MSG_WTX)` downloads. Zakura's compatibility request stream also carries
  these messages (experimental types 19/20).
- The manifest contains 2–32 autonome originals, including the carrier before
  re-stamping. Only the carrier's stamp may change. The entire package is limited
  to 4 MiB and each transaction still obeys the local transaction-size policy.
- Before admission, the node verifies every original, the aggregate's coverage
  and proof, signatures, fees, anchors, and chain context. Adjuncts are never
  admitted or relayed as standalone transactions.
- Original effects live once in the ordinary mempool. Aggregate authorization
  alternatives live in a separate exact-ID cache. They neither duplicate fees
  nor create duplicate spend/output accounting.
- At most 64 packages / 64 MiB are retained. An advertisement renews a ten-minute
  serving lease. Reads do not renew leases. Mempool eviction or a tip change stops
  advertising affected entries but does not discard bytes promised to peers.
  Retention is in memory and ends with the connection/process lifetime.
- Pending packages are bounded to four globally, one per announcing peer, and two new
  attempts per second. Downloads time out after ten seconds per exchange, with a
  thirty-second end-to-end attempt limit. Proof workers remain capacity-limited
  after cancellation. Missing data/refusals are not invalid-transaction evidence;
  dependency failures are not attributed to the aggregate advertiser.
- A small exact-ID cache reuses recently verified originals, but they are
  reverified against the current tip before a package is admitted. There are no
  recursive dependency fetches.

## Mining and publication

Block selection still chooses ordinary transactions. An aggregate is substituted
only if all exact originals were selected, its anchor/tachygrams remain valid for
the candidate tip, coverage is complete, and substitution does not increase the
selected byte budget. Otherwise the selected autonomes remain usable.

The miner may merge received aggregates with other selected proof stamps in the
same epoch, lifting within that epoch as needed. All affected adjunct pointers
are updated to the final carrier's `wtxid`. Non-cancellable local proving is
limited to one worker, and failure or timeout preserves the safe candidate set.

New locally mined aggregates within the relay limits are queued for normal
mempool verification and publication with their original dependency data.
Publication does not delay the block template until peer relay completes. The
implementation does not run a separate always-on aggregation/proving daemon;
nodes can receive and relay aggregates without constructing templates.

## Validation

On the Homebrew development host, prefix every state-dependent Cargo invocation
with `ROCKSDB_LIB_DIR="/opt/homebrew/lib"` to use the installed RocksDB library.
For example:

```sh
ROCKSDB_LIB_DIR="/opt/homebrew/lib" RUSTFLAGS='--cfg zcash_unstable="nutachyon"' \
  cargo +1.97.1 test -p zakura-chain -p zakura-network -p zakura-rpc \
  -p zakura-consensus -p zakura --lib aggregate --locked
```

The corresponding default-configuration build must continue to compile without
the unstable cfg. Once this branch has a PR number, add its numbered changelog
fragment and replace the pending references in the parameter ledger as required
by the repository's changelog policy.

The wider NuTachyon test filter currently also reaches an inherited assertion in
`zakura-state`'s `roundtrip_block_info_with_tachyon_value_pool`: it expects 60 bytes,
but the serialized record is 68 bytes after the NSM balance addition. This branch
does not change that disk format or test. Protocol, mining, and the new mempool
epoch-boundary checks should be distinguished from that existing failure.
