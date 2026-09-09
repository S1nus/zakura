# zakura (Tachyon internal testnet)

the `cnode-working` branch of this fork tracks the main branch of [upstream Zakura](https://github.com/zakura-core/zakura), with a few extra PRs merged:
- the [Tachyon PR](https://github.com/zakura-core/zakura/pull/795)
- [a PR](https://github.com/S1nus/zakura/pull/1) to print out upgrades in stdout as they happen


## How to build:
`RUSTFLAGS='--cfg zcash_unstable="nutachyon"' cargo build --release --bin zakurad --features internal-miner`

## Sample config:

```
[consensus]
checkpoint_sync = true

[health]
enforce_on_test_networks = false
min_connected_peers = 1
ready_max_blocks_behind = 2
ready_max_tip_age = "5m"

[mempool]
eviction_memory_time = "1h"
max_datacarrier_bytes = 83
max_transaction_bytes = 250000
tx_cost_limit = 80000000

[metrics]

[mining]
internal_miner = true
tachyon_workload = true
miner_address = "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"
extra_coinbase_data = "TachyonTestnet"

[network]
cache_dir = "/var/cache/zakura"
crawl_new_peer_interval = "1m 1s"
expose_peer_addresses = false
identity_dir = "/var/lib/zakura/identity"
initial_mainnet_peers = []
initial_testnet_peers = []
listen_addr = "[::]:18233"
max_connections_per_ip = 1
network = "Testnet"
p2p_stack = "dual"
peerset_initial_target_size = 100

[network.testnet_parameters]
network_name = "TachyonTestnet"
network_magic = [84, 65, 67, 72]
slow_start_interval = 0
disable_pow = true
max_block_time_start_height = 1
checkpoints = false
lockbox_disbursements = [
    { address = "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8", amount = 0 },
]

[network.testnet_parameters.activation_heights]
Canopy = 1
NU5 = 2
NU6 = 3
"NU6.1" = 4
"NU6.2" = 5
"NU6.3" = 6
NuTachyon = 8

[network.zakura]
bootstrap_peers = []
dev_network = "TachyonTestnet"
listen_addr = "0.0.0.0:18234"
max_connections = 256
max_connections_per_ip = 16
max_pending_handshakes = 32
message_rate_per_second = 2048
stream_open_rate_per_second = 32

[rpc]
listen_addr = "0.0.0.0:18232"
cookie_dir = "/var/lib/zakura/rpc"
cookie_file_name = ".cookie"
debug_force_finished_sync = false
enable_cookie_auth = false
max_response_body_size = 52428800
parallel_cpu_threads = 0

[state]
cache_dir = "/var/lib/zakura"
debug_skip_non_finalized_state_backup_task = false
delete_old_database = true
ephemeral = false
should_backup_non_finalized_state = true
storage_mode = "archive"

[sync]
checkpoint_verify_concurrency_limit = 1000
download_concurrency_limit = 100
full_verify_concurrency_limit = 20
parallel_cpu_threads = 0
zakura_block_apply_concurrency_limit = 32

[tracing]
buffer_limit = 128000
force_use_color = true
use_color = true
use_journald = false

[zcashd_compat]
block_gossip_peer_ips = []
enabled = false
manage_zcashd = false
restart_backoff = "2s"
restart_backoff_max = "5m"
restart_reset_after = "1h"
shutdown_grace_period = "5m"
startup_delay = "1s"
zcashd_extra_args = []
zcashd_source = "path"
```
