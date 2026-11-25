# Scapolite: EVM Mempool Transaction Dependency DAG Scheduler

Scapolite is a high-performance, freestanding concurrent EVM transaction scheduler written in pure Go with zero external dependencies. It analyzes pending mempool transactions, decodes state access footprints (contract addresses, read slots, and write slots), and constructs a Directed Acyclic Graph (DAG) to partition transactions into non-conflicting parallel execution waves.

Engineered as a complementary scheduling engine for `rusty-chain-kit` and parallel EVM block builders, Scapolite resolves state hazards (Read-After-Write, Write-After-Read, Write-After-Write) before transaction execution, unlocking 3x to 7x multi-core execution speedup without consensus divergence.

---

## Architectural Overview

```
Pending Mempool Transactions (JSON / RPC)
        |  EVM Transactions (From, To, Nonce, ReadSlots, WriteSlots)
        v
+-----------------------------------------------------------+
| Scapolite Dependency Analysis Pipeline                    |
|                                                           |
| 1. Nonce Sequence Ordering                                |
|    - Groups by sender account: Nonce(i) -> Nonce(i+1)     |
|                                                           |
| 2. State Hazard Detection Engine                          |
|    - Mode Fast: Coarse contract address contention        |
|    - Mode Deep: Fine-grained SSTORE / SLOAD slot tracking |
|    - Flags RAW, WAR, and WAW memory dependencies          |
|                                                           |
| 3. Topological Wave Partitioner (Kahn's Algorithm)        |
|    - In-degree 0 isolation for immediate multi-core dispatch|
|    - Iterative dependency clearance (< 400 us / 1000 txs) |
|    - Validates absence of cyclic dependencies             |
+-----------------------------------------------------------+
        |
        v
Parallel Execution Schedule (Wave 1 || Wave 2 || ... || Wave N)
```

---

## Technical Specifications

| Dimension / Metric | Scapolite v0.1.0 Specification |
| :--- | :--- |
| **Language & Runtime** | Pure Go 1.23 (Standard Library Only) |
| **External Dependencies** | 0 external packages (Standard Library sort, sync, crypto, encoding) |
| **Graph Construction Latency** | 606 us / 1,000 transactions (~1.65M txs/sec) |
| **Wave Partitioning Latency** | 394 us / 1,000 transactions (~2.53M txs/sec) |
| **Theoretical Concurrency** | 3.12x - 7.14x speedup over sequential single-thread EVM |
| **Cycle Detection** | Complete validation via iterative in-degree topological reduction |
| **State Contention Types** | Nonce sequences, SLOAD/SSTORE Read-After-Write, Write-After-Write |

---

## Dual Operational Procedures

Scapolite provides two explicit scheduling procedures balancing scheduling latency against execution concurrency:

### Procedure 1: Coarse-Grained Address Contention (`--mode=fast`)
- **Execution**: `scapolite -mode=fast -f mempool.json`
- **Behavior**: Treats entire contract addresses and recipient accounts as single locks. If two transactions interact with the same smart contract, they are scheduled in sequential waves.
- **Latency**: Under 26 microseconds per 25-transaction block.
- **Target Environments**: Low-complexity ERC-20 transfers and high-throughput L2 sequencers prioritizing minimal scheduling overhead.

### Procedure 2: Fine-Grained Storage Slot Scheduling (`--mode=deep`)
- **Execution**: `scapolite -mode=deep -f mempool.json`
- **Behavior**: Tracks explicit 32-byte storage slots (`SSTORE` writes and `SLOAD` reads). Multiple transactions calling the same contract (e.g., Uniswap pool swaps touching distinct tick ranges or disjoint user balances) execute concurrently within the same parallel wave.
- **Speedup**: Unlocks up to 7.1x speedup over sequential block execution.
- **Target Environments**: MEV block building, high-contention DeFi execution engines, and multi-core validator nodes.

---

## CLI Reference & Usage

### 1. Build from Source
```bash
go build -v -o bin/scapolite ./cmd/scapolite
```

### 2. Schedule a Synthetic Mempool Demo
```bash
./bin/scapolite -mode=deep
```

Output:
```
Scapolite v0.1.0 - EVM Dependency DAG Scheduler [deep mode]
Scheduled 25 transactions into 8 parallel waves in 34µs
--------------------------------------------------------------------------------
Max Concurrency:      5 concurrent tx/wave
Theoretical Speedup:  3.12x over sequential EVM
State Contention:     3 slot conflicts detected
Nonce Dependencies:   20 sender ordering edges
--------------------------------------------------------------------------------

Parallel Execution Waves:
  Wave  1 ( 4 txs): 0x3eea4b5f.., 0xa8b73123.., 0xdad81ec2.., 0xbab1bbdc..
  Wave  2 ( 4 txs): 0xd9faebac.., 0x0873fc2d.., 0x21ed351b.., 0x9cc5e845..
  Wave  3 ( 5 txs): 0x27995156.., 0xd6643390.., 0x850eeb0a.., 0x49c29568.., 0xa0f6b140..
  Wave  4 ( 4 txs): 0xc1b455d5.., 0x2bce7c3e.., 0x2bd352c4.., 0x46d82678..
  Wave  5 ( 3 txs): 0xddb86e2b.., 0xcb7bf958.., 0xbce6070d..
  Wave  6 ( 3 txs): 0x31abc41b.., 0x97c8fc56.., 0x492092e8..
  Wave  7 ( 1 txs): 0x147a2d8d..
  Wave  8 ( 1 txs): 0xcb71241b..
```

### 3. Generate Benchmark Mempool & Export
```bash
./bin/scapolite -gen 500 -out mempool_500.json
./bin/scapolite -f mempool_500.json -mode=deep -json
```

---

## Benchmarks

Microbenchmarks measured on Apple M4 (macOS Sonoma, Go 1.23):

```
goos: darwin
goarch: arm64
pkg: github.com/omakasekim/Scapolite/pkg/dag
BenchmarkBuildGraph_1000Tx-10                 1676        606252 ns/op      768492 B/op      6932 allocs/op
BenchmarkComputeParallelWaves_1000Tx-10       3080        394235 ns/op       79544 B/op       185 allocs/op
```

---

## Supply-Chain & Code Quality Verification

All commits undergo automated supply-chain auditing via `repo_guard.py`:
- 0 tracking of sensitive credentials or tokens.
- 0 invisible unicode characters or bidirectional overrides.
- Pinned GitHub Actions dependencies with strict read-only tokens.

Run verification locally:
```bash
python3 .github/scripts/repo_guard.py .
go test -v -race -cover ./...
```

---

## 0.2.0: Conflict DAG, MEV Detection and Benchmarks

```bash
# Deterministic 500-transaction mempool, exported then scheduled
scapolite -gen 500 -seed 7 -out mempool.json
scapolite -f mempool.json -mode deep -workers 16 -json > schedule.json

# Address-level scheduling compared with slot-level scheduling
scapolite -f mempool.json -mode fast
scapolite -f mempool.json -mode deep

# Scheduler throughput across batch sizes
scapolite -bench 100,500,1000,5000 -json
```

Deep mode builds the conflict DAG over storage slots, balances, nonces and contract creation keys. It reports parallel waves, the gas-weighted critical path, speedup and efficiency for the configured worker count, and MEV alerts covering sandwiches, priority-fee frontrunning and contention hotspots.

## License

Copyright (c) 2026 Jeong Hoon Kim (Aether Systems & Research Lab). All rights reserved.  
Released under the [MIT License](LICENSE).

