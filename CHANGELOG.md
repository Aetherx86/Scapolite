# Changelog

All notable changes to Scapolite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-20

### Added
- `pkg/evm`: typed transactions with access modes, ERC-20 `transfer`, `transferFrom` and `approve` decoding, native value transfers, contract creation, EIP-2930 access lists, and AMM swap and WETH selectors for footprint inference.
- Conflict DAG (`conflict_dag.go`) over storage slots, balances, nonces and contract creation keys. It models read-after-write, write-after-read and write-after-write hazards, sender nonce ordering, and overdraft checks against initial balances.
- Explicit cycle detection and gas-weighted critical path analysis, with a work-stealing efficiency estimate for the `-workers` setting.
- MEV detector (`mev.go`) for sandwich attacks, priority-fee frontrunning, backrunning and contention hotspots.
- CLI: `-gen` for deterministic synthetic mempools, `-out` for exporting them, `-bench` for multi-size scheduler benchmarks, `-seed`, and `-json` reports.
- Tests for ERC-20 parallelism, balance hazards, cycle detection, critical path, MEV detection, JSON round trips, concurrent scheduling under the race detector, and the CLI.

## [0.1.0] - 2026-11-08

### Added
- Scratch implementation of EVM transaction dependency DAG scheduler in pure Go.
- Kahn's algorithm topological sorting and cycle detection engine.
- Sequential sender nonce ordering preservation.
- Fine-grained SSTORE / SLOAD storage slot conflict detection (RAW, WAR, WAW hazards).
- Parallel execution wave partitioner calculating theoretical block multi-core speedup.
- Dual Operational Procedures:
  - `--mode=fast`: Coarse-grained contract and recipient address contention for maximal scheduler throughput.
  - `--mode=deep`: Fine-grained storage slot footprint tracking and multi-core parallel wave scheduling.
- Deterministic synthetic mempool generator for high-load benchmarks.
- Structured JSON and NDJSON output formats for block builder pipelines.

