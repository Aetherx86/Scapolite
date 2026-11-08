# Changelog

All notable changes to Scapolite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
