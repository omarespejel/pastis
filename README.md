# Pastis

Pastis is an AI-first Starknet full node architecture, implemented in Rust, with a hard line between:

- `Foundation`: storage, sync, L1 finality tracking, health surfaces
- `Execution`: canonical and fast execution backends with explicit mismatch policy
- `Agent`: ExEx-style post-execution consumers (telemetry, MCP, BTCFi monitoring)

The project is designed for production safety first: fail-closed versioning, bounded queues, WAL recovery, deterministic delivery ordering, and explicit degradation paths.

## Why This Exists

Starknet has had real sequencer/prover mismatch incidents. Pastis treats those as first-class architecture constraints:

- Fast execution is allowed.
- Canonical verification is required where correctness matters.
- Any mismatch policy is explicit, observable, and testable.

The goal is a node architecture that is fast enough for operators and strict enough for auditors.

## Current State

This repository currently ships core, tested building blocks:

- Versioned ExEx notifications with backward-compatible WAL decode
- ExEx delivery planning with dependencies, priorities, cycle detection, and depth limits
- L1 finality tracker with reorg invalidation and contiguous verified-block progression
- Protocol-version constant resolution with fail-closed behavior
- Storage backends with Papyrus adapter tests against real Starknet fixture data
- Dual execution orchestration with canonical fallback/mismatch handling
- ExEx manager with bounded WAL replay and failing-sink circuit breaker
- Type-state node builder that enforces composition order

Source entry points:

- [crates/node-spec-core/src/notification.rs](/Users/espejelomar/StarkNet/pastis/crates/node-spec-core/src/notification.rs)
- [crates/node-spec-core/src/exex_delivery.rs](/Users/espejelomar/StarkNet/pastis/crates/node-spec-core/src/exex_delivery.rs)
- [crates/node-spec-core/src/l1_finality.rs](/Users/espejelomar/StarkNet/pastis/crates/node-spec-core/src/l1_finality.rs)
- [crates/node-spec-core/src/protocol_version.rs](/Users/espejelomar/StarkNet/pastis/crates/node-spec-core/src/protocol_version.rs)
- [crates/storage/src/lib.rs](/Users/espejelomar/StarkNet/pastis/crates/storage/src/lib.rs)
- [crates/execution/src/lib.rs](/Users/espejelomar/StarkNet/pastis/crates/execution/src/lib.rs)
- [crates/exex-manager/src/lib.rs](/Users/espejelomar/StarkNet/pastis/crates/exex-manager/src/lib.rs)
- [crates/node/src/lib.rs](/Users/espejelomar/StarkNet/pastis/crates/node/src/lib.rs)

## Architecture Narrative

1. A block is received and validated by consensus/sync components.
2. Execution runs through a selected backend mode:
   - `CanonicalOnly`
   - `FastOnly`
   - `DualWithVerification`
3. State is committed via storage backend behavior (Papyrus for canonical Starknet semantics).
4. ExEx notifications are WAL-persisted before delivery.
5. ExEx manager delivers notifications in deterministic dependency tiers.
6. Agent-facing surfaces (e.g., MCP) consume notifications and read state without mutating consensus-critical paths.

## Local Development

### Prerequisites

- Rust toolchain (stable)
- `cargo`

### Validate Everything

```bash
cargo fmt --all --check
cargo test --workspace
cargo test -p starknet-node-storage --features papyrus-adapter
cargo test -p starknet-node-execution --features blockifier-adapter
cargo clippy --workspace --all-targets --all-features -- -D warnings
./scripts/check-vendored-size-of.sh
```

## Dependency Integrity

Pastis currently patches `size-of` to a reviewed vendored copy for upstream compatibility.

- Integrity is enforced in CI by:
  - [scripts/check-vendored-size-of.sh](/Users/espejelomar/StarkNet/pastis/scripts/check-vendored-size-of.sh)
  - [vendor/size-of/VENDOR_SHA256](/Users/espejelomar/StarkNet/pastis/vendor/size-of/VENDOR_SHA256)

## Performance Gate (Next Step)

The project now includes an internal performance budget harness (debug-mode regression guard) to catch severe overhead regressions in critical control-plane operations.

Run it locally:

```bash
cargo run -p starknet-node --bin perf-budget
```

Tune limits with environment variables:

- `PASTIS_PERF_ITERATIONS`
- `PASTIS_BUDGET_DUAL_EXECUTE_P99_US`
- `PASTIS_BUDGET_NOTIFICATION_DECODE_P99_US`
- `PASTIS_BUDGET_MCP_VALIDATE_P99_US`
- `PASTIS_BUDGET_DUAL_EXECUTE_MIN_OPS_PER_SEC`

## Roadmap

Near-term implementation sequence:

1. Expand Papyrus-backed adapters and fixture coverage
2. Continue hardening Blockifier adapter semantics across protocol versions
3. Extend ExEx manager recovery and failure isolation behavior
4. Land MCP server implementation and tool auth/rate limits
5. Add BTCFi/strkBTC monitoring ExEx implementations

Long-term:

- Full production node binary wiring
- End-to-end sync pipeline integration
- Optional proving pipeline integration
