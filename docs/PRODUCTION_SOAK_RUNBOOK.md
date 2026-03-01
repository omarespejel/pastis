# Production Soak Runbook (Real Daemon, Real Data)

This runbook executes a long-lived real-data soak against the `starknet-node` daemon and produces a machine-readable PASS/FAIL report.

## 1. Prerequisites

- `cargo`
- `curl`
- `jq`
- stable upstream Starknet RPC endpoint

## 2. Run the Soak Gate

Example (6h):

```bash
./scripts/daemon-soak-gate.sh \
  --rpc-url "https://rpc.starknet.lava.build" \
  --duration-minutes 360 \
  --poll-seconds 15
```

Default run length if omitted: 60 minutes.

## 3. Output Artifacts

State directory default: `.pastis/soak-daemon`

- `daemon.log`: daemon stdout/stderr
- `soak-timeline.jsonl`: per-sample health/readiness/status timeline
- `soak-report.json`: final summary and gate verdict

## 4. Gate Semantics

The soak run fails if any of these are true:

- daemon process exits during soak
- any `/healthz` sample is non-200
- `/status` sampling fails
- no block progress is observed during the soak window
- max `consecutive_failures` exceeds `3`

It passes only if none of the above occur.

## 5. Synthetic Fallback Policy

By default the soak uses production-adapters build with synthetic fallback disabled.

Troubleshooting-only override:

```bash
./scripts/daemon-soak-gate.sh --rpc-url "..." --allow-synthetic-fallback
```

For production validation, do **not** enable this override.

## 6. Suggested Cloud Procedure

1. Run a short smoke soak (15-30 min) to validate host/network.
2. Run 6h soak and review `soak-report.json`.
3. Run 24h soak as funding-grade evidence.
4. Attach report + timeline + daemon log to decision docs.
