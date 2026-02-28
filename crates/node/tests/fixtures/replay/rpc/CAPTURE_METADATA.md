# Starknet RPC Capture Metadata

- Source endpoint: `https://rpc.starknet.lava.build`
- Network: Starknet mainnet
- Capture date (UTC): 2026-02-28
- Methods:
  - `starknet_getBlockWithTxs`
  - `starknet_getStateUpdate`
- Captured blocks:
  - `7242623`
  - `7242624`
  - `7242625`

These fixtures are immutable replay inputs for deterministic parser and state-diff ingestion tests.

Integrity lock:
- Check: `./scripts/check-replay-fixture-lock.sh`
- Refresh after intentional fixture changes: `PASTIS_FIXTURE_LOCK_UPDATE=1 ./scripts/check-replay-fixture-lock.sh`
