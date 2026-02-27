use std::sync::Mutex;

use node_spec_core::protocol_version::{
    VersionResolutionError, VersionedConstants, VersionedConstantsResolver,
};
use semver::Version;
use starknet_node_types::{
    BlockContext, ExecutionOutput, MutableState, SimulationResult, StarknetBlock,
    StarknetTransaction, StateReader,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExecutionError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("execution mismatch detected at block {block_number}")]
    Mismatch { block_number: u64 },
    #[error("execution halted due to mismatch policy")]
    Halted,
    #[error("missing constants for protocol version {0}")]
    MissingConstants(Version),
}

pub trait ExecutionBackend: Send + Sync {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError>;

    fn simulate_tx(
        &self,
        tx: &StarknetTransaction,
        state: &dyn StateReader,
        block_context: &BlockContext,
    ) -> Result<SimulationResult, ExecutionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasmTrace {
    pub steps: usize,
}

pub trait TracingExecutionBackend: ExecutionBackend {
    fn execute_block_with_traces(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<(ExecutionOutput, Vec<CasmTrace>), ExecutionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    CanonicalOnly,
    FastOnly,
    DualWithVerification { verification_depth: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchPolicy {
    WarnAndFallback,
    Halt,
    CooldownThenRetry { cooldown_blocks: u64 },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DualExecutionMetrics {
    pub mismatches: u64,
    pub fast_executions: u64,
    pub canonical_executions: u64,
}

pub struct DualExecutionBackend {
    fast: Option<Box<dyn ExecutionBackend>>,
    canonical: Box<dyn ExecutionBackend>,
    mode: ExecutionMode,
    mismatch_policy: MismatchPolicy,
    metrics: Mutex<DualExecutionMetrics>,
    cooldown_remaining: Mutex<u64>,
}

impl DualExecutionBackend {
    pub fn new(
        fast: Option<Box<dyn ExecutionBackend>>,
        canonical: Box<dyn ExecutionBackend>,
        mode: ExecutionMode,
        mismatch_policy: MismatchPolicy,
    ) -> Self {
        Self {
            fast,
            canonical,
            mode,
            mismatch_policy,
            metrics: Mutex::new(DualExecutionMetrics::default()),
            cooldown_remaining: Mutex::new(0),
        }
    }

    pub fn metrics(&self) -> DualExecutionMetrics {
        self.metrics.lock().expect("lock metrics").clone()
    }

    pub fn execute_verified(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        let mut cooldown = self.cooldown_remaining.lock().expect("lock cooldown");
        let forced_canonical = if *cooldown > 0 {
            *cooldown -= 1;
            true
        } else {
            false
        };
        drop(cooldown);

        let effective_mode = if forced_canonical {
            ExecutionMode::CanonicalOnly
        } else {
            self.mode.clone()
        };

        match effective_mode {
            ExecutionMode::CanonicalOnly => {
                let out = self.canonical.execute_block(block, state)?;
                self.metrics
                    .lock()
                    .expect("lock metrics")
                    .canonical_executions += 1;
                Ok(out)
            }
            ExecutionMode::FastOnly => {
                if let Some(fast) = &self.fast {
                    let out = fast.execute_block(block, state)?;
                    self.metrics.lock().expect("lock metrics").fast_executions += 1;
                    Ok(out)
                } else {
                    let out = self.canonical.execute_block(block, state)?;
                    self.metrics
                        .lock()
                        .expect("lock metrics")
                        .canonical_executions += 1;
                    Ok(out)
                }
            }
            ExecutionMode::DualWithVerification { .. } => {
                let Some(fast) = &self.fast else {
                    let out = self.canonical.execute_block(block, state)?;
                    self.metrics
                        .lock()
                        .expect("lock metrics")
                        .canonical_executions += 1;
                    return Ok(out);
                };

                // Canonical mutates the authoritative state; fast runs on a cloned state.
                let mut fast_state = state.boxed_clone();
                let fast_output = fast.execute_block(block, fast_state.as_mut())?;
                let canonical_output = self.canonical.execute_block(block, state)?;

                let mut metrics = self.metrics.lock().expect("lock metrics");
                metrics.fast_executions += 1;
                metrics.canonical_executions += 1;

                if fast_output == canonical_output {
                    return Ok(fast_output);
                }

                metrics.mismatches += 1;
                drop(metrics);

                match self.mismatch_policy {
                    MismatchPolicy::WarnAndFallback => Ok(canonical_output),
                    MismatchPolicy::Halt => Err(ExecutionError::Halted),
                    MismatchPolicy::CooldownThenRetry { cooldown_blocks } => {
                        *self.cooldown_remaining.lock().expect("lock cooldown") = cooldown_blocks;
                        Ok(canonical_output)
                    }
                }
            }
        }
    }
}

pub struct ProtocolVersionSelector {
    resolver: VersionedConstantsResolver,
}

impl ProtocolVersionSelector {
    pub fn new(entries: impl IntoIterator<Item = (Version, VersionedConstants)>) -> Self {
        Self {
            resolver: VersionedConstantsResolver::new(entries),
        }
    }

    pub fn constants_for_block(
        &self,
        block: &StarknetBlock,
    ) -> Result<&VersionedConstants, ExecutionError> {
        self.resolver
            .resolve_for_protocol(&block.protocol_version)
            .map_err(|VersionResolutionError::Missing { requested }| {
                ExecutionError::MissingConstants(requested)
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use semver::Version;
    use starknet_node_types::{
        BuiltinStats, ExecutionOutput, InMemoryState, MutableState, SimulationResult,
        StarknetBlock, StarknetReceipt, StarknetStateDiff, StarknetTransaction, StateReader,
    };

    use super::*;

    struct ScriptedBackend {
        outputs: BTreeMap<u64, ExecutionOutput>,
        default: ExecutionOutput,
        _name: &'static str,
    }

    impl ScriptedBackend {
        fn new(
            name: &'static str,
            outputs: BTreeMap<u64, ExecutionOutput>,
            default: ExecutionOutput,
        ) -> Self {
            Self {
                outputs,
                default,
                _name: name,
            }
        }
    }

    impl ExecutionBackend for ScriptedBackend {
        fn execute_block(
            &self,
            block: &StarknetBlock,
            _state: &mut dyn MutableState,
        ) -> Result<ExecutionOutput, ExecutionError> {
            Ok(self
                .outputs
                .get(&block.number)
                .cloned()
                .unwrap_or_else(|| self.default.clone()))
        }

        fn simulate_tx(
            &self,
            tx: &StarknetTransaction,
            _state: &dyn StateReader,
            _block_context: &BlockContext,
        ) -> Result<SimulationResult, ExecutionError> {
            Ok(SimulationResult {
                receipt: StarknetReceipt {
                    tx_hash: tx.hash.clone(),
                    execution_status: true,
                    events: 0,
                    gas_consumed: 1,
                },
                estimated_fee: 1,
            })
        }
    }

    struct StateWritingBackend {
        gas: u64,
        value: u64,
    }

    impl ExecutionBackend for StateWritingBackend {
        fn execute_block(
            &self,
            _block: &StarknetBlock,
            state: &mut dyn MutableState,
        ) -> Result<ExecutionOutput, ExecutionError> {
            state.set_storage("0x1".to_string(), "slot".to_string(), self.value);
            Ok(output(self.gas))
        }

        fn simulate_tx(
            &self,
            tx: &StarknetTransaction,
            _state: &dyn StateReader,
            _block_context: &BlockContext,
        ) -> Result<SimulationResult, ExecutionError> {
            Ok(SimulationResult {
                receipt: StarknetReceipt {
                    tx_hash: tx.hash.clone(),
                    execution_status: true,
                    events: 0,
                    gas_consumed: self.gas,
                },
                estimated_fee: self.gas,
            })
        }
    }

    fn block(number: u64, version: &str) -> StarknetBlock {
        StarknetBlock {
            number,
            protocol_version: Version::parse(version).expect("valid version"),
            transactions: vec![StarknetTransaction {
                hash: format!("0x{number:x}"),
            }],
        }
    }

    fn output(gas: u64) -> ExecutionOutput {
        ExecutionOutput {
            receipts: vec![StarknetReceipt {
                tx_hash: format!("0x{gas:x}"),
                execution_status: true,
                events: gas,
                gas_consumed: gas,
            }],
            state_diff: StarknetStateDiff::default(),
            builtin_stats: BuiltinStats::default(),
            execution_time: Duration::from_millis(1),
        }
    }

    #[test]
    fn falls_back_to_canonical_when_fast_backend_missing() {
        let canonical =
            ScriptedBackend::new("canonical", BTreeMap::from([(1, output(5))]), output(9));
        let backend = DualExecutionBackend::new(
            None,
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::WarnAndFallback,
        );

        let mut state = InMemoryState::default();
        let result = backend.execute_verified(&block(1, "0.14.2"), &mut state);
        assert_eq!(result.expect("must fallback").receipts[0].gas_consumed, 5);
    }

    #[test]
    fn mismatch_with_warn_policy_returns_canonical_output() {
        let fast = ScriptedBackend::new("fast", BTreeMap::from([(1, output(1))]), output(1));
        let canonical =
            ScriptedBackend::new("canonical", BTreeMap::from([(1, output(7))]), output(7));
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::WarnAndFallback,
        );

        let mut state = InMemoryState::default();
        let result = backend.execute_verified(&block(1, "0.14.2"), &mut state);
        assert_eq!(result.expect("fallback").receipts[0].gas_consumed, 7);
    }

    #[test]
    fn mismatch_with_halt_policy_fails_closed() {
        let fast = ScriptedBackend::new("fast", BTreeMap::from([(1, output(1))]), output(1));
        let canonical =
            ScriptedBackend::new("canonical", BTreeMap::from([(1, output(7))]), output(7));
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::Halt,
        );

        let mut state = InMemoryState::default();
        let err = backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect_err("must halt");
        assert_eq!(err, ExecutionError::Halted);
    }

    #[test]
    fn canonical_state_wins_when_dual_execution_mismatches() {
        let fast = StateWritingBackend { gas: 1, value: 111 };
        let canonical = StateWritingBackend { gas: 7, value: 999 };
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::WarnAndFallback,
        );

        let mut state = InMemoryState::default();
        let result = backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect("fallback");
        assert_eq!(result.receipts[0].gas_consumed, 7);
        assert_eq!(state.get_storage(&"0x1".to_string(), "slot"), Some(999));
    }

    #[test]
    fn mismatch_with_cooldown_uses_canonical_for_following_blocks() {
        let fast = ScriptedBackend::new(
            "fast",
            BTreeMap::from([(1, output(1)), (2, output(2)), (3, output(3))]),
            output(3),
        );
        let canonical = ScriptedBackend::new(
            "canonical",
            BTreeMap::from([(1, output(10)), (2, output(20)), (3, output(30))]),
            output(30),
        );
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::CooldownThenRetry { cooldown_blocks: 2 },
        );

        let mut state = InMemoryState::default();
        let first = backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect("first");
        let second = backend
            .execute_verified(&block(2, "0.14.2"), &mut state)
            .expect("second");
        let third = backend
            .execute_verified(&block(3, "0.14.2"), &mut state)
            .expect("third");

        assert_eq!(first.receipts[0].gas_consumed, 10);
        assert_eq!(second.receipts[0].gas_consumed, 20);
        assert_eq!(third.receipts[0].gas_consumed, 30);
    }

    #[test]
    fn selects_constants_by_protocol_version() {
        let selector = ProtocolVersionSelector::new([
            (
                Version::parse("0.14.0").expect("valid"),
                VersionedConstants {
                    id: "v14_0".to_string(),
                },
            ),
            (
                Version::parse("0.14.2").expect("valid"),
                VersionedConstants {
                    id: "v14_2".to_string(),
                },
            ),
        ]);

        let selected = selector
            .constants_for_block(&block(100, "0.14.3"))
            .expect("same minor fallback");
        assert_eq!(selected.id, "v14_2");
    }

    #[test]
    fn rejects_unknown_minor_for_protocol_constants() {
        let selector = ProtocolVersionSelector::new([(
            Version::parse("0.14.0").expect("valid"),
            VersionedConstants {
                id: "v14_0".to_string(),
            },
        )]);

        let err = selector
            .constants_for_block(&block(100, "0.15.0"))
            .expect_err("must fail");
        assert_eq!(
            err,
            ExecutionError::MissingConstants(Version::parse("0.15.0").expect("valid"))
        );
    }
}
