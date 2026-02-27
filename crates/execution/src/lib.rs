#[cfg(feature = "blockifier-adapter")]
use std::sync::Arc;
use std::sync::Mutex;

use node_spec_core::protocol_version::{
    VersionResolutionError, VersionedConstants, VersionedConstantsResolver,
};
use semver::Version;
use starknet_node_types::{
    BlockContext, ExecutionOutput, MutableState, SimulationResult, StarknetBlock,
    StarknetTransaction, StateReader,
};
#[cfg(feature = "blockifier-adapter")]
use std::collections::BTreeMap;
#[cfg(feature = "blockifier-adapter")]
use std::str::FromStr;
#[cfg(feature = "blockifier-adapter")]
use std::time::Instant;

#[cfg(feature = "blockifier-adapter")]
use blockifier::blockifier::config::TransactionExecutorConfig;
#[cfg(feature = "blockifier-adapter")]
use blockifier::blockifier::transaction_executor::{TransactionExecutor, TransactionExecutorError};
#[cfg(feature = "blockifier-adapter")]
use blockifier::blockifier_versioned_constants::VersionedConstants as BlockifierVersionedConstants;
#[cfg(feature = "blockifier-adapter")]
use blockifier::bouncer::BouncerConfig;
#[cfg(feature = "blockifier-adapter")]
use blockifier::context::{
    BlockContext as BlockifierBlockContext, ChainInfo as BlockifierChainInfo,
};
#[cfg(feature = "blockifier-adapter")]
use blockifier::execution::contract_class::RunnableCompiledClass;
#[cfg(feature = "blockifier-adapter")]
use blockifier::state::errors::StateError;
#[cfg(feature = "blockifier-adapter")]
use blockifier::state::state_api::StateReader as BlockifierStateReader;
#[cfg(feature = "blockifier-adapter")]
use blockifier::transaction::transaction_execution::Transaction as BlockifierTransaction;
#[cfg(feature = "blockifier-adapter")]
use num_traits::ToPrimitive;
#[cfg(feature = "blockifier-adapter")]
use starknet_api::block::{
    BlockHash as BlockifierBlockHash, BlockHashAndNumber as BlockifierBlockHashAndNumber,
    BlockInfo as BlockifierBlockInfo, BlockNumber as BlockifierBlockNumber,
    BlockTimestamp as BlockifierBlockTimestamp, GasPrices as BlockifierGasPrices,
    StarknetVersion as BlockifierProtocolVersion,
};
#[cfg(feature = "blockifier-adapter")]
use starknet_api::core::{
    ClassHash as BlockifierClassHash, CompiledClassHash as BlockifierCompiledClassHash,
    ContractAddress as BlockifierContractAddress, Nonce as BlockifierNonce,
};
#[cfg(feature = "blockifier-adapter")]
use starknet_api::hash::StarkHash as BlockifierFelt;
#[cfg(feature = "blockifier-adapter")]
use starknet_api::state::StorageKey as BlockifierStorageKey;
#[cfg(feature = "blockifier-adapter")]
use starknet_api::versioned_constants_logic::VersionedConstantsTrait;
#[cfg(feature = "blockifier-adapter")]
use starknet_node_types::ExecutableStarknetTransaction;

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

#[cfg(feature = "blockifier-adapter")]
#[derive(Clone, Debug)]
pub struct BlockifierProtocolVersionResolver {
    versions: BTreeMap<Version, BlockifierProtocolVersion>,
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierProtocolVersionResolver {
    pub fn new(entries: impl IntoIterator<Item = (Version, BlockifierProtocolVersion)>) -> Self {
        Self {
            versions: entries.into_iter().collect(),
        }
    }

    pub fn starknet_mainnet_defaults() -> Self {
        let versions = [
            ("0.13.0", "0.13.0"),
            ("0.13.1", "0.13.1"),
            ("0.13.1-1", "0.13.1.1"),
            ("0.13.2", "0.13.2"),
            ("0.13.2-1", "0.13.2.1"),
            ("0.13.3", "0.13.3"),
            ("0.13.4", "0.13.4"),
            ("0.13.5", "0.13.5"),
            ("0.13.6", "0.13.6"),
            ("0.14.0", "0.14.0"),
            ("0.14.1", "0.14.1"),
            ("0.14.2", "0.14.2"),
        ];
        let entries = versions.into_iter().map(|(semver, raw)| {
            let semver = Version::parse(semver).expect("known valid protocol version");
            let protocol = BlockifierProtocolVersion::try_from(raw)
                .expect("blockifier supports bundled protocol version");
            (semver, protocol)
        });
        Self::new(entries)
    }

    pub fn resolve_for_block(
        &self,
        requested: &Version,
    ) -> Result<BlockifierProtocolVersion, ExecutionError> {
        if let Some(exact) = self.versions.get(requested) {
            return Ok(*exact);
        }

        self.versions
            .iter()
            .filter(|(version, _)| {
                version.major == requested.major
                    && version.minor == requested.minor
                    && version.patch <= requested.patch
            })
            .max_by(|(left, _), (right, _)| left.patch.cmp(&right.patch))
            .map(|(_, resolved)| *resolved)
            .ok_or_else(|| ExecutionError::MissingConstants(requested.clone()))
    }
}

#[cfg(feature = "blockifier-adapter")]
fn felt_to_u64(value: BlockifierFelt, field: &'static str) -> Result<u64, ExecutionError> {
    value.to_u64().ok_or_else(|| {
        ExecutionError::Backend(format!("value out of range for {field}: {:#x}", value))
    })
}

#[cfg(feature = "blockifier-adapter")]
struct BlockifierStateReaderAdapter {
    state: Box<dyn MutableState>,
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierStateReaderAdapter {
    fn new(state: &dyn MutableState) -> Self {
        Self {
            state: state.boxed_clone(),
        }
    }
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierStateReader for BlockifierStateReaderAdapter {
    fn get_storage_at(
        &self,
        contract_address: BlockifierContractAddress,
        key: BlockifierStorageKey,
    ) -> Result<BlockifierFelt, StateError> {
        let contract_felt: BlockifierFelt = contract_address.into();
        let key_felt: BlockifierFelt = key.into();
        Ok(self
            .state
            .get_storage(
                &format!("{:#x}", contract_felt),
                &format!("{:#x}", key_felt),
            )
            .map(BlockifierFelt::from)
            .unwrap_or(BlockifierFelt::ZERO))
    }

    fn get_nonce_at(
        &self,
        contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierNonce, StateError> {
        let contract_felt: BlockifierFelt = contract_address.into();
        let nonce = self
            .state
            .nonce_of(&format!("{:#x}", contract_felt))
            .unwrap_or_default();
        Ok(BlockifierNonce(BlockifierFelt::from(nonce)))
    }

    fn get_class_hash_at(
        &self,
        _contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierClassHash, StateError> {
        Err(StateError::StateReadError(
            "class hash lookup unsupported by generic MutableState adapter".to_string(),
        ))
    }

    fn get_compiled_class(
        &self,
        _class_hash: BlockifierClassHash,
    ) -> Result<RunnableCompiledClass, StateError> {
        Err(StateError::StateReadError(
            "compiled class lookup unsupported by generic MutableState adapter".to_string(),
        ))
    }

    fn get_compiled_class_hash(
        &self,
        _class_hash: BlockifierClassHash,
    ) -> Result<BlockifierCompiledClassHash, StateError> {
        Err(StateError::StateReadError(
            "compiled class hash lookup unsupported by generic MutableState adapter".to_string(),
        ))
    }
}

#[cfg(feature = "blockifier-adapter")]
pub trait ExecutableTransactionResolver: Send + Sync {
    fn resolve(
        &self,
        block_number: u64,
        tx: &StarknetTransaction,
    ) -> Result<ExecutableStarknetTransaction, ExecutionError>;
}

#[cfg(feature = "blockifier-adapter")]
#[derive(Default)]
pub struct EmbeddedExecutablePayloadResolver;

#[cfg(feature = "blockifier-adapter")]
impl ExecutableTransactionResolver for EmbeddedExecutablePayloadResolver {
    fn resolve(
        &self,
        block_number: u64,
        tx: &StarknetTransaction,
    ) -> Result<ExecutableStarknetTransaction, ExecutionError> {
        tx.executable.clone().ok_or_else(|| {
            ExecutionError::Backend(format!(
                "missing executable payload for tx {} in block {}",
                tx.hash, block_number
            ))
        })
    }
}

#[cfg(feature = "blockifier-adapter")]
pub struct BlockifierVmBackend {
    version_resolver: BlockifierProtocolVersionResolver,
    chain_info: BlockifierChainInfo,
    executor_config: TransactionExecutorConfig,
    tx_resolver: Arc<dyn ExecutableTransactionResolver>,
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierVmBackend {
    pub fn new(
        version_resolver: BlockifierProtocolVersionResolver,
        chain_info: BlockifierChainInfo,
        executor_config: TransactionExecutorConfig,
    ) -> Self {
        Self {
            version_resolver,
            chain_info,
            executor_config,
            tx_resolver: Arc::new(EmbeddedExecutablePayloadResolver),
        }
    }

    pub fn with_tx_resolver(mut self, tx_resolver: Arc<dyn ExecutableTransactionResolver>) -> Self {
        self.tx_resolver = tx_resolver;
        self
    }

    pub fn starknet_mainnet() -> Self {
        Self::new(
            BlockifierProtocolVersionResolver::starknet_mainnet_defaults(),
            BlockifierChainInfo::default(),
            TransactionExecutorConfig::default(),
        )
    }

    fn build_block_context(
        &self,
        block: &StarknetBlock,
    ) -> Result<BlockifierBlockContext, ExecutionError> {
        let protocol = self
            .version_resolver
            .resolve_for_block(&block.protocol_version)?;
        let versioned_constants = BlockifierVersionedConstants::get(&protocol)
            .map_err(|error| ExecutionError::Backend(error.to_string()))?;

        let block_info = BlockifierBlockInfo {
            block_number: BlockifierBlockNumber(block.number),
            block_timestamp: BlockifierBlockTimestamp(block.number),
            starknet_version: protocol,
            sequencer_address: BlockifierContractAddress::default(),
            gas_prices: BlockifierGasPrices::default(),
            use_kzg_da: false,
        };

        Ok(BlockifierBlockContext::new(
            block_info,
            self.chain_info.clone(),
            versioned_constants.clone(),
            BouncerConfig::default(),
        ))
    }

    fn map_block_transactions(
        &self,
        block: &StarknetBlock,
    ) -> Result<Vec<BlockifierTransaction>, ExecutionError> {
        block
            .transactions
            .iter()
            .map(|tx| {
                let executable = self.tx_resolver.resolve(block.number, tx)?;
                let expected_hash = BlockifierFelt::from_str(&tx.hash).map_err(|error| {
                    ExecutionError::Backend(format!(
                        "invalid tx hash '{}' in block {}: {}",
                        tx.hash, block.number, error
                    ))
                })?;
                if executable.tx_hash().0 != expected_hash {
                    return Err(ExecutionError::Backend(format!(
                        "resolver tx hash mismatch for block {}: expected {:#x}, got {:#x}",
                        block.number,
                        expected_hash,
                        executable.tx_hash().0
                    )));
                }
                Ok(BlockifierTransaction::new_for_sequencing(executable))
            })
            .collect()
    }

    fn gas_consumed_from_receipt(receipt: &blockifier::fee::receipt::TransactionReceipt) -> u64 {
        receipt
            .gas
            .l1_gas
            .0
            .saturating_add(receipt.gas.l1_data_gas.0)
            .saturating_add(receipt.gas.l2_gas.0)
    }
}

#[cfg(feature = "blockifier-adapter")]
impl ExecutionBackend for BlockifierVmBackend {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        let started_at = Instant::now();
        let block_context = self.build_block_context(block)?;
        let txs = self.map_block_transactions(block)?;

        let mut executor = TransactionExecutor::pre_process_and_create(
            BlockifierStateReaderAdapter::new(state),
            block_context,
            (block.number >= 10).then_some(BlockifierBlockHashAndNumber {
                hash: BlockifierBlockHash::default(),
                number: BlockifierBlockNumber(block.number.saturating_sub(10)),
            }),
            self.executor_config.clone(),
        )
        .map_err(|error| ExecutionError::Backend(error.to_string()))?;

        let execution_results = executor.execute_txs(
            &txs,
            Some(Instant::now() + std::time::Duration::from_secs(1)),
        );
        if execution_results.len() != txs.len() {
            return Err(ExecutionError::Backend(format!(
                "blockifier returned {} execution results for {} transactions in block {}",
                execution_results.len(),
                txs.len(),
                block.number
            )));
        }

        let receipts = execution_results
            .into_iter()
            .zip(block.transactions.iter())
            .map(|(result, tx)| match result {
                Ok((execution_info, _)) => Ok(starknet_node_types::StarknetReceipt {
                    tx_hash: tx.hash.clone(),
                    execution_status: !execution_info.is_reverted(),
                    events: execution_info
                        .non_optional_call_infos()
                        .map(|call_info| call_info.execution.events.len() as u64)
                        .sum(),
                    gas_consumed: Self::gas_consumed_from_receipt(&execution_info.receipt),
                }),
                Err(TransactionExecutorError::TransactionExecutionError(_)) => {
                    Ok(starknet_node_types::StarknetReceipt {
                        tx_hash: tx.hash.clone(),
                        execution_status: false,
                        events: 0,
                        gas_consumed: 0,
                    })
                }
                Err(other) => Err(ExecutionError::Backend(format!(
                    "fatal blockifier transaction executor error at block {}: {}",
                    block.number, other
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let summary = executor
            .finalize()
            .map_err(|error| ExecutionError::Backend(error.to_string()))?;

        let mut state_diff = starknet_node_types::StarknetStateDiff::default();
        for (address, writes) in summary.state_diff.storage_updates {
            let contract_felt: BlockifierFelt = address.into();
            let contract = format!("{:#x}", contract_felt);
            let contract_writes = state_diff.storage_diffs.entry(contract).or_default();
            for (key, value) in writes {
                let key_felt: BlockifierFelt = key.into();
                contract_writes.insert(
                    format!("{:#x}", key_felt),
                    felt_to_u64(value, "state_diff.storage_diffs")?,
                );
            }
        }
        for (address, nonce) in summary.state_diff.address_to_nonce {
            let contract_felt: BlockifierFelt = address.into();
            state_diff.nonces.insert(
                format!("{:#x}", contract_felt),
                felt_to_u64(nonce.0, "state_diff.nonces")?,
            );
        }
        for class_hash in summary.state_diff.class_hash_to_compiled_class_hash.keys() {
            state_diff
                .declared_classes
                .push(format!("{:#x}", class_hash.0));
        }

        for (contract, writes) in &state_diff.storage_diffs {
            for (key, value) in writes {
                state.set_storage(contract.clone(), key.clone(), *value);
            }
        }
        for (contract, nonce) in &state_diff.nonces {
            state.set_nonce(contract.clone(), *nonce);
        }

        Ok(ExecutionOutput {
            receipts,
            state_diff,
            builtin_stats: starknet_node_types::BuiltinStats::default(),
            execution_time: started_at.elapsed(),
        })
    }

    fn simulate_tx(
        &self,
        _tx: &StarknetTransaction,
        _state: &dyn StateReader,
        _block_context: &BlockContext,
    ) -> Result<SimulationResult, ExecutionError> {
        Err(ExecutionError::Backend(
            "simulate_tx is not yet implemented for blockifier backend adapter".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(feature = "blockifier-adapter")]
    use std::sync::Arc;
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
            transactions: vec![StarknetTransaction::new(format!("0x{number:x}"))],
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    fn empty_block(number: u64, version: &str) -> StarknetBlock {
        StarknetBlock {
            number,
            protocol_version: Version::parse(version).expect("valid version"),
            transactions: Vec::new(),
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

    #[cfg(feature = "blockifier-adapter")]
    fn executable_l1_handler_tx(hash: &str) -> StarknetTransaction {
        use starknet_api::executable_transaction::{
            L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
        };
        use starknet_api::transaction::TransactionHash;
        let mut executable = ExecutableL1Handler::default();
        // L1Handler payload size is calldata_len - 1; keep one slot to avoid underflow.
        executable.tx.calldata = vec![Default::default()].into();
        executable.tx_hash =
            TransactionHash(BlockifierFelt::from_str(hash).expect("valid tx hash"));

        StarknetTransaction::with_executable(hash.to_string(), ExecutableTx::L1Handler(executable))
    }

    #[cfg(feature = "blockifier-adapter")]
    struct StaticExecutableResolver;

    #[cfg(feature = "blockifier-adapter")]
    impl ExecutableTransactionResolver for StaticExecutableResolver {
        fn resolve(
            &self,
            _block_number: u64,
            tx: &StarknetTransaction,
        ) -> Result<ExecutableStarknetTransaction, ExecutionError> {
            use starknet_api::executable_transaction::{
                L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
            };
            use starknet_api::transaction::TransactionHash;

            let mut executable = ExecutableL1Handler::default();
            executable.tx.calldata = vec![Default::default()].into();
            executable.tx_hash =
                TransactionHash(BlockifierFelt::from_str(&tx.hash).map_err(|error| {
                    ExecutionError::Backend(format!(
                        "invalid tx hash provided to StaticExecutableResolver: {error}"
                    ))
                })?);
            Ok(ExecutableTx::L1Handler(executable))
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    struct MismatchedHashResolver;

    #[cfg(feature = "blockifier-adapter")]
    impl ExecutableTransactionResolver for MismatchedHashResolver {
        fn resolve(
            &self,
            _block_number: u64,
            _tx: &StarknetTransaction,
        ) -> Result<ExecutableStarknetTransaction, ExecutionError> {
            use starknet_api::executable_transaction::{
                L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
            };

            let mut executable = ExecutableL1Handler::default();
            executable.tx.calldata = vec![Default::default()].into();
            // Keep default tx hash (0x0) to force mismatch against requested tx hash.
            Ok(ExecutableTx::L1Handler(executable))
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

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_protocol_resolver_falls_back_to_latest_patch() {
        let resolver = BlockifierProtocolVersionResolver::starknet_mainnet_defaults();
        let resolved = resolver
            .resolve_for_block(&Version::parse("0.14.3").expect("valid"))
            .expect("resolve with patch fallback");
        assert_eq!(resolved.to_string(), "0.14.2");
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_protocol_resolver_supports_four_component_versions() {
        let resolver = BlockifierProtocolVersionResolver::starknet_mainnet_defaults();
        let resolved = resolver
            .resolve_for_block(&Version::parse("0.13.1-1").expect("valid"))
            .expect("resolve explicit 4th component mapping");
        assert_eq!(resolved.to_string(), "0.13.1.1");
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_executes_empty_block_with_real_context() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let mut state = InMemoryState::default();

        let output = backend
            .execute_block(&empty_block(11, "0.14.2"), &mut state)
            .expect("execute empty block");
        assert!(output.receipts.is_empty());
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_fails_closed_for_unconverted_transactions() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let mut state = InMemoryState::default();

        let err = backend
            .execute_block(&block(12, "0.14.2"), &mut state)
            .expect_err("must fail");
        match err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("missing executable payload"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_accepts_external_transaction_resolver() {
        let backend = BlockifierVmBackend::starknet_mainnet()
            .with_tx_resolver(Arc::new(StaticExecutableResolver));
        let mut state = InMemoryState::default();

        let output = backend
            .execute_block(&block(12, "0.14.2"), &mut state)
            .expect("execute via external resolver");
        assert_eq!(output.receipts.len(), 1);
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_rejects_resolver_hash_mismatches() {
        let backend = BlockifierVmBackend::starknet_mainnet()
            .with_tx_resolver(Arc::new(MismatchedHashResolver));
        let mut state = InMemoryState::default();

        let err = backend
            .execute_block(&block(12, "0.14.2"), &mut state)
            .expect_err("must reject mismatched hashes");
        match err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("resolver tx hash mismatch"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_processes_non_empty_blocks_with_executable_payloads() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let mut state = InMemoryState::default();
        let block = StarknetBlock {
            number: 12,
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: vec![executable_l1_handler_tx("0xabc")],
        };

        let output = backend
            .execute_block(&block, &mut state)
            .expect("execute non-empty block");
        assert_eq!(output.receipts.len(), 1);
        assert_eq!(output.receipts[0].tx_hash, "0xabc");
        assert!(!output.receipts[0].execution_status);
    }
}
