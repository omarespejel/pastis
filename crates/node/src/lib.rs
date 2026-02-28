#![forbid(unsafe_code)]

pub mod replay;

#[cfg(feature = "production-adapters")]
use std::cell::RefCell;
#[cfg(feature = "production-adapters")]
use std::collections::BTreeMap;
#[cfg(feature = "production-adapters")]
use std::sync::Arc;
#[cfg(feature = "production-adapters")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "production-adapters")]
use apollo_storage::StorageReader as ApolloStorageReader;
#[cfg(feature = "production-adapters")]
use apollo_storage::class::ClassStorageReader;
#[cfg(feature = "production-adapters")]
use apollo_storage::compiled_class::CasmStorageReader;
#[cfg(feature = "production-adapters")]
use apollo_storage::state::StateStorageReader;
#[cfg(feature = "production-adapters")]
use blockifier::execution::contract_class::RunnableCompiledClass;
#[cfg(feature = "production-adapters")]
use blockifier::state::errors::StateError;
use node_spec_core::mcp::{McpAccessController, ValidationLimits};
#[cfg(feature = "production-adapters")]
use starknet_api::block::BlockNumber as StarknetApiBlockNumber;
#[cfg(feature = "production-adapters")]
use starknet_api::contract_class::{
    ContractClass as StarknetApiContractClass, SierraVersion as StarknetApiSierraVersion,
};
#[cfg(feature = "production-adapters")]
use starknet_api::core::{
    ClassHash as StarknetApiClassHash, CompiledClassHash as StarknetApiCompiledClassHash,
    ContractAddress as StarknetApiContractAddress,
};
#[cfg(feature = "production-adapters")]
use starknet_api::state::StateNumber as StarknetApiStateNumber;
use starknet_node_execution::ExecutionBackend;
#[cfg(feature = "production-adapters")]
use starknet_node_execution::{
    BlockifierClassProvider, BlockifierVmBackend, DualExecutionBackend, DualExecutionMetrics,
    ExecutionError, ExecutionMode, MismatchPolicy,
};
use starknet_node_mcp_server::{AnomalySource, McpServer};
use starknet_node_proving::ProvingBackend;
use starknet_node_rpc::StarknetRpcServer;
#[cfg(feature = "production-adapters")]
use starknet_node_storage::ApolloStorageAdapter;
use starknet_node_storage::StorageBackend;
#[cfg(feature = "production-adapters")]
use starknet_node_types::BlockId;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainId {
    Mainnet,
    Sepolia,
    Custom(String),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ChainIdError {
    #[error("chain id cannot be empty")]
    Empty,
    #[error("invalid chain id format '{0}'")]
    InvalidFormat(String),
}

impl ChainId {
    /// Parses canonical Starknet public chain IDs and strict-format custom IDs.
    /// Custom IDs are intentionally supported for private networks/appchains.
    pub fn parse(raw: impl Into<String>) -> Result<Self, ChainIdError> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(ChainIdError::Empty);
        }
        match raw.as_str() {
            "SN_MAIN" => Ok(Self::Mainnet),
            "SN_SEPOLIA" => Ok(Self::Sepolia),
            _ => {
                let valid = raw.starts_with("SN_")
                    && raw
                        .chars()
                        .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_');
                if !valid {
                    return Err(ChainIdError::InvalidFormat(raw));
                }
                Ok(Self::Custom(raw))
            }
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Mainnet => "SN_MAIN",
            Self::Sepolia => "SN_SEPOLIA",
            Self::Custom(raw) => raw.as_str(),
        }
    }

    pub fn is_mainnet(&self) -> bool {
        matches!(self, Self::Mainnet)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub chain_id: ChainId,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            chain_id: ChainId::Mainnet,
        }
    }
}

pub struct NoStorage;
pub struct WithStorage<S>(pub S);
pub struct NoExecution;
pub struct WithExecution<E>(pub E);

pub struct StarknetNode<S, E> {
    pub config: NodeConfig,
    pub storage: S,
    pub execution: E,
    pub rpc_enabled: bool,
    pub mcp_enabled: bool,
    proving: Option<Box<dyn ProvingBackend>>,
}

#[cfg(feature = "production-adapters")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionDualExecutionConfig {
    pub enable_shadow_verification: bool,
    pub verification_depth: u64,
    pub mismatch_policy: MismatchPolicy,
}

#[cfg(feature = "production-adapters")]
impl Default for ProductionDualExecutionConfig {
    fn default() -> Self {
        Self {
            enable_shadow_verification: false,
            verification_depth: 10,
            mismatch_policy: MismatchPolicy::WarnAndFallback,
        }
    }
}

#[cfg(feature = "production-adapters")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionExecutionTelemetry {
    pub metrics: DualExecutionMetrics,
}

impl<S: StorageBackend, E> StarknetNode<S, E> {
    pub fn new_rpc_server(&self) -> StarknetRpcServer<'_> {
        StarknetRpcServer::new(&self.storage, self.config.chain_id.as_str())
    }

    pub fn new_mcp_server(
        &self,
        access_controller: McpAccessController,
        validation_limits: ValidationLimits,
    ) -> McpServer<'_> {
        McpServer::new(&self.storage, access_controller, validation_limits)
    }

    pub fn new_mcp_server_with_anomalies<'a>(
        &'a self,
        access_controller: McpAccessController,
        validation_limits: ValidationLimits,
        anomaly_source: &'a dyn AnomalySource,
    ) -> McpServer<'a> {
        McpServer::new(&self.storage, access_controller, validation_limits)
            .with_anomaly_source(anomaly_source)
    }

    pub fn proving_backend(&self) -> Option<&dyn ProvingBackend> {
        self.proving.as_deref()
    }
}

#[cfg(feature = "production-adapters")]
impl<S: StorageBackend> StarknetNode<S, DualExecutionBackend> {
    pub fn production_execution_telemetry(
        &self,
    ) -> Result<ProductionExecutionTelemetry, ExecutionError> {
        Ok(ProductionExecutionTelemetry {
            metrics: self.execution.metrics()?,
        })
    }
}

pub struct StarknetNodeBuilder<Stor, Exec> {
    config: NodeConfig,
    rpc_enabled: bool,
    mcp_enabled: bool,
    proving: Option<Box<dyn ProvingBackend>>,
    storage: Stor,
    execution: Exec,
}

#[cfg(feature = "production-adapters")]
#[derive(Debug, Error)]
pub enum NodeInitError {
    #[error("storage bootstrap check failed: {0}")]
    StorageCheck(String),
    #[error("storage backend reported unhealthy status")]
    StorageUnhealthy,
    #[error("storage integrity check failed: {0}")]
    StorageIntegrity(String),
}

#[cfg(feature = "production-adapters")]
const STARKNET_MAINNET_GENESIS_STATE_ROOT: &str =
    "0x21870ba80540e7831fb21c591ee93481f5ae1bb71ff85a86ddd465be4eddee6";

#[cfg(feature = "production-adapters")]
fn normalize_hex(input: &str) -> String {
    let raw = input.trim();
    let stripped = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    let normalized = stripped.trim_start_matches('0').to_ascii_lowercase();
    if normalized.is_empty() {
        "0x0".to_string()
    } else {
        format!("0x{normalized}")
    }
}

#[cfg(feature = "production-adapters")]
fn validate_bootstrap_storage<S: StorageBackend>(
    storage: &S,
    config: &NodeConfig,
) -> Result<(), NodeInitError> {
    let latest = storage
        .latest_block_number()
        .map_err(|error| NodeInitError::StorageCheck(error.to_string()))?;
    storage
        .get_state_reader(0)
        .map_err(|error| NodeInitError::StorageIntegrity(error.to_string()))?;
    if config.chain_id.is_mainnet()
        && let Some(genesis) = storage
            .get_block(BlockId::Number(0))
            .map_err(|error| NodeInitError::StorageIntegrity(error.to_string()))?
    {
        let expected = normalize_hex(STARKNET_MAINNET_GENESIS_STATE_ROOT);
        let actual = normalize_hex(&genesis.state_root);
        if actual != expected {
            return Err(NodeInitError::StorageIntegrity(format!(
                "mainnet genesis state root mismatch: expected {expected}, got {actual}"
            )));
        }
    }
    if latest > 0 {
        let tip = storage
            .get_block(BlockId::Number(latest))
            .map_err(|error| NodeInitError::StorageIntegrity(error.to_string()))?
            .ok_or_else(|| {
                NodeInitError::StorageIntegrity(format!(
                    "latest block marker points to {latest}, but block body is missing"
                ))
            })?;
        if tip.number != latest {
            return Err(NodeInitError::StorageIntegrity(format!(
                "latest block mismatch: expected {latest}, got {}",
                tip.number
            )));
        }
    }
    if !storage.is_healthy() {
        return Err(NodeInitError::StorageUnhealthy);
    }
    Ok(())
}

impl StarknetNodeBuilder<NoStorage, NoExecution> {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            rpc_enabled: false,
            mcp_enabled: false,
            proving: None,
            storage: NoStorage,
            execution: NoExecution,
        }
    }

    pub fn with_storage<S: StorageBackend>(
        self,
        storage: S,
    ) -> StarknetNodeBuilder<WithStorage<S>, NoExecution> {
        StarknetNodeBuilder {
            config: self.config,
            rpc_enabled: self.rpc_enabled,
            mcp_enabled: self.mcp_enabled,
            proving: self.proving,
            storage: WithStorage(storage),
            execution: NoExecution,
        }
    }
}

impl<S: StorageBackend> StarknetNodeBuilder<WithStorage<S>, NoExecution> {
    pub fn with_execution<E: ExecutionBackend>(
        self,
        execution: E,
    ) -> StarknetNodeBuilder<WithStorage<S>, WithExecution<E>> {
        StarknetNodeBuilder {
            config: self.config,
            rpc_enabled: self.rpc_enabled,
            mcp_enabled: self.mcp_enabled,
            proving: self.proving,
            storage: self.storage,
            execution: WithExecution(execution),
        }
    }
}

impl<S: StorageBackend, E: ExecutionBackend> StarknetNodeBuilder<WithStorage<S>, WithExecution<E>> {
    pub fn with_rpc(mut self, enabled: bool) -> Self {
        self.rpc_enabled = enabled;
        self
    }

    pub fn with_mcp(mut self, enabled: bool) -> Self {
        self.mcp_enabled = enabled;
        self
    }

    pub fn with_proving(mut self, proving: impl ProvingBackend + 'static) -> Self {
        self.proving = Some(Box::new(proving));
        self
    }

    pub fn build(self) -> StarknetNode<S, E> {
        StarknetNode {
            config: self.config,
            storage: self.storage.0,
            execution: self.execution.0,
            rpc_enabled: self.rpc_enabled,
            mcp_enabled: self.mcp_enabled,
            proving: self.proving,
        }
    }
}

#[cfg(feature = "production-adapters")]
struct ApolloBlockifierClassProvider {
    reader: ApolloStorageReader,
    instance_id: u64,
}

#[cfg(feature = "production-adapters")]
impl ApolloBlockifierClassProvider {
    fn next_instance_id() -> u64 {
        static NEXT_CLASS_PROVIDER_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_CLASS_PROVIDER_ID.fetch_add(1, Ordering::Relaxed)
    }

    fn with_thread_local_state<T>(
        f: impl FnOnce(&mut BTreeMap<u64, StarknetApiStateNumber>) -> T,
    ) -> T {
        thread_local! {
            static STATE_BY_PROVIDER_ID: RefCell<BTreeMap<u64, StarknetApiStateNumber>> =
                const { RefCell::new(BTreeMap::new()) };
        }
        STATE_BY_PROVIDER_ID.with(|state| f(&mut state.borrow_mut()))
    }

    fn new(reader: ApolloStorageReader) -> Self {
        Self {
            reader,
            instance_id: Self::next_instance_id(),
        }
    }

    fn pre_execution_state_number(block_number: u64) -> StarknetApiStateNumber {
        if block_number == 0 {
            StarknetApiStateNumber::right_before_block(StarknetApiBlockNumber(0))
        } else {
            StarknetApiStateNumber::unchecked_right_after_block(StarknetApiBlockNumber(
                block_number.saturating_sub(1),
            ))
        }
    }

    fn simulation_state_number(block_number: u64) -> StarknetApiStateNumber {
        Self::pre_execution_state_number(block_number)
    }

    fn read_state_number(&self) -> StarknetApiStateNumber {
        Self::with_thread_local_state(|state| {
            state
                .get(&self.instance_id)
                .copied()
                .unwrap_or_else(|| Self::pre_execution_state_number(0))
        })
    }

    fn write_state_number(&self, state_number: StarknetApiStateNumber) {
        Self::with_thread_local_state(|state| {
            state.insert(self.instance_id, state_number);
        });
    }

    fn map_storage_error(context: &'static str, error: impl std::fmt::Display) -> StateError {
        StateError::StateReadError(format!("{context}: {error}"))
    }
}

#[cfg(feature = "production-adapters")]
impl BlockifierClassProvider for ApolloBlockifierClassProvider {
    fn supports_account_execution(&self) -> bool {
        true
    }

    fn prepare_for_block_execution(&self, block_number: u64) -> Result<(), StateError> {
        self.write_state_number(Self::pre_execution_state_number(block_number));
        Ok(())
    }

    fn prepare_for_simulation(&self, block_number: u64) -> Result<(), StateError> {
        self.write_state_number(Self::simulation_state_number(block_number));
        Ok(())
    }

    fn get_class_hash_at(
        &self,
        contract_address: StarknetApiContractAddress,
    ) -> Result<StarknetApiClassHash, StateError> {
        let state_number = self.read_state_number();
        let txn = self.reader.begin_ro_txn().map_err(|error| {
            Self::map_storage_error("open apollo read txn for class hash", error)
        })?;
        let state_reader = txn
            .get_state_reader()
            .map_err(|error| Self::map_storage_error("create apollo state reader", error))?;
        state_reader
            .get_class_hash_at(state_number, &contract_address)
            .map_err(|error| Self::map_storage_error("read class hash from apollo", error))
            .map(|value| value.unwrap_or_default())
    }

    fn get_compiled_class(
        &self,
        class_hash: StarknetApiClassHash,
    ) -> Result<RunnableCompiledClass, StateError> {
        let txn = self.reader.begin_ro_txn().map_err(|error| {
            Self::map_storage_error("open apollo read txn for class fetch", error)
        })?;

        let (casm, sierra) = txn
            .get_casm_and_sierra(&class_hash)
            .map_err(|error| Self::map_storage_error("read CASM/Sierra pair from apollo", error))?;
        if let Some(casm) = casm {
            let sierra = sierra.ok_or_else(|| {
                StateError::StateReadError(format!(
                    "missing Sierra class for CASM class hash {:#x}",
                    class_hash.0
                ))
            })?;
            let sierra_version: StarknetApiSierraVersion =
                sierra.get_sierra_version().map_err(|error| {
                    StateError::StateReadError(format!(
                        "failed to derive Sierra version for class hash {:#x}: {}",
                        class_hash.0, error
                    ))
                })?;

            return RunnableCompiledClass::try_from(StarknetApiContractClass::V1((
                casm,
                sierra_version,
            )))
            .map_err(|error| {
                StateError::StateReadError(format!(
                    "failed to build runnable Cairo 1 class for {:#x}: {}",
                    class_hash.0, error
                ))
            });
        }

        if let Some(deprecated) = txn
            .get_deprecated_class(&class_hash)
            .map_err(|error| Self::map_storage_error("read deprecated class from apollo", error))?
        {
            return RunnableCompiledClass::try_from(StarknetApiContractClass::V0(deprecated))
                .map_err(|error| {
                    StateError::StateReadError(format!(
                        "failed to build runnable Cairo 0 class for {:#x}: {}",
                        class_hash.0, error
                    ))
                });
        }

        Err(StateError::UndeclaredClassHash(class_hash))
    }

    fn get_compiled_class_hash(
        &self,
        class_hash: StarknetApiClassHash,
    ) -> Result<StarknetApiCompiledClassHash, StateError> {
        let state_number = self.read_state_number();
        let txn = self.reader.begin_ro_txn().map_err(|error| {
            Self::map_storage_error("open apollo read txn for compiled class hash", error)
        })?;
        let state_reader = txn
            .get_state_reader()
            .map_err(|error| Self::map_storage_error("create apollo state reader", error))?;
        state_reader
            .get_compiled_class_hash_at(state_number, &class_hash)
            .map_err(|error| Self::map_storage_error("read compiled class hash from apollo", error))
            .map(|value| value.unwrap_or_default())
    }
}

#[cfg(feature = "production-adapters")]
pub fn build_mainnet_production_node(
    config: NodeConfig,
    storage: ApolloStorageAdapter,
) -> Result<StarknetNode<ApolloStorageAdapter, DualExecutionBackend>, NodeInitError> {
    build_mainnet_production_node_with_dual_config(
        config,
        storage,
        ProductionDualExecutionConfig::default(),
    )
}

#[cfg(feature = "production-adapters")]
pub fn build_mainnet_production_node_with_dual_config(
    config: NodeConfig,
    storage: ApolloStorageAdapter,
    dual_config: ProductionDualExecutionConfig,
) -> Result<StarknetNode<ApolloStorageAdapter, DualExecutionBackend>, NodeInitError> {
    validate_bootstrap_storage(&storage, &config)?;
    let class_provider: Arc<dyn BlockifierClassProvider> =
        Arc::new(ApolloBlockifierClassProvider::new(storage.reader_handle()));
    let canonical =
        BlockifierVmBackend::starknet_mainnet().with_class_provider(Arc::clone(&class_provider));
    let fast = dual_config.enable_shadow_verification.then(|| {
        Box::new(
            BlockifierVmBackend::starknet_mainnet()
                .with_class_provider(Arc::clone(&class_provider)),
        ) as Box<dyn ExecutionBackend>
    });
    let mode = if dual_config.enable_shadow_verification {
        ExecutionMode::DualWithVerification {
            verification_depth: dual_config.verification_depth,
        }
    } else {
        ExecutionMode::CanonicalOnly
    };
    let dual =
        DualExecutionBackend::new(fast, Box::new(canonical), mode, dual_config.mismatch_policy);
    Ok(StarknetNodeBuilder::new(config)
        .with_storage(storage)
        .with_execution(dual)
        .build())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    #[cfg(feature = "production-adapters")]
    use std::path::Path;
    #[cfg(feature = "production-adapters")]
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    #[cfg(feature = "production-adapters")]
    use apollo_storage::class::ClassStorageWriter;
    #[cfg(feature = "production-adapters")]
    use apollo_storage::compiled_class::CasmStorageWriter;
    #[cfg(feature = "production-adapters")]
    use apollo_storage::db::DbConfig;
    #[cfg(feature = "production-adapters")]
    use apollo_storage::header::HeaderStorageWriter;
    #[cfg(feature = "production-adapters")]
    use apollo_storage::mmap_file::MmapFileConfig;
    #[cfg(feature = "production-adapters")]
    use apollo_storage::state::StateStorageWriter;
    #[cfg(feature = "production-adapters")]
    use apollo_storage::{
        StorageConfig as ApolloStorageConfig, StorageScope, StorageWriter as ApolloStorageWriter,
        open_storage as open_apollo_storage,
    };
    #[cfg(feature = "production-adapters")]
    use cairo_lang_starknet_classes::casm_contract_class::CasmContractClass;
    use node_spec_core::mcp::{
        AgentPolicy, McpAccessController, McpTool, ToolPermission, ValidationLimits,
    };
    #[cfg(feature = "production-adapters")]
    use semver::Version;
    #[cfg(feature = "production-adapters")]
    use starknet_api::block::{
        BlockHeader as StarknetApiBlockHeader, BlockNumber as StarknetApiBlockNumber,
        BlockTimestamp as StarknetApiBlockTimestamp, StarknetVersion as StarknetApiVersion,
    };
    #[cfg(feature = "production-adapters")]
    use starknet_api::contract_class::compiled_class_hash::{HashVersion, HashableCompiledClass};
    #[cfg(feature = "production-adapters")]
    use starknet_api::core::{
        ChainId as StarknetApiChainId, ClassHash as StarknetApiClassHash,
        CompiledClassHash as StarknetApiCompiledClassHash,
        ContractAddress as StarknetApiContractAddress, GlobalRoot, Nonce as StarknetApiNonce,
    };
    #[cfg(feature = "production-adapters")]
    use starknet_api::executable_transaction::{
        AccountTransaction as StarknetApiExecutableAccountTransaction,
        DeployAccountTransaction as StarknetApiExecutableDeployAccountTransaction,
        Transaction as StarknetApiExecutableTransaction,
    };
    #[cfg(feature = "production-adapters")]
    use starknet_api::hash::StarkHash as StarknetApiFelt;
    #[cfg(feature = "production-adapters")]
    use starknet_api::state::{
        StorageKey as StarknetApiStorageKey, ThinStateDiff as StarknetApiThinStateDiff,
    };
    #[cfg(feature = "production-adapters")]
    use starknet_api::transaction::fields::Fee as StarknetApiFee;
    #[cfg(feature = "production-adapters")]
    use starknet_api::transaction::{
        DeployAccountTransaction as StarknetApiDeployAccountTransaction,
        DeployAccountTransactionV1 as StarknetApiDeployAccountTransactionV1,
    };
    use starknet_node_execution::ExecutionError;
    use starknet_node_mcp_server::{AnomalySource, BtcfiAnomaly, McpRequest, McpResponse};
    use starknet_node_proving::{ProvingError, StarkProof};
    use starknet_node_rpc::JsonRpcRequest;
    use starknet_node_storage::InMemoryStorage;
    #[cfg(feature = "production-adapters")]
    use starknet_node_storage::StorageError;
    use starknet_node_types::{
        BlockContext, BuiltinStats, ExecutionOutput, InMemoryState, MutableState, SimulationResult,
        StarknetBlock, StarknetReceipt, StarknetStateDiff, StarknetTransaction, StateReader,
    };
    #[cfg(feature = "production-adapters")]
    use starknet_node_types::{
        BlockGasPrices, BlockId, ComponentHealth, GasPricePerToken, HealthCheck, HealthStatus,
    };
    #[cfg(feature = "production-adapters")]
    use tempfile::tempdir;

    use super::*;

    struct DummyExecution;

    impl ExecutionBackend for DummyExecution {
        fn execute_block(
            &self,
            block: &StarknetBlock,
            _state: &mut dyn MutableState,
        ) -> Result<ExecutionOutput, ExecutionError> {
            Ok(ExecutionOutput {
                receipts: vec![StarknetReceipt {
                    tx_hash: format!("0x{:x}", block.number).into(),
                    execution_status: true,
                    events: 0,
                    gas_consumed: 1,
                }],
                state_diff: StarknetStateDiff::default(),
                builtin_stats: BuiltinStats::default(),
                execution_time: Duration::from_millis(1),
            })
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

    struct DummyProving;

    impl ProvingBackend for DummyProving {
        fn verify_proof(&self, proof: &StarkProof) -> Result<bool, ProvingError> {
            Ok(!proof.proof_bytes.is_empty())
        }
    }

    struct DummyAnomalySource;

    impl AnomalySource for DummyAnomalySource {
        fn recent_anomalies(&self, _limit: usize) -> Result<Vec<BtcfiAnomaly>, String> {
            Ok(vec![BtcfiAnomaly::StrkBtcCommitmentFlood {
                block_number: 7,
                delta: 12,
                threshold: 8,
            }])
        }
    }

    #[test]
    fn builds_node_after_storage_and_execution_are_provided() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig::default())
            .with_storage(storage)
            .with_execution(DummyExecution)
            .with_rpc(true)
            .with_mcp(true)
            .build();

        assert!(node.rpc_enabled);
        assert!(node.mcp_enabled);
        assert_eq!(node.config.chain_id, ChainId::Mainnet);
        assert!(node.proving_backend().is_none());
    }

    #[test]
    fn builder_wires_optional_proving_backend() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig::default())
            .with_storage(storage)
            .with_execution(DummyExecution)
            .with_proving(DummyProving)
            .build();

        let backend = node.proving_backend().expect("proving backend configured");
        assert!(
            backend
                .verify_proof(&StarkProof {
                    block_number: 1,
                    proof_bytes: vec![1],
                })
                .expect("proof verification should succeed")
        );
        assert!(
            !backend
                .verify_proof(&StarkProof {
                    block_number: 1,
                    proof_bytes: Vec::new(),
                })
                .expect("proof verification should succeed")
        );
    }

    #[test]
    fn node_can_construct_mcp_server_bound_to_storage() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig::default())
            .with_storage(storage)
            .with_execution(DummyExecution)
            .with_mcp(true)
            .build();
        let policies = [(
            "agent-a".to_string(),
            AgentPolicy::new("api-key", BTreeSet::from([ToolPermission::QueryState]), 5),
        )];
        let limits = ValidationLimits {
            max_batch_size: 8,
            max_depth: 2,
            max_total_tools: 16,
        };
        let server = node.new_mcp_server(McpAccessController::new(policies), limits);
        let response = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_000,
            })
            .expect("request should succeed");
        assert_eq!(response.agent_id, "agent-a");
        assert!(matches!(response.response, McpResponse::QueryState(_)));
    }

    #[test]
    fn node_can_construct_mcp_server_with_anomaly_source() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig::default())
            .with_storage(storage)
            .with_execution(DummyExecution)
            .with_mcp(true)
            .build();
        let policies = [(
            "agent-a".to_string(),
            AgentPolicy::new("api-key", BTreeSet::from([ToolPermission::GetAnomalies]), 5),
        )];
        let limits = ValidationLimits {
            max_batch_size: 8,
            max_depth: 2,
            max_total_tools: 16,
        };
        let source = DummyAnomalySource;
        let server =
            node.new_mcp_server_with_anomalies(McpAccessController::new(policies), limits, &source);
        let response = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::GetAnomalies { limit: 10 },
                now_unix_seconds: 1_000,
            })
            .expect("request should succeed");
        assert_eq!(response.agent_id, "agent-a");
        assert!(matches!(
            response.response,
            McpResponse::GetAnomalies { ref anomalies } if anomalies.len() == 1
        ));
    }

    #[test]
    fn node_can_construct_rpc_server_bound_to_storage() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig {
            chain_id: ChainId::parse("SN_SEPOLIA").expect("chain id"),
        })
        .with_storage(storage)
        .with_execution(DummyExecution)
        .with_rpc(true)
        .build();
        let rpc = node.new_rpc_server();
        let response = rpc.handle_request(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "starknet_chainId".to_string(),
            params: serde_json::json!([]),
            id: serde_json::json!(7),
        });
        assert_eq!(response.result, Some(serde_json::json!("SN_SEPOLIA")));
    }

    #[test]
    fn builder_requires_ordered_setup() {
        // This test validates runtime state after the type-state transitions complete.
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig {
            chain_id: ChainId::parse("SN_SEPOLIA").expect("valid chain id"),
        })
        .with_storage(storage)
        .with_execution(DummyExecution)
        .build();

        assert_eq!(node.config.chain_id, ChainId::Sepolia);
    }

    #[test]
    fn rejects_invalid_chain_id_format() {
        let err = ChainId::parse("sn_main").expect_err("must reject invalid casing");
        assert!(matches!(err, ChainIdError::InvalidFormat(_)));
    }

    #[cfg(feature = "production-adapters")]
    fn apollo_config(path: &Path) -> ApolloStorageConfig {
        ApolloStorageConfig {
            db_config: DbConfig {
                path_prefix: path.to_path_buf(),
                chain_id: StarknetApiChainId::Other("SN_MAIN".to_string()),
                enforce_file_exists: false,
                min_size: 1 << 20,
                max_size: 1 << 35,
                growth_step: 1 << 26,
                max_readers: 1 << 13,
            },
            mmap_file_config: MmapFileConfig {
                max_size: 1 << 24,
                growth_step: 1 << 20,
                max_object_size: 1 << 16,
            },
            scope: StorageScope::FullArchive,
        }
    }

    #[cfg(feature = "production-adapters")]
    fn seed_header_and_state_with_class_hash(
        writer: &mut ApolloStorageWriter,
    ) -> (
        StarknetApiContractAddress,
        StarknetApiClassHash,
        StarknetApiCompiledClassHash,
    ) {
        let mut header = StarknetApiBlockHeader::default();
        header.block_header_without_hash.state_root = GlobalRoot(StarknetApiFelt::from(1_u64));
        header.block_header_without_hash.starknet_version =
            StarknetApiVersion::try_from("0.14.2").expect("valid protocol version");
        header.block_header_without_hash.timestamp = StarknetApiBlockTimestamp(1_700_000_000);
        writer
            .begin_rw_txn()
            .expect("begin rw tx")
            .append_header(StarknetApiBlockNumber(0), &header)
            .expect("append header")
            .commit()
            .expect("commit header");

        let contract = StarknetApiContractAddress::from(0xabc_u64);
        let class_hash = StarknetApiClassHash(StarknetApiFelt::from(0x55_u64));
        let compiled_class_hash = StarknetApiCompiledClassHash(StarknetApiFelt::from(0x66_u64));
        let diff = StarknetApiThinStateDiff {
            storage_diffs: std::iter::once((
                contract,
                std::iter::once((
                    StarknetApiStorageKey::from(1_u64),
                    StarknetApiFelt::from(7_u64),
                ))
                .collect(),
            ))
            .collect(),
            deployed_contracts: std::iter::once((contract, class_hash)).collect(),
            class_hash_to_compiled_class_hash: std::iter::once((class_hash, compiled_class_hash))
                .collect(),
            nonces: std::iter::once((contract, StarknetApiNonce(StarknetApiFelt::from(1_u64))))
                .collect(),
            ..StarknetApiThinStateDiff::default()
        };
        writer
            .begin_rw_txn()
            .expect("begin rw tx")
            .append_state_diff(StarknetApiBlockNumber(0), diff)
            .expect("append state diff")
            .commit()
            .expect("commit state diff");

        (contract, class_hash, compiled_class_hash)
    }

    #[cfg(feature = "production-adapters")]
    fn parse_account_sierra_fixture() -> starknet_api::state::SierraContractClass {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/account_with_dummy_validate.sierra.json"
        ))
        .expect("valid account sierra fixture json");
        if let Some(abi) = value.get_mut("abi")
            && !abi.is_string()
        {
            *abi = serde_json::Value::String(
                serde_json::to_string(abi).expect("serialize sierra fixture ABI"),
            );
        }
        serde_json::from_value(value).expect("valid account sierra fixture")
    }

    #[cfg(feature = "production-adapters")]
    fn parse_account_casm_fixture() -> CasmContractClass {
        serde_json::from_str(include_str!(
            "../tests/fixtures/account_with_dummy_validate.casm.json"
        ))
        .expect("valid account casm fixture")
    }

    #[cfg(feature = "production-adapters")]
    fn seed_declared_account_class_with_casm(
        writer: &mut ApolloStorageWriter,
    ) -> (StarknetApiClassHash, StarknetApiCompiledClassHash) {
        let mut header = StarknetApiBlockHeader::default();
        header.block_header_without_hash.state_root = GlobalRoot(StarknetApiFelt::from(1_u64));
        header.block_header_without_hash.starknet_version =
            StarknetApiVersion::try_from("0.14.2").expect("valid protocol version");
        header.block_header_without_hash.timestamp = StarknetApiBlockTimestamp(1_700_000_000);
        writer
            .begin_rw_txn()
            .expect("begin rw tx")
            .append_header(StarknetApiBlockNumber(0), &header)
            .expect("append header")
            .commit()
            .expect("commit header");

        let sierra = parse_account_sierra_fixture();
        let casm = parse_account_casm_fixture();
        let class_hash = sierra.calculate_class_hash();
        let compiled_class_hash = casm.hash(&HashVersion::V2);

        writer
            .begin_rw_txn()
            .expect("begin rw tx")
            .append_state_diff(
                StarknetApiBlockNumber(0),
                StarknetApiThinStateDiff {
                    class_hash_to_compiled_class_hash: std::iter::once((
                        class_hash,
                        compiled_class_hash,
                    ))
                    .collect(),
                    ..StarknetApiThinStateDiff::default()
                },
            )
            .expect("append state diff")
            .append_classes(StarknetApiBlockNumber(0), &[(class_hash, &sierra)], &[])
            .expect("append classes")
            .append_casm(&class_hash, &casm)
            .expect("append casm")
            .commit()
            .expect("commit class fixtures");

        (class_hash, compiled_class_hash)
    }

    #[cfg(feature = "production-adapters")]
    fn deploy_account_transaction_for_class_hash(
        class_hash: StarknetApiClassHash,
    ) -> StarknetTransaction {
        let api_tx =
            StarknetApiDeployAccountTransaction::V1(StarknetApiDeployAccountTransactionV1 {
                class_hash,
                max_fee: StarknetApiFee(1_000_000_000_u128),
                ..Default::default()
            });
        let executable = StarknetApiExecutableDeployAccountTransaction::create(
            api_tx,
            &StarknetApiChainId::Mainnet,
        )
        .expect("create executable deploy-account tx");
        let tx_hash = format!("{:#x}", executable.tx_hash.0);
        StarknetTransaction::with_executable(
            tx_hash,
            StarknetApiExecutableTransaction::Account(
                StarknetApiExecutableAccountTransaction::DeployAccount(executable),
            ),
        )
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn apollo_class_provider_reads_class_hashes_from_execution_and_simulation_state() {
        let dir = tempdir().expect("temp dir");
        let (reader, mut writer) =
            open_apollo_storage(apollo_config(dir.path())).expect("open apollo storage");
        let (contract, class_hash, compiled_class_hash) =
            seed_header_and_state_with_class_hash(&mut writer);
        let adapter = ApolloStorageAdapter::from_parts(reader, writer);
        let provider = ApolloBlockifierClassProvider::new(adapter.reader_handle());

        provider
            .prepare_for_block_execution(0)
            .expect("prepare block zero execution");
        assert_eq!(
            provider
                .get_class_hash_at(contract)
                .expect("read class hash before genesis"),
            StarknetApiClassHash::default()
        );

        provider
            .prepare_for_block_execution(1)
            .expect("prepare block one execution");
        assert_eq!(
            provider
                .get_class_hash_at(contract)
                .expect("read class hash at block one"),
            class_hash
        );
        assert_eq!(
            provider
                .get_compiled_class_hash(class_hash)
                .expect("read compiled class hash"),
            compiled_class_hash
        );

        provider
            .prepare_for_simulation(1)
            .expect("prepare simulation at block one");
        assert_eq!(
            provider
                .get_class_hash_at(contract)
                .expect("read class hash at simulation state"),
            class_hash
        );
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn apollo_class_provider_fails_closed_when_runnable_class_is_unavailable() {
        let dir = tempdir().expect("temp dir");
        let (reader, mut writer) =
            open_apollo_storage(apollo_config(dir.path())).expect("open apollo storage");
        let (_, class_hash, _) = seed_header_and_state_with_class_hash(&mut writer);
        let adapter = ApolloStorageAdapter::from_parts(reader, writer);
        let provider = ApolloBlockifierClassProvider::new(adapter.reader_handle());
        provider
            .prepare_for_block_execution(1)
            .expect("prepare execution state");

        let err = provider
            .get_compiled_class(class_hash)
            .expect_err("must fail without declared class artifacts");
        assert!(matches!(err, StateError::UndeclaredClassHash(hash) if hash == class_hash));
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn apollo_class_provider_state_binding_is_thread_local() {
        let dir = tempdir().expect("temp dir");
        let (reader, mut writer) =
            open_apollo_storage(apollo_config(dir.path())).expect("open apollo storage");
        let (contract, class_hash, _) = seed_header_and_state_with_class_hash(&mut writer);
        let adapter = ApolloStorageAdapter::from_parts(reader, writer);
        let provider = Arc::new(ApolloBlockifierClassProvider::new(adapter.reader_handle()));
        let barrier = Arc::new(Barrier::new(2));

        let provider_a = Arc::clone(&provider);
        let barrier_a = Arc::clone(&barrier);
        let thread_a = std::thread::spawn(move || {
            provider_a
                .prepare_for_block_execution(0)
                .expect("prepare pre-genesis state in thread A");
            barrier_a.wait();
            provider_a
                .get_class_hash_at(contract)
                .expect("thread A class hash lookup")
        });

        let provider_b = Arc::clone(&provider);
        let barrier_b = Arc::clone(&barrier);
        let thread_b = std::thread::spawn(move || {
            provider_b
                .prepare_for_block_execution(1)
                .expect("prepare block one state in thread B");
            barrier_b.wait();
            provider_b
                .get_class_hash_at(contract)
                .expect("thread B class hash lookup")
        });

        let hash_a = thread_a.join().expect("thread A should join");
        let hash_b = thread_b.join().expect("thread B should join");
        assert_eq!(hash_a, StarknetApiClassHash::default());
        assert_eq!(hash_b, class_hash);
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn blockifier_backend_replays_account_deploy_with_declared_casm_fixtures() {
        let dir = tempdir().expect("temp dir");
        let (reader, mut writer) =
            open_apollo_storage(apollo_config(dir.path())).expect("open apollo storage");
        let (class_hash, _compiled_class_hash) = seed_declared_account_class_with_casm(&mut writer);
        let adapter = ApolloStorageAdapter::from_parts(reader, writer);
        let provider = Arc::new(ApolloBlockifierClassProvider::new(adapter.reader_handle()));

        provider
            .prepare_for_block_execution(1)
            .expect("prepare class provider for block one");
        provider
            .get_compiled_class(class_hash)
            .expect("declared class + casm fixtures must be runnable");

        let backend = BlockifierVmBackend::starknet_mainnet().with_class_provider(provider);
        let tx = deploy_account_transaction_for_class_hash(class_hash);
        let expected_hash = tx.hash.clone();
        let block = StarknetBlock {
            number: 1,
            parent_hash: "0x0".to_string(),
            state_root: "0x1".to_string(),
            timestamp: 1_700_000_001,
            sequencer_address: "0x1".into(),
            gas_prices: BlockGasPrices {
                l1_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
                l1_data_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
                l2_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
            },
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: vec![tx],
        };

        let mut state = InMemoryState::default();
        let output = backend
            .execute_block(&block, &mut state)
            .expect("account deploy replay should not fatally fail");
        assert_eq!(output.receipts.len(), 1);
        assert_eq!(output.receipts[0].tx_hash, expected_hash);
    }

    #[cfg(feature = "production-adapters")]
    fn seed_mainnet_genesis_header(writer: &mut ApolloStorageWriter) {
        let mut header = StarknetApiBlockHeader::default();
        header.block_header_without_hash.state_root = GlobalRoot(
            StarknetApiFelt::from_hex(STARKNET_MAINNET_GENESIS_STATE_ROOT)
                .expect("valid genesis root felt"),
        );
        header.block_header_without_hash.starknet_version =
            StarknetApiVersion::try_from("0.14.2").expect("valid protocol version");
        header.block_header_without_hash.timestamp = StarknetApiBlockTimestamp(1_700_000_000);
        writer
            .begin_rw_txn()
            .expect("begin rw tx")
            .append_header(StarknetApiBlockNumber(0), &header)
            .expect("append genesis header")
            .commit()
            .expect("commit genesis header");
    }

    #[cfg(feature = "production-adapters")]
    fn empty_test_block(number: u64) -> StarknetBlock {
        StarknetBlock {
            number,
            parent_hash: format!("0x{:x}", number.saturating_sub(1)),
            state_root: format!("0x{number:x}"),
            timestamp: 1_700_000_000 + number,
            sequencer_address: "0x1".into(),
            gas_prices: BlockGasPrices {
                l1_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
                l1_data_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
                l2_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
            },
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: Vec::new(),
        }
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn production_node_uses_dual_execution_backend_and_exposes_telemetry() {
        let dir = tempdir().expect("temp dir");
        let (reader, mut writer) =
            open_apollo_storage(apollo_config(dir.path())).expect("open apollo storage");
        seed_mainnet_genesis_header(&mut writer);
        let adapter = ApolloStorageAdapter::from_parts(reader, writer);
        let dual_config = ProductionDualExecutionConfig {
            enable_shadow_verification: true,
            verification_depth: 8,
            mismatch_policy: MismatchPolicy::WarnAndFallback,
        };
        let node = build_mainnet_production_node_with_dual_config(
            NodeConfig::default(),
            adapter,
            dual_config,
        )
        .expect("build production node");

        let initial = node
            .production_execution_telemetry()
            .expect("initial telemetry");
        assert_eq!(initial.metrics.mismatches, 0);
        assert_eq!(initial.metrics.fast_executions, 0);
        assert_eq!(initial.metrics.canonical_executions, 0);

        let mut state = InMemoryState::default();
        node.execution
            .execute_verified(&empty_test_block(1), &mut state)
            .expect("execute empty block");

        let after = node
            .production_execution_telemetry()
            .expect("execution telemetry");
        assert_eq!(after.metrics.mismatches, 0);
        assert_eq!(after.metrics.fast_executions, 1);
        assert_eq!(after.metrics.canonical_executions, 1);
    }

    #[cfg(feature = "production-adapters")]
    struct UnhealthyStorage(InMemoryStorage);

    #[cfg(feature = "production-adapters")]
    impl HealthCheck for UnhealthyStorage {
        fn is_healthy(&self) -> bool {
            false
        }

        fn detailed_status(&self) -> ComponentHealth {
            ComponentHealth {
                name: "unhealthy-storage".to_string(),
                status: HealthStatus::Unhealthy,
                last_block_processed: None,
                sync_lag: None,
                error: Some("forced unhealthy".to_string()),
            }
        }
    }

    #[cfg(feature = "production-adapters")]
    impl StorageBackend for UnhealthyStorage {
        fn get_state_reader(
            &self,
            block_number: u64,
        ) -> Result<Box<dyn StateReader>, StorageError> {
            self.0.get_state_reader(block_number)
        }

        fn apply_state_diff(&mut self, diff: &StarknetStateDiff) -> Result<(), StorageError> {
            self.0.apply_state_diff(diff)
        }

        fn insert_block(
            &mut self,
            block: StarknetBlock,
            state_diff: StarknetStateDiff,
        ) -> Result<(), StorageError> {
            self.0.insert_block(block, state_diff)
        }

        fn get_block(&self, id: BlockId) -> Result<Option<StarknetBlock>, StorageError> {
            self.0.get_block(id)
        }

        fn get_state_diff(
            &self,
            block_number: u64,
        ) -> Result<Option<StarknetStateDiff>, StorageError> {
            self.0.get_state_diff(block_number)
        }

        fn latest_block_number(&self) -> Result<u64, StorageError> {
            self.0.latest_block_number()
        }

        fn current_state_root(&self) -> Result<String, StorageError> {
            self.0.current_state_root()
        }
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn bootstrap_validation_rejects_unhealthy_storage() {
        let storage = UnhealthyStorage(InMemoryStorage::new(InMemoryState::default()));
        let err =
            validate_bootstrap_storage(&storage, &NodeConfig::default()).expect_err("must fail");
        assert!(matches!(err, NodeInitError::StorageUnhealthy));
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn bootstrap_validation_accepts_healthy_storage() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        validate_bootstrap_storage(&storage, &NodeConfig::default()).expect("healthy storage");
    }

    #[cfg(feature = "production-adapters")]
    struct InconsistentLatestStorage;

    #[cfg(feature = "production-adapters")]
    struct GenesisBlockStorage {
        inner: InMemoryStorage,
        block0: StarknetBlock,
    }

    #[cfg(feature = "production-adapters")]
    fn block_zero_with_state_root(state_root: &str) -> StarknetBlock {
        StarknetBlock {
            number: 0,
            parent_hash: "0x0".to_string(),
            state_root: state_root.to_string(),
            timestamp: 0,
            sequencer_address: "0x1".into(),
            gas_prices: BlockGasPrices {
                l1_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
                l1_data_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
                l2_gas: GasPricePerToken {
                    price_in_fri: 1,
                    price_in_wei: 1,
                },
            },
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: Vec::new(),
        }
    }

    #[cfg(feature = "production-adapters")]
    impl HealthCheck for GenesisBlockStorage {
        fn is_healthy(&self) -> bool {
            self.inner.is_healthy()
        }

        fn detailed_status(&self) -> ComponentHealth {
            self.inner.detailed_status()
        }
    }

    #[cfg(feature = "production-adapters")]
    impl StorageBackend for GenesisBlockStorage {
        fn get_state_reader(
            &self,
            block_number: u64,
        ) -> Result<Box<dyn StateReader>, StorageError> {
            self.inner.get_state_reader(block_number)
        }

        fn apply_state_diff(&mut self, diff: &StarknetStateDiff) -> Result<(), StorageError> {
            self.inner.apply_state_diff(diff)
        }

        fn insert_block(
            &mut self,
            block: StarknetBlock,
            state_diff: StarknetStateDiff,
        ) -> Result<(), StorageError> {
            self.inner.insert_block(block, state_diff)
        }

        fn get_block(&self, id: BlockId) -> Result<Option<StarknetBlock>, StorageError> {
            if matches!(id, BlockId::Number(0)) {
                return Ok(Some(self.block0.clone()));
            }
            self.inner.get_block(id)
        }

        fn get_state_diff(
            &self,
            block_number: u64,
        ) -> Result<Option<StarknetStateDiff>, StorageError> {
            self.inner.get_state_diff(block_number)
        }

        fn latest_block_number(&self) -> Result<u64, StorageError> {
            self.inner.latest_block_number()
        }

        fn current_state_root(&self) -> Result<String, StorageError> {
            self.inner.current_state_root()
        }
    }

    #[cfg(feature = "production-adapters")]
    impl HealthCheck for InconsistentLatestStorage {
        fn is_healthy(&self) -> bool {
            true
        }

        fn detailed_status(&self) -> ComponentHealth {
            ComponentHealth {
                name: "inconsistent-latest-storage".to_string(),
                status: HealthStatus::Healthy,
                last_block_processed: Some(1),
                sync_lag: None,
                error: None,
            }
        }
    }

    #[cfg(feature = "production-adapters")]
    impl StorageBackend for InconsistentLatestStorage {
        fn get_state_reader(
            &self,
            block_number: u64,
        ) -> Result<Box<dyn StateReader>, StorageError> {
            if block_number == 0 {
                Ok(Box::new(InMemoryState::default()))
            } else {
                Err(StorageError::BlockOutOfRange {
                    requested: block_number,
                    latest: 0,
                })
            }
        }

        fn apply_state_diff(&mut self, _diff: &StarknetStateDiff) -> Result<(), StorageError> {
            Ok(())
        }

        fn insert_block(
            &mut self,
            _block: StarknetBlock,
            _state_diff: StarknetStateDiff,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        fn get_block(&self, _id: BlockId) -> Result<Option<StarknetBlock>, StorageError> {
            Ok(None)
        }

        fn get_state_diff(
            &self,
            _block_number: u64,
        ) -> Result<Option<StarknetStateDiff>, StorageError> {
            Ok(None)
        }

        fn latest_block_number(&self) -> Result<u64, StorageError> {
            Ok(1)
        }

        fn current_state_root(&self) -> Result<String, StorageError> {
            Ok("0x0".to_string())
        }
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn bootstrap_validation_rejects_missing_tip_block_body() {
        let storage = InconsistentLatestStorage;
        let err =
            validate_bootstrap_storage(&storage, &NodeConfig::default()).expect_err("must fail");
        assert!(matches!(err, NodeInitError::StorageIntegrity(_)));
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn bootstrap_validation_rejects_mainnet_genesis_root_mismatch_when_block_zero_exists() {
        let storage = GenesisBlockStorage {
            inner: InMemoryStorage::new(InMemoryState::default()),
            block0: block_zero_with_state_root("0xdead"),
        };

        let err =
            validate_bootstrap_storage(&storage, &NodeConfig::default()).expect_err("must fail");
        assert!(matches!(err, NodeInitError::StorageIntegrity(_)));
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn bootstrap_validation_accepts_mainnet_genesis_root_match_when_block_zero_exists() {
        let storage = GenesisBlockStorage {
            inner: InMemoryStorage::new(InMemoryState::default()),
            block0: block_zero_with_state_root(STARKNET_MAINNET_GENESIS_STATE_ROOT),
        };

        validate_bootstrap_storage(&storage, &NodeConfig::default())
            .expect("matching mainnet genesis root");
    }
}
