#![forbid(unsafe_code)]

#[cfg(feature = "production-adapters")]
use starknet_node_execution::BlockifierVmBackend;
use starknet_node_execution::ExecutionBackend;
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
}

pub struct StarknetNodeBuilder<Stor, Exec> {
    config: NodeConfig,
    rpc_enabled: bool,
    mcp_enabled: bool,
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

    pub fn build(self) -> StarknetNode<S, E> {
        StarknetNode {
            config: self.config,
            storage: self.storage.0,
            execution: self.execution.0,
            rpc_enabled: self.rpc_enabled,
            mcp_enabled: self.mcp_enabled,
        }
    }
}

#[cfg(feature = "production-adapters")]
pub fn build_mainnet_production_node(
    config: NodeConfig,
    storage: ApolloStorageAdapter,
) -> Result<StarknetNode<ApolloStorageAdapter, BlockifierVmBackend>, NodeInitError> {
    validate_bootstrap_storage(&storage, &config)?;
    Ok(StarknetNodeBuilder::new(config)
        .with_storage(storage)
        .with_execution(BlockifierVmBackend::starknet_mainnet())
        .build())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[cfg(feature = "production-adapters")]
    use semver::Version;
    use starknet_node_execution::ExecutionError;
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
                    tx_hash: format!("0x{:x}", block.number),
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

        fn current_state_root(&self) -> String {
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
            sequencer_address: "0x1".to_string(),
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

        fn current_state_root(&self) -> String {
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

        fn current_state_root(&self) -> String {
            "0x0".to_string()
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
