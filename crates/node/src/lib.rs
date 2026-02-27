#[cfg(feature = "production-adapters")]
use starknet_node_execution::BlockifierVmBackend;
use starknet_node_execution::ExecutionBackend;
#[cfg(feature = "production-adapters")]
use starknet_node_storage::PapyrusStorageAdapter;
use starknet_node_storage::StorageBackend;
#[cfg(feature = "production-adapters")]
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub chain_id: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            chain_id: "SN_MAIN".to_string(),
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
}

#[cfg(feature = "production-adapters")]
fn validate_bootstrap_storage<S: StorageBackend>(storage: &S) -> Result<(), NodeInitError> {
    storage
        .latest_block_number()
        .map_err(|error| NodeInitError::StorageCheck(error.to_string()))?;
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
    storage: PapyrusStorageAdapter,
) -> Result<StarknetNode<PapyrusStorageAdapter, BlockifierVmBackend>, NodeInitError> {
    validate_bootstrap_storage(&storage)?;
    Ok(StarknetNodeBuilder::new(config)
        .with_storage(storage)
        .with_execution(BlockifierVmBackend::starknet_mainnet())
        .build())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use starknet_node_execution::ExecutionError;
    use starknet_node_storage::InMemoryStorage;
    #[cfg(feature = "production-adapters")]
    use starknet_node_storage::StorageError;
    use starknet_node_types::{
        BlockContext, BuiltinStats, ExecutionOutput, InMemoryState, MutableState, SimulationResult,
        StarknetBlock, StarknetReceipt, StarknetStateDiff, StarknetTransaction, StateReader,
    };
    #[cfg(feature = "production-adapters")]
    use starknet_node_types::{BlockId, ComponentHealth, HealthCheck, HealthStatus};

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
        assert_eq!(node.config.chain_id, "SN_MAIN");
    }

    #[test]
    fn builder_requires_ordered_setup() {
        // This test validates runtime state after the type-state transitions complete.
        let storage = InMemoryStorage::new(InMemoryState::default());
        let node = StarknetNodeBuilder::new(NodeConfig {
            chain_id: "SN_SEPOLIA".to_string(),
        })
        .with_storage(storage)
        .with_execution(DummyExecution)
        .build();

        assert_eq!(node.config.chain_id, "SN_SEPOLIA");
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
        let err = validate_bootstrap_storage(&storage).expect_err("must fail");
        assert!(matches!(err, NodeInitError::StorageUnhealthy));
    }

    #[cfg(feature = "production-adapters")]
    #[test]
    fn bootstrap_validation_accepts_healthy_storage() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        validate_bootstrap_storage(&storage).expect("healthy storage");
    }
}
