#[cfg(feature = "production-adapters")]
use starknet_node_execution::BlockifierVmBackend;
use starknet_node_execution::ExecutionBackend;
#[cfg(feature = "production-adapters")]
use starknet_node_storage::PapyrusStorageAdapter;
use starknet_node_storage::StorageBackend;

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
) -> StarknetNode<PapyrusStorageAdapter, BlockifierVmBackend> {
    StarknetNodeBuilder::new(config)
        .with_storage(storage)
        .with_execution(BlockifierVmBackend::starknet_mainnet())
        .build()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use starknet_node_execution::ExecutionError;
    use starknet_node_storage::InMemoryStorage;
    use starknet_node_types::{
        BlockContext, BuiltinStats, ExecutionOutput, InMemoryState, MutableState, SimulationResult,
        StarknetBlock, StarknetReceipt, StarknetStateDiff, StarknetTransaction, StateReader,
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
}
