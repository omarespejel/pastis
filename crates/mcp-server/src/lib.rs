#![forbid(unsafe_code)]

use std::sync::Mutex;

use node_spec_core::mcp::{
    AccessError, McpAccessController, McpTool, ValidationError, ValidationLimits, validate_tool,
};
use starknet_node_storage::{StateRootSemantics, StorageBackend, StorageError};
use starknet_node_types::{BlockNumber, ComponentHealth};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRequest {
    pub api_key: String,
    pub tool: McpTool,
    pub now_unix_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryStateResponse {
    pub latest_block_number: BlockNumber,
    pub state_root: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatusResponse {
    pub storage: ComponentHealth,
    pub latest_block_number: BlockNumber,
    pub state_root_semantics: StateRootSemantics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpResponse {
    QueryState(QueryStateResponse),
    GetNodeStatus(NodeStatusResponse),
    BatchQuery { responses: Vec<McpResponse> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpResponseEnvelope {
    pub agent_id: String,
    pub response: McpResponse,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum McpServerError {
    #[error(transparent)]
    Access(#[from] AccessError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("mcp access controller lock poisoned")]
    AccessControllerPoisoned,
}

pub struct McpServer<'a> {
    storage: &'a dyn StorageBackend,
    access: Mutex<McpAccessController>,
    validation_limits: ValidationLimits,
}

impl<'a> McpServer<'a> {
    pub fn new(
        storage: &'a dyn StorageBackend,
        access_controller: McpAccessController,
        validation_limits: ValidationLimits,
    ) -> Self {
        Self {
            storage,
            access: Mutex::new(access_controller),
            validation_limits,
        }
    }

    pub fn handle_request(
        &self,
        request: McpRequest,
    ) -> Result<McpResponseEnvelope, McpServerError> {
        validate_tool(&request.tool, self.validation_limits)?;
        let agent_id = self
            .access
            .lock()
            .map_err(|_| McpServerError::AccessControllerPoisoned)?
            .authorize(&request.api_key, &request.tool, request.now_unix_seconds)?;
        let response = self.execute_tool(&request.tool)?;
        Ok(McpResponseEnvelope { agent_id, response })
    }

    fn execute_tool(&self, tool: &McpTool) -> Result<McpResponse, McpServerError> {
        match tool {
            McpTool::QueryState => {
                let latest_block_number = self.storage.latest_block_number()?;
                let state_root = self.storage.current_state_root()?;
                Ok(McpResponse::QueryState(QueryStateResponse {
                    latest_block_number,
                    state_root,
                }))
            }
            McpTool::GetNodeStatus => {
                let latest_block_number = self.storage.latest_block_number()?;
                let status = self.storage.detailed_status();
                Ok(McpResponse::GetNodeStatus(NodeStatusResponse {
                    storage: status,
                    latest_block_number,
                    state_root_semantics: self.storage.state_root_semantics(),
                }))
            }
            McpTool::BatchQuery { queries } => {
                let mut responses = Vec::with_capacity(queries.len());
                for query in queries {
                    responses.push(self.execute_tool(query)?);
                }
                Ok(McpResponse::BatchQuery { responses })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use node_spec_core::mcp::{AgentPolicy, ToolPermission};
    use semver::Version;
    use starknet_node_storage::InMemoryStorage;
    use starknet_node_types::{
        BlockGasPrices, ContractAddress, GasPricePerToken, InMemoryState, StarknetBlock,
        StarknetStateDiff,
    };

    use super::*;

    fn sample_limits() -> ValidationLimits {
        ValidationLimits {
            max_batch_size: 16,
            max_depth: 4,
            max_total_tools: 64,
        }
    }

    fn read_policy(perms: BTreeSet<ToolPermission>, rpm: u32) -> AgentPolicy {
        AgentPolicy::new("api-key", perms, rpm)
    }

    fn sample_block(number: u64) -> StarknetBlock {
        StarknetBlock {
            number,
            parent_hash: format!("0x{:x}", number.saturating_sub(1)),
            state_root: format!("0x{:x}", number),
            timestamp: 1_700_000_000 + number,
            sequencer_address: ContractAddress::from("0x1"),
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
            protocol_version: Version::parse("0.14.2").expect("valid"),
            transactions: Vec::new(),
        }
    }

    fn server_with_storage<'a>(
        storage: &'a dyn StorageBackend,
        policies: impl IntoIterator<Item = (String, AgentPolicy)>,
    ) -> McpServer<'a> {
        McpServer::new(storage, McpAccessController::new(policies), sample_limits())
    }

    #[test]
    fn query_state_returns_latest_block_and_root() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(sample_block(1), StarknetStateDiff::default())
            .expect("insert block");
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
            )],
        );
        let response = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_000,
            })
            .expect("query state succeeds");
        assert_eq!(response.agent_id, "agent-a");
        assert_eq!(
            response.response,
            McpResponse::QueryState(QueryStateResponse {
                latest_block_number: 1,
                state_root: "0x0".to_string(),
            })
        );
    }

    #[test]
    fn get_node_status_requires_permission() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
            )],
        );
        let err = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::GetNodeStatus,
                now_unix_seconds: 1_000,
            })
            .expect_err("must deny missing permission");
        assert_eq!(
            err,
            McpServerError::Access(AccessError::PermissionDenied {
                agent_id: "agent-a".to_string(),
                permission: ToolPermission::GetNodeStatus,
            })
        );
    }

    #[test]
    fn batch_query_executes_in_declared_order() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(
                    BTreeSet::from([ToolPermission::QueryState, ToolPermission::GetNodeStatus]),
                    5,
                ),
            )],
        );
        let response = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::BatchQuery {
                    queries: vec![McpTool::QueryState, McpTool::GetNodeStatus],
                },
                now_unix_seconds: 1_000,
            })
            .expect("batch query succeeds");
        match response.response {
            McpResponse::BatchQuery { responses } => {
                assert_eq!(responses.len(), 2);
                assert!(matches!(responses[0], McpResponse::QueryState(_)));
                assert!(matches!(responses[1], McpResponse::GetNodeStatus(_)));
            }
            other => panic!("expected batch response, got {other:?}"),
        }
    }

    #[test]
    fn batch_query_respects_depth_limits() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let mut server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
            )],
        );
        server.validation_limits = ValidationLimits {
            max_batch_size: 10,
            max_depth: 1,
            max_total_tools: 10,
        };
        let err = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::BatchQuery {
                    queries: vec![McpTool::BatchQuery {
                        queries: vec![McpTool::QueryState],
                    }],
                },
                now_unix_seconds: 1_000,
            })
            .expect_err("must fail");
        assert_eq!(
            err,
            McpServerError::Validation(ValidationError::BatchDepthExceeded {
                depth: 2,
                max_depth: 1,
            })
        );
    }

    #[test]
    fn rejects_non_monotonic_request_times() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
            )],
        );
        server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_000,
            })
            .expect("first request");
        let err = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 999,
            })
            .expect_err("must reject time reversal");
        assert_eq!(
            err,
            McpServerError::Access(AccessError::NonMonotonicTime {
                agent_id: "agent-a".to_string(),
            })
        );
    }

    #[test]
    fn enforces_rate_limit() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::QueryState]), 1),
            )],
        );
        server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_000,
            })
            .expect("first request");
        let err = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_001,
            })
            .expect_err("must rate limit");
        assert_eq!(
            err,
            McpServerError::Access(AccessError::RateLimited {
                agent_id: "agent-a".to_string(),
                limit_per_minute: 1,
            })
        );
    }

    #[test]
    fn rejects_invalid_api_key() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
            )],
        );
        let err = server
            .handle_request(McpRequest {
                api_key: "wrong".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_000,
            })
            .expect_err("must reject");
        assert_eq!(err, McpServerError::Access(AccessError::InvalidApiKey));
    }

    #[test]
    fn reports_poisoned_access_controller_lock() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
            )],
        );
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = server.access.lock().expect("lock");
            panic!("poison lock");
        }));
        let err = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_000,
            })
            .expect_err("must fail on poisoned access lock");
        assert_eq!(err, McpServerError::AccessControllerPoisoned);
    }

    #[test]
    fn rejects_ambiguous_api_key_identity() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [
                (
                    "agent-a".to_string(),
                    read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
                ),
                (
                    "agent-b".to_string(),
                    read_policy(BTreeSet::from([ToolPermission::QueryState]), 5),
                ),
            ],
        );
        let err = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::QueryState,
                now_unix_seconds: 1_000,
            })
            .expect_err("must reject ambiguous identity");
        assert_eq!(
            err,
            McpServerError::Access(AccessError::AmbiguousApiKey {
                matching_agents: vec!["agent-a".to_string(), "agent-b".to_string()],
            })
        );
    }
}
