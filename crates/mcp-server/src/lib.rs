#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::Mutex;

use node_spec_core::mcp::{
    AccessError, McpAccessController, McpTool, ValidationError, ValidationLimits, validate_tool,
};
pub use starknet_node_exex_btcfi::BtcfiAnomaly;
use starknet_node_exex_btcfi::BtcfiExEx;
use starknet_node_storage::{StateRootSemantics, StorageBackend, StorageError};
use starknet_node_types::{BlockNumber, ComponentHealth, ContractAddress, StarknetFelt};

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
pub struct QueryStorageResponse {
    pub block_number: BlockNumber,
    pub contract_address: ContractAddress,
    pub storage_values: BTreeMap<String, Option<StarknetFelt>>,
    pub nonce: Option<StarknetFelt>,
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
    QueryStorage(QueryStorageResponse),
    GetNodeStatus(NodeStatusResponse),
    GetAnomalies { anomalies: Vec<BtcfiAnomaly> },
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
    #[error("state read failed: {0}")]
    StateRead(String),
    #[error("anomaly source is unavailable for this node")]
    AnomalySourceUnavailable,
    #[error("anomaly source failed: {0}")]
    AnomalySource(String),
    #[error("mcp access controller lock poisoned")]
    AccessControllerPoisoned,
}

pub trait AnomalySource: Send + Sync {
    fn recent_anomalies(&self, limit: usize) -> Result<Vec<BtcfiAnomaly>, String>;
}

impl AnomalySource for Mutex<BtcfiExEx> {
    fn recent_anomalies(&self, limit: usize) -> Result<Vec<BtcfiAnomaly>, String> {
        let guard = self
            .lock()
            .map_err(|_| "btcfi anomaly source lock poisoned".to_string())?;
        Ok(guard.recent_anomalies(limit))
    }
}

pub struct McpServer<'a> {
    storage: &'a dyn StorageBackend,
    anomaly_source: Option<&'a dyn AnomalySource>,
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
            anomaly_source: None,
            access: Mutex::new(access_controller),
            validation_limits,
        }
    }

    pub fn with_anomaly_source(mut self, anomaly_source: &'a dyn AnomalySource) -> Self {
        self.anomaly_source = Some(anomaly_source);
        self
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
            McpTool::QueryStorage {
                contract_address,
                storage_keys,
                block_number,
                include_nonce,
            } => {
                let contract = ContractAddress::parse(contract_address.clone()).map_err(|_| {
                    McpServerError::Validation(ValidationError::InvalidHexIdentifier {
                        field: "contract_address",
                        value: contract_address.clone(),
                    })
                })?;
                let block_number = match block_number {
                    Some(number) => *number,
                    None => self.storage.latest_block_number()?,
                };
                let reader = self.storage.get_state_reader(block_number)?;
                let mut storage_values = BTreeMap::new();
                for key in storage_keys {
                    let value = reader
                        .get_storage(&contract, key)
                        .map_err(|error| McpServerError::StateRead(error.to_string()))?;
                    storage_values.insert(key.clone(), value);
                }
                let nonce = if *include_nonce {
                    reader
                        .nonce_of(&contract)
                        .map_err(|error| McpServerError::StateRead(error.to_string()))?
                } else {
                    None
                };
                Ok(McpResponse::QueryStorage(QueryStorageResponse {
                    block_number,
                    contract_address: contract,
                    storage_values,
                    nonce,
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
            McpTool::GetAnomalies { limit } => {
                let source = self
                    .anomaly_source
                    .ok_or(McpServerError::AnomalySourceUnavailable)?;
                let limit = (*limit).min(10_000) as usize;
                let anomalies = source
                    .recent_anomalies(limit)
                    .map_err(McpServerError::AnomalySource)?;
                Ok(McpResponse::GetAnomalies { anomalies })
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
    use std::sync::Mutex;

    use node_spec_core::mcp::{AgentPolicy, ToolPermission};
    use semver::Version;
    use starknet_node_exex_btcfi::{BtcfiExEx, StandardWrapperMonitor, StrkBtcMonitor};
    use starknet_node_storage::InMemoryStorage;
    use starknet_node_types::{
        BlockGasPrices, ContractAddress, GasPricePerToken, InMemoryState, StarknetBlock,
        StarknetFelt, StarknetStateDiff,
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
        AgentPolicy::new("api-key", perms, rpm).expect("test policy should build")
    }

    fn sample_block(number: u64) -> StarknetBlock {
        StarknetBlock {
            number,
            parent_hash: format!("0x{:x}", number.saturating_sub(1)),
            state_root: format!("0x{:x}", number),
            timestamp: 1_700_000_000 + number,
            sequencer_address: ContractAddress::parse("0x1").expect("valid contract address"),
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

    fn sample_state_diff_with_single_slot(
        contract: &str,
        key: &str,
        value: &str,
        nonce: &str,
    ) -> StarknetStateDiff {
        let contract = ContractAddress::parse(contract).expect("valid contract");
        let mut diff = StarknetStateDiff::default();
        diff.storage_diffs
            .entry(contract.clone())
            .or_default()
            .insert(
                key.to_string(),
                StarknetFelt::from_hex(value).expect("valid felt"),
            );
        diff.nonces.insert(
            contract,
            StarknetFelt::from_hex(nonce).expect("valid nonce felt"),
        );
        diff
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
    fn query_storage_returns_values_and_nonce_for_requested_block() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(
                sample_block(1),
                sample_state_diff_with_single_slot("0xabc", "0x10", "0x55", "0x1"),
            )
            .expect("insert block 1");
        storage
            .insert_block(
                sample_block(2),
                sample_state_diff_with_single_slot("0xabc", "0x10", "0x66", "0x2"),
            )
            .expect("insert block 2");

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
                tool: McpTool::QueryStorage {
                    contract_address: "0xabc".to_string(),
                    storage_keys: vec!["0x10".to_string(), "0x20".to_string()],
                    block_number: Some(2),
                    include_nonce: true,
                },
                now_unix_seconds: 1_000,
            })
            .expect("query storage succeeds");

        assert_eq!(response.agent_id, "agent-a");
        match response.response {
            McpResponse::QueryStorage(storage) => {
                assert_eq!(storage.block_number, 2);
                assert_eq!(
                    storage.contract_address,
                    ContractAddress::parse("0xabc").expect("valid contract")
                );
                assert_eq!(
                    storage.storage_values.get("0x10"),
                    Some(&Some(StarknetFelt::from_hex("0x66").expect("valid felt")))
                );
                assert_eq!(storage.storage_values.get("0x20"), Some(&None));
                assert_eq!(
                    storage.nonce,
                    Some(StarknetFelt::from_hex("0x2").expect("valid nonce"))
                );
            }
            other => panic!("expected QueryStorage response, got {other:?}"),
        }
    }

    #[test]
    fn query_storage_defaults_to_latest_block_when_block_number_is_omitted() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(
                sample_block(1),
                sample_state_diff_with_single_slot("0xabc", "0x10", "0x55", "0x1"),
            )
            .expect("insert block 1");
        storage
            .insert_block(
                sample_block(2),
                sample_state_diff_with_single_slot("0xabc", "0x10", "0x66", "0x2"),
            )
            .expect("insert block 2");

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
                tool: McpTool::QueryStorage {
                    contract_address: "0xabc".to_string(),
                    storage_keys: vec!["0x10".to_string()],
                    block_number: None,
                    include_nonce: false,
                },
                now_unix_seconds: 1_000,
            })
            .expect("query storage succeeds");

        match response.response {
            McpResponse::QueryStorage(storage) => {
                assert_eq!(storage.block_number, 2);
                assert_eq!(
                    storage.storage_values.get("0x10"),
                    Some(&Some(StarknetFelt::from_hex("0x66").expect("valid felt")))
                );
                assert_eq!(storage.nonce, None);
            }
            other => panic!("expected QueryStorage response, got {other:?}"),
        }
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
        let btcfi = Mutex::new(BtcfiExEx::new(
            Vec::<StandardWrapperMonitor>::new(),
            StrkBtcMonitor::new(starknet_node_exex_btcfi::StrkBtcMonitorConfig {
                shielded_pool_contract: ContractAddress::parse("0x222")
                    .expect("valid contract address"),
                merkle_root_key: "0x10".to_string(),
                commitment_count_key: "0x11".to_string(),
                nullifier_count_key: "0x12".to_string(),
                nullifier_key_prefix: "0xdead".to_string(),
                commitment_flood_threshold: 8,
                unshield_cluster_threshold: 4,
                unshield_cluster_window_blocks: 3,
                light_client_max_lag_blocks: 6,
                bridge_timeout_blocks: 20,
                max_tracked_nullifiers: 100,
            }),
            8,
        ));
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(
                    BTreeSet::from([
                        ToolPermission::QueryState,
                        ToolPermission::GetNodeStatus,
                        ToolPermission::GetAnomalies,
                    ]),
                    5,
                ),
            )],
        )
        .with_anomaly_source(&btcfi);
        let response = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::BatchQuery {
                    queries: vec![
                        McpTool::QueryState,
                        McpTool::GetNodeStatus,
                        McpTool::GetAnomalies { limit: 10 },
                    ],
                },
                now_unix_seconds: 1_000,
            })
            .expect("batch query succeeds");
        match response.response {
            McpResponse::BatchQuery { responses } => {
                assert_eq!(responses.len(), 3);
                assert!(matches!(responses[0], McpResponse::QueryState(_)));
                assert!(matches!(responses[1], McpResponse::GetNodeStatus(_)));
                assert!(matches!(responses[2], McpResponse::GetAnomalies { .. }));
            }
            other => panic!("expected batch response, got {other:?}"),
        }
    }

    #[test]
    fn get_anomalies_requires_permission() {
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
                tool: McpTool::GetAnomalies { limit: 10 },
                now_unix_seconds: 1_000,
            })
            .expect_err("must deny missing permission");
        assert_eq!(
            err,
            McpServerError::Access(AccessError::PermissionDenied {
                agent_id: "agent-a".to_string(),
                permission: ToolPermission::GetAnomalies,
            })
        );
    }

    #[test]
    fn get_anomalies_fails_when_source_is_missing() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let server = server_with_storage(
            &storage,
            [(
                "agent-a".to_string(),
                read_policy(BTreeSet::from([ToolPermission::GetAnomalies]), 5),
            )],
        );
        let err = server
            .handle_request(McpRequest {
                api_key: "api-key".to_string(),
                tool: McpTool::GetAnomalies { limit: 10 },
                now_unix_seconds: 1_000,
            })
            .expect_err("missing anomaly source should fail");
        assert_eq!(err, McpServerError::AnomalySourceUnavailable);
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
