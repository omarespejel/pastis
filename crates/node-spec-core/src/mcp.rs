use std::collections::{BTreeMap, BTreeSet, VecDeque};

use argon2::{Algorithm, Argon2, Params, Version};
use pbkdf2::pbkdf2_hmac_array;
use rand::{RngCore, rngs::OsRng};
use sha2::Sha256;

const API_KEY_ARGON2_MEMORY_KIB: u32 = 19 * 1024;
const API_KEY_ARGON2_ITERATIONS: u32 = 2;
const API_KEY_ARGON2_PARALLELISM: u32 = 1;
const API_KEY_LEGACY_PBKDF2_ITERATIONS: u32 = 150_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyKdf {
    Argon2id,
    LegacyPbkdf2Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpTool {
    QueryState,
    GetNodeStatus,
    GetAnomalies { limit: u64 },
    BatchQuery { queries: Vec<McpTool> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ToolPermission {
    QueryState,
    GetNodeStatus,
    GetAnomalies,
}

impl McpTool {
    fn collect_required_permissions(&self, permissions: &mut BTreeSet<ToolPermission>) {
        match self {
            McpTool::QueryState => {
                permissions.insert(ToolPermission::QueryState);
            }
            McpTool::GetNodeStatus => {
                permissions.insert(ToolPermission::GetNodeStatus);
            }
            McpTool::GetAnomalies { .. } => {
                permissions.insert(ToolPermission::GetAnomalies);
            }
            McpTool::BatchQuery { queries } => {
                for query in queries {
                    query.collect_required_permissions(permissions);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationLimits {
    pub max_batch_size: usize,
    pub max_depth: usize,
    pub max_total_tools: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("batch query depth {depth} exceeds max depth {max_depth}")]
    BatchDepthExceeded { depth: usize, max_depth: usize },
    #[error("batch query size {size} exceeds max size {max_batch_size}")]
    BatchSizeExceeded { size: usize, max_batch_size: usize },
    #[error("batch query total tool count {count} exceeds max total {max_total_tools}")]
    BatchTotalToolsExceeded {
        count: usize,
        max_total_tools: usize,
    },
    #[error("anomaly query limit {limit} exceeds max {max_limit}")]
    AnomalyLimitExceeded { limit: u64, max_limit: u64 },
}

const MAX_GET_ANOMALIES_LIMIT: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPolicy {
    pub api_key_salt: [u8; 16],
    pub api_key_hash: [u8; 32],
    pub api_key_kdf: ApiKeyKdf,
    pub permissions: BTreeSet<ToolPermission>,
    pub max_requests_per_minute: u32,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AgentPolicyBuildError {
    #[error("argon2id api-key derivation failed: {0}")]
    ApiKeyDerivation(String),
}

impl AgentPolicy {
    pub fn new(
        api_key: impl AsRef<str>,
        permissions: BTreeSet<ToolPermission>,
        max_requests_per_minute: u32,
    ) -> Self {
        Self::try_new(api_key, permissions, max_requests_per_minute)
            .expect("agent policy construction must derive API key with argon2id")
    }

    pub fn try_new(
        api_key: impl AsRef<str>,
        permissions: BTreeSet<ToolPermission>,
        max_requests_per_minute: u32,
    ) -> Result<Self, AgentPolicyBuildError> {
        let mut api_key_salt = [0_u8; 16];
        OsRng.fill_bytes(&mut api_key_salt);
        let api_key_hash = hash_api_key_argon2id(api_key.as_ref(), &api_key_salt)
            .map_err(|error| AgentPolicyBuildError::ApiKeyDerivation(error.to_string()))?;
        Ok(Self {
            api_key_salt,
            api_key_hash,
            api_key_kdf: ApiKeyKdf::Argon2id,
            permissions,
            max_requests_per_minute,
        })
    }

    fn verify_api_key(&self, api_key: &str) -> bool {
        match self.api_key_kdf {
            ApiKeyKdf::Argon2id => hash_api_key_argon2id(api_key, &self.api_key_salt)
                .map(|hash| constant_time_eq_32(&hash, &self.api_key_hash))
                .unwrap_or(false),
            ApiKeyKdf::LegacyPbkdf2Sha256 => constant_time_eq_32(
                &self.api_key_hash,
                &hash_api_key_legacy_pbkdf2(api_key, &self.api_key_salt),
            ),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AccessError {
    #[error("invalid api key")]
    InvalidApiKey,
    #[error("api key maps to multiple agents: {matching_agents:?}")]
    AmbiguousApiKey { matching_agents: Vec<String> },
    #[error("agent '{agent_id}' lacks permission '{permission:?}'")]
    PermissionDenied {
        agent_id: String,
        permission: ToolPermission,
    },
    #[error("agent '{agent_id}' exceeded rate limit: {limit_per_minute}/minute")]
    RateLimited {
        agent_id: String,
        limit_per_minute: u32,
    },
    #[error("non-monotonic request time for agent '{agent_id}'")]
    NonMonotonicTime { agent_id: String },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PolicyValidationError {
    #[error("duplicate api key salt detected between '{first_agent}' and '{second_agent}'")]
    DuplicateApiKeySalt {
        first_agent: String,
        second_agent: String,
    },
}

#[derive(Debug, Default)]
pub struct McpAccessController {
    policies: BTreeMap<String, AgentPolicy>,
    requests: BTreeMap<String, VecDeque<u64>>,
    latest_request_time: BTreeMap<String, u64>,
}

impl McpAccessController {
    pub fn new(policies: impl IntoIterator<Item = (String, AgentPolicy)>) -> Self {
        let mut salts: BTreeMap<[u8; 16], String> = BTreeMap::new();
        let mut policy_map = BTreeMap::new();
        for (agent_id, policy) in policies {
            if salts.contains_key(&policy.api_key_salt) {
                // Keep the first policy bound to a salt and drop duplicates to avoid
                // ambiguous API-key resolution or constructor panics.
                continue;
            }
            salts.insert(policy.api_key_salt, agent_id.clone());
            policy_map.insert(agent_id, policy);
        }
        Self {
            policies: policy_map,
            requests: BTreeMap::new(),
            latest_request_time: BTreeMap::new(),
        }
    }

    pub fn try_new(
        policies: impl IntoIterator<Item = (String, AgentPolicy)>,
    ) -> Result<Self, PolicyValidationError> {
        let mut salts: BTreeMap<[u8; 16], String> = BTreeMap::new();
        let mut policy_map = BTreeMap::new();
        for (agent_id, policy) in policies {
            if let Some(first_agent) = salts.get(&policy.api_key_salt) {
                return Err(PolicyValidationError::DuplicateApiKeySalt {
                    first_agent: first_agent.clone(),
                    second_agent: agent_id,
                });
            }
            salts.insert(policy.api_key_salt, agent_id.clone());
            policy_map.insert(agent_id, policy);
        }
        Ok(Self {
            policies: policy_map,
            requests: BTreeMap::new(),
            latest_request_time: BTreeMap::new(),
        })
    }

    fn resolve_agent_id_for_api_key(&self, api_key: &str) -> Result<String, AccessError> {
        let mut matching_agents: Vec<String> = self
            .policies
            .iter()
            .filter_map(|(agent_id, policy)| {
                if policy.verify_api_key(api_key) {
                    Some(agent_id.clone())
                } else {
                    None
                }
            })
            .collect();
        matching_agents.sort();
        match matching_agents.len() {
            0 => Err(AccessError::InvalidApiKey),
            1 => Ok(matching_agents.remove(0)),
            _ => Err(AccessError::AmbiguousApiKey { matching_agents }),
        }
    }

    pub fn authorize(
        &mut self,
        api_key: &str,
        tool: &McpTool,
        now_unix_seconds: u64,
    ) -> Result<String, AccessError> {
        let agent_id = self.resolve_agent_id_for_api_key(api_key)?;
        let policy = self
            .policies
            .get(&agent_id)
            .ok_or(AccessError::InvalidApiKey)?;

        let mut required = BTreeSet::new();
        tool.collect_required_permissions(&mut required);
        for permission in required {
            if !policy.permissions.contains(&permission) {
                return Err(AccessError::PermissionDenied {
                    agent_id: agent_id.clone(),
                    permission,
                });
            }
        }

        if let Some(latest_seen) = self.latest_request_time.get(&agent_id)
            && now_unix_seconds < *latest_seen
        {
            return Err(AccessError::NonMonotonicTime {
                agent_id: agent_id.clone(),
            });
        }

        let requests = self.requests.entry(agent_id.clone()).or_default();
        while let Some(ts) = requests.front() {
            if now_unix_seconds.saturating_sub(*ts) < 60 {
                break;
            }
            requests.pop_front();
        }

        if requests.len() as u32 >= policy.max_requests_per_minute {
            return Err(AccessError::RateLimited {
                agent_id: agent_id.clone(),
                limit_per_minute: policy.max_requests_per_minute,
            });
        }
        requests.push_back(now_unix_seconds);
        self.latest_request_time
            .insert(agent_id.clone(), now_unix_seconds);
        Ok(agent_id)
    }
}

fn hash_api_key_argon2id(api_key: &str, salt: &[u8; 16]) -> Result<[u8; 32], String> {
    let params = Params::new(
        API_KEY_ARGON2_MEMORY_KIB,
        API_KEY_ARGON2_ITERATIONS,
        API_KEY_ARGON2_PARALLELISM,
        Some(32),
    )
    .map_err(|error| format!("invalid Argon2 params: {error}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0_u8; 32];
    argon2
        .hash_password_into(api_key.as_bytes(), salt, &mut output)
        .map_err(|error| format!("argon2 key derivation failed: {error}"))?;
    Ok(output)
}

fn hash_api_key_legacy_pbkdf2(api_key: &str, salt: &[u8; 16]) -> [u8; 32] {
    pbkdf2_hmac_array::<Sha256, 32>(api_key.as_bytes(), salt, API_KEY_LEGACY_PBKDF2_ITERATIONS)
}

fn constant_time_eq_32(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut diff = 0_u8;
    for (lhs, rhs) in left.iter().zip(right.iter()) {
        diff |= lhs ^ rhs;
    }
    diff == 0
}

pub fn validate_tool(tool: &McpTool, limits: ValidationLimits) -> Result<(), ValidationError> {
    let total = validate_tool_inner(tool, limits, 0)?;
    if total > limits.max_total_tools {
        return Err(ValidationError::BatchTotalToolsExceeded {
            count: total,
            max_total_tools: limits.max_total_tools,
        });
    }
    Ok(())
}

fn validate_tool_inner(
    tool: &McpTool,
    limits: ValidationLimits,
    depth: usize,
) -> Result<usize, ValidationError> {
    match tool {
        McpTool::BatchQuery { queries } => {
            let batch_depth = depth + 1;
            if batch_depth > limits.max_depth {
                return Err(ValidationError::BatchDepthExceeded {
                    depth: batch_depth,
                    max_depth: limits.max_depth,
                });
            }
            if queries.len() > limits.max_batch_size {
                return Err(ValidationError::BatchSizeExceeded {
                    size: queries.len(),
                    max_batch_size: limits.max_batch_size,
                });
            }
            let mut total = 1usize;
            for query in queries {
                let nested = validate_tool_inner(query, limits, batch_depth)?;
                total = total.saturating_add(nested);
                if total > limits.max_total_tools {
                    return Err(ValidationError::BatchTotalToolsExceeded {
                        count: total,
                        max_total_tools: limits.max_total_tools,
                    });
                }
            }
            Ok(total)
        }
        McpTool::GetAnomalies { limit } => {
            if *limit > MAX_GET_ANOMALIES_LIMIT {
                return Err(ValidationError::AnomalyLimitExceeded {
                    limit: *limit,
                    max_limit: MAX_GET_ANOMALIES_LIMIT,
                });
            }
            Ok(1)
        }
        McpTool::QueryState | McpTool::GetNodeStatus => Ok(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_non_recursive_batch() {
        let tool = McpTool::BatchQuery {
            queries: vec![
                McpTool::QueryState,
                McpTool::GetNodeStatus,
                McpTool::GetAnomalies { limit: 10 },
            ],
        };

        validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 1,
                max_total_tools: 10,
            },
        )
        .expect("valid");
    }

    #[test]
    fn blocks_nested_batch_when_depth_limit_is_one() {
        let tool = McpTool::BatchQuery {
            queries: vec![McpTool::BatchQuery {
                queries: vec![McpTool::QueryState],
            }],
        };

        let err = validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 1,
                max_total_tools: 10,
            },
        )
        .expect_err("must fail");

        assert_eq!(
            err,
            ValidationError::BatchDepthExceeded {
                depth: 2,
                max_depth: 1,
            }
        );
    }

    #[test]
    fn enforces_batch_size_limits() {
        let tool = McpTool::BatchQuery {
            queries: vec![
                McpTool::QueryState,
                McpTool::GetNodeStatus,
                McpTool::QueryState,
            ],
        };

        let err = validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 2,
                max_depth: 1,
                max_total_tools: 10,
            },
        )
        .expect_err("must fail");

        assert_eq!(
            err,
            ValidationError::BatchSizeExceeded {
                size: 3,
                max_batch_size: 2,
            }
        );
    }

    #[test]
    fn enforces_total_tool_count_across_nested_batches() {
        let nested = McpTool::BatchQuery {
            queries: vec![
                McpTool::BatchQuery {
                    queries: vec![
                        McpTool::QueryState,
                        McpTool::GetNodeStatus,
                        McpTool::QueryState,
                    ],
                },
                McpTool::BatchQuery {
                    queries: vec![
                        McpTool::GetNodeStatus,
                        McpTool::QueryState,
                        McpTool::GetNodeStatus,
                    ],
                },
            ],
        };

        let err = validate_tool(
            &nested,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 3,
                max_total_tools: 5,
            },
        )
        .expect_err("must fail");
        assert!(matches!(
            err,
            ValidationError::BatchTotalToolsExceeded {
                max_total_tools: 5,
                ..
            }
        ));
    }

    fn read_only_policy(limit: u32) -> AgentPolicy {
        AgentPolicy::new(
            "secret",
            BTreeSet::from([ToolPermission::QueryState]),
            limit,
        )
    }

    #[test]
    fn rejects_excessive_anomaly_query_limit() {
        let tool = McpTool::GetAnomalies {
            limit: MAX_GET_ANOMALIES_LIMIT + 1,
        };

        let err = validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 2,
                max_total_tools: 10,
            },
        )
        .expect_err("must reject excessive anomaly query");

        assert_eq!(
            err,
            ValidationError::AnomalyLimitExceeded {
                limit: MAX_GET_ANOMALIES_LIMIT + 1,
                max_limit: MAX_GET_ANOMALIES_LIMIT,
            }
        );
    }

    #[test]
    fn rejects_duplicate_policy_salts_in_controller_config() {
        let first = read_only_policy(1);
        let duplicate = AgentPolicy {
            api_key_salt: first.api_key_salt,
            api_key_hash: first.api_key_hash,
            api_key_kdf: first.api_key_kdf,
            permissions: first.permissions.clone(),
            max_requests_per_minute: first.max_requests_per_minute,
        };
        let err = McpAccessController::try_new([
            ("agent-a".to_string(), first),
            ("agent-b".to_string(), duplicate),
        ])
        .expect_err("must fail");
        assert_eq!(
            err,
            PolicyValidationError::DuplicateApiKeySalt {
                first_agent: "agent-a".to_string(),
                second_agent: "agent-b".to_string(),
            }
        );
    }

    #[test]
    fn infallible_controller_constructor_drops_duplicate_policy_salts() {
        let first = read_only_policy(1);
        let duplicate = AgentPolicy {
            api_key_salt: first.api_key_salt,
            api_key_hash: first.api_key_hash,
            api_key_kdf: first.api_key_kdf,
            permissions: first.permissions.clone(),
            max_requests_per_minute: first.max_requests_per_minute,
        };

        let mut access = McpAccessController::new([
            ("agent-a".to_string(), first),
            ("agent-b".to_string(), duplicate),
        ]);

        let resolved = access
            .authorize("secret", &McpTool::QueryState, 1_000)
            .expect("first policy should remain active");
        assert_eq!(resolved, "agent-a");
    }

    #[test]
    fn authorizes_valid_agent_with_permission_and_rate_budget() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        let resolved = access
            .authorize("secret", &McpTool::QueryState, 1_000)
            .expect("authorized");
        assert_eq!(resolved, "agent-a");
    }

    #[test]
    fn rejects_invalid_api_key() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        let err = access
            .authorize("wrong-key", &McpTool::QueryState, 1_000)
            .expect_err("must reject");
        assert_eq!(err, AccessError::InvalidApiKey);
    }

    #[test]
    fn rejects_ambiguous_api_keys_shared_across_agents() {
        let mut access = McpAccessController::new([
            ("agent-a".to_string(), read_only_policy(2)),
            ("agent-b".to_string(), read_only_policy(2)),
        ]);
        let err = access
            .authorize("secret", &McpTool::QueryState, 1_000)
            .expect_err("must reject ambiguous credential identity");
        assert_eq!(
            err,
            AccessError::AmbiguousApiKey {
                matching_agents: vec!["agent-a".to_string(), "agent-b".to_string()],
            }
        );
    }

    #[test]
    fn rejects_missing_permissions_for_batch_queries() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        let tool = McpTool::BatchQuery {
            queries: vec![McpTool::QueryState, McpTool::GetNodeStatus],
        };
        let err = access
            .authorize("secret", &tool, 1_000)
            .expect_err("must reject");
        assert_eq!(
            err,
            AccessError::PermissionDenied {
                agent_id: "agent-a".to_string(),
                permission: ToolPermission::GetNodeStatus,
            }
        );
    }

    #[test]
    fn enforces_per_agent_rate_limits() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        access
            .authorize("secret", &McpTool::QueryState, 1_000)
            .expect("first");
        access
            .authorize("secret", &McpTool::QueryState, 1_010)
            .expect("second");
        let err = access
            .authorize("secret", &McpTool::QueryState, 1_020)
            .expect_err("must rate-limit");
        assert_eq!(
            err,
            AccessError::RateLimited {
                agent_id: "agent-a".to_string(),
                limit_per_minute: 2,
            }
        );

        access
            .authorize("secret", &McpTool::QueryState, 1_061)
            .expect("window advanced");
    }

    #[test]
    fn rejects_non_monotonic_request_times() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        access
            .authorize("secret", &McpTool::QueryState, 1_000)
            .expect("first");
        let err = access
            .authorize("secret", &McpTool::QueryState, 999)
            .expect_err("must reject backwards time");
        assert_eq!(
            err,
            AccessError::NonMonotonicTime {
                agent_id: "agent-a".to_string(),
            }
        );
    }

    #[test]
    fn allows_requests_with_same_timestamp() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(3))]);
        access
            .authorize("secret", &McpTool::QueryState, 1_000)
            .expect("first");
        access
            .authorize("secret", &McpTool::QueryState, 1_000)
            .expect("same timestamp should be accepted");
    }

    #[test]
    fn agent_policy_defaults_to_argon2id() {
        let policy = read_only_policy(2);
        assert_eq!(policy.api_key_kdf, ApiKeyKdf::Argon2id);
    }
}
