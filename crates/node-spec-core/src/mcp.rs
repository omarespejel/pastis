use std::collections::{BTreeMap, BTreeSet, VecDeque};

use argon2::{Algorithm, Argon2, Params, Version};
use pbkdf2::pbkdf2_hmac_array;
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};

const API_KEY_ARGON2_MEMORY_KIB: u32 = 19 * 1024;
const API_KEY_ARGON2_ITERATIONS: u32 = 2;
const API_KEY_ARGON2_PARALLELISM: u32 = 1;
const API_KEY_LEGACY_PBKDF2_ITERATIONS: u32 = 150_000;
#[cfg(not(test))]
const AUTH_CACHE_MAX_ENTRIES: usize = 1_024;
#[cfg(test)]
const AUTH_CACHE_MAX_ENTRIES: usize = 8;
const MAX_API_KEY_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyKdf {
    Argon2id,
    LegacyPbkdf2Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpTool {
    QueryState,
    QueryStorage {
        contract_address: String,
        storage_keys: Vec<String>,
        block_number: Option<u64>,
        include_nonce: bool,
    },
    GetNodeStatus,
    GetAnomalies {
        limit: u64,
    },
    BatchQuery {
        queries: Vec<McpTool>,
    },
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
            McpTool::QueryStorage { .. } => {
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
    #[error("query storage key count {count} exceeds max keys {max_keys} for a single query")]
    StorageQueryTooManyKeys { count: usize, max_keys: usize },
    #[error("query storage must request at least one storage key or include nonce")]
    StorageQueryEmptySelection,
    #[error("invalid hex identifier for {field}: {value}")]
    InvalidHexIdentifier { field: &'static str, value: String },
}

const MAX_GET_ANOMALIES_LIMIT: u64 = 10_000;
const MAX_QUERY_STORAGE_KEYS: usize = 512;
const MAX_QUERY_IDENTIFIER_HEX_LEN: usize = 64;
const MAX_IDENTIFIER_ERROR_VALUE_LEN: usize = 128;

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
    ) -> Result<Self, AgentPolicyBuildError> {
        Self::try_new(api_key, permissions, max_requests_per_minute)
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
    auth_cache: BTreeMap<[u8; 32], String>,
    auth_cache_order: VecDeque<[u8; 32]>,
    denied_cache: BTreeSet<[u8; 32]>,
    denied_cache_order: VecDeque<[u8; 32]>,
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
            auth_cache: BTreeMap::new(),
            auth_cache_order: VecDeque::new(),
            denied_cache: BTreeSet::new(),
            denied_cache_order: VecDeque::new(),
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
            auth_cache: BTreeMap::new(),
            auth_cache_order: VecDeque::new(),
            denied_cache: BTreeSet::new(),
            denied_cache_order: VecDeque::new(),
        })
    }

    fn resolve_agent_id_for_api_key(&mut self, api_key: &str) -> Result<String, AccessError> {
        if api_key.len() > MAX_API_KEY_BYTES {
            return Err(AccessError::InvalidApiKey);
        }
        let fingerprint = sha256_digest(api_key.as_bytes());
        if self.denied_cache.contains(&fingerprint) {
            return Err(AccessError::InvalidApiKey);
        }
        if let Some(agent_id) = self.auth_cache.get(&fingerprint) {
            if self.policies.contains_key(agent_id) {
                return Ok(agent_id.clone());
            }
            self.auth_cache.remove(&fingerprint);
            self.auth_cache_order
                .retain(|cached| *cached != fingerprint);
        }

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
            1 => {
                let resolved = matching_agents.remove(0);
                self.cache_authorized_api_key(fingerprint, resolved.clone());
                Ok(resolved)
            }
            _ => Err(AccessError::AmbiguousApiKey { matching_agents }),
        }
    }

    fn cache_authorized_api_key(&mut self, fingerprint: [u8; 32], agent_id: String) {
        if self.auth_cache.contains_key(&fingerprint) {
            return;
        }
        self.denied_cache.remove(&fingerprint);
        self.denied_cache_order
            .retain(|cached| *cached != fingerprint);
        if self.auth_cache.len() >= AUTH_CACHE_MAX_ENTRIES
            && let Some(oldest) = self.auth_cache_order.pop_front()
        {
            self.auth_cache.remove(&oldest);
        }
        self.auth_cache.insert(fingerprint, agent_id);
        self.auth_cache_order.push_back(fingerprint);
    }

    fn cache_denied_api_key(&mut self, fingerprint: [u8; 32]) {
        if self.denied_cache.contains(&fingerprint) {
            return;
        }
        if self.denied_cache.len() >= AUTH_CACHE_MAX_ENTRIES
            && let Some(oldest) = self.denied_cache_order.pop_front()
        {
            self.denied_cache.remove(&oldest);
        }
        self.denied_cache.insert(fingerprint);
        self.denied_cache_order.push_back(fingerprint);
    }

    pub fn authorize(
        &mut self,
        api_key: &str,
        tool: &McpTool,
        now_unix_seconds: u64,
    ) -> Result<String, AccessError> {
        let agent_id = match self.resolve_agent_id_for_api_key(api_key) {
            Ok(agent_id) => agent_id,
            Err(AccessError::InvalidApiKey) => {
                if api_key.len() <= MAX_API_KEY_BYTES {
                    let fingerprint = sha256_digest(api_key.as_bytes());
                    self.cache_denied_api_key(fingerprint);
                }
                return Err(AccessError::InvalidApiKey);
            }
            Err(error) => return Err(error),
        };
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

    #[cfg(test)]
    fn auth_cache_len_for_tests(&self) -> usize {
        self.auth_cache.len()
    }

    #[cfg(test)]
    fn denied_cache_len_for_tests(&self) -> usize {
        self.denied_cache.len()
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

fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut out = [0_u8; 32];
    out.copy_from_slice(&digest);
    out
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
        McpTool::QueryStorage {
            contract_address,
            storage_keys,
            include_nonce,
            ..
        } => {
            if storage_keys.len() > MAX_QUERY_STORAGE_KEYS {
                return Err(ValidationError::StorageQueryTooManyKeys {
                    count: storage_keys.len(),
                    max_keys: MAX_QUERY_STORAGE_KEYS,
                });
            }
            if storage_keys.is_empty() && !*include_nonce {
                return Err(ValidationError::StorageQueryEmptySelection);
            }
            validate_hex_identifier("contract_address", contract_address)?;
            for key in storage_keys {
                validate_hex_identifier("storage_key", key)?;
            }
            Ok(1)
        }
        McpTool::QueryState | McpTool::GetNodeStatus => Ok(1),
    }
}

fn validate_hex_identifier(field: &'static str, value: &str) -> Result<(), ValidationError> {
    let trimmed = value.trim();
    let raw = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    let valid = !raw.is_empty()
        && raw.len() <= MAX_QUERY_IDENTIFIER_HEX_LEN
        && raw.chars().all(|ch| ch.is_ascii_hexdigit());
    if valid {
        return Ok(());
    }
    Err(ValidationError::InvalidHexIdentifier {
        field,
        value: truncate_for_error(value),
    })
}

fn truncate_for_error(value: &str) -> String {
    let total = value.chars().count();
    if total <= MAX_IDENTIFIER_ERROR_VALUE_LEN {
        return value.to_string();
    }
    let prefix: String = value.chars().take(MAX_IDENTIFIER_ERROR_VALUE_LEN).collect();
    format!("{prefix}...(truncated, {total} chars total)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_non_recursive_batch() {
        let tool = McpTool::BatchQuery {
            queries: vec![
                McpTool::QueryState,
                McpTool::QueryStorage {
                    contract_address: "0x1".to_string(),
                    storage_keys: vec!["0x2".to_string()],
                    block_number: Some(1),
                    include_nonce: false,
                },
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
        .expect("test policy should build")
    }

    fn legacy_read_only_policy(api_key: &str, salt_byte: u8, limit: u32) -> AgentPolicy {
        let salt = [salt_byte; 16];
        AgentPolicy {
            api_key_salt: salt,
            api_key_hash: hash_api_key_legacy_pbkdf2(api_key, &salt),
            api_key_kdf: ApiKeyKdf::LegacyPbkdf2Sha256,
            permissions: BTreeSet::from([ToolPermission::QueryState]),
            max_requests_per_minute: limit,
        }
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
    fn rejects_query_storage_without_requested_keys_or_nonce() {
        let tool = McpTool::QueryStorage {
            contract_address: "0x1".to_string(),
            storage_keys: Vec::new(),
            block_number: None,
            include_nonce: false,
        };

        let err = validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 2,
                max_total_tools: 10,
            },
        )
        .expect_err("must reject empty query selection");

        assert_eq!(err, ValidationError::StorageQueryEmptySelection);
    }

    #[test]
    fn rejects_query_storage_with_too_many_keys() {
        let tool = McpTool::QueryStorage {
            contract_address: "0x1".to_string(),
            storage_keys: vec!["0x2".to_string(); MAX_QUERY_STORAGE_KEYS + 1],
            block_number: None,
            include_nonce: false,
        };

        let err = validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 2,
                max_total_tools: 10,
            },
        )
        .expect_err("must reject oversized storage key vector");

        assert_eq!(
            err,
            ValidationError::StorageQueryTooManyKeys {
                count: MAX_QUERY_STORAGE_KEYS + 1,
                max_keys: MAX_QUERY_STORAGE_KEYS,
            }
        );
    }

    #[test]
    fn rejects_query_storage_with_invalid_hex_identifiers() {
        let tool = McpTool::QueryStorage {
            contract_address: "not-a-felt".to_string(),
            storage_keys: vec!["0x2".to_string()],
            block_number: None,
            include_nonce: true,
        };

        let err = validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 2,
                max_total_tools: 10,
            },
        )
        .expect_err("must reject malformed contract identifier");

        assert_eq!(
            err,
            ValidationError::InvalidHexIdentifier {
                field: "contract_address",
                value: "not-a-felt".to_string(),
            }
        );
    }

    #[test]
    fn allows_valid_query_storage_identifier_shapes() {
        let tool = McpTool::QueryStorage {
            contract_address: "0xABC".to_string(),
            storage_keys: vec!["10".to_string(), "0X20".to_string()],
            block_number: Some(42),
            include_nonce: true,
        };

        validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 2,
                max_total_tools: 10,
            },
        )
        .expect("valid storage query should pass shape checks");
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
    fn query_storage_uses_query_state_permission() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        let tool = McpTool::QueryStorage {
            contract_address: "0x1".to_string(),
            storage_keys: vec!["0x2".to_string()],
            block_number: None,
            include_nonce: false,
        };
        let resolved = access
            .authorize("secret", &tool, 1_000)
            .expect("query-storage should authorize under QueryState permission");
        assert_eq!(resolved, "agent-a");
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

    #[test]
    fn caches_authorized_api_keys_after_first_resolution() {
        let mut access = McpAccessController::new([(
            "agent-a".to_string(),
            legacy_read_only_policy("k1", 1, 3),
        )]);
        assert_eq!(access.auth_cache_len_for_tests(), 0);

        access
            .authorize("k1", &McpTool::QueryState, 1_000)
            .expect("first request should authorize");
        assert_eq!(access.auth_cache_len_for_tests(), 1);

        access
            .authorize("k1", &McpTool::QueryState, 1_001)
            .expect("second request should reuse cache");
        assert_eq!(access.auth_cache_len_for_tests(), 1);
    }

    #[test]
    fn auth_cache_is_bounded() {
        let mut access = McpAccessController::new([(
            "agent-a".to_string(),
            legacy_read_only_policy("k1", 1, 10),
        )]);
        let entries = AUTH_CACHE_MAX_ENTRIES + 2;
        for idx in 0..entries {
            let mut fingerprint = [0_u8; 32];
            fingerprint[0] = idx as u8;
            access.cache_authorized_api_key(fingerprint, format!("agent-{idx}"));
        }

        assert_eq!(access.auth_cache_len_for_tests(), AUTH_CACHE_MAX_ENTRIES);
    }

    #[test]
    fn caches_invalid_api_keys_and_bounds_cache_size() {
        let mut access = McpAccessController::new([(
            "agent-a".to_string(),
            legacy_read_only_policy("k1", 1, 10),
        )]);

        for idx in 0..(AUTH_CACHE_MAX_ENTRIES + 4) {
            let api_key = format!("invalid-{idx}");
            let err = access
                .authorize(&api_key, &McpTool::QueryState, 1_000 + idx as u64)
                .expect_err("invalid key should be rejected");
            assert_eq!(err, AccessError::InvalidApiKey);
        }

        assert_eq!(access.denied_cache_len_for_tests(), AUTH_CACHE_MAX_ENTRIES);
    }

    #[test]
    fn rejects_oversized_api_keys_without_caching_them() {
        let mut access = McpAccessController::new([(
            "agent-a".to_string(),
            legacy_read_only_policy("k1", 1, 10),
        )]);
        let oversized = "k".repeat(MAX_API_KEY_BYTES + 1);
        let err = access
            .authorize(&oversized, &McpTool::QueryState, 1_000)
            .expect_err("oversized key should be rejected");
        assert_eq!(err, AccessError::InvalidApiKey);
        assert_eq!(access.denied_cache_len_for_tests(), 0);
    }
}
