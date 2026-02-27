use std::collections::{BTreeMap, BTreeSet, VecDeque};

use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpTool {
    QueryState,
    GetNodeStatus,
    BatchQuery { queries: Vec<McpTool> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ToolPermission {
    QueryState,
    GetNodeStatus,
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
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("batch query depth {depth} exceeds max depth {max_depth}")]
    BatchDepthExceeded { depth: usize, max_depth: usize },
    #[error("batch query size {size} exceeds max size {max_batch_size}")]
    BatchSizeExceeded { size: usize, max_batch_size: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPolicy {
    pub api_key_hash: [u8; 32],
    pub permissions: BTreeSet<ToolPermission>,
    pub max_requests_per_minute: u32,
}

impl AgentPolicy {
    pub fn new(
        api_key: impl AsRef<str>,
        permissions: BTreeSet<ToolPermission>,
        max_requests_per_minute: u32,
    ) -> Self {
        Self {
            api_key_hash: hash_api_key(api_key.as_ref()),
            permissions,
            max_requests_per_minute,
        }
    }

    fn verify_api_key(&self, api_key: &str) -> bool {
        self.api_key_hash == hash_api_key(api_key)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AccessError {
    #[error("unknown agent '{agent_id}'")]
    UnknownAgent { agent_id: String },
    #[error("invalid api key for agent '{agent_id}'")]
    InvalidApiKey { agent_id: String },
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

#[derive(Debug, Default)]
pub struct McpAccessController {
    policies: BTreeMap<String, AgentPolicy>,
    requests: BTreeMap<String, VecDeque<u64>>,
    latest_request_time: BTreeMap<String, u64>,
}

impl McpAccessController {
    pub fn new(policies: impl IntoIterator<Item = (String, AgentPolicy)>) -> Self {
        Self {
            policies: policies.into_iter().collect(),
            requests: BTreeMap::new(),
            latest_request_time: BTreeMap::new(),
        }
    }

    pub fn authorize(
        &mut self,
        agent_id: &str,
        api_key: &str,
        tool: &McpTool,
        now_unix_seconds: u64,
    ) -> Result<(), AccessError> {
        let policy = self
            .policies
            .get(agent_id)
            .ok_or_else(|| AccessError::UnknownAgent {
                agent_id: agent_id.to_string(),
            })?;
        if !policy.verify_api_key(api_key) {
            return Err(AccessError::InvalidApiKey {
                agent_id: agent_id.to_string(),
            });
        }

        let mut required = BTreeSet::new();
        tool.collect_required_permissions(&mut required);
        for permission in required {
            if !policy.permissions.contains(&permission) {
                return Err(AccessError::PermissionDenied {
                    agent_id: agent_id.to_string(),
                    permission,
                });
            }
        }

        if let Some(latest_seen) = self.latest_request_time.get(agent_id)
            && now_unix_seconds < *latest_seen
        {
            return Err(AccessError::NonMonotonicTime {
                agent_id: agent_id.to_string(),
            });
        }
        self.latest_request_time
            .insert(agent_id.to_string(), now_unix_seconds);

        let requests = self.requests.entry(agent_id.to_string()).or_default();
        while let Some(ts) = requests.front() {
            if now_unix_seconds.saturating_sub(*ts) < 60 {
                break;
            }
            requests.pop_front();
        }

        if requests.len() as u32 >= policy.max_requests_per_minute {
            return Err(AccessError::RateLimited {
                agent_id: agent_id.to_string(),
                limit_per_minute: policy.max_requests_per_minute,
            });
        }
        requests.push_back(now_unix_seconds);
        Ok(())
    }
}

fn hash_api_key(api_key: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    hasher.finalize().into()
}

pub fn validate_tool(tool: &McpTool, limits: ValidationLimits) -> Result<(), ValidationError> {
    validate_tool_inner(tool, limits, 0)
}

fn validate_tool_inner(
    tool: &McpTool,
    limits: ValidationLimits,
    depth: usize,
) -> Result<(), ValidationError> {
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
            for query in queries {
                validate_tool_inner(query, limits, batch_depth)?;
            }
            Ok(())
        }
        McpTool::QueryState | McpTool::GetNodeStatus => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_non_recursive_batch() {
        let tool = McpTool::BatchQuery {
            queries: vec![McpTool::QueryState, McpTool::GetNodeStatus],
        };

        validate_tool(
            &tool,
            ValidationLimits {
                max_batch_size: 10,
                max_depth: 1,
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

    fn read_only_policy(limit: u32) -> AgentPolicy {
        AgentPolicy::new(
            "secret",
            BTreeSet::from([ToolPermission::QueryState]),
            limit,
        )
    }

    #[test]
    fn authorizes_valid_agent_with_permission_and_rate_budget() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        access
            .authorize("agent-a", "secret", &McpTool::QueryState, 1_000)
            .expect("authorized");
    }

    #[test]
    fn rejects_invalid_api_key() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        let err = access
            .authorize("agent-a", "wrong-key", &McpTool::QueryState, 1_000)
            .expect_err("must reject");
        assert_eq!(
            err,
            AccessError::InvalidApiKey {
                agent_id: "agent-a".to_string(),
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
            .authorize("agent-a", "secret", &tool, 1_000)
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
            .authorize("agent-a", "secret", &McpTool::QueryState, 1_000)
            .expect("first");
        access
            .authorize("agent-a", "secret", &McpTool::QueryState, 1_010)
            .expect("second");
        let err = access
            .authorize("agent-a", "secret", &McpTool::QueryState, 1_020)
            .expect_err("must rate-limit");
        assert_eq!(
            err,
            AccessError::RateLimited {
                agent_id: "agent-a".to_string(),
                limit_per_minute: 2,
            }
        );

        access
            .authorize("agent-a", "secret", &McpTool::QueryState, 1_061)
            .expect("window advanced");
    }

    #[test]
    fn rejects_non_monotonic_request_times() {
        let mut access = McpAccessController::new([("agent-a".to_string(), read_only_policy(2))]);
        access
            .authorize("agent-a", "secret", &McpTool::QueryState, 1_000)
            .expect("first");
        let err = access
            .authorize("agent-a", "secret", &McpTool::QueryState, 999)
            .expect_err("must reject backwards time");
        assert_eq!(
            err,
            AccessError::NonMonotonicTime {
                agent_id: "agent-a".to_string(),
            }
        );
    }
}
