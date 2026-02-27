#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpTool {
    QueryState,
    GetNodeStatus,
    BatchQuery { queries: Vec<McpTool> },
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
}
