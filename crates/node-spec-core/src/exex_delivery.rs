use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExExHandleMeta {
    pub name: String,
    pub depends_on: Vec<String>,
    pub priority: u32,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DeliveryPlanError {
    #[error("duplicate exex registration '{name}'")]
    DuplicateName { name: String },
    #[error("unknown dependency '{dependency}' for exex '{exex}'")]
    UnknownDependency { exex: String, dependency: String },
    #[error("dependency cycle detected among exex registrations")]
    DependencyCycle,
    #[error("dependency depth {depth} exceeds maximum supported depth {max}")]
    DependencyDepthExceeded { depth: usize, max: usize },
}

const MAX_DEPENDENCY_DEPTH: usize = 128;

pub fn build_delivery_tiers(
    handles: &[ExExHandleMeta],
) -> Result<Vec<Vec<String>>, DeliveryPlanError> {
    let mut priorities = HashMap::<String, u32>::new();
    let mut indegree = HashMap::<String, usize>::new();
    let mut outgoing = HashMap::<String, Vec<String>>::new();

    for handle in handles {
        if priorities
            .insert(handle.name.clone(), handle.priority)
            .is_some()
        {
            return Err(DeliveryPlanError::DuplicateName {
                name: handle.name.clone(),
            });
        }
        indegree.insert(handle.name.clone(), handle.depends_on.len());
        outgoing.entry(handle.name.clone()).or_default();
    }

    for handle in handles {
        for dep in &handle.depends_on {
            if !priorities.contains_key(dep) {
                return Err(DeliveryPlanError::UnknownDependency {
                    exex: handle.name.clone(),
                    dependency: dep.clone(),
                });
            }
            outgoing
                .entry(dep.clone())
                .or_default()
                .push(handle.name.clone());
        }
    }

    let mut unscheduled: BTreeSet<String> = priorities.keys().cloned().collect();
    let mut tiers = Vec::new();

    while !unscheduled.is_empty() {
        let mut ready: Vec<String> = unscheduled
            .iter()
            .filter(|name| indegree.get(*name).copied().unwrap_or_default() == 0)
            .cloned()
            .collect();

        if ready.is_empty() {
            return Err(DeliveryPlanError::DependencyCycle);
        }

        ready.sort_by(|a, b| priorities[b].cmp(&priorities[a]).then_with(|| a.cmp(b)));

        for name in &ready {
            unscheduled.remove(name);
            if let Some(children) = outgoing.get(name) {
                for child in children {
                    let Some(entry) = indegree.get_mut(child) else {
                        return Err(DeliveryPlanError::UnknownDependency {
                            exex: child.clone(),
                            dependency: name.clone(),
                        });
                    };
                    *entry = entry.saturating_sub(1);
                }
            }
        }

        tiers.push(ready);
        if tiers.len() > MAX_DEPENDENCY_DEPTH {
            return Err(DeliveryPlanError::DependencyDepthExceeded {
                depth: tiers.len(),
                max: MAX_DEPENDENCY_DEPTH,
            });
        }
    }

    Ok(tiers)
}

pub fn all_names(plan: &[Vec<String>]) -> HashSet<String> {
    plan.iter().flatten().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(name: &str, deps: &[&str], priority: u32) -> ExExHandleMeta {
        ExExHandleMeta {
            name: name.to_string(),
            depends_on: deps.iter().map(|d| d.to_string()).collect(),
            priority,
        }
    }

    #[test]
    fn creates_tiers_and_respects_dependencies() {
        let handles = vec![
            handle("otel", &[], 1),
            handle("btcfi", &["otel"], 5),
            handle("mcp", &["btcfi"], 2),
        ];

        let tiers = build_delivery_tiers(&handles).expect("plan");
        assert_eq!(
            tiers,
            vec![
                vec!["otel".to_string()],
                vec!["btcfi".to_string()],
                vec!["mcp".to_string()]
            ]
        );
    }

    #[test]
    fn sorts_within_tier_by_priority_then_name() {
        let handles = vec![
            handle("zeta", &[], 10),
            handle("alpha", &[], 10),
            handle("mid", &[], 4),
        ];

        let tiers = build_delivery_tiers(&handles).expect("plan");
        assert_eq!(tiers.len(), 1);
        assert_eq!(
            tiers[0],
            vec!["alpha".to_string(), "zeta".to_string(), "mid".to_string()]
        );
    }

    #[test]
    fn fails_on_unknown_dependency() {
        let handles = vec![handle("btcfi", &["missing"], 1)];

        let err = build_delivery_tiers(&handles).expect_err("must fail");
        assert_eq!(
            err,
            DeliveryPlanError::UnknownDependency {
                exex: "btcfi".to_string(),
                dependency: "missing".to_string(),
            }
        );
    }

    #[test]
    fn fails_on_dependency_cycle() {
        let handles = vec![
            handle("a", &["b"], 1),
            handle("b", &["c"], 1),
            handle("c", &["a"], 1),
        ];

        let err = build_delivery_tiers(&handles).expect_err("must fail");
        assert_eq!(err, DeliveryPlanError::DependencyCycle);
    }

    #[test]
    fn fails_on_excessive_dependency_depth() {
        let depth = MAX_DEPENDENCY_DEPTH + 1;
        let mut handles = Vec::with_capacity(depth);
        for idx in 0..depth {
            let name = format!("exex-{idx}");
            if idx == 0 {
                handles.push(ExExHandleMeta {
                    name,
                    depends_on: Vec::new(),
                    priority: 1,
                });
            } else {
                handles.push(ExExHandleMeta {
                    name,
                    depends_on: vec![format!("exex-{}", idx - 1)],
                    priority: 1,
                });
            }
        }

        let err = build_delivery_tiers(&handles).expect_err("must fail");
        assert_eq!(
            err,
            DeliveryPlanError::DependencyDepthExceeded {
                depth: MAX_DEPENDENCY_DEPTH + 1,
                max: MAX_DEPENDENCY_DEPTH,
            }
        );
    }
}
