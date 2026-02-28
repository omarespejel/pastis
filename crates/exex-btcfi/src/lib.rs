#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::str::FromStr;

use starknet_node_types::{BlockNumber, ContractAddress, StarknetFelt, StarknetStateDiff};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StandardWrapper {
    Wbtc,
    Tbtc,
    SolvBtc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BtcfiAnomaly {
    DataDecodeError {
        block_number: BlockNumber,
        component: String,
        field: String,
        value_hex: String,
        target_type: String,
    },
    SupplyMismatch {
        wrapper: StandardWrapper,
        block_number: BlockNumber,
        expected_supply_sats: u128,
        observed_supply_sats: u128,
    },
    StrkBtcMerkleInconsistency {
        block_number: BlockNumber,
        reason: String,
    },
    StrkBtcNullifierReuse {
        block_number: BlockNumber,
        nullifier_key: String,
    },
    StrkBtcNullifierTrackingSaturated {
        block_number: BlockNumber,
        tracked: usize,
        max: usize,
    },
    StrkBtcCommitmentFlood {
        block_number: BlockNumber,
        delta: u64,
        threshold: u64,
    },
    StrkBtcUnshieldCluster {
        block_number: BlockNumber,
        window_blocks: u64,
        delta: u64,
        threshold: u64,
    },
    StrkBtcLightClientDivergence {
        block_number: BlockNumber,
        contract_btc_height: u64,
        observed_btc_height: u64,
        max_lag_blocks: u64,
    },
    StrkBtcBridgeReconciliationTimeout {
        block_number: BlockNumber,
        operation_id: String,
        observed_after_blocks: u64,
        timeout_blocks: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandardWrapperConfig {
    pub wrapper: StandardWrapper,
    pub token_contract: ContractAddress,
    pub total_supply_key: String,
    pub expected_supply_sats: u128,
    pub allowed_deviation_bps: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandardWrapperMonitor {
    config: StandardWrapperConfig,
}

impl StandardWrapperMonitor {
    pub fn new(config: StandardWrapperConfig) -> Self {
        Self { config }
    }

    pub fn process_state_diff(
        &self,
        block_number: BlockNumber,
        diff: &StarknetStateDiff,
    ) -> Vec<BtcfiAnomaly> {
        let Some(writes) = find_writes_for_contract(diff, &self.config.token_contract) else {
            return Vec::new();
        };
        let Some(total_supply_raw) = writes.get(&self.config.total_supply_key) else {
            return Vec::new();
        };
        let total_supply = match felt_to_u128(total_supply_raw) {
            Ok(value) => value,
            Err(value_hex) => {
                return vec![BtcfiAnomaly::DataDecodeError {
                    block_number,
                    component: format!("{:?}", self.config.wrapper),
                    field: "total_supply".to_string(),
                    value_hex,
                    target_type: "u128".to_string(),
                }];
            }
        };
        let expected = self.config.expected_supply_sats;
        if expected == 0 {
            return Vec::new();
        }
        let deviation = total_supply.abs_diff(expected);
        let deviation_bps = deviation.saturating_mul(10_000) / expected;
        if deviation_bps > u128::from(self.config.allowed_deviation_bps) {
            return vec![BtcfiAnomaly::SupplyMismatch {
                wrapper: self.config.wrapper,
                block_number,
                expected_supply_sats: expected,
                observed_supply_sats: total_supply,
            }];
        }
        Vec::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrkBtcMonitorConfig {
    pub shielded_pool_contract: ContractAddress,
    pub merkle_root_key: String,
    pub commitment_count_key: String,
    pub nullifier_count_key: String,
    pub nullifier_key_prefix: String,
    pub commitment_flood_threshold: u64,
    pub unshield_cluster_threshold: u64,
    pub unshield_cluster_window_blocks: u64,
    pub light_client_max_lag_blocks: u64,
    pub bridge_timeout_blocks: u64,
    pub max_tracked_nullifiers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrkBtcMonitor {
    config: StrkBtcMonitorConfig,
    last_merkle_root: Option<String>,
    last_commitment_count: Option<u64>,
    last_nullifier_count: Option<u64>,
    seen_nullifiers: BTreeSet<String>,
    nullifier_order: VecDeque<String>,
    recent_unshield_deltas: VecDeque<(BlockNumber, u64)>,
}

impl StrkBtcMonitor {
    pub fn new(mut config: StrkBtcMonitorConfig) -> Self {
        config.max_tracked_nullifiers = config.max_tracked_nullifiers.max(1);
        Self {
            config,
            last_merkle_root: None,
            last_commitment_count: None,
            last_nullifier_count: None,
            seen_nullifiers: BTreeSet::new(),
            nullifier_order: VecDeque::new(),
            recent_unshield_deltas: VecDeque::new(),
        }
    }

    pub fn process_state_diff(
        &mut self,
        block_number: BlockNumber,
        diff: &StarknetStateDiff,
    ) -> Vec<BtcfiAnomaly> {
        let Some(writes) = find_writes_for_contract(diff, &self.config.shielded_pool_contract)
        else {
            return Vec::new();
        };
        let mut anomalies = Vec::new();
        let mut nullifier_tracking_saturated = false;

        for (key, value) in writes {
            if key == &self.config.merkle_root_key
                || key == &self.config.commitment_count_key
                || key == &self.config.nullifier_count_key
            {
                continue;
            }
            if !key.starts_with(&self.config.nullifier_key_prefix) {
                continue;
            }
            if felt_is_zero(value) {
                continue;
            }
            if self.seen_nullifiers.contains(key) {
                anomalies.push(BtcfiAnomaly::StrkBtcNullifierReuse {
                    block_number,
                    nullifier_key: key.clone(),
                });
                continue;
            }
            if self.seen_nullifiers.len() >= self.config.max_tracked_nullifiers {
                nullifier_tracking_saturated = true;
                if let Some(evicted) = self.nullifier_order.pop_front() {
                    self.seen_nullifiers.remove(&evicted);
                }
            }
            self.seen_nullifiers.insert(key.clone());
            self.nullifier_order.push_back(key.clone());
        }

        if nullifier_tracking_saturated {
            anomalies.push(BtcfiAnomaly::StrkBtcNullifierTrackingSaturated {
                block_number,
                tracked: self.seen_nullifiers.len(),
                max: self.config.max_tracked_nullifiers,
            });
        }

        let next_merkle_root = writes
            .get(&self.config.merkle_root_key)
            .map(felt_to_hex_prefixed);
        let next_commitment_count = match writes.get(&self.config.commitment_count_key) {
            Some(value) => match felt_to_u64(value) {
                Ok(parsed) => Some(parsed),
                Err(value_hex) => {
                    anomalies.push(BtcfiAnomaly::DataDecodeError {
                        block_number,
                        component: "strkbtc".to_string(),
                        field: "commitment_count".to_string(),
                        value_hex,
                        target_type: "u64".to_string(),
                    });
                    None
                }
            },
            None => None,
        };
        let next_nullifier_count = match writes.get(&self.config.nullifier_count_key) {
            Some(value) => match felt_to_u64(value) {
                Ok(parsed) => Some(parsed),
                Err(value_hex) => {
                    anomalies.push(BtcfiAnomaly::DataDecodeError {
                        block_number,
                        component: "strkbtc".to_string(),
                        field: "nullifier_count".to_string(),
                        value_hex,
                        target_type: "u64".to_string(),
                    });
                    None
                }
            },
            None => None,
        };

        if let (Some(previous), Some(current)) = (self.last_commitment_count, next_commitment_count)
        {
            if current < previous {
                anomalies.push(BtcfiAnomaly::StrkBtcMerkleInconsistency {
                    block_number,
                    reason: format!(
                        "commitment counter regressed: previous={previous} current={current}"
                    ),
                });
            }
            let delta = current.saturating_sub(previous);
            if delta > self.config.commitment_flood_threshold {
                anomalies.push(BtcfiAnomaly::StrkBtcCommitmentFlood {
                    block_number,
                    delta,
                    threshold: self.config.commitment_flood_threshold,
                });
            }
            if delta > 0
                && let (Some(prev_root), Some(next_root)) =
                    (self.last_merkle_root.as_ref(), next_merkle_root.as_ref())
                && prev_root == next_root.as_str()
            {
                anomalies.push(BtcfiAnomaly::StrkBtcMerkleInconsistency {
                    block_number,
                    reason: "merkle root unchanged while commitments increased".to_string(),
                });
            }
        }

        if let (Some(previous), Some(current)) = (self.last_nullifier_count, next_nullifier_count) {
            if current < previous {
                anomalies.push(BtcfiAnomaly::StrkBtcMerkleInconsistency {
                    block_number,
                    reason: format!(
                        "nullifier counter regressed: previous={previous} current={current}"
                    ),
                });
            }
            let delta = current.saturating_sub(previous);
            self.recent_unshield_deltas.push_back((block_number, delta));
            while let Some((observed_at, _)) = self.recent_unshield_deltas.front() {
                if block_number.saturating_sub(*observed_at)
                    > self.config.unshield_cluster_window_blocks
                {
                    self.recent_unshield_deltas.pop_front();
                } else {
                    break;
                }
            }
            let clustered_delta: u64 = self.recent_unshield_deltas.iter().map(|(_, v)| *v).sum();
            if clustered_delta > self.config.unshield_cluster_threshold {
                anomalies.push(BtcfiAnomaly::StrkBtcUnshieldCluster {
                    block_number,
                    window_blocks: self.config.unshield_cluster_window_blocks,
                    delta: clustered_delta,
                    threshold: self.config.unshield_cluster_threshold,
                });
            }
        }

        if let Some(root) = next_merkle_root {
            self.last_merkle_root = Some(root);
        }
        if let Some(count) = next_commitment_count {
            self.last_commitment_count = Some(count);
        }
        if let Some(count) = next_nullifier_count {
            self.last_nullifier_count = Some(count);
        }

        anomalies
    }

    pub fn observe_light_client_heights(
        &self,
        block_number: BlockNumber,
        contract_btc_height: u64,
        observed_btc_height: u64,
    ) -> Vec<BtcfiAnomaly> {
        let lag = observed_btc_height.abs_diff(contract_btc_height);
        if lag > self.config.light_client_max_lag_blocks {
            return vec![BtcfiAnomaly::StrkBtcLightClientDivergence {
                block_number,
                contract_btc_height,
                observed_btc_height,
                max_lag_blocks: self.config.light_client_max_lag_blocks,
            }];
        }
        Vec::new()
    }

    pub fn observe_bridge_operation_age(
        &self,
        block_number: BlockNumber,
        operation_id: impl Into<String>,
        observed_after_blocks: u64,
    ) -> Vec<BtcfiAnomaly> {
        if observed_after_blocks > self.config.bridge_timeout_blocks {
            return vec![BtcfiAnomaly::StrkBtcBridgeReconciliationTimeout {
                block_number,
                operation_id: operation_id.into(),
                observed_after_blocks,
                timeout_blocks: self.config.bridge_timeout_blocks,
            }];
        }
        Vec::new()
    }
}

fn canonical_felt_hex(raw: &str) -> Option<String> {
    StarknetFelt::from_str(raw)
        .ok()
        .map(|felt| format!("{:#x}", felt))
}

fn find_writes_for_contract<'a>(
    diff: &'a StarknetStateDiff,
    target: &ContractAddress,
) -> Option<&'a BTreeMap<String, StarknetFelt>> {
    let target = canonical_felt_hex(target.as_ref())?;
    diff.storage_diffs.iter().find_map(|(contract, writes)| {
        (canonical_felt_hex(contract.as_ref()).as_deref() == Some(target.as_str())).then_some(writes)
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtcfiExEx {
    wrappers: Vec<StandardWrapperMonitor>,
    strkbtc: StrkBtcMonitor,
    retained_anomalies: VecDeque<BtcfiAnomaly>,
    max_retained_anomalies: usize,
}

impl BtcfiExEx {
    pub fn new(
        wrappers: Vec<StandardWrapperMonitor>,
        strkbtc: StrkBtcMonitor,
        max_retained_anomalies: usize,
    ) -> Self {
        Self {
            wrappers,
            strkbtc,
            retained_anomalies: VecDeque::new(),
            max_retained_anomalies,
        }
    }

    pub fn process_block(
        &mut self,
        block_number: BlockNumber,
        diff: &StarknetStateDiff,
    ) -> Vec<BtcfiAnomaly> {
        let mut anomalies = Vec::new();
        for wrapper in &self.wrappers {
            anomalies.extend(wrapper.process_state_diff(block_number, diff));
        }
        anomalies.extend(self.strkbtc.process_state_diff(block_number, diff));
        self.retain_anomalies(&anomalies);
        anomalies
    }

    pub fn recent_anomalies(&self, limit: usize) -> Vec<BtcfiAnomaly> {
        self.retained_anomalies
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    fn retain_anomalies(&mut self, anomalies: &[BtcfiAnomaly]) {
        if self.max_retained_anomalies == 0 {
            return;
        }
        for anomaly in anomalies {
            self.retained_anomalies.push_back(anomaly.clone());
            while self.retained_anomalies.len() > self.max_retained_anomalies {
                self.retained_anomalies.pop_front();
            }
        }
    }
}

fn felt_to_hex_prefixed(value: &StarknetFelt) -> String {
    format!("{:#x}", value)
}

fn felt_to_u64(value: &StarknetFelt) -> Result<u64, String> {
    let hex = felt_to_hex_prefixed(value);
    u64::from_str_radix(hex.trim_start_matches("0x"), 16).map_err(|_| hex)
}

fn felt_to_u128(value: &StarknetFelt) -> Result<u128, String> {
    let hex = felt_to_hex_prefixed(value);
    u128::from_str_radix(hex.trim_start_matches("0x"), 16).map_err(|_| hex)
}

fn felt_is_zero(value: &StarknetFelt) -> bool {
    felt_to_hex_prefixed(value) == "0x0"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;

    fn wrapper_config() -> StandardWrapperConfig {
        StandardWrapperConfig {
            wrapper: StandardWrapper::Wbtc,
            token_contract: ContractAddress::parse("0x111").expect("valid contract address"),
            total_supply_key: "0x1".to_string(),
            expected_supply_sats: 1_000_000,
            allowed_deviation_bps: 50,
        }
    }

    fn strkbtc_config() -> StrkBtcMonitorConfig {
        StrkBtcMonitorConfig {
            shielded_pool_contract: ContractAddress::parse("0x222").expect("valid contract address"),
            merkle_root_key: "0x10".to_string(),
            commitment_count_key: "0x11".to_string(),
            nullifier_count_key: "0x12".to_string(),
            nullifier_key_prefix: "0xdead".to_string(),
            commitment_flood_threshold: 8,
            unshield_cluster_threshold: 4,
            unshield_cluster_window_blocks: 3,
            light_client_max_lag_blocks: 6,
            bridge_timeout_blocks: 20,
            max_tracked_nullifiers: 10_000,
        }
    }

    fn diff_with_write(
        contract: ContractAddress,
        key: &str,
        value: StarknetFelt,
    ) -> StarknetStateDiff {
        let mut writes = BTreeMap::new();
        writes.insert(key.to_string(), value);
        StarknetStateDiff {
            storage_diffs: BTreeMap::from([(contract, writes)]),
            nonces: BTreeMap::new(),
            declared_classes: Vec::new(),
        }
    }

    #[test]
    fn standard_wrapper_monitor_emits_supply_mismatch() {
        let monitor = StandardWrapperMonitor::new(wrapper_config());
        let diff = diff_with_write(
            ContractAddress::parse("0x111").expect("valid contract address"),
            "0x1",
            StarknetFelt::from(1_030_000_u64),
        );
        let anomalies = monitor.process_state_diff(10, &diff);
        assert_eq!(anomalies.len(), 1);
        assert!(matches!(
            anomalies[0],
            BtcfiAnomaly::SupplyMismatch {
                wrapper: StandardWrapper::Wbtc,
                block_number: 10,
                expected_supply_sats: 1_000_000,
                observed_supply_sats: 1_030_000,
            }
        ));
    }

    #[test]
    fn standard_wrapper_monitor_ignores_small_supply_drift() {
        let monitor = StandardWrapperMonitor::new(wrapper_config());
        let diff = diff_with_write(
            ContractAddress::parse("0x111").expect("valid contract address"),
            "0x1",
            StarknetFelt::from(1_003_000_u64),
        );
        let anomalies = monitor.process_state_diff(11, &diff);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn standard_wrapper_monitor_matches_equivalent_contract_hex_encodings() {
        let monitor = StandardWrapperMonitor::new(wrapper_config());
        let diff: StarknetStateDiff = serde_json::from_value(json!({
            "storage_diffs": {
                "0x0111": {
                    "0x1": "0xfb830"
                }
            },
            "nonces": {},
            "declared_classes": []
        }))
        .expect("valid state diff json");
        let anomalies = monitor.process_state_diff(12, &diff);
        assert!(anomalies.iter().any(|anomaly| matches!(
            anomaly,
            BtcfiAnomaly::SupplyMismatch {
                block_number: 12,
                ..
            }
        )));
    }

    #[test]
    fn strkbtc_monitor_detects_nullifier_reuse() {
        let mut monitor = StrkBtcMonitor::new(strkbtc_config());
        let mut first = diff_with_write(
            ContractAddress::parse("0x222").expect("valid contract address"),
            "0xdead01",
            StarknetFelt::from(1_u64),
        );
        first
            .storage_diffs
            .entry(ContractAddress::parse("0x222").expect("valid contract address"))
            .or_default()
            .insert("0x11".to_string(), StarknetFelt::from(1_u64));
        first
            .storage_diffs
            .entry(ContractAddress::parse("0x222").expect("valid contract address"))
            .or_default()
            .insert("0x12".to_string(), StarknetFelt::from(1_u64));
        first
            .storage_diffs
            .entry(ContractAddress::parse("0x222").expect("valid contract address"))
            .or_default()
            .insert("0x10".to_string(), StarknetFelt::from(123_u64));
        assert!(monitor.process_state_diff(20, &first).is_empty());

        let second = diff_with_write(
            ContractAddress::parse("0x222").expect("valid contract address"),
            "0xdead01",
            StarknetFelt::from(1_u64),
        );
        let anomalies = monitor.process_state_diff(21, &second);
        assert!(matches!(
            anomalies.as_slice(),
            [BtcfiAnomaly::StrkBtcNullifierReuse {
                block_number: 21,
                ..
            }]
        ));
    }

    #[test]
    fn strkbtc_monitor_bounds_nullifier_tracking_state() {
        let mut cfg = strkbtc_config();
        cfg.max_tracked_nullifiers = 1;
        let mut monitor = StrkBtcMonitor::new(cfg);

        let first = diff_with_write(
            ContractAddress::parse("0x222").expect("valid contract address"),
            "0xdead01",
            StarknetFelt::from(1_u64),
        );
        assert!(monitor.process_state_diff(30, &first).is_empty());
        assert_eq!(monitor.seen_nullifiers.len(), 1);

        let second = diff_with_write(
            ContractAddress::parse("0x222").expect("valid contract address"),
            "0xdead02",
            StarknetFelt::from(1_u64),
        );
        let anomalies = monitor.process_state_diff(31, &second);
        assert!(matches!(
            anomalies.as_slice(),
            [BtcfiAnomaly::StrkBtcNullifierTrackingSaturated {
                block_number: 31,
                tracked: 1,
                max: 1,
            }]
        ));
        assert_eq!(monitor.seen_nullifiers.len(), 1);
    }

    #[test]
    fn strkbtc_monitor_keeps_reuse_detection_after_tracking_saturation() {
        let mut cfg = strkbtc_config();
        cfg.max_tracked_nullifiers = 2;
        let mut monitor = StrkBtcMonitor::new(cfg);

        let mut first = StarknetStateDiff::default();
        first.storage_diffs.insert(
            ContractAddress::parse("0x222").expect("valid contract address"),
            BTreeMap::from([
                ("0xdead01".to_string(), StarknetFelt::from(1_u64)),
                ("0xdead02".to_string(), StarknetFelt::from(1_u64)),
            ]),
        );
        assert!(monitor.process_state_diff(40, &first).is_empty());

        let second = diff_with_write(
            ContractAddress::parse("0x222").expect("valid contract address"),
            "0xdead03",
            StarknetFelt::from(1_u64),
        );
        let saturated = monitor.process_state_diff(41, &second);
        assert!(saturated.iter().any(|anomaly| matches!(
            anomaly,
            BtcfiAnomaly::StrkBtcNullifierTrackingSaturated {
                block_number: 41,
                tracked: 2,
                max: 2,
            }
        )));

        let reused_tracked = diff_with_write(
            ContractAddress::parse("0x222").expect("valid contract address"),
            "0xdead02",
            StarknetFelt::from(1_u64),
        );
        let anomalies = monitor.process_state_diff(42, &reused_tracked);
        assert!(anomalies.iter().any(|anomaly| matches!(
            anomaly,
            BtcfiAnomaly::StrkBtcNullifierReuse {
                block_number: 42,
                nullifier_key,
            } if nullifier_key == "0xdead02"
        )));
    }

    #[test]
    fn strkbtc_monitor_detects_commitment_flood_and_merkle_stall() {
        let mut monitor = StrkBtcMonitor::new(strkbtc_config());
        let mut first = StarknetStateDiff::default();
        first.storage_diffs.insert(
            ContractAddress::parse("0x222").expect("valid contract address"),
            BTreeMap::from([
                ("0x10".to_string(), StarknetFelt::from(7_u64)),
                ("0x11".to_string(), StarknetFelt::from(10_u64)),
                ("0x12".to_string(), StarknetFelt::from(10_u64)),
            ]),
        );
        assert!(monitor.process_state_diff(1, &first).is_empty());

        let mut second = StarknetStateDiff::default();
        second.storage_diffs.insert(
            ContractAddress::parse("0x222").expect("valid contract address"),
            BTreeMap::from([
                ("0x10".to_string(), StarknetFelt::from(7_u64)),
                ("0x11".to_string(), StarknetFelt::from(25_u64)),
                ("0x12".to_string(), StarknetFelt::from(10_u64)),
            ]),
        );
        let anomalies = monitor.process_state_diff(2, &second);
        assert_eq!(anomalies.len(), 2);
        assert!(anomalies.iter().any(|a| matches!(
            a,
            BtcfiAnomaly::StrkBtcCommitmentFlood {
                delta: 15,
                threshold: 8,
                ..
            }
        )));
        assert!(
            anomalies
                .iter()
                .any(|a| matches!(a, BtcfiAnomaly::StrkBtcMerkleInconsistency { .. }))
        );
    }

    #[test]
    fn strkbtc_monitor_detects_unshield_cluster() {
        let mut monitor = StrkBtcMonitor::new(strkbtc_config());
        let mut baseline = StarknetStateDiff::default();
        baseline.storage_diffs.insert(
            ContractAddress::parse("0x222").expect("valid contract address"),
            BTreeMap::from([
                ("0x10".to_string(), StarknetFelt::from(1_u64)),
                ("0x11".to_string(), StarknetFelt::from(1_u64)),
                ("0x12".to_string(), StarknetFelt::from(1_u64)),
            ]),
        );
        monitor.process_state_diff(1, &baseline);

        let mut next = StarknetStateDiff::default();
        next.storage_diffs.insert(
            ContractAddress::parse("0x222").expect("valid contract address"),
            BTreeMap::from([
                ("0x10".to_string(), StarknetFelt::from(2_u64)),
                ("0x11".to_string(), StarknetFelt::from(2_u64)),
                ("0x12".to_string(), StarknetFelt::from(7_u64)),
            ]),
        );
        let anomalies = monitor.process_state_diff(2, &next);
        assert!(anomalies.iter().any(|a| matches!(
            a,
            BtcfiAnomaly::StrkBtcUnshieldCluster {
                threshold: 4,
                delta: 6,
                ..
            }
        )));
    }

    #[test]
    fn strkbtc_monitor_detects_light_client_and_bridge_anomalies() {
        let monitor = StrkBtcMonitor::new(strkbtc_config());
        let light_client = monitor.observe_light_client_heights(100, 1_000, 1_010);
        assert!(matches!(
            light_client.as_slice(),
            [BtcfiAnomaly::StrkBtcLightClientDivergence {
                max_lag_blocks: 6,
                ..
            }]
        ));

        let reverse_lag = monitor.observe_light_client_heights(100, 1_010, 1_000);
        assert!(matches!(
            reverse_lag.as_slice(),
            [BtcfiAnomaly::StrkBtcLightClientDivergence {
                max_lag_blocks: 6,
                ..
            }]
        ));

        let bridge = monitor.observe_bridge_operation_age(101, "op-1", 30);
        assert!(matches!(
            bridge.as_slice(),
            [BtcfiAnomaly::StrkBtcBridgeReconciliationTimeout {
                timeout_blocks: 20,
                ..
            }]
        ));
    }

    #[test]
    fn btcfi_exex_retains_recent_anomalies() {
        let wrapper = StandardWrapperMonitor::new(wrapper_config());
        let strkbtc = StrkBtcMonitor::new(strkbtc_config());
        let mut exex = BtcfiExEx::new(vec![wrapper], strkbtc, 2);

        let diff1 = diff_with_write(
            ContractAddress::parse("0x111").expect("valid contract address"),
            "0x1",
            StarknetFelt::from(1_030_000_u64),
        );
        let diff2 = diff_with_write(
            ContractAddress::parse("0x111").expect("valid contract address"),
            "0x1",
            StarknetFelt::from(1_040_000_u64),
        );
        let diff3 = diff_with_write(
            ContractAddress::parse("0x111").expect("valid contract address"),
            "0x1",
            StarknetFelt::from(1_050_000_u64),
        );

        exex.process_block(1, &diff1);
        exex.process_block(2, &diff2);
        exex.process_block(3, &diff3);

        let recent = exex.recent_anomalies(10);
        assert_eq!(recent.len(), 2);
        assert!(matches!(
            recent[0],
            BtcfiAnomaly::SupplyMismatch {
                block_number: 3,
                ..
            }
        ));
        assert!(matches!(
            recent[1],
            BtcfiAnomaly::SupplyMismatch {
                block_number: 2,
                ..
            }
        ));
    }
}
