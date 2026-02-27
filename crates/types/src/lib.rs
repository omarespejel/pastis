use std::collections::BTreeMap;
use std::time::Duration;

use semver::Version;
use serde::{Deserialize, Serialize};

pub type BlockNumber = u64;
pub type TxHash = String;
pub type ClassHash = String;
pub type ContractAddress = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StarknetTransaction {
    pub hash: TxHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StarknetReceipt {
    pub tx_hash: TxHash,
    pub execution_status: bool,
    pub events: u64,
    pub gas_consumed: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltinStats {
    pub range_check: usize,
    pub pedersen: usize,
    pub bitwise: usize,
    pub ec_op: usize,
    pub poseidon: usize,
    pub range_check96: usize,
    pub add_mod: usize,
    pub mul_mod: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StarknetStateDiff {
    pub storage_diffs: BTreeMap<ContractAddress, BTreeMap<String, u64>>,
    pub nonces: BTreeMap<ContractAddress, u64>,
    pub declared_classes: Vec<ClassHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StarknetBlock {
    pub number: BlockNumber,
    pub protocol_version: Version,
    pub transactions: Vec<StarknetTransaction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockId {
    Number(BlockNumber),
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockContext {
    pub block_number: BlockNumber,
    pub protocol_version: Version,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutput {
    pub receipts: Vec<StarknetReceipt>,
    pub state_diff: StarknetStateDiff,
    pub builtin_stats: BuiltinStats,
    pub execution_time: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationResult {
    pub receipt: StarknetReceipt,
    pub estimated_fee: u64,
}

pub trait StarknetNodeTypes: Send + Sync + 'static {
    type Block;
    type Transaction;
    type Receipt;
    type StateDiff;
}

pub struct StarknetMainnet;

impl StarknetNodeTypes for StarknetMainnet {
    type Block = StarknetBlock;
    type Transaction = StarknetTransaction;
    type Receipt = StarknetReceipt;
    type StateDiff = StarknetStateDiff;
}

pub trait StateReader: Send + Sync {
    fn get_storage(&self, contract: &ContractAddress, key: &str) -> Option<u64>;
    fn nonce_of(&self, contract: &ContractAddress) -> Option<u64>;
}

pub trait MutableState: StateReader {
    fn set_storage(&mut self, contract: ContractAddress, key: String, value: u64);
    fn set_nonce(&mut self, contract: ContractAddress, nonce: u64);
    fn boxed_clone(&self) -> Box<dyn MutableState>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InMemoryState {
    pub storage: BTreeMap<ContractAddress, BTreeMap<String, u64>>,
    pub nonces: BTreeMap<ContractAddress, u64>,
}

impl InMemoryState {
    pub fn apply_state_diff(&mut self, diff: &StarknetStateDiff) {
        for (contract, writes) in &diff.storage_diffs {
            for (key, value) in writes {
                self.set_storage(contract.clone(), key.clone(), *value);
            }
        }
        for (contract, nonce) in &diff.nonces {
            self.set_nonce(contract.clone(), *nonce);
        }
    }
}

impl StateReader for InMemoryState {
    fn get_storage(&self, contract: &ContractAddress, key: &str) -> Option<u64> {
        self.storage
            .get(contract)
            .and_then(|slots| slots.get(key).copied())
    }

    fn nonce_of(&self, contract: &ContractAddress) -> Option<u64> {
        self.nonces.get(contract).copied()
    }
}

impl MutableState for InMemoryState {
    fn set_storage(&mut self, contract: ContractAddress, key: String, value: u64) {
        self.storage.entry(contract).or_default().insert(key, value);
    }

    fn set_nonce(&mut self, contract: ContractAddress, nonce: u64) {
        self.nonces.insert(contract, nonce);
    }

    fn boxed_clone(&self) -> Box<dyn MutableState> {
        Box::new(self.clone())
    }
}

pub trait HealthCheck: Send + Sync {
    fn is_healthy(&self) -> bool;
    fn detailed_status(&self) -> ComponentHealth;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub last_block_processed: Option<BlockNumber>,
    pub sync_lag: Option<Duration>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_state_diff_into_state() {
        let mut state = InMemoryState::default();
        let mut diff = StarknetStateDiff::default();
        diff.storage_diffs
            .entry("0xabc".to_string())
            .or_default()
            .insert("balance".to_string(), 7);
        diff.nonces.insert("0xabc".to_string(), 2);

        state.apply_state_diff(&diff);

        assert_eq!(state.get_storage(&"0xabc".to_string(), "balance"), Some(7));
        assert_eq!(state.nonce_of(&"0xabc".to_string()), Some(2));
    }
}
