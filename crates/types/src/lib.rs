#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use semver::Version;
use serde::{Deserialize, Serialize};
#[cfg(feature = "blockifier-adapter")]
use starknet_api::hash::StarkHash as ExecutableFelt;
use starknet_types_core::felt::Felt;

pub type BlockNumber = u64;
pub type BlockHash = String;
pub type StateRoot = String;
pub type TxHash = String;
pub type ClassHash = String;
pub type ContractAddress = String;
pub type StarknetFelt = Felt;

#[cfg(feature = "blockifier-adapter")]
pub type ExecutableStarknetTransaction = starknet_api::executable_transaction::Transaction;

#[cfg(feature = "blockifier-adapter")]
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TransactionValidationError {
    #[error("invalid tx hash '{hash}': {error}")]
    InvalidHash { hash: String, error: String },
    #[error("tx hash mismatch: declared {declared}, executable {executable}")]
    HashMismatch {
        declared: String,
        executable: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StarknetTransaction {
    pub hash: TxHash,
    #[cfg(feature = "blockifier-adapter")]
    #[serde(default)]
    pub executable: Option<ExecutableStarknetTransaction>,
}

impl StarknetTransaction {
    pub fn new(hash: impl Into<TxHash>) -> Self {
        Self {
            hash: hash.into(),
            #[cfg(feature = "blockifier-adapter")]
            executable: None,
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    pub fn with_executable(
        hash: impl Into<TxHash>,
        executable: ExecutableStarknetTransaction,
    ) -> Self {
        Self::try_with_executable(hash, executable)
            .expect("hash in StarknetTransaction::with_executable must match executable payload")
    }

    #[cfg(feature = "blockifier-adapter")]
    pub fn try_with_executable(
        hash: impl Into<TxHash>,
        executable: ExecutableStarknetTransaction,
    ) -> Result<Self, TransactionValidationError> {
        let hash = hash.into();
        let declared = ExecutableFelt::from_str(&hash).map_err(|error| {
            TransactionValidationError::InvalidHash {
                hash: hash.clone(),
                error: error.to_string(),
            }
        })?;
        let executable_hash = executable.tx_hash().0;
        if executable_hash != declared {
            return Err(TransactionValidationError::HashMismatch {
                declared: format!("{:#x}", declared),
                executable: format!("{:#x}", executable_hash),
            });
        }
        Ok(Self {
            hash,
            executable: Some(executable),
        })
    }
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
    pub storage_diffs: BTreeMap<ContractAddress, BTreeMap<String, StarknetFelt>>,
    pub nonces: BTreeMap<ContractAddress, StarknetFelt>,
    pub declared_classes: Vec<ClassHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GasPricePerToken {
    pub price_in_fri: u128,
    pub price_in_wei: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockGasPrices {
    pub l1_gas: GasPricePerToken,
    pub l1_data_gas: GasPricePerToken,
    pub l2_gas: GasPricePerToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StarknetBlock {
    pub number: BlockNumber,
    pub parent_hash: BlockHash,
    pub state_root: StateRoot,
    pub timestamp: u64,
    pub sequencer_address: ContractAddress,
    pub gas_prices: BlockGasPrices,
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
    pub estimated_fee: u128,
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
    fn get_storage(&self, contract: &ContractAddress, key: &str) -> Option<StarknetFelt>;
    fn nonce_of(&self, contract: &ContractAddress) -> Option<StarknetFelt>;
}

pub trait MutableState: StateReader {
    fn set_storage(&mut self, contract: ContractAddress, key: String, value: StarknetFelt);
    fn set_nonce(&mut self, contract: ContractAddress, nonce: StarknetFelt);
    fn boxed_clone(&self) -> Box<dyn MutableState>;
}

fn normalize_felt_hex(input: &str) -> String {
    StarknetFelt::from_str(input)
        .map(|felt| format!("{:#x}", felt))
        .unwrap_or_else(|_| input.to_string())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InMemoryState {
    pub storage: Arc<BTreeMap<ContractAddress, BTreeMap<String, StarknetFelt>>>,
    pub nonces: Arc<BTreeMap<ContractAddress, StarknetFelt>>,
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
    fn get_storage(&self, contract: &ContractAddress, key: &str) -> Option<StarknetFelt> {
        let contract = normalize_felt_hex(contract);
        let key = normalize_felt_hex(key);
        self.storage
            .get(&contract)
            .and_then(|slots| slots.get(&key).copied())
    }

    fn nonce_of(&self, contract: &ContractAddress) -> Option<StarknetFelt> {
        let contract = normalize_felt_hex(contract);
        self.nonces.get(&contract).copied()
    }
}

impl MutableState for InMemoryState {
    fn set_storage(&mut self, contract: ContractAddress, key: String, value: StarknetFelt) {
        let contract = normalize_felt_hex(&contract);
        let key = normalize_felt_hex(&key);
        Arc::make_mut(&mut self.storage)
            .entry(contract)
            .or_default()
            .insert(key, value);
    }

    fn set_nonce(&mut self, contract: ContractAddress, nonce: StarknetFelt) {
        let contract = normalize_felt_hex(&contract);
        Arc::make_mut(&mut self.nonces).insert(contract, nonce);
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
    #[cfg(feature = "blockifier-adapter")]
    use starknet_api::executable_transaction::{
        L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
    };
    #[cfg(feature = "blockifier-adapter")]
    use starknet_api::transaction::TransactionHash;

    #[test]
    fn applies_state_diff_into_state() {
        let mut state = InMemoryState::default();
        let mut diff = StarknetStateDiff::default();
        diff.storage_diffs
            .entry("0xabc".to_string())
            .or_default()
            .insert("balance".to_string(), StarknetFelt::from(7_u64));
        diff.nonces
            .insert("0xabc".to_string(), StarknetFelt::from(2_u64));

        state.apply_state_diff(&diff);

        assert_eq!(
            state.get_storage(&"0xabc".to_string(), "balance"),
            Some(StarknetFelt::from(7_u64))
        );
        assert_eq!(
            state.nonce_of(&"0xabc".to_string()),
            Some(StarknetFelt::from(2_u64))
        );
    }

    #[test]
    fn normalizes_equivalent_hex_encodings_for_state_keys() {
        let mut state = InMemoryState::default();
        state.set_storage(
            "0x01".to_string(),
            "0x0002".to_string(),
            StarknetFelt::from(9_u64),
        );
        state.set_nonce("0x0001".to_string(), StarknetFelt::from(3_u64));

        assert_eq!(
            state.get_storage(&"0x1".to_string(), "0x2"),
            Some(StarknetFelt::from(9_u64))
        );
        assert_eq!(
            state.nonce_of(&"0x1".to_string()),
            Some(StarknetFelt::from(3_u64))
        );
    }

    #[test]
    fn clone_shares_maps_until_first_mutation() {
        let mut state = InMemoryState::default();
        state.set_storage(
            "0x1".to_string(),
            "0x2".to_string(),
            StarknetFelt::from(9_u64),
        );
        state.set_nonce("0x1".to_string(), StarknetFelt::from(3_u64));

        let snapshot = state.clone();
        assert_eq!(Arc::as_ptr(&state.storage), Arc::as_ptr(&snapshot.storage));
        assert_eq!(Arc::as_ptr(&state.nonces), Arc::as_ptr(&snapshot.nonces));

        state.set_storage(
            "0x1".to_string(),
            "0x3".to_string(),
            StarknetFelt::from(11_u64),
        );
        assert_ne!(Arc::as_ptr(&state.storage), Arc::as_ptr(&snapshot.storage));
        assert_eq!(Arc::as_ptr(&state.nonces), Arc::as_ptr(&snapshot.nonces));
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn validates_hash_against_executable_payload() {
        let executable = ExecutableL1Handler {
            tx_hash: TransactionHash(
                ExecutableFelt::from_str("0xabc").expect("valid executable hash"),
            ),
            tx: starknet_api::transaction::L1HandlerTransaction {
                calldata: vec![Default::default()].into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let tx =
            StarknetTransaction::try_with_executable("0xabc", ExecutableTx::L1Handler(executable))
                .expect("matching hashes");
        assert_eq!(tx.hash, "0xabc");
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn rejects_mismatched_hash_and_executable_payload() {
        let executable = ExecutableL1Handler {
            tx_hash: TransactionHash(
                ExecutableFelt::from_str("0xabc").expect("valid executable hash"),
            ),
            tx: starknet_api::transaction::L1HandlerTransaction {
                calldata: vec![Default::default()].into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let err =
            StarknetTransaction::try_with_executable("0xdef", ExecutableTx::L1Handler(executable))
                .expect_err("must reject mismatched hash");
        assert!(matches!(
            err,
            TransactionValidationError::HashMismatch { .. }
        ));
    }
}
