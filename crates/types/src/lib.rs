#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Deref;
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
pub type StarknetFelt = Felt;

macro_rules! felt_identifier {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn parse(raw: impl AsRef<str>) -> Result<Self, IdentifierValidationError> {
                let canonical = canonicalize_felt_hex(stringify!($name), raw.as_ref())?;
                Ok(Self(canonical))
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

felt_identifier!(TxHash);
felt_identifier!(ClassHash);
felt_identifier!(ContractAddress);

pub const MAX_TRANSACTIONS_PER_BLOCK: usize = 100_000;
pub const MAX_STORAGE_WRITES_PER_STATE_DIFF: usize = 500_000;
pub const MAX_NONCE_UPDATES_PER_STATE_DIFF: usize = 100_000;
pub const MAX_DECLARED_CLASSES_PER_STATE_DIFF: usize = 50_000;
pub const MAX_REASONABLE_BLOCK_TIMESTAMP: u64 = 4_000_000_000;
pub const MAX_CONTRACTS_IN_MEMORY_STATE: usize = 100_000;
pub const MAX_STORAGE_SLOTS_PER_CONTRACT: usize = 1_000_000;
pub const MAX_TOTAL_STORAGE_ENTRIES: usize = 10_000_000;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IdentifierValidationError {
    #[error("invalid {field} '{value}': {error}")]
    InvalidHexFelt {
        field: &'static str,
        value: String,
        error: String,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BlockValidationError {
    #[error("block {number} has {count} transactions, exceeding limit {max}")]
    TooManyTransactions {
        number: BlockNumber,
        count: usize,
        max: usize,
    },
    #[error("invalid {field} in block {number}: {source}")]
    InvalidIdentifier {
        number: BlockNumber,
        field: &'static str,
        source: IdentifierValidationError,
    },
    #[error("invalid timestamp {timestamp} for block {number}: {reason}")]
    InvalidTimestamp {
        number: BlockNumber,
        timestamp: u64,
        reason: &'static str,
    },
    #[error("invalid zero gas price for {field} in block {number}")]
    ZeroGasPrice {
        number: BlockNumber,
        field: &'static str,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StateDiffValidationError {
    #[error("state diff has {count} storage writes, exceeding limit {max}")]
    TooManyStorageWrites { count: usize, max: usize },
    #[error("state diff has {count} nonce updates, exceeding limit {max}")]
    TooManyNonceUpdates { count: usize, max: usize },
    #[error("state diff has {count} declared classes, exceeding limit {max}")]
    TooManyDeclaredClasses { count: usize, max: usize },
    #[error("invalid {field} in state diff: {source}")]
    InvalidIdentifier {
        field: &'static str,
        source: IdentifierValidationError,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StateLimitError {
    #[error(transparent)]
    InvalidStateDiff(#[from] StateDiffValidationError),
    #[error("in-memory state contract count {count} exceeds limit {max}")]
    TooManyContracts { count: usize, max: usize },
    #[error(
        "in-memory state contract '{contract}' has {count} storage slots, exceeding limit {max}"
    )]
    TooManyStorageSlotsForContract {
        contract: ContractAddress,
        count: usize,
        max: usize,
    },
    #[error("in-memory state has {count} storage entries, exceeding limit {max}")]
    TooManyStorageEntries { count: usize, max: usize },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StateReadError {
    #[error("state read backend failure: {0}")]
    Backend(String),
}

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

fn canonicalize_felt_hex(
    field: &'static str,
    value: &str,
) -> Result<String, IdentifierValidationError> {
    let normalized = value.trim();
    let prefixed = if normalized.starts_with("0x") || normalized.starts_with("0X") {
        normalized.to_string()
    } else {
        format!("0x{normalized}")
    };
    StarknetFelt::from_str(&prefixed)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| IdentifierValidationError::InvalidHexFelt {
            field,
            value: value.to_string(),
            error: error.to_string(),
        })
}

fn validate_hex_felt(field: &'static str, value: &str) -> Result<(), IdentifierValidationError> {
    canonicalize_felt_hex(field, value).map(|_| ())
}

impl StarknetTransaction {
    pub fn new(hash: impl Into<TxHash>) -> Self {
        Self {
            hash: hash.into(),
            #[cfg(feature = "blockifier-adapter")]
            executable: None,
        }
    }

    pub fn validate_hash(&self) -> Result<(), IdentifierValidationError> {
        validate_hex_felt("tx_hash", self.hash.as_ref())
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
        let declared = ExecutableFelt::from_str(hash.as_ref()).map_err(|error| {
            TransactionValidationError::InvalidHash {
                hash: hash.to_string(),
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

impl StarknetStateDiff {
    pub fn validate(&self) -> Result<(), StateDiffValidationError> {
        let mut total_storage_writes: usize = 0;
        for (contract, writes) in &self.storage_diffs {
            validate_hex_felt("contract_address", contract).map_err(|source| {
                StateDiffValidationError::InvalidIdentifier {
                    field: "contract_address",
                    source,
                }
            })?;
            for key in writes.keys() {
                validate_hex_felt("storage_key", key).map_err(|source| {
                    StateDiffValidationError::InvalidIdentifier {
                        field: "storage_key",
                        source,
                    }
                })?;
            }
            total_storage_writes = total_storage_writes.checked_add(writes.len()).ok_or(
                StateDiffValidationError::TooManyStorageWrites {
                    count: usize::MAX,
                    max: MAX_STORAGE_WRITES_PER_STATE_DIFF,
                },
            )?;
        }
        if total_storage_writes > MAX_STORAGE_WRITES_PER_STATE_DIFF {
            return Err(StateDiffValidationError::TooManyStorageWrites {
                count: total_storage_writes,
                max: MAX_STORAGE_WRITES_PER_STATE_DIFF,
            });
        }

        if self.nonces.len() > MAX_NONCE_UPDATES_PER_STATE_DIFF {
            return Err(StateDiffValidationError::TooManyNonceUpdates {
                count: self.nonces.len(),
                max: MAX_NONCE_UPDATES_PER_STATE_DIFF,
            });
        }
        for contract in self.nonces.keys() {
            validate_hex_felt("nonce_contract_address", contract).map_err(|source| {
                StateDiffValidationError::InvalidIdentifier {
                    field: "nonce_contract_address",
                    source,
                }
            })?;
        }

        if self.declared_classes.len() > MAX_DECLARED_CLASSES_PER_STATE_DIFF {
            return Err(StateDiffValidationError::TooManyDeclaredClasses {
                count: self.declared_classes.len(),
                max: MAX_DECLARED_CLASSES_PER_STATE_DIFF,
            });
        }
        for class_hash in &self.declared_classes {
            validate_hex_felt("class_hash", class_hash).map_err(|source| {
                StateDiffValidationError::InvalidIdentifier {
                    field: "class_hash",
                    source,
                }
            })?;
        }

        Ok(())
    }
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

impl StarknetBlock {
    fn validate_non_zero_gas_prices(&self) -> Result<(), BlockValidationError> {
        let checks = [
            ("l1_gas.price_in_fri", self.gas_prices.l1_gas.price_in_fri),
            ("l1_gas.price_in_wei", self.gas_prices.l1_gas.price_in_wei),
            (
                "l1_data_gas.price_in_fri",
                self.gas_prices.l1_data_gas.price_in_fri,
            ),
            (
                "l1_data_gas.price_in_wei",
                self.gas_prices.l1_data_gas.price_in_wei,
            ),
            ("l2_gas.price_in_fri", self.gas_prices.l2_gas.price_in_fri),
            ("l2_gas.price_in_wei", self.gas_prices.l2_gas.price_in_wei),
        ];
        for (field, value) in checks {
            if value == 0 {
                return Err(BlockValidationError::ZeroGasPrice {
                    number: self.number,
                    field,
                });
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), BlockValidationError> {
        if self.transactions.len() > MAX_TRANSACTIONS_PER_BLOCK {
            return Err(BlockValidationError::TooManyTransactions {
                number: self.number,
                count: self.transactions.len(),
                max: MAX_TRANSACTIONS_PER_BLOCK,
            });
        }

        if self.timestamp == 0 {
            return Err(BlockValidationError::InvalidTimestamp {
                number: self.number,
                timestamp: self.timestamp,
                reason: "timestamp cannot be zero",
            });
        }
        if self.timestamp > MAX_REASONABLE_BLOCK_TIMESTAMP {
            return Err(BlockValidationError::InvalidTimestamp {
                number: self.number,
                timestamp: self.timestamp,
                reason: "timestamp exceeds sane upper bound",
            });
        }

        self.validate_non_zero_gas_prices()?;

        validate_hex_felt("parent_hash", &self.parent_hash).map_err(|source| {
            BlockValidationError::InvalidIdentifier {
                number: self.number,
                field: "parent_hash",
                source,
            }
        })?;
        validate_hex_felt("state_root", &self.state_root).map_err(|source| {
            BlockValidationError::InvalidIdentifier {
                number: self.number,
                field: "state_root",
                source,
            }
        })?;
        validate_hex_felt("sequencer_address", self.sequencer_address.as_ref()).map_err(
            |source| BlockValidationError::InvalidIdentifier {
                number: self.number,
                field: "sequencer_address",
                source,
            },
        )?;

        for tx in &self.transactions {
            tx.validate_hash()
                .map_err(|source| BlockValidationError::InvalidIdentifier {
                    number: self.number,
                    field: "tx_hash",
                    source,
                })?;
        }

        Ok(())
    }
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
    fn get_storage(
        &self,
        contract: &ContractAddress,
        key: &str,
    ) -> Result<Option<StarknetFelt>, StateReadError>;
    fn nonce_of(&self, contract: &ContractAddress) -> Result<Option<StarknetFelt>, StateReadError>;
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
    pub fn apply_state_diff(&mut self, diff: &StarknetStateDiff) -> Result<(), StateLimitError> {
        diff.validate()?;

        let mut contract_count = self.storage.len();
        let mut total_storage_entries = self
            .storage
            .values()
            .fold(0usize, |acc, slots| acc.saturating_add(slots.len()));
        let mut new_slots_by_contract: BTreeMap<ContractAddress, usize> = BTreeMap::new();
        let mut seen_new_slots: std::collections::BTreeSet<(ContractAddress, String)> =
            std::collections::BTreeSet::new();

        for (contract, writes) in &diff.storage_diffs {
            let normalized_contract = ContractAddress::from(normalize_felt_hex(contract));
            if !self.storage.contains_key(&normalized_contract)
                && !new_slots_by_contract.contains_key(&normalized_contract)
            {
                contract_count = contract_count.saturating_add(1);
                if contract_count > MAX_CONTRACTS_IN_MEMORY_STATE {
                    return Err(StateLimitError::TooManyContracts {
                        count: contract_count,
                        max: MAX_CONTRACTS_IN_MEMORY_STATE,
                    });
                }
            }

            for (key, value) in writes {
                let normalized_key = normalize_felt_hex(key);
                let slot_marker = (normalized_contract.clone(), normalized_key.clone());
                if seen_new_slots.contains(&slot_marker) {
                    continue;
                }

                let exists = self
                    .storage
                    .get(&normalized_contract)
                    .and_then(|slots| slots.get(&normalized_key))
                    .is_some();
                if !exists {
                    seen_new_slots.insert(slot_marker);
                    total_storage_entries = total_storage_entries.saturating_add(1);
                    if total_storage_entries > MAX_TOTAL_STORAGE_ENTRIES {
                        return Err(StateLimitError::TooManyStorageEntries {
                            count: total_storage_entries,
                            max: MAX_TOTAL_STORAGE_ENTRIES,
                        });
                    }
                    let per_contract = new_slots_by_contract
                        .entry(normalized_contract.clone())
                        .or_insert(0);
                    *per_contract = per_contract.saturating_add(1);
                    let existing_slots = self
                        .storage
                        .get(&normalized_contract)
                        .map(|slots| slots.len())
                        .unwrap_or_default();
                    let projected_slots = existing_slots.saturating_add(*per_contract);
                    if projected_slots > MAX_STORAGE_SLOTS_PER_CONTRACT {
                        return Err(StateLimitError::TooManyStorageSlotsForContract {
                            contract: normalized_contract.clone(),
                            count: projected_slots,
                            max: MAX_STORAGE_SLOTS_PER_CONTRACT,
                        });
                    }
                }

                self.set_storage(contract.clone(), key.clone(), *value);
            }
        }
        for (contract, nonce) in &diff.nonces {
            self.set_nonce(contract.clone(), *nonce);
        }
        Ok(())
    }
}

impl StateReader for InMemoryState {
    fn get_storage(
        &self,
        contract: &ContractAddress,
        key: &str,
    ) -> Result<Option<StarknetFelt>, StateReadError> {
        let contract = ContractAddress::from(normalize_felt_hex(contract));
        let key = normalize_felt_hex(key);
        Ok(self
            .storage
            .get(&contract)
            .and_then(|slots| slots.get(&key).copied()))
    }

    fn nonce_of(&self, contract: &ContractAddress) -> Result<Option<StarknetFelt>, StateReadError> {
        let contract = ContractAddress::from(normalize_felt_hex(contract));
        Ok(self.nonces.get(&contract).copied())
    }
}

impl MutableState for InMemoryState {
    fn set_storage(&mut self, contract: ContractAddress, key: String, value: StarknetFelt) {
        let contract = ContractAddress::from(normalize_felt_hex(&contract));
        let key = normalize_felt_hex(&key);
        Arc::make_mut(&mut self.storage)
            .entry(contract)
            .or_default()
            .insert(key, value);
    }

    fn set_nonce(&mut self, contract: ContractAddress, nonce: StarknetFelt) {
        let contract = ContractAddress::from(normalize_felt_hex(&contract));
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
    use std::collections::BTreeMap;

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
            .entry(ContractAddress::from("0xabc"))
            .or_default()
            .insert("0x2".to_string(), StarknetFelt::from(7_u64));
        diff.nonces
            .insert(ContractAddress::from("0xabc"), StarknetFelt::from(2_u64));

        state.apply_state_diff(&diff).expect("valid state diff");

        assert_eq!(
            state.get_storage(&ContractAddress::from("0xabc"), "0x2"),
            Ok(Some(StarknetFelt::from(7_u64)))
        );
        assert_eq!(
            state.nonce_of(&ContractAddress::from("0xabc")),
            Ok(Some(StarknetFelt::from(2_u64)))
        );
    }

    #[test]
    fn normalizes_equivalent_hex_encodings_for_state_keys() {
        let mut state = InMemoryState::default();
        state.set_storage(
            ContractAddress::from("0x01"),
            "0x0002".to_string(),
            StarknetFelt::from(9_u64),
        );
        state.set_nonce(ContractAddress::from("0x0001"), StarknetFelt::from(3_u64));

        assert_eq!(
            state.get_storage(&ContractAddress::from("0x1"), "0x2"),
            Ok(Some(StarknetFelt::from(9_u64)))
        );
        assert_eq!(
            state.nonce_of(&ContractAddress::from("0x1")),
            Ok(Some(StarknetFelt::from(3_u64)))
        );
    }

    #[test]
    fn clone_shares_maps_until_first_mutation() {
        let mut state = InMemoryState::default();
        state.set_storage(
            ContractAddress::from("0x1"),
            "0x2".to_string(),
            StarknetFelt::from(9_u64),
        );
        state.set_nonce(ContractAddress::from("0x1"), StarknetFelt::from(3_u64));

        let snapshot = state.clone();
        assert_eq!(Arc::as_ptr(&state.storage), Arc::as_ptr(&snapshot.storage));
        assert_eq!(Arc::as_ptr(&state.nonces), Arc::as_ptr(&snapshot.nonces));

        state.set_storage(
            ContractAddress::from("0x1"),
            "0x3".to_string(),
            StarknetFelt::from(11_u64),
        );
        assert_ne!(Arc::as_ptr(&state.storage), Arc::as_ptr(&snapshot.storage));
        assert_eq!(Arc::as_ptr(&state.nonces), Arc::as_ptr(&snapshot.nonces));
    }

    #[test]
    fn rejects_blocks_with_too_many_transactions() {
        let mut txs = Vec::with_capacity(MAX_TRANSACTIONS_PER_BLOCK + 1);
        for i in 0..(MAX_TRANSACTIONS_PER_BLOCK + 1) {
            txs.push(StarknetTransaction::new(format!("0x{i:x}")));
        }
        let block = StarknetBlock {
            number: 1,
            parent_hash: "0x0".to_string(),
            state_root: "0x1".to_string(),
            timestamp: 1_700_000_001,
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
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: txs,
        };

        let err = block.validate().expect_err("must reject oversized block");
        assert!(matches!(
            err,
            BlockValidationError::TooManyTransactions { .. }
        ));
    }

    #[test]
    fn rejects_state_diffs_with_too_many_writes() {
        let mut diff = StarknetStateDiff::default();
        let mut writes = BTreeMap::new();
        for i in 0..(MAX_STORAGE_WRITES_PER_STATE_DIFF + 1) {
            writes.insert(format!("0x{:x}", i + 1), StarknetFelt::from(i as u64));
        }
        diff.storage_diffs
            .insert(ContractAddress::from("0x1"), writes);

        let err = diff
            .validate()
            .expect_err("must reject oversized state diff");
        assert!(matches!(
            err,
            StateDiffValidationError::TooManyStorageWrites { .. }
        ));
    }

    #[test]
    fn rejects_state_diffs_with_invalid_storage_keys() {
        let mut diff = StarknetStateDiff::default();
        diff.storage_diffs
            .entry(ContractAddress::from("0x1"))
            .or_default()
            .insert("not-a-felt".to_string(), StarknetFelt::from(1_u64));

        let err = diff
            .validate()
            .expect_err("must reject invalid storage key");
        assert!(matches!(
            err,
            StateDiffValidationError::InvalidIdentifier {
                field: "storage_key",
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_transaction_hashes() {
        let tx = StarknetTransaction::new("not-a-hash");
        let err = tx.validate_hash().expect_err("must reject invalid felt");
        assert!(matches!(
            err,
            IdentifierValidationError::InvalidHexFelt { .. }
        ));
    }

    #[test]
    fn rejects_blocks_with_zero_gas_prices() {
        let block = StarknetBlock {
            number: 1,
            parent_hash: "0x0".to_string(),
            state_root: "0x1".to_string(),
            timestamp: 1_700_000_001,
            sequencer_address: ContractAddress::from("0x1"),
            gas_prices: BlockGasPrices {
                l1_gas: GasPricePerToken {
                    price_in_fri: 0,
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
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: vec![StarknetTransaction::new("0x1")],
        };

        let err = block.validate().expect_err("must reject zero gas prices");
        assert!(matches!(err, BlockValidationError::ZeroGasPrice { .. }));
    }

    #[test]
    fn rejects_blocks_with_invalid_timestamps() {
        let gas_prices = BlockGasPrices {
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
        };

        let zero = StarknetBlock {
            number: 1,
            parent_hash: "0x0".to_string(),
            state_root: "0x1".to_string(),
            timestamp: 0,
            sequencer_address: ContractAddress::from("0x1"),
            gas_prices,
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: vec![StarknetTransaction::new("0x1")],
        };
        assert!(matches!(
            zero.validate().expect_err("zero timestamp must fail"),
            BlockValidationError::InvalidTimestamp { .. }
        ));

        let future = StarknetBlock {
            timestamp: MAX_REASONABLE_BLOCK_TIMESTAMP + 1,
            ..zero
        };
        assert!(matches!(
            future.validate().expect_err("future timestamp must fail"),
            BlockValidationError::InvalidTimestamp { .. }
        ));
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
        assert_eq!(tx.hash, "0xabc".into());
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
