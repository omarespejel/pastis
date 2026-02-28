#![forbid(unsafe_code)]

use std::collections::BTreeMap;
#[cfg(feature = "apollo-adapter")]
use std::str::FromStr;
use std::sync::{Arc, RwLock};

#[cfg(feature = "apollo-adapter")]
use apollo_storage::body::BodyStorageReader;
#[cfg(feature = "apollo-adapter")]
use apollo_storage::header::HeaderStorageReader;
#[cfg(feature = "apollo-adapter")]
use apollo_storage::state::StateStorageReader;
#[cfg(feature = "apollo-adapter")]
use apollo_storage::{
    StorageConfig as ApolloStorageConfig, StorageReader as ApolloStorageReader,
    StorageWriter as ApolloStorageWriter, open_storage as open_apollo_storage,
};
#[cfg(feature = "apollo-adapter")]
use starknet_api::block::BlockNumber as ApolloBlockNumber;
#[cfg(feature = "apollo-adapter")]
use starknet_api::core::{ContractAddress as ApolloContractAddress, Nonce as ApolloNonce};
#[cfg(feature = "apollo-adapter")]
use starknet_api::hash::StarkHash as ApolloFelt;
#[cfg(feature = "apollo-adapter")]
use starknet_api::state::ThinStateDiff as ApolloThinStateDiff;
#[cfg(feature = "apollo-adapter")]
use starknet_api::state::{StateNumber as ApolloStateNumber, StorageKey as ApolloStorageKey};

#[cfg(feature = "apollo-adapter")]
use starknet_node_types::ContractAddress;
#[cfg(feature = "apollo-adapter")]
use starknet_node_types::StarknetFelt;
#[cfg(feature = "apollo-adapter")]
use starknet_node_types::StarknetTransaction;
#[cfg(feature = "apollo-adapter")]
use starknet_node_types::StateReadError;
#[cfg(feature = "apollo-adapter")]
use starknet_node_types::{BlockGasPrices, GasPricePerToken};
use starknet_node_types::{
    BlockId, BlockNumber, ComponentHealth, HealthCheck, HealthStatus, InMemoryState, StarknetBlock,
    StarknetStateDiff, StateReader,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("block {0} does not extend current tip")]
    NonSequentialBlock(BlockNumber),
    #[error("block number overflow while extending chain tip {tip}")]
    BlockNumberOverflow { tip: BlockNumber },
    #[error("requested block {requested} is beyond available state at block {latest}")]
    BlockOutOfRange {
        requested: BlockNumber,
        latest: BlockNumber,
    },
    #[error(
        "block {block} timestamp {timestamp} is earlier than parent timestamp {parent_timestamp}"
    )]
    NonMonotonicTimestamp {
        block: BlockNumber,
        timestamp: u64,
        parent_timestamp: u64,
    },
    #[error("operation not supported by backend: {0}")]
    UnsupportedOperation(&'static str),
    #[error("invalid felt encoding for {field}: {value} ({error})")]
    InvalidFeltEncoding {
        field: &'static str,
        value: String,
        error: String,
    },
    #[error("invalid block: {0}")]
    InvalidBlock(String),
    #[error("invalid state diff: {0}")]
    InvalidStateDiff(String),
    #[error("state limits exceeded while applying diff: {0}")]
    StateLimitExceeded(String),
    #[cfg(feature = "apollo-adapter")]
    #[error("apollo storage error: {0}")]
    Apollo(String),
    #[cfg(feature = "apollo-adapter")]
    #[error("invalid protocol version in apollo header: {0}")]
    InvalidProtocolVersion(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CheckpointError {
    #[error("state root mismatch: expected {expected}, actual {actual}")]
    StateRootMismatch { expected: String, actual: String },
    #[error("checkpoint verification requires canonical state roots from backend '{backend}'")]
    UnsupportedStateRootSemantics { backend: String },
    #[error("failed to read canonical state root from backend: {error}")]
    StateRootReadFailed { error: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateRootSemantics {
    Canonical,
    Approximate,
}

pub trait StorageBackend: Send + Sync + HealthCheck {
    /// Implementations expose mutating methods via `&mut self`.
    /// For shared concurrent access, callers should only expose `ThreadSafeStorage`
    /// to avoid leaking unsynchronized backend instances.
    fn get_state_reader(
        &self,
        block_number: BlockNumber,
    ) -> Result<Box<dyn StateReader>, StorageError>;
    fn apply_state_diff(&mut self, diff: &StarknetStateDiff) -> Result<(), StorageError>;
    fn insert_block(
        &mut self,
        block: StarknetBlock,
        state_diff: StarknetStateDiff,
    ) -> Result<(), StorageError>;
    fn get_block(&self, id: BlockId) -> Result<Option<StarknetBlock>, StorageError>;
    fn get_state_diff(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<StarknetStateDiff>, StorageError>;
    fn latest_block_number(&self) -> Result<BlockNumber, StorageError>;
    fn current_state_root(&self) -> Result<String, StorageError>;
    fn state_root_semantics(&self) -> StateRootSemantics {
        StateRootSemantics::Canonical
    }
}

#[derive(Debug, Clone)]
pub struct ThreadSafeStorage<S> {
    inner: Arc<RwLock<S>>,
}

impl<S> ThreadSafeStorage<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl<S: StorageBackend> HealthCheck for ThreadSafeStorage<S> {
    fn is_healthy(&self) -> bool {
        self.inner
            .read()
            .map(|guard| guard.is_healthy())
            .unwrap_or(false)
    }

    fn detailed_status(&self) -> ComponentHealth {
        match self.inner.read() {
            Ok(guard) => {
                let mut health = guard.detailed_status();
                health.name = format!("thread-safe-{}", health.name);
                health
            }
            Err(_) => ComponentHealth {
                name: "thread-safe-storage".to_string(),
                status: HealthStatus::Unhealthy,
                last_block_processed: None,
                sync_lag: None,
                error: Some("thread-safe storage lock poisoned".to_string()),
            },
        }
    }
}

impl<S: StorageBackend> StorageBackend for ThreadSafeStorage<S> {
    fn get_state_reader(
        &self,
        block_number: BlockNumber,
    ) -> Result<Box<dyn StateReader>, StorageError> {
        let guard = self
            .inner
            .read()
            .map_err(|_| StorageError::UnsupportedOperation("thread-safe storage lock poisoned"))?;
        guard.get_state_reader(block_number)
    }

    fn apply_state_diff(&mut self, diff: &StarknetStateDiff) -> Result<(), StorageError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| StorageError::UnsupportedOperation("thread-safe storage lock poisoned"))?;
        guard.apply_state_diff(diff)
    }

    fn insert_block(
        &mut self,
        block: StarknetBlock,
        state_diff: StarknetStateDiff,
    ) -> Result<(), StorageError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| StorageError::UnsupportedOperation("thread-safe storage lock poisoned"))?;
        guard.insert_block(block, state_diff)
    }

    fn get_block(&self, id: BlockId) -> Result<Option<StarknetBlock>, StorageError> {
        let guard = self
            .inner
            .read()
            .map_err(|_| StorageError::UnsupportedOperation("thread-safe storage lock poisoned"))?;
        guard.get_block(id)
    }

    fn get_state_diff(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<StarknetStateDiff>, StorageError> {
        let guard = self
            .inner
            .read()
            .map_err(|_| StorageError::UnsupportedOperation("thread-safe storage lock poisoned"))?;
        guard.get_state_diff(block_number)
    }

    fn latest_block_number(&self) -> Result<BlockNumber, StorageError> {
        let guard = self
            .inner
            .read()
            .map_err(|_| StorageError::UnsupportedOperation("thread-safe storage lock poisoned"))?;
        guard.latest_block_number()
    }

    fn current_state_root(&self) -> Result<String, StorageError> {
        let guard = self
            .inner
            .read()
            .map_err(|_| StorageError::UnsupportedOperation("thread-safe storage lock poisoned"))?;
        guard.current_state_root()
    }

    fn state_root_semantics(&self) -> StateRootSemantics {
        self.inner
            .read()
            .map(|guard| guard.state_root_semantics())
            .unwrap_or(StateRootSemantics::Approximate)
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    // Lightweight backend for tests and local development.
    // This backend is not consensus-canonical for state roots. Use `ApolloStorageAdapter`
    // for canonical roots and wrap in `ThreadSafeStorage` for shared multi-thread access.
    blocks: BTreeMap<BlockNumber, StarknetBlock>,
    state_diffs: BTreeMap<BlockNumber, StarknetStateDiff>,
    states: BTreeMap<BlockNumber, InMemoryState>,
}

impl InMemoryStorage {
    pub fn new(genesis_state: InMemoryState) -> Self {
        let mut states = BTreeMap::new();
        states.insert(0, genesis_state);
        Self {
            blocks: BTreeMap::new(),
            state_diffs: BTreeMap::new(),
            states,
        }
    }
}

impl HealthCheck for InMemoryStorage {
    fn is_healthy(&self) -> bool {
        true
    }

    fn detailed_status(&self) -> ComponentHealth {
        ComponentHealth {
            name: "in-memory-storage".to_string(),
            status: HealthStatus::Healthy,
            last_block_processed: self.states.keys().max().copied(),
            sync_lag: None,
            error: None,
        }
    }
}

impl StorageBackend for InMemoryStorage {
    fn get_state_reader(
        &self,
        block_number: BlockNumber,
    ) -> Result<Box<dyn StateReader>, StorageError> {
        let latest = self.states.keys().next_back().copied().unwrap_or(0);
        if block_number > latest {
            return Err(StorageError::BlockOutOfRange {
                requested: block_number,
                latest,
            });
        }
        let snapshot = self
            .states
            .range(..=block_number)
            .next_back()
            .map(|(_, state)| state.clone())
            .unwrap_or_default();
        Ok(Box::new(snapshot))
    }

    fn apply_state_diff(&mut self, diff: &StarknetStateDiff) -> Result<(), StorageError> {
        diff.validate()
            .map_err(|error| StorageError::InvalidStateDiff(error.to_string()))?;
        let tip = self.latest_block_number()?;
        self.states
            .entry(tip)
            .or_default()
            .apply_state_diff(diff)
            .map_err(|error| StorageError::StateLimitExceeded(error.to_string()))?;
        Ok(())
    }

    fn insert_block(
        &mut self,
        block: StarknetBlock,
        state_diff: StarknetStateDiff,
    ) -> Result<(), StorageError> {
        block
            .validate()
            .map_err(|error| StorageError::InvalidBlock(error.to_string()))?;
        state_diff
            .validate()
            .map_err(|error| StorageError::InvalidStateDiff(error.to_string()))?;

        let tip = self.latest_block_number()?;
        let expected = tip
            .checked_add(1)
            .ok_or(StorageError::BlockNumberOverflow { tip })?;
        if block.number != expected {
            return Err(StorageError::NonSequentialBlock(block.number));
        }

        let parent = expected
            .checked_sub(1)
            .ok_or(StorageError::BlockNumberOverflow { tip })?;
        let mut next_state = self.states.get(&parent).cloned().unwrap_or_default();
        next_state
            .apply_state_diff(&state_diff)
            .map_err(|error| StorageError::StateLimitExceeded(error.to_string()))?;
        if let Some(parent_block) = self.blocks.get(&parent)
            && block.timestamp < parent_block.timestamp
        {
            return Err(StorageError::NonMonotonicTimestamp {
                block: block.number,
                timestamp: block.timestamp,
                parent_timestamp: parent_block.timestamp,
            });
        }

        self.blocks.insert(block.number, block);
        self.state_diffs.insert(expected, state_diff);
        self.states.insert(expected, next_state);
        Ok(())
    }

    fn get_block(&self, id: BlockId) -> Result<Option<StarknetBlock>, StorageError> {
        let block = match id {
            BlockId::Number(number) => self.blocks.get(&number).cloned(),
            BlockId::Latest => self
                .blocks
                .iter()
                .next_back()
                .map(|(_, block)| block.clone()),
        };
        Ok(block)
    }

    fn get_state_diff(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<StarknetStateDiff>, StorageError> {
        Ok(self.state_diffs.get(&block_number).cloned())
    }

    fn latest_block_number(&self) -> Result<BlockNumber, StorageError> {
        Ok(self.blocks.keys().next_back().copied().unwrap_or(0))
    }

    fn current_state_root(&self) -> Result<String, StorageError> {
        // In-memory backend does not maintain Starknet's Patricia tries. Returning a sentinel
        // prevents accidental treatment of this value as a consensus state commitment.
        Ok("0x0".to_string())
    }

    fn state_root_semantics(&self) -> StateRootSemantics {
        StateRootSemantics::Approximate
    }
}

pub struct CheckpointSyncVerifier;

impl CheckpointSyncVerifier {
    pub fn verify(
        storage: &dyn StorageBackend,
        expected_root: &str,
    ) -> Result<(), CheckpointError> {
        if storage.state_root_semantics() != StateRootSemantics::Canonical {
            return Err(CheckpointError::UnsupportedStateRootSemantics {
                backend: storage.detailed_status().name,
            });
        }
        let actual =
            storage
                .current_state_root()
                .map_err(|error| CheckpointError::StateRootReadFailed {
                    error: error.to_string(),
                })?;
        if actual == expected_root {
            return Ok(());
        }
        Err(CheckpointError::StateRootMismatch {
            expected: expected_root.to_string(),
            actual,
        })
    }
}

#[cfg(feature = "apollo-adapter")]
#[derive(Clone)]
struct ApolloStateReaderAdapter {
    reader: ApolloStorageReader,
    state_number: ApolloStateNumber,
}

#[cfg(feature = "apollo-adapter")]
impl StateReader for ApolloStateReaderAdapter {
    fn get_storage(
        &self,
        contract: &ContractAddress,
        key: &str,
    ) -> Result<Option<StarknetFelt>, StateReadError> {
        let contract = parse_contract_address(contract.as_ref()).ok_or_else(|| {
            StateReadError::Backend(format!("invalid contract address '{contract}'"))
        })?;
        let key = parse_storage_key(key)
            .ok_or_else(|| StateReadError::Backend(format!("invalid storage key '{key}'")))?;
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StateReadError::Backend(error.to_string()))?;
        let state_reader = txn
            .get_state_reader()
            .map_err(|error| StateReadError::Backend(error.to_string()))?;
        let value = state_reader
            .get_storage_at(self.state_number, &contract, &key)
            .map_err(|error| StateReadError::Backend(error.to_string()))?;
        apollo_felt_to_node_felt(value)
            .map(Some)
            .map_err(|error| StateReadError::Backend(error.to_string()))
    }

    fn nonce_of(&self, contract: &ContractAddress) -> Result<Option<StarknetFelt>, StateReadError> {
        let contract = parse_contract_address(contract.as_ref()).ok_or_else(|| {
            StateReadError::Backend(format!("invalid contract address '{contract}'"))
        })?;
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StateReadError::Backend(error.to_string()))?;
        let state_reader = txn
            .get_state_reader()
            .map_err(|error| StateReadError::Backend(error.to_string()))?;
        let nonce: Option<ApolloNonce> = state_reader
            .get_nonce_at(self.state_number, &contract)
            .map_err(|error| StateReadError::Backend(error.to_string()))?;
        nonce
            .map(|nonce| {
                apollo_felt_to_node_felt(nonce.0)
                    .map_err(|error| StateReadError::Backend(error.to_string()))
            })
            .transpose()
    }
}

#[cfg(feature = "apollo-adapter")]
pub struct ApolloStorageAdapter {
    reader: ApolloStorageReader,
    _writer: ApolloStorageWriter,
}

#[cfg(feature = "apollo-adapter")]
impl ApolloStorageAdapter {
    pub fn open(config: ApolloStorageConfig) -> Result<Self, StorageError> {
        let (reader, writer) =
            open_apollo_storage(config).map_err(|error| StorageError::Apollo(error.to_string()))?;
        Ok(Self {
            reader,
            _writer: writer,
        })
    }

    pub fn from_parts(reader: ApolloStorageReader, writer: ApolloStorageWriter) -> Self {
        Self {
            reader,
            _writer: writer,
        }
    }

    pub fn read_current_state_root(&self) -> Result<String, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let marker = txn
            .get_header_marker()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let Some(latest_number) = latest_block_from_marker(marker) else {
            return Ok("0x0".to_string());
        };

        let header = txn
            .get_block_header(latest_number)
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        match header {
            Some(header) => Ok(format!(
                "{:#x}",
                header.block_header_without_hash.state_root.0
            )),
            None => Ok("0x0".to_string()),
        }
    }
}

#[cfg(feature = "apollo-adapter")]
fn latest_block_from_marker(marker: ApolloBlockNumber) -> Option<ApolloBlockNumber> {
    marker.0.checked_sub(1).map(ApolloBlockNumber)
}

#[cfg(feature = "apollo-adapter")]
fn parse_contract_address(raw: &str) -> Option<ApolloContractAddress> {
    let felt = ApolloFelt::from_str(raw).ok()?;
    ApolloContractAddress::try_from(felt).ok()
}

#[cfg(feature = "apollo-adapter")]
fn parse_storage_key(raw: &str) -> Option<ApolloStorageKey> {
    let felt = ApolloFelt::from_str(raw).ok()?;
    ApolloStorageKey::try_from(felt).ok()
}

#[cfg(feature = "apollo-adapter")]
fn apollo_felt_to_node_felt(value: ApolloFelt) -> Result<StarknetFelt, StorageError> {
    let encoded = format!("{:#x}", value);
    StarknetFelt::from_hex(&encoded).map_err(|error| StorageError::InvalidFeltEncoding {
        field: "apollo_felt",
        value: encoded,
        error: error.to_string(),
    })
}

#[cfg(feature = "apollo-adapter")]
fn map_thin_state_diff(diff: ApolloThinStateDiff) -> Result<StarknetStateDiff, StorageError> {
    let mut mapped = StarknetStateDiff::default();
    for (address, writes) in diff.storage_diffs {
        let contract = ContractAddress::from(format!("{:#x}", address.0.key()));
        let mapped_writes = mapped.storage_diffs.entry(contract).or_default();
        for (key, value) in writes {
            mapped_writes.insert(
                format!("{:#x}", key.0.key()),
                apollo_felt_to_node_felt(value)?,
            );
        }
    }
    for (address, nonce) in diff.nonces {
        mapped.nonces.insert(
            format!("{:#x}", address.0.key()).into(),
            apollo_felt_to_node_felt(nonce.0)?,
        );
    }
    for class_hash in diff.class_hash_to_compiled_class_hash.keys() {
        mapped
            .declared_classes
            .push(format!("{:#x}", class_hash.0).into());
    }
    for class_hash in &diff.deprecated_declared_classes {
        mapped
            .declared_classes
            .push(format!("{:#x}", class_hash.0).into());
    }
    Ok(mapped)
}

#[cfg(feature = "apollo-adapter")]
fn parse_starknet_version_to_semver(raw: &str) -> Result<semver::Version, StorageError> {
    if let Ok(version) = semver::Version::parse(raw) {
        return Ok(version);
    }

    let parts: Vec<&str> = raw.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|part| !part.is_empty()) {
        let normalized = format!("{}.{}.{}-{}", parts[0], parts[1], parts[2], parts[3]);
        return semver::Version::parse(&normalized).map_err(|error| {
            StorageError::InvalidProtocolVersion(format!(
                "failed to normalize 4-component version '{raw}': {error}"
            ))
        });
    }

    Err(StorageError::InvalidProtocolVersion(format!(
        "unsupported Starknet version format '{raw}'; expected semver (x.y.z) or x.y.z.w"
    )))
}

#[cfg(feature = "apollo-adapter")]
impl HealthCheck for ApolloStorageAdapter {
    fn is_healthy(&self) -> bool {
        self.read_current_state_root().is_ok()
    }

    fn detailed_status(&self) -> ComponentHealth {
        let latest_block_processed = self.latest_block_number().ok();
        let error = self
            .read_current_state_root()
            .err()
            .map(|error| error.to_string());
        ComponentHealth {
            name: "apollo-storage-adapter".to_string(),
            status: if error.is_none() {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            last_block_processed: latest_block_processed,
            sync_lag: None,
            error,
        }
    }
}

#[cfg(feature = "apollo-adapter")]
impl StorageBackend for ApolloStorageAdapter {
    fn get_state_reader(
        &self,
        block_number: BlockNumber,
    ) -> Result<Box<dyn StateReader>, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let state_marker = txn
            .get_state_marker()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let latest_state_block = latest_block_from_marker(state_marker);
        let state_number = if let Some(latest) = latest_state_block {
            if block_number > latest.0 {
                return Err(StorageError::BlockOutOfRange {
                    requested: block_number,
                    latest: latest.0,
                });
            }
            ApolloStateNumber::unchecked_right_after_block(ApolloBlockNumber(block_number))
        } else if block_number == 0 {
            ApolloStateNumber::right_before_block(ApolloBlockNumber(0))
        } else {
            return Err(StorageError::BlockOutOfRange {
                requested: block_number,
                latest: 0,
            });
        };
        Ok(Box::new(ApolloStateReaderAdapter {
            reader: self.reader.clone(),
            state_number,
        }))
    }

    fn apply_state_diff(&mut self, _diff: &StarknetStateDiff) -> Result<(), StorageError> {
        Err(StorageError::UnsupportedOperation(
            "apply_state_diff is not supported for apollo adapter",
        ))
    }

    fn insert_block(
        &mut self,
        _block: StarknetBlock,
        _state_diff: StarknetStateDiff,
    ) -> Result<(), StorageError> {
        Err(StorageError::UnsupportedOperation(
            "insert_block is not supported for apollo adapter",
        ))
    }

    fn get_block(&self, id: BlockId) -> Result<Option<StarknetBlock>, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let number = match id {
            BlockId::Number(number) => ApolloBlockNumber(number),
            BlockId::Latest => {
                let marker = txn
                    .get_header_marker()
                    .map_err(|error| StorageError::Apollo(error.to_string()))?;
                let Some(latest) = latest_block_from_marker(marker) else {
                    return Ok(None);
                };
                latest
            }
        };

        let header = txn
            .get_block_header(number)
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let Some(header) = header else {
            return Ok(None);
        };

        let header_without_hash = header.block_header_without_hash;
        let protocol_version =
            parse_starknet_version_to_semver(&header_without_hash.starknet_version.to_string())?;
        let l1_gas = GasPricePerToken {
            price_in_fri: header_without_hash.l1_gas_price.price_in_fri.0,
            price_in_wei: header_without_hash.l1_gas_price.price_in_wei.0,
        };
        let l1_data_gas = GasPricePerToken {
            price_in_fri: header_without_hash.l1_data_gas_price.price_in_fri.0,
            price_in_wei: header_without_hash.l1_data_gas_price.price_in_wei.0,
        };
        let l2_gas = GasPricePerToken {
            price_in_fri: header_without_hash.l2_gas_price.price_in_fri.0,
            price_in_wei: header_without_hash.l2_gas_price.price_in_wei.0,
        };
        let transactions = txn
            .get_block_transaction_hashes(number)
            .map_err(|error| StorageError::Apollo(error.to_string()))?
            .unwrap_or_default()
            .into_iter()
            .map(|hash| StarknetTransaction::new(format!("{:#x}", hash.0)))
            .collect();
        Ok(Some(StarknetBlock {
            number: number.0,
            parent_hash: format!("{:#x}", header_without_hash.parent_hash.0),
            state_root: format!("{:#x}", header_without_hash.state_root.0),
            timestamp: header_without_hash.timestamp.0,
            sequencer_address: format!("{:#x}", header_without_hash.sequencer.0.0.key()).into(),
            gas_prices: BlockGasPrices {
                l1_gas,
                l1_data_gas,
                l2_gas,
            },
            protocol_version,
            transactions,
        }))
    }

    fn get_state_diff(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<StarknetStateDiff>, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let maybe_diff = txn
            .get_state_diff(ApolloBlockNumber(block_number))
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        maybe_diff.map(map_thin_state_diff).transpose()
    }

    fn latest_block_number(&self) -> Result<BlockNumber, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let marker = txn
            .get_header_marker()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        Ok(latest_block_from_marker(marker).map(|n| n.0).unwrap_or(0))
    }

    fn current_state_root(&self) -> Result<String, StorageError> {
        self.read_current_state_root()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use semver::Version;
    use starknet_node_types::{
        BlockGasPrices, ContractAddress, GasPricePerToken, StarknetFelt, StarknetTransaction,
    };

    use super::*;

    fn block(number: BlockNumber) -> StarknetBlock {
        StarknetBlock {
            number,
            parent_hash: format!("0x{:x}", number.saturating_sub(1)),
            state_root: format!("0x{:x}", number),
            timestamp: 1_700_000_000 + number,
            sequencer_address: ContractAddress::from("0x1"),
            gas_prices: BlockGasPrices {
                l1_gas: GasPricePerToken {
                    price_in_fri: 2,
                    price_in_wei: 3,
                },
                l1_data_gas: GasPricePerToken {
                    price_in_fri: 4,
                    price_in_wei: 5,
                },
                l2_gas: GasPricePerToken {
                    price_in_fri: 6,
                    price_in_wei: 7,
                },
            },
            protocol_version: Version::parse("0.14.2").expect("valid semver"),
            transactions: vec![StarknetTransaction::new(format!("0x{number:x}"))],
        }
    }

    fn diff_with_balance(contract: &str, value: u64) -> StarknetStateDiff {
        let mut diff = StarknetStateDiff::default();
        diff.storage_diffs
            .entry(ContractAddress::from(contract))
            .or_default()
            .insert("0x2".to_string(), StarknetFelt::from(value));
        diff
    }

    #[test]
    fn inserts_block_and_persists_state_snapshot() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(block(1), diff_with_balance("0xabc", 11))
            .expect("insert");

        assert_eq!(storage.latest_block_number().expect("latest"), 1);

        let reader = storage.get_state_reader(1).expect("state reader");
        assert_eq!(
            reader.get_storage(&ContractAddress::from("0xabc"), "0x2"),
            Ok(Some(StarknetFelt::from(11_u64)))
        );
    }

    #[test]
    fn rejects_non_sequential_blocks() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        let err = storage
            .insert_block(block(2), StarknetStateDiff::default())
            .expect_err("must fail");
        assert_eq!(err, StorageError::NonSequentialBlock(2));
    }

    #[test]
    fn rejects_non_monotonic_block_timestamps() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(block(1), StarknetStateDiff::default())
            .expect("insert block 1");

        let mut stale_timestamp = block(2);
        stale_timestamp.timestamp = block(1).timestamp.saturating_sub(1);
        let err = storage
            .insert_block(stale_timestamp, StarknetStateDiff::default())
            .expect_err("must reject timestamp regression");
        assert!(matches!(
            err,
            StorageError::NonMonotonicTimestamp { block: 2, .. }
        ));
    }

    #[test]
    fn rejects_invalid_block_identifiers_on_insert() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        let mut invalid = block(1);
        invalid.parent_hash = "not-a-felt".to_string();

        let err = storage
            .insert_block(invalid, StarknetStateDiff::default())
            .expect_err("must reject invalid block");
        assert!(matches!(err, StorageError::InvalidBlock(_)));
    }

    #[test]
    fn rejects_invalid_state_diff_identifiers_on_insert() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        let mut diff = StarknetStateDiff::default();
        diff.storage_diffs
            .insert(ContractAddress::from("bad-contract"), BTreeMap::new());

        let err = storage
            .insert_block(block(1), diff)
            .expect_err("must reject invalid state diff");
        assert!(matches!(err, StorageError::InvalidStateDiff(_)));
    }

    #[test]
    fn checkpoint_verifier_detects_mismatch() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let err = CheckpointSyncVerifier::verify(&storage, "0xdeadbeef").expect_err("must fail");
        assert_eq!(
            err,
            CheckpointError::UnsupportedStateRootSemantics {
                backend: "in-memory-storage".to_string(),
            }
        );
    }

    #[test]
    fn thread_safe_storage_allows_shared_read_access() {
        let mut writer = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        writer
            .insert_block(block(1), diff_with_balance("0xabc", 11))
            .expect("insert");

        let storage = writer.clone();
        let handle = std::thread::spawn(move || {
            let reader = storage.get_state_reader(1).expect("state reader");
            reader.get_storage(&ContractAddress::from("0xabc"), "0x2")
        });

        assert_eq!(
            handle.join().expect("thread join"),
            Ok(Some(StarknetFelt::from(11_u64)))
        );
    }

    #[test]
    fn thread_safe_storage_reports_unhealthy_when_lock_is_poisoned() {
        let storage = ThreadSafeStorage::new(InMemoryStorage::new(InMemoryState::default()));
        let poison_handle = storage.clone();

        let _ = std::thread::spawn(move || {
            let _guard = poison_handle.inner.write().expect("lock");
            panic!("intentional poison");
        })
        .join();

        assert!(!storage.is_healthy());
        let health = storage.detailed_status();
        assert_eq!(health.status, HealthStatus::Unhealthy);
        assert_eq!(health.name, "thread-safe-storage");
    }

    #[test]
    fn returns_latest_block_by_id() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(block(1), diff_with_balance("0xabc", 11))
            .expect("insert 1");
        storage
            .insert_block(block(2), diff_with_balance("0xabc", 22))
            .expect("insert 2");

        let latest = storage
            .get_block(BlockId::Latest)
            .expect("get latest")
            .expect("must exist");
        assert_eq!(latest.number, 2);
    }

    #[test]
    fn in_memory_state_reader_rejects_future_block_requests() {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(block(1), diff_with_balance("0xabc", 11))
            .expect("insert");

        let err = match storage.get_state_reader(2) {
            Ok(_) => panic!("must fail"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            StorageError::BlockOutOfRange {
                requested: 2,
                latest: 1,
            }
        );
    }
}

#[cfg(all(test, feature = "apollo-adapter"))]
mod apollo_tests {
    use std::path::Path;

    use apollo_storage::body::BodyStorageWriter;
    use apollo_storage::db::DbConfig;
    use apollo_storage::header::HeaderStorageWriter;
    use apollo_storage::mmap_file::MmapFileConfig;
    use apollo_storage::state::StateStorageWriter;
    use apollo_storage::{StorageConfig as ApolloStorageConfig, StorageScope};
    use semver::Version;
    use serde::Deserialize;
    use starknet_api::block::{
        BlockHeader, BlockNumber as ApolloBlockNumber, BlockTimestamp as ApolloBlockTimestamp,
        StarknetVersion as ApolloVersion,
    };
    use starknet_api::core::{
        ChainId, ClassHash as ApolloClassHash, CompiledClassHash as ApolloCompiledClassHash,
        ContractAddress as ApolloContractAddress, GlobalRoot, Nonce as ApolloNonce,
    };
    use starknet_api::hash::StarkHash as ApolloFelt;
    use starknet_api::state::{
        StorageKey as ApolloStorageKey, ThinStateDiff as ApolloThinStateDiff,
    };
    use starknet_api::transaction::{
        L1HandlerTransaction as ApolloL1HandlerTransaction,
        L1HandlerTransactionOutput as ApolloL1HandlerTransactionOutput,
        Transaction as ApolloTransaction, TransactionHash as ApolloTransactionHash,
        TransactionOutput as ApolloTransactionOutput,
    };
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct StarknetFixture {
        block_number: u64,
        state_root: String,
        protocol_version: String,
    }

    fn apollo_config(path: &Path) -> ApolloStorageConfig {
        ApolloStorageConfig {
            db_config: DbConfig {
                path_prefix: path.to_path_buf(),
                chain_id: ChainId::Other("SN_MAIN".to_string()),
                enforce_file_exists: false,
                min_size: 1 << 20,
                max_size: 1 << 35,
                growth_step: 1 << 26,
                max_readers: 1 << 13,
            },
            mmap_file_config: MmapFileConfig {
                max_size: 1 << 24,
                growth_step: 1 << 20,
                max_object_size: 1 << 16,
            },
            scope: StorageScope::FullArchive,
        }
    }

    fn parse_fixture() -> StarknetFixture {
        serde_json::from_str(include_str!(
            "../tests/fixtures/starknet_mainnet_block0.json"
        ))
        .expect("valid fixture json")
    }

    fn seed_header(
        adapter: &mut ApolloStorageAdapter,
        fixture: &StarknetFixture,
    ) -> Result<(), StorageError> {
        let version = ApolloVersion::try_from(fixture.protocol_version.as_str())
            .map_err(|error| StorageError::InvalidProtocolVersion(error.to_string()))?;
        let root = ApolloFelt::from_hex(&fixture.state_root)
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        let mut header = BlockHeader::default();
        header.block_header_without_hash.state_root = GlobalRoot(root);
        header.block_header_without_hash.starknet_version = version;
        header.block_header_without_hash.timestamp =
            ApolloBlockTimestamp(1_700_000_000 + fixture.block_number);

        adapter
            ._writer
            .begin_rw_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?
            .append_header(ApolloBlockNumber(fixture.block_number), &header)
            .map_err(|error| StorageError::Apollo(error.to_string()))?
            .commit()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;

        Ok(())
    }

    fn seed_state_diff(
        adapter: &mut ApolloStorageAdapter,
        block_number: u64,
        storage_value: ApolloFelt,
    ) -> Result<(ContractAddress, String, starknet_node_types::ClassHash), StorageError> {
        let contract = ApolloContractAddress::from(0xabc_u64);
        let key = ApolloStorageKey::from(0x1_u64);
        let nonce = ApolloNonce(ApolloFelt::from(5_u64));
        let class_hash = ApolloClassHash(ApolloFelt::from(0x55_u64));
        let compiled_class_hash = ApolloCompiledClassHash(ApolloFelt::from(0x66_u64));

        let thin_diff = ApolloThinStateDiff {
            storage_diffs: std::iter::once((
                contract,
                std::iter::once((key, storage_value)).collect(),
            ))
            .collect(),
            class_hash_to_compiled_class_hash: std::iter::once((class_hash, compiled_class_hash))
                .collect(),
            nonces: std::iter::once((contract, nonce)).collect(),
            ..ApolloThinStateDiff::default()
        };

        adapter
            ._writer
            .begin_rw_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?
            .append_state_diff(ApolloBlockNumber(block_number), thin_diff)
            .map_err(|error| StorageError::Apollo(error.to_string()))?
            .commit()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;

        Ok((
            format!("{:#x}", contract.0.key()).into(),
            format!("{:#x}", key.0.key()),
            format!("{:#x}", class_hash.0).into(),
        ))
    }

    fn seed_body_with_single_tx_hash(
        adapter: &mut ApolloStorageAdapter,
        block_number: u64,
        tx_hash: ApolloFelt,
    ) -> Result<(), StorageError> {
        let tx = ApolloL1HandlerTransaction::default();

        let body = starknet_api::block::BlockBody {
            transactions: vec![ApolloTransaction::L1Handler(tx)],
            transaction_outputs: vec![ApolloTransactionOutput::L1Handler(
                ApolloL1HandlerTransactionOutput::default(),
            )],
            transaction_hashes: vec![ApolloTransactionHash(tx_hash)],
        };

        adapter
            ._writer
            .begin_rw_txn()
            .map_err(|error| StorageError::Apollo(error.to_string()))?
            .append_body(ApolloBlockNumber(block_number), body)
            .map_err(|error| StorageError::Apollo(error.to_string()))?
            .commit()
            .map_err(|error| StorageError::Apollo(error.to_string()))?;
        Ok(())
    }

    #[test]
    fn apollo_adapter_reads_fixture_root_and_latest_block() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            ApolloStorageAdapter::open(apollo_config(dir.path())).expect("open apollo");

        seed_header(&mut adapter, &fixture).expect("seed header");

        assert_eq!(
            adapter.latest_block_number().expect("latest"),
            fixture.block_number
        );
        let root = adapter.read_current_state_root().expect("root");
        let actual = ApolloFelt::from_hex(&root).expect("hex root");
        let expected = ApolloFelt::from_hex(&fixture.state_root).expect("fixture root");
        assert_eq!(actual, expected);
    }

    #[test]
    fn checkpoint_verifier_accepts_fixture_root_from_apollo_adapter() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            ApolloStorageAdapter::open(apollo_config(dir.path())).expect("open apollo");

        seed_header(&mut adapter, &fixture).expect("seed header");

        CheckpointSyncVerifier::verify(&adapter, &fixture.state_root).expect("checkpoint ok");
    }

    #[test]
    fn apollo_adapter_normalizes_four_component_protocol_versions() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            ApolloStorageAdapter::open(apollo_config(dir.path())).expect("open apollo");

        let mut fixture = fixture;
        fixture.protocol_version = "0.13.1.1".to_string();
        seed_header(&mut adapter, &fixture).expect("seed header");

        let block = adapter
            .get_block(BlockId::Number(fixture.block_number))
            .expect("get block")
            .expect("block exists");
        assert_eq!(
            block.protocol_version,
            Version::parse("0.13.1-1").expect("normalized semver")
        );
        assert_eq!(block.timestamp, 1_700_000_000 + fixture.block_number);
    }

    #[test]
    fn apollo_adapter_maps_state_diff_and_state_reader() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            ApolloStorageAdapter::open(apollo_config(dir.path())).expect("open apollo");

        seed_header(&mut adapter, &fixture).expect("seed header");
        let (contract, key, declared_class) =
            seed_state_diff(&mut adapter, fixture.block_number, ApolloFelt::from(77_u64))
                .expect("seed state diff");

        let reader = adapter
            .get_state_reader(fixture.block_number)
            .expect("state reader");
        assert_eq!(
            reader.get_storage(&contract, &key),
            Ok(Some(StarknetFelt::from(77_u64)))
        );
        assert_eq!(
            reader.nonce_of(&contract),
            Ok(Some(StarknetFelt::from(5_u64)))
        );

        let diff = adapter
            .get_state_diff(fixture.block_number)
            .expect("state diff")
            .expect("state diff present");
        assert_eq!(
            diff.storage_diffs
                .get(&contract)
                .and_then(|writes| writes.get(&key))
                .copied(),
            Some(StarknetFelt::from(77_u64))
        );
        assert_eq!(
            diff.nonces.get(&contract).copied(),
            Some(StarknetFelt::from(5_u64))
        );
        assert!(diff.declared_classes.contains(&declared_class));
    }

    #[test]
    fn apollo_adapter_preserves_large_state_diff_values_without_truncation() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            ApolloStorageAdapter::open(apollo_config(dir.path())).expect("open apollo");

        seed_header(&mut adapter, &fixture).expect("seed header");
        let huge = ApolloFelt::from_hex("0x10000000000000000").expect("felt");
        seed_state_diff(&mut adapter, fixture.block_number, huge).expect("seed state diff");

        let diff = adapter
            .get_state_diff(fixture.block_number)
            .expect("state diff")
            .expect("state diff present");

        let value = diff
            .storage_diffs
            .values()
            .next()
            .and_then(|writes| writes.get("0x1"))
            .copied()
            .expect("huge value present");
        assert_eq!(
            value,
            StarknetFelt::from_hex("0x10000000000000000").expect("felt")
        );
    }

    #[test]
    fn apollo_adapter_rejects_future_state_reader_requests() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            ApolloStorageAdapter::open(apollo_config(dir.path())).expect("open apollo");
        seed_header(&mut adapter, &fixture).expect("seed header");

        let err = match adapter.get_state_reader(fixture.block_number + 1) {
            Ok(_) => panic!("future block must fail"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            StorageError::BlockOutOfRange {
                requested: fixture.block_number + 1,
                latest: fixture.block_number,
            }
        );
    }

    #[test]
    fn apollo_adapter_populates_block_transactions_from_body_hashes() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            ApolloStorageAdapter::open(apollo_config(dir.path())).expect("open apollo");

        seed_header(&mut adapter, &fixture).expect("seed header");
        seed_body_with_single_tx_hash(
            &mut adapter,
            fixture.block_number,
            ApolloFelt::from(0xabc_u64),
        )
        .expect("seed body");

        let block = adapter
            .get_block(BlockId::Number(fixture.block_number))
            .expect("get block")
            .expect("block exists");
        assert_eq!(block.transactions.len(), 1);
        assert_eq!(block.transactions[0].hash, "0xabc".into());
    }

    #[test]
    fn parse_starknet_version_supports_semver_and_four_component_variants() {
        assert_eq!(
            parse_starknet_version_to_semver("0.14.2").expect("semver"),
            Version::parse("0.14.2").expect("version")
        );
        assert_eq!(
            parse_starknet_version_to_semver("0.13.1.1").expect("four component"),
            Version::parse("0.13.1-1").expect("normalized version")
        );
    }

    #[test]
    fn parse_starknet_version_rejects_unknown_shapes() {
        let err = parse_starknet_version_to_semver("0.13.1.1.9").expect_err("must fail");
        assert!(matches!(err, StorageError::InvalidProtocolVersion(_)));
    }
}
