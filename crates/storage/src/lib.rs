use std::collections::BTreeMap;
#[cfg(feature = "papyrus-adapter")]
use std::str::FromStr;

#[cfg(feature = "papyrus-adapter")]
use num_traits::cast::ToPrimitive;
#[cfg(feature = "papyrus-adapter")]
use papyrus_storage::header::HeaderStorageReader;
#[cfg(feature = "papyrus-adapter")]
use papyrus_storage::state::StateStorageReader;
#[cfg(feature = "papyrus-adapter")]
use papyrus_storage::{
    StorageConfig as PapyrusStorageConfig, StorageReader as PapyrusStorageReader,
    StorageWriter as PapyrusStorageWriter, open_storage as open_papyrus_storage,
};
#[cfg(feature = "papyrus-adapter")]
use starknet_api::block::BlockNumber as PapyrusBlockNumber;
#[cfg(feature = "papyrus-adapter")]
use starknet_api::core::{ContractAddress as PapyrusContractAddress, Nonce as PapyrusNonce};
#[cfg(feature = "papyrus-adapter")]
use starknet_api::hash::StarkHash as PapyrusFelt;
#[cfg(feature = "papyrus-adapter")]
use starknet_api::state::ThinStateDiff as PapyrusThinStateDiff;
#[cfg(feature = "papyrus-adapter")]
use starknet_api::state::{StateNumber as PapyrusStateNumber, StorageKey as PapyrusStorageKey};

use sha2::{Digest, Sha256};
use starknet_node_types::{
    BlockId, BlockNumber, ComponentHealth, HealthCheck, HealthStatus, InMemoryState, StarknetBlock,
    StarknetStateDiff, StateReader,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("block {0} does not extend current tip")]
    NonSequentialBlock(BlockNumber),
    #[error("operation not supported by backend: {0}")]
    UnsupportedOperation(&'static str),
    #[error("value out of range for {field}: {value}")]
    ValueOutOfRange { field: &'static str, value: String },
    #[cfg(feature = "papyrus-adapter")]
    #[error("papyrus storage error: {0}")]
    Papyrus(String),
    #[cfg(feature = "papyrus-adapter")]
    #[error("invalid protocol version in papyrus header: {0}")]
    InvalidProtocolVersion(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CheckpointError {
    #[error("state root mismatch: expected {expected}, actual {actual}")]
    StateRootMismatch { expected: String, actual: String },
}

pub trait StorageBackend: Send + Sync + HealthCheck {
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
    fn current_state_root(&self) -> String;
}

#[derive(Debug, Clone)]
pub struct InMemoryStorage {
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
        let snapshot = self
            .states
            .range(..=block_number)
            .next_back()
            .map(|(_, state)| state.clone())
            .unwrap_or_default();
        Ok(Box::new(snapshot))
    }

    fn apply_state_diff(&mut self, diff: &StarknetStateDiff) -> Result<(), StorageError> {
        let tip = self.latest_block_number()?;
        let mut state = self.states.get(&tip).cloned().unwrap_or_default();
        state.apply_state_diff(diff);
        self.states.insert(tip, state);
        Ok(())
    }

    fn insert_block(
        &mut self,
        block: StarknetBlock,
        state_diff: StarknetStateDiff,
    ) -> Result<(), StorageError> {
        let expected = self.latest_block_number()? + 1;
        if block.number != expected {
            return Err(StorageError::NonSequentialBlock(block.number));
        }

        let mut next_state = self
            .states
            .get(&(expected - 1))
            .cloned()
            .unwrap_or_default();
        next_state.apply_state_diff(&state_diff);

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

    fn current_state_root(&self) -> String {
        let latest = self.latest_block_number().unwrap_or(0);
        let state = self.states.get(&latest).cloned().unwrap_or_default();

        let mut hasher = Sha256::new();
        for (contract, writes) in &state.storage {
            hasher.update(contract.as_bytes());
            for (key, value) in writes {
                hasher.update(key.as_bytes());
                hasher.update(value.to_be_bytes());
            }
        }
        for (contract, nonce) in &state.nonces {
            hasher.update(contract.as_bytes());
            hasher.update(nonce.to_be_bytes());
        }

        format!("0x{}", hex::encode(hasher.finalize()))
    }
}

pub struct CheckpointSyncVerifier;

impl CheckpointSyncVerifier {
    pub fn verify(
        storage: &dyn StorageBackend,
        expected_root: &str,
    ) -> Result<(), CheckpointError> {
        let actual = storage.current_state_root();
        if actual == expected_root {
            return Ok(());
        }
        Err(CheckpointError::StateRootMismatch {
            expected: expected_root.to_string(),
            actual,
        })
    }
}

#[cfg(feature = "papyrus-adapter")]
#[derive(Clone)]
struct PapyrusStateReaderAdapter {
    reader: PapyrusStorageReader,
    state_number: PapyrusStateNumber,
}

#[cfg(feature = "papyrus-adapter")]
impl StateReader for PapyrusStateReaderAdapter {
    fn get_storage(&self, contract: &String, key: &str) -> Option<u64> {
        let contract = parse_contract_address(contract)?;
        let key = parse_storage_key(key)?;
        let txn = self.reader.begin_ro_txn().ok()?;
        let state_reader = txn.get_state_reader().ok()?;
        let value = state_reader
            .get_storage_at(self.state_number, &contract, &key)
            .ok()?;
        value.to_u64()
    }

    fn nonce_of(&self, contract: &String) -> Option<u64> {
        let contract = parse_contract_address(contract)?;
        let txn = self.reader.begin_ro_txn().ok()?;
        let state_reader = txn.get_state_reader().ok()?;
        let nonce: PapyrusNonce = state_reader
            .get_nonce_at(self.state_number, &contract)
            .ok()??;
        nonce.0.to_u64()
    }
}

#[cfg(feature = "papyrus-adapter")]
pub struct PapyrusStorageAdapter {
    reader: PapyrusStorageReader,
    _writer: PapyrusStorageWriter,
}

#[cfg(feature = "papyrus-adapter")]
impl PapyrusStorageAdapter {
    pub fn open(config: PapyrusStorageConfig) -> Result<Self, StorageError> {
        let (reader, writer) = open_papyrus_storage(config)
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        Ok(Self {
            reader,
            _writer: writer,
        })
    }

    pub fn from_parts(reader: PapyrusStorageReader, writer: PapyrusStorageWriter) -> Self {
        Self {
            reader,
            _writer: writer,
        }
    }

    pub fn read_current_state_root(&self) -> Result<String, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let marker = txn
            .get_header_marker()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let Some(latest_number) = latest_block_from_marker(marker) else {
            return Ok("0x0".to_string());
        };

        let header = txn
            .get_block_header(latest_number)
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        match header {
            Some(header) => Ok(format!("{:#x}", header.state_root.0)),
            None => Ok("0x0".to_string()),
        }
    }
}

#[cfg(feature = "papyrus-adapter")]
fn latest_block_from_marker(marker: PapyrusBlockNumber) -> Option<PapyrusBlockNumber> {
    marker.0.checked_sub(1).map(PapyrusBlockNumber)
}

#[cfg(feature = "papyrus-adapter")]
fn parse_contract_address(raw: &str) -> Option<PapyrusContractAddress> {
    let felt = PapyrusFelt::from_str(raw).ok()?;
    PapyrusContractAddress::try_from(felt).ok()
}

#[cfg(feature = "papyrus-adapter")]
fn parse_storage_key(raw: &str) -> Option<PapyrusStorageKey> {
    let felt = PapyrusFelt::from_str(raw).ok()?;
    PapyrusStorageKey::try_from(felt).ok()
}

#[cfg(feature = "papyrus-adapter")]
fn felt_to_u64(value: PapyrusFelt, field: &'static str) -> Result<u64, StorageError> {
    value.to_u64().ok_or_else(|| StorageError::ValueOutOfRange {
        field,
        value: format!("{:#x}", value),
    })
}

#[cfg(feature = "papyrus-adapter")]
fn map_thin_state_diff(diff: PapyrusThinStateDiff) -> Result<StarknetStateDiff, StorageError> {
    let mut mapped = StarknetStateDiff::default();
    for (address, writes) in diff.storage_diffs {
        let contract = format!("{:#x}", address.0.key());
        let mapped_writes = mapped.storage_diffs.entry(contract).or_default();
        for (key, value) in writes {
            mapped_writes.insert(
                format!("{:#x}", key.0.key()),
                felt_to_u64(value, "state_diff.storage_diffs")?,
            );
        }
    }
    for (address, nonce) in diff.nonces {
        mapped.nonces.insert(
            format!("{:#x}", address.0.key()),
            felt_to_u64(nonce.0, "state_diff.nonces")?,
        );
    }
    for class_hash in diff.declared_classes.keys() {
        mapped.declared_classes.push(format!("{:#x}", class_hash.0));
    }
    for class_hash in &diff.deprecated_declared_classes {
        mapped.declared_classes.push(format!("{:#x}", class_hash.0));
    }
    Ok(mapped)
}

#[cfg(feature = "papyrus-adapter")]
fn parse_starknet_version_to_semver(raw: &str) -> Result<semver::Version, StorageError> {
    if let Ok(version) = semver::Version::parse(raw) {
        return Ok(version);
    }

    // Starknet versions can have a 4th component (for example, 0.13.1.1).
    let parts: Vec<&str> = raw.split('.').collect();
    if parts.len() == 4 {
        let normalized = format!("{}.{}.{}-{}", parts[0], parts[1], parts[2], parts[3]);
        return semver::Version::parse(&normalized)
            .map_err(|error| StorageError::InvalidProtocolVersion(error.to_string()));
    }

    Err(StorageError::InvalidProtocolVersion(format!(
        "unsupported Starknet version format: {raw}"
    )))
}

#[cfg(feature = "papyrus-adapter")]
impl HealthCheck for PapyrusStorageAdapter {
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
            name: "papyrus-storage-adapter".to_string(),
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

#[cfg(feature = "papyrus-adapter")]
impl StorageBackend for PapyrusStorageAdapter {
    fn get_state_reader(
        &self,
        block_number: BlockNumber,
    ) -> Result<Box<dyn StateReader>, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let state_marker = txn
            .get_state_marker()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let latest_state_block = latest_block_from_marker(state_marker);
        let effective_block = latest_state_block
            .map(|latest| PapyrusBlockNumber(block_number.min(latest.0)))
            .unwrap_or(PapyrusBlockNumber(0));
        let state_number = if latest_state_block.is_some() {
            PapyrusStateNumber::unchecked_right_after_block(effective_block)
        } else {
            PapyrusStateNumber::right_before_block(PapyrusBlockNumber(0))
        };
        Ok(Box::new(PapyrusStateReaderAdapter {
            reader: self.reader.clone(),
            state_number,
        }))
    }

    fn apply_state_diff(&mut self, _diff: &StarknetStateDiff) -> Result<(), StorageError> {
        Err(StorageError::UnsupportedOperation(
            "apply_state_diff is not supported for papyrus adapter",
        ))
    }

    fn insert_block(
        &mut self,
        _block: StarknetBlock,
        _state_diff: StarknetStateDiff,
    ) -> Result<(), StorageError> {
        Err(StorageError::UnsupportedOperation(
            "insert_block is not supported for papyrus adapter",
        ))
    }

    fn get_block(&self, id: BlockId) -> Result<Option<StarknetBlock>, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let number = match id {
            BlockId::Number(number) => PapyrusBlockNumber(number),
            BlockId::Latest => {
                let marker = txn
                    .get_header_marker()
                    .map_err(|error| StorageError::Papyrus(error.to_string()))?;
                let Some(latest) = latest_block_from_marker(marker) else {
                    return Ok(None);
                };
                latest
            }
        };

        let header = txn
            .get_block_header(number)
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let Some(header) = header else {
            return Ok(None);
        };

        let protocol_version =
            parse_starknet_version_to_semver(&header.starknet_version.to_string())?;
        Ok(Some(StarknetBlock {
            number: number.0,
            protocol_version,
            transactions: Vec::new(),
        }))
    }

    fn get_state_diff(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<StarknetStateDiff>, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let maybe_diff = txn
            .get_state_diff(PapyrusBlockNumber(block_number))
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        maybe_diff.map(map_thin_state_diff).transpose()
    }

    fn latest_block_number(&self) -> Result<BlockNumber, StorageError> {
        let txn = self
            .reader
            .begin_ro_txn()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let marker = txn
            .get_header_marker()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        Ok(latest_block_from_marker(marker).map(|n| n.0).unwrap_or(0))
    }

    fn current_state_root(&self) -> String {
        self.read_current_state_root()
            .unwrap_or_else(|_| "0x0".to_string())
    }
}

#[cfg(test)]
mod tests {
    use semver::Version;
    use starknet_node_types::{ContractAddress, StarknetTransaction};

    use super::*;

    fn block(number: BlockNumber) -> StarknetBlock {
        StarknetBlock {
            number,
            protocol_version: Version::parse("0.14.2").expect("valid semver"),
            transactions: vec![StarknetTransaction {
                hash: format!("0x{number:x}"),
            }],
        }
    }

    fn diff_with_balance(contract: &str, value: u64) -> StarknetStateDiff {
        let mut diff = StarknetStateDiff::default();
        diff.storage_diffs
            .entry(contract.to_string())
            .or_default()
            .insert("balance".to_string(), value);
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
            reader.get_storage(&ContractAddress::from("0xabc"), "balance"),
            Some(11)
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
    fn checkpoint_verifier_detects_mismatch() {
        let storage = InMemoryStorage::new(InMemoryState::default());
        let err = CheckpointSyncVerifier::verify(&storage, "0xdeadbeef").expect_err("must fail");
        match err {
            CheckpointError::StateRootMismatch { expected, actual } => {
                assert_eq!(expected, "0xdeadbeef".to_string());
                assert_ne!(expected, actual);
            }
        }
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
}

#[cfg(all(test, feature = "papyrus-adapter"))]
mod papyrus_tests {
    use std::path::Path;

    use papyrus_storage::db::DbConfig;
    use papyrus_storage::header::HeaderStorageWriter;
    use papyrus_storage::mmap_file::MmapFileConfig;
    use papyrus_storage::state::StateStorageWriter;
    use papyrus_storage::{StorageConfig as PapyrusStorageConfig, StorageScope};
    use semver::Version;
    use serde::Deserialize;
    use starknet_api::block::{
        BlockHeader, BlockNumber as PapyrusBlockNumber, StarknetVersion as PapyrusVersion,
    };
    use starknet_api::core::{
        ChainId, ClassHash as PapyrusClassHash, CompiledClassHash as PapyrusCompiledClassHash,
        ContractAddress as PapyrusContractAddress, Nonce as PapyrusNonce,
    };
    use starknet_api::hash::StarkHash as PapyrusFelt;
    use starknet_api::state::{
        StorageKey as PapyrusStorageKey, ThinStateDiff as PapyrusThinStateDiff,
    };
    use starknet_types_core::felt::Felt;
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct StarknetFixture {
        block_number: u64,
        state_root: String,
        protocol_version: String,
    }

    fn papyrus_config(path: &Path) -> PapyrusStorageConfig {
        PapyrusStorageConfig {
            db_config: DbConfig {
                path_prefix: path.to_path_buf(),
                chain_id: ChainId::Other("SN_MAIN".to_string()),
                enforce_file_exists: false,
                min_size: 1 << 20,
                max_size: 1 << 35,
                growth_step: 1 << 26,
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
        adapter: &mut PapyrusStorageAdapter,
        fixture: &StarknetFixture,
    ) -> Result<(), StorageError> {
        let root = Felt::from_hex(&fixture.state_root)
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;
        let header = BlockHeader {
            state_root: starknet_api::core::GlobalRoot(root),
            starknet_version: PapyrusVersion(fixture.protocol_version.clone()),
            ..BlockHeader::default()
        };

        adapter
            ._writer
            .begin_rw_txn()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?
            .append_header(PapyrusBlockNumber(fixture.block_number), &header)
            .map_err(|error| StorageError::Papyrus(error.to_string()))?
            .commit()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;

        Ok(())
    }

    fn seed_state_diff(
        adapter: &mut PapyrusStorageAdapter,
        block_number: u64,
        storage_value: PapyrusFelt,
    ) -> Result<(String, String, String), StorageError> {
        let contract = PapyrusContractAddress::from(0xabc_u64);
        let key = PapyrusStorageKey::from(0x1_u64);
        let nonce = PapyrusNonce(PapyrusFelt::from(5_u64));
        let class_hash = PapyrusClassHash(PapyrusFelt::from(0x55_u64));
        let compiled_class_hash = PapyrusCompiledClassHash(PapyrusFelt::from(0x66_u64));

        let thin_diff = PapyrusThinStateDiff {
            storage_diffs: std::iter::once((
                contract,
                std::iter::once((key, storage_value)).collect(),
            ))
            .collect(),
            declared_classes: std::iter::once((class_hash, compiled_class_hash)).collect(),
            nonces: std::iter::once((contract, nonce)).collect(),
            ..PapyrusThinStateDiff::default()
        };

        adapter
            ._writer
            .begin_rw_txn()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?
            .append_state_diff(PapyrusBlockNumber(block_number), thin_diff)
            .map_err(|error| StorageError::Papyrus(error.to_string()))?
            .commit()
            .map_err(|error| StorageError::Papyrus(error.to_string()))?;

        Ok((
            format!("{:#x}", contract.0.key()),
            format!("{:#x}", key.0.key()),
            format!("{:#x}", class_hash.0),
        ))
    }

    #[test]
    fn papyrus_adapter_reads_fixture_root_and_latest_block() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            PapyrusStorageAdapter::open(papyrus_config(dir.path())).expect("open papyrus");

        seed_header(&mut adapter, &fixture).expect("seed header");

        assert_eq!(
            adapter.latest_block_number().expect("latest"),
            fixture.block_number
        );
        let root = adapter.read_current_state_root().expect("root");
        let actual = Felt::from_hex(&root).expect("hex root");
        let expected = Felt::from_hex(&fixture.state_root).expect("fixture root");
        assert_eq!(actual, expected);
    }

    #[test]
    fn checkpoint_verifier_accepts_fixture_root_from_papyrus_adapter() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            PapyrusStorageAdapter::open(papyrus_config(dir.path())).expect("open papyrus");

        seed_header(&mut adapter, &fixture).expect("seed header");

        CheckpointSyncVerifier::verify(&adapter, &fixture.state_root).expect("checkpoint ok");
    }

    #[test]
    fn papyrus_adapter_normalizes_four_component_protocol_versions() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            PapyrusStorageAdapter::open(papyrus_config(dir.path())).expect("open papyrus");

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
    }

    #[test]
    fn papyrus_adapter_maps_state_diff_and_state_reader() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            PapyrusStorageAdapter::open(papyrus_config(dir.path())).expect("open papyrus");

        seed_header(&mut adapter, &fixture).expect("seed header");
        let (contract, key, declared_class) = seed_state_diff(
            &mut adapter,
            fixture.block_number,
            PapyrusFelt::from(77_u64),
        )
        .expect("seed state diff");

        let reader = adapter
            .get_state_reader(fixture.block_number)
            .expect("state reader");
        assert_eq!(reader.get_storage(&contract, &key), Some(77));
        assert_eq!(reader.nonce_of(&contract), Some(5));

        let diff = adapter
            .get_state_diff(fixture.block_number)
            .expect("state diff")
            .expect("state diff present");
        assert_eq!(
            diff.storage_diffs
                .get(&contract)
                .and_then(|writes| writes.get(&key))
                .copied(),
            Some(77)
        );
        assert_eq!(diff.nonces.get(&contract).copied(), Some(5));
        assert!(diff.declared_classes.contains(&declared_class));
    }

    #[test]
    fn papyrus_adapter_rejects_state_diff_values_that_overflow_u64() {
        let fixture = parse_fixture();
        let dir = tempdir().expect("temp dir");
        let mut adapter =
            PapyrusStorageAdapter::open(papyrus_config(dir.path())).expect("open papyrus");

        seed_header(&mut adapter, &fixture).expect("seed header");
        let huge = PapyrusFelt::from_hex("0x10000000000000000").expect("felt");
        seed_state_diff(&mut adapter, fixture.block_number, huge).expect("seed state diff");

        let err = adapter
            .get_state_diff(fixture.block_number)
            .expect_err("must fail on overflow");
        assert!(matches!(
            err,
            StorageError::ValueOutOfRange {
                field: "state_diff.storage_diffs",
                ..
            }
        ));
    }
}
