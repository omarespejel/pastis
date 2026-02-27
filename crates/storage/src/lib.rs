use std::collections::BTreeMap;

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
        _block_number: BlockNumber,
    ) -> Result<Box<dyn StateReader>, StorageError> {
        Err(StorageError::UnsupportedOperation(
            "get_state_reader is not yet implemented for papyrus adapter",
        ))
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

        let protocol_version = semver::Version::parse(&header.starknet_version.to_string())
            .map_err(|error| StorageError::InvalidProtocolVersion(error.to_string()))?;
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
        Ok(maybe_diff.map(|_| StarknetStateDiff::default()))
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
    use papyrus_storage::{StorageConfig as PapyrusStorageConfig, StorageScope};
    use serde::Deserialize;
    use starknet_api::block::{
        BlockHeader, BlockNumber as PapyrusBlockNumber, StarknetVersion as PapyrusVersion,
    };
    use starknet_api::core::ChainId;
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
}
