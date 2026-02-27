use std::collections::BTreeMap;

use sha2::{Digest, Sha256};
use starknet_node_types::{
    BlockId, BlockNumber, ComponentHealth, HealthCheck, HealthStatus, InMemoryState, StarknetBlock,
    StarknetStateDiff, StateReader,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("block {0} does not extend current tip")]
    NonSequentialBlock(BlockNumber),
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
