#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub const REPLAY_CHECKPOINT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayHashWindowEntry {
    pub external_block_number: u64,
    pub block_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    pub version: u32,
    pub replay_window: u64,
    pub max_replay_per_poll: u64,
    pub next_external_block: Option<u64>,
    pub expected_parent_hash: Option<String>,
    pub next_local_block: u64,
    pub reorg_events: u64,
    pub recent_hashes: Vec<ReplayHashWindowEntry>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReplayCheckpointError {
    #[error("checkpoint io error at {path}: {error}")]
    Io { path: String, error: String },
    #[error("checkpoint decode error at {path}: {error}")]
    Decode { path: String, error: String },
    #[error("checkpoint encode error at {path}: {error}")]
    Encode { path: String, error: String },
    #[error("unsupported replay checkpoint version {version}")]
    UnsupportedVersion { version: u32 },
    #[error(
        "replay checkpoint config mismatch: expected window={expected_replay_window}, max_per_poll={expected_max_replay_per_poll}; found window={actual_replay_window}, max_per_poll={actual_max_replay_per_poll}"
    )]
    ConfigMismatch {
        expected_replay_window: u64,
        expected_max_replay_per_poll: u64,
        actual_replay_window: u64,
        actual_max_replay_per_poll: u64,
    },
    #[error("invalid replay checkpoint: {0}")]
    InvalidCheckpoint(String),
}

pub trait ReplayCheckpointStore: Send + Sync {
    fn load(&self) -> Result<Option<ReplayCheckpoint>, ReplayCheckpointError>;
    fn save(&self, checkpoint: &ReplayCheckpoint) -> Result<(), ReplayCheckpointError>;
}

#[derive(Debug, Clone)]
pub struct FileReplayCheckpointStore {
    path: PathBuf,
}

impl FileReplayCheckpointStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl ReplayCheckpointStore for FileReplayCheckpointStore {
    fn load(&self) -> Result<Option<ReplayCheckpoint>, ReplayCheckpointError> {
        if !self.path.exists() {
            return Ok(None);
        }
        let raw = fs::read(&self.path).map_err(|error| ReplayCheckpointError::Io {
            path: self.path.display().to_string(),
            error: error.to_string(),
        })?;
        let checkpoint = serde_json::from_slice::<ReplayCheckpoint>(&raw).map_err(|error| {
            ReplayCheckpointError::Decode {
                path: self.path.display().to_string(),
                error: error.to_string(),
            }
        })?;
        Ok(Some(checkpoint))
    }

    fn save(&self, checkpoint: &ReplayCheckpoint) -> Result<(), ReplayCheckpointError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| ReplayCheckpointError::Io {
                path: parent.display().to_string(),
                error: error.to_string(),
            })?;
        }

        let encoded = serde_json::to_vec_pretty(checkpoint).map_err(|error| {
            ReplayCheckpointError::Encode {
                path: self.path.display().to_string(),
                error: error.to_string(),
            }
        })?;

        let tmp_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmp_path = self
            .path
            .with_extension(format!("tmp-{}-{tmp_nonce}", std::process::id()));
        fs::write(&tmp_path, encoded).map_err(|error| ReplayCheckpointError::Io {
            path: tmp_path.display().to_string(),
            error: error.to_string(),
        })?;
        fs::rename(&tmp_path, &self.path).map_err(|error| ReplayCheckpointError::Io {
            path: self.path.display().to_string(),
            error: error.to_string(),
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayCursor {
    next_external_block: Option<u64>,
    expected_parent_hash: Option<String>,
    replay_window: u64,
    max_replay_per_poll: u64,
}

impl ReplayCursor {
    pub fn new(replay_window: u64, max_replay_per_poll: u64) -> Self {
        Self {
            next_external_block: None,
            expected_parent_hash: None,
            replay_window: replay_window.max(1),
            max_replay_per_poll: max_replay_per_poll.max(1),
        }
    }

    pub fn from_checkpoint(
        replay_window: u64,
        max_replay_per_poll: u64,
        next_external_block: Option<u64>,
        expected_parent_hash: Option<String>,
    ) -> Self {
        Self {
            next_external_block,
            expected_parent_hash,
            replay_window: replay_window.max(1),
            max_replay_per_poll: max_replay_per_poll.max(1),
        }
    }

    pub fn plan(&mut self, external_head_block: u64) -> Vec<u64> {
        if self.next_external_block.is_none() {
            let start = external_head_block.saturating_sub(self.replay_window.saturating_sub(1));
            self.next_external_block = Some(start);
        }
        let Some(start) = self.next_external_block else {
            return Vec::new();
        };
        if start > external_head_block {
            return Vec::new();
        }
        let end = start
            .saturating_add(self.max_replay_per_poll.saturating_sub(1))
            .min(external_head_block);
        (start..=end).collect()
    }

    pub fn parent_hash_matches(&self, observed_parent_hash: &str) -> bool {
        match &self.expected_parent_hash {
            Some(expected) => expected == observed_parent_hash,
            None => true,
        }
    }

    pub fn mark_processed(&mut self, external_block_number: u64, block_hash: &str) {
        self.next_external_block = Some(external_block_number.saturating_add(1));
        self.expected_parent_hash = Some(block_hash.to_string());
    }

    pub fn reset_for_reorg(&mut self, conflicting_external_block: u64) {
        let start = conflicting_external_block.saturating_sub(self.replay_window.saturating_sub(1));
        self.next_external_block = Some(start);
        self.expected_parent_hash = None;
    }

    pub fn replay_lag_blocks(&self, external_head_block: u64) -> u64 {
        match self.next_external_block {
            Some(next) if next <= external_head_block => external_head_block - next + 1,
            _ => 0,
        }
    }

    pub fn next_external_block(&self) -> Option<u64> {
        self.next_external_block
    }

    pub fn expected_parent_hash(&self) -> Option<&str> {
        self.expected_parent_hash.as_deref()
    }

    pub fn replay_window(&self) -> u64 {
        self.replay_window
    }

    pub fn max_replay_per_poll(&self) -> u64 {
        self.max_replay_per_poll
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEngine {
    cursor: ReplayCursor,
    next_local_block: u64,
    reorg_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayCheck {
    Continue {
        local_block_number: u64,
    },
    Reorg {
        conflicting_external_block: u64,
        restart_external_block: u64,
    },
}

impl ReplayEngine {
    pub fn new(replay_window: u64, max_replay_per_poll: u64) -> Self {
        Self {
            cursor: ReplayCursor::new(replay_window, max_replay_per_poll),
            next_local_block: 1,
            reorg_events: 0,
        }
    }

    fn from_checkpoint(checkpoint: &ReplayCheckpoint) -> Result<Self, ReplayCheckpointError> {
        if checkpoint.next_local_block == 0 {
            return Err(ReplayCheckpointError::InvalidCheckpoint(
                "next_local_block must be >= 1".to_string(),
            ));
        }
        Ok(Self {
            cursor: ReplayCursor::from_checkpoint(
                checkpoint.replay_window,
                checkpoint.max_replay_per_poll,
                checkpoint.next_external_block,
                checkpoint.expected_parent_hash.clone(),
            ),
            next_local_block: checkpoint.next_local_block,
            reorg_events: checkpoint.reorg_events,
        })
    }

    fn checkpoint(&self, recent_hashes: Vec<ReplayHashWindowEntry>) -> ReplayCheckpoint {
        ReplayCheckpoint {
            version: REPLAY_CHECKPOINT_VERSION,
            replay_window: self.cursor.replay_window(),
            max_replay_per_poll: self.cursor.max_replay_per_poll(),
            next_external_block: self.cursor.next_external_block(),
            expected_parent_hash: self.cursor.expected_parent_hash().map(str::to_string),
            next_local_block: self.next_local_block,
            reorg_events: self.reorg_events,
            recent_hashes,
        }
    }

    pub fn plan(&mut self, external_head_block: u64) -> Vec<u64> {
        self.cursor.plan(external_head_block)
    }

    pub fn replay_lag_blocks(&self, external_head_block: u64) -> u64 {
        self.cursor.replay_lag_blocks(external_head_block)
    }

    pub fn force_reorg(&mut self, conflicting_external_block: u64) -> ReplayCheck {
        self.cursor.reset_for_reorg(conflicting_external_block);
        self.next_local_block = 1;
        self.reorg_events = self.reorg_events.saturating_add(1);
        ReplayCheck::Reorg {
            conflicting_external_block,
            restart_external_block: self
                .cursor
                .next_external_block()
                .unwrap_or(conflicting_external_block),
        }
    }

    pub fn check_block(
        &mut self,
        external_block_number: u64,
        observed_parent_hash: &str,
    ) -> ReplayCheck {
        if !self.cursor.parent_hash_matches(observed_parent_hash) {
            return self.force_reorg(external_block_number);
        }
        ReplayCheck::Continue {
            local_block_number: self.next_local_block,
        }
    }

    pub fn mark_committed(&mut self, external_block_number: u64, block_hash: &str) {
        self.cursor
            .mark_processed(external_block_number, block_hash);
        self.next_local_block = self.next_local_block.saturating_add(1);
    }

    pub fn next_local_block(&self) -> u64 {
        self.next_local_block
    }

    pub fn next_external_block(&self) -> Option<u64> {
        self.cursor.next_external_block()
    }

    pub fn expected_parent_hash(&self) -> Option<&str> {
        self.cursor.expected_parent_hash()
    }

    pub fn replay_window(&self) -> u64 {
        self.cursor.replay_window()
    }

    pub fn max_replay_per_poll(&self) -> u64 {
        self.cursor.max_replay_per_poll()
    }

    pub fn reorg_events(&self) -> u64 {
        self.reorg_events
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayPipelineStep {
    Continue {
        local_block_number: u64,
    },
    ReorgRecoverable {
        conflicting_external_block: u64,
        restart_external_block: u64,
        depth: u64,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReplayPipelineError {
    #[error(transparent)]
    Checkpoint(#[from] ReplayCheckpointError),
    #[error(
        "deep reorg beyond replay window {replay_window} at external block {conflicting_external_block}: expected parent {expected_parent_hash}, observed parent {observed_parent_hash}"
    )]
    DeepReorgBeyondWindow {
        conflicting_external_block: u64,
        observed_parent_hash: String,
        expected_parent_hash: String,
        replay_window: u64,
    },
    #[error("replay pipeline invariant violated: {0}")]
    InvariantViolation(String),
}

pub struct ReplayPipeline {
    engine: ReplayEngine,
    recent_hashes: VecDeque<ReplayHashWindowEntry>,
    checkpoint_store: Option<Arc<dyn ReplayCheckpointStore>>,
}

impl ReplayPipeline {
    pub fn new(replay_window: u64, max_replay_per_poll: u64) -> Self {
        Self {
            engine: ReplayEngine::new(replay_window, max_replay_per_poll),
            recent_hashes: VecDeque::new(),
            checkpoint_store: None,
        }
    }

    pub fn with_checkpoint_store(
        mut self,
        checkpoint_store: Arc<dyn ReplayCheckpointStore>,
    ) -> Result<Self, ReplayPipelineError> {
        if let Some(checkpoint) = checkpoint_store.load()? {
            self.load_checkpoint(checkpoint)?;
        }
        self.checkpoint_store = Some(checkpoint_store);
        Ok(self)
    }

    pub fn plan(&mut self, external_head_block: u64) -> Vec<u64> {
        self.engine.plan(external_head_block)
    }

    pub fn replay_lag_blocks(&self, external_head_block: u64) -> u64 {
        self.engine.replay_lag_blocks(external_head_block)
    }

    pub fn evaluate_block(
        &mut self,
        external_block_number: u64,
        observed_parent_hash: &str,
    ) -> Result<ReplayPipelineStep, ReplayPipelineError> {
        if self.engine.expected_parent_hash().is_none()
            || self
                .engine
                .expected_parent_hash()
                .is_some_and(|expected| expected == observed_parent_hash)
        {
            return Ok(ReplayPipelineStep::Continue {
                local_block_number: self.engine.next_local_block(),
            });
        }

        let expected_parent_hash = self
            .engine
            .expected_parent_hash()
            .map(str::to_string)
            .unwrap_or_else(|| "<none>".to_string());
        let ancestor = self
            .recent_hashes
            .iter()
            .rev()
            .find(|entry| entry.block_hash == observed_parent_hash)
            .map(|entry| entry.external_block_number);

        let Some(ancestor_external_block) = ancestor else {
            return Err(ReplayPipelineError::DeepReorgBeyondWindow {
                conflicting_external_block: external_block_number,
                observed_parent_hash: observed_parent_hash.to_string(),
                expected_parent_hash,
                replay_window: self.engine.replay_window(),
            });
        };

        let depth = external_block_number.saturating_sub(ancestor_external_block.saturating_add(1));
        let reorg = self.engine.force_reorg(external_block_number);
        self.persist_checkpoint()?;
        match reorg {
            ReplayCheck::Reorg {
                conflicting_external_block,
                restart_external_block,
            } => Ok(ReplayPipelineStep::ReorgRecoverable {
                conflicting_external_block,
                restart_external_block,
                depth,
            }),
            ReplayCheck::Continue { .. } => Err(ReplayPipelineError::InvariantViolation(
                "force_reorg returned Continue while recovering mismatch".to_string(),
            )),
        }
    }

    pub fn mark_committed(
        &mut self,
        external_block_number: u64,
        block_hash: &str,
    ) -> Result<(), ReplayPipelineError> {
        self.engine
            .mark_committed(external_block_number, block_hash);
        self.recent_hashes.push_back(ReplayHashWindowEntry {
            external_block_number,
            block_hash: block_hash.to_string(),
        });
        self.trim_recent_hashes();
        self.persist_checkpoint()?;
        Ok(())
    }

    pub fn next_local_block(&self) -> u64 {
        self.engine.next_local_block()
    }

    pub fn next_external_block(&self) -> Option<u64> {
        self.engine.next_external_block()
    }

    pub fn expected_parent_hash(&self) -> Option<&str> {
        self.engine.expected_parent_hash()
    }

    pub fn reorg_events(&self) -> u64 {
        self.engine.reorg_events()
    }

    pub fn replay_window(&self) -> u64 {
        self.engine.replay_window()
    }

    pub fn checkpoint(&self) -> ReplayCheckpoint {
        self.engine
            .checkpoint(self.recent_hashes.iter().cloned().collect())
    }

    fn load_checkpoint(&mut self, checkpoint: ReplayCheckpoint) -> Result<(), ReplayPipelineError> {
        if checkpoint.version != REPLAY_CHECKPOINT_VERSION {
            return Err(ReplayCheckpointError::UnsupportedVersion {
                version: checkpoint.version,
            }
            .into());
        }
        if checkpoint.replay_window != self.engine.replay_window()
            || checkpoint.max_replay_per_poll != self.engine.max_replay_per_poll()
        {
            return Err(ReplayCheckpointError::ConfigMismatch {
                expected_replay_window: self.engine.replay_window(),
                expected_max_replay_per_poll: self.engine.max_replay_per_poll(),
                actual_replay_window: checkpoint.replay_window,
                actual_max_replay_per_poll: checkpoint.max_replay_per_poll,
            }
            .into());
        }
        validate_checkpoint_recent_hashes(&checkpoint)?;
        self.engine = ReplayEngine::from_checkpoint(&checkpoint)?;
        self.recent_hashes = checkpoint.recent_hashes.into_iter().collect();
        self.trim_recent_hashes();
        Ok(())
    }

    fn trim_recent_hashes(&mut self) {
        while self.recent_hashes.len() > self.engine.replay_window() as usize {
            let _ = self.recent_hashes.pop_front();
        }
    }

    fn persist_checkpoint(&self) -> Result<(), ReplayPipelineError> {
        if let Some(store) = &self.checkpoint_store {
            store.save(&self.checkpoint())?;
        }
        Ok(())
    }
}

fn validate_checkpoint_recent_hashes(
    checkpoint: &ReplayCheckpoint,
) -> Result<(), ReplayCheckpointError> {
    if checkpoint.recent_hashes.len() as u64 > checkpoint.replay_window {
        return Err(ReplayCheckpointError::InvalidCheckpoint(format!(
            "recent_hashes length {} exceeds replay_window {}",
            checkpoint.recent_hashes.len(),
            checkpoint.replay_window
        )));
    }

    let mut previous_block: Option<u64> = None;
    for entry in &checkpoint.recent_hashes {
        if entry.block_hash.is_empty() {
            return Err(ReplayCheckpointError::InvalidCheckpoint(
                "recent_hashes contains empty block hash".to_string(),
            ));
        }
        if let Some(previous) = previous_block
            && entry.external_block_number <= previous
        {
            return Err(ReplayCheckpointError::InvalidCheckpoint(format!(
                "recent_hashes must be strictly increasing; saw {} then {}",
                previous, entry.external_block_number
            )));
        }
        previous_block = Some(entry.external_block_number);
    }

    if let Some(expected_parent_hash) = checkpoint.expected_parent_hash.as_ref() {
        let Some(last) = checkpoint.recent_hashes.last() else {
            return Err(ReplayCheckpointError::InvalidCheckpoint(
                "expected_parent_hash is set but recent_hashes is empty".to_string(),
            ));
        };
        if &last.block_hash != expected_parent_hash {
            return Err(ReplayCheckpointError::InvalidCheckpoint(format!(
                "expected_parent_hash {} does not match last recent hash {}",
                expected_parent_hash, last.block_hash
            )));
        }
    }

    if let (Some(next_external_block), Some(last)) =
        (checkpoint.next_external_block, checkpoint.recent_hashes.last())
        && next_external_block <= last.external_block_number
    {
        return Err(ReplayCheckpointError::InvalidCheckpoint(format!(
            "next_external_block {} must be greater than last recent hash block {}",
            next_external_block, last.external_block_number
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::{
        FileReplayCheckpointStore, REPLAY_CHECKPOINT_VERSION, ReplayCheck, ReplayCheckpoint,
        ReplayCheckpointError, ReplayCheckpointStore, ReplayHashWindowEntry, ReplayPipeline,
        ReplayPipelineError, ReplayPipelineStep,
    };

    #[test]
    fn replay_engine_plans_bootstrap_window_and_lag() {
        let mut engine = super::ReplayEngine::new(4, 3);
        assert_eq!(engine.plan(10), vec![7, 8, 9]);
        assert_eq!(
            engine.check_block(7, "0x0"),
            ReplayCheck::Continue {
                local_block_number: 1
            }
        );
        engine.mark_committed(7, "0x7");
        assert_eq!(engine.expected_parent_hash(), Some("0x7"));
        assert_eq!(engine.replay_lag_blocks(10), 3);
    }

    #[test]
    fn replay_engine_reorg_resets_execution_cursor_and_window() {
        let mut engine = super::ReplayEngine::new(5, 2);
        let _ = engine.plan(20);
        assert_eq!(
            engine.check_block(16, "0x0"),
            ReplayCheck::Continue {
                local_block_number: 1
            }
        );
        engine.mark_committed(16, "0x16");

        let check = engine.check_block(17, "0xdead");
        assert_eq!(
            check,
            ReplayCheck::Reorg {
                conflicting_external_block: 17,
                restart_external_block: 13,
            }
        );
        assert_eq!(engine.next_local_block(), 1);
        assert_eq!(engine.reorg_events(), 1);
        assert_eq!(engine.plan(20), vec![13, 14]);
    }

    #[test]
    fn replay_engine_advances_only_after_commit() {
        let mut engine = super::ReplayEngine::new(8, 4);
        let _ = engine.plan(50);
        assert_eq!(
            engine.check_block(43, "0x0"),
            ReplayCheck::Continue {
                local_block_number: 1
            }
        );
        assert_eq!(
            engine.check_block(43, "0x0"),
            ReplayCheck::Continue {
                local_block_number: 1
            }
        );
        engine.mark_committed(43, "0x43");
        assert_eq!(
            engine.check_block(44, "0x43"),
            ReplayCheck::Continue {
                local_block_number: 2
            }
        );
    }

    #[test]
    fn replay_checkpoint_file_store_roundtrip() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("replay-checkpoint.json");
        let store = FileReplayCheckpointStore::new(path.clone());

        let checkpoint = ReplayCheckpoint {
            version: REPLAY_CHECKPOINT_VERSION,
            replay_window: 64,
            max_replay_per_poll: 16,
            next_external_block: Some(123),
            expected_parent_hash: Some("0xabc".to_string()),
            next_local_block: 99,
            reorg_events: 3,
            recent_hashes: vec![ReplayHashWindowEntry {
                external_block_number: 122,
                block_hash: "0xdef".to_string(),
            }],
        };
        store.save(&checkpoint).expect("save checkpoint");
        let loaded = store
            .load()
            .expect("load checkpoint")
            .expect("checkpoint exists");
        assert_eq!(loaded, checkpoint);
        assert_eq!(store.path(), path.as_path());
    }

    #[test]
    fn replay_pipeline_persists_checkpoint_and_recovers_after_restart() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("replay-checkpoint.json");
        let store = Arc::new(FileReplayCheckpointStore::new(path));

        let mut pipeline = ReplayPipeline::new(4, 3)
            .with_checkpoint_store(store.clone())
            .expect("attach checkpoint store");
        assert_eq!(pipeline.plan(10), vec![7, 8, 9]);
        assert_eq!(
            pipeline
                .evaluate_block(7, "0x0")
                .expect("evaluation should succeed"),
            ReplayPipelineStep::Continue {
                local_block_number: 1
            }
        );
        pipeline
            .mark_committed(7, "0x7")
            .expect("commit should persist checkpoint");

        let restored = ReplayPipeline::new(4, 3)
            .with_checkpoint_store(store)
            .expect("restore from checkpoint");
        assert_eq!(restored.next_local_block(), 2);
        assert_eq!(restored.next_external_block(), Some(8));
        assert_eq!(restored.expected_parent_hash(), Some("0x7"));
    }

    #[test]
    fn replay_pipeline_fails_closed_on_deep_reorg_beyond_window() {
        let mut pipeline = ReplayPipeline::new(3, 10);
        assert_eq!(
            pipeline
                .evaluate_block(5, "0x0")
                .expect("block 5 should continue"),
            ReplayPipelineStep::Continue {
                local_block_number: 1
            }
        );
        pipeline.mark_committed(5, "0x5").expect("commit 5");
        assert_eq!(
            pipeline
                .evaluate_block(6, "0x5")
                .expect("block 6 should continue"),
            ReplayPipelineStep::Continue {
                local_block_number: 2
            }
        );
        pipeline.mark_committed(6, "0x6").expect("commit 6");
        assert_eq!(
            pipeline
                .evaluate_block(7, "0x6")
                .expect("block 7 should continue"),
            ReplayPipelineStep::Continue {
                local_block_number: 3
            }
        );
        pipeline.mark_committed(7, "0x7").expect("commit 7");

        let err = pipeline
            .evaluate_block(8, "0x1")
            .expect_err("deep reorg outside window must fail closed");
        assert_eq!(
            err,
            ReplayPipelineError::DeepReorgBeyondWindow {
                conflicting_external_block: 8,
                observed_parent_hash: "0x1".to_string(),
                expected_parent_hash: "0x7".to_string(),
                replay_window: 3,
            }
        );
        assert_eq!(pipeline.next_local_block(), 4);
        assert_eq!(pipeline.reorg_events(), 0);
    }

    #[test]
    fn replay_pipeline_recovers_reorg_when_ancestor_is_in_window() {
        let mut pipeline = ReplayPipeline::new(4, 8);
        for (external, parent, hash) in [
            (10_u64, "0x0", "0xa"),
            (11_u64, "0xa", "0xb"),
            (12_u64, "0xb", "0xc"),
        ] {
            assert_eq!(
                pipeline
                    .evaluate_block(external, parent)
                    .expect("block should continue"),
                ReplayPipelineStep::Continue {
                    local_block_number: (external - 9),
                }
            );
            pipeline.mark_committed(external, hash).expect("commit");
        }

        let step = pipeline
            .evaluate_block(13, "0xa")
            .expect("reorg in window should be recoverable");
        assert_eq!(
            step,
            ReplayPipelineStep::ReorgRecoverable {
                conflicting_external_block: 13,
                restart_external_block: 10,
                depth: 2,
            }
        );
        assert_eq!(pipeline.reorg_events(), 1);
        assert_eq!(pipeline.next_local_block(), 1);
        assert_eq!(pipeline.plan(13), vec![10, 11, 12, 13]);
    }

    #[test]
    fn replay_pipeline_rejects_checkpoint_config_mismatch() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("replay-checkpoint.json");
        let store = FileReplayCheckpointStore::new(path.clone());
        store
            .save(&ReplayCheckpoint {
                version: REPLAY_CHECKPOINT_VERSION,
                replay_window: 8,
                max_replay_per_poll: 8,
                next_external_block: Some(200),
                expected_parent_hash: Some("0xbeef".to_string()),
                next_local_block: 2,
                reorg_events: 0,
                recent_hashes: Vec::new(),
            })
            .expect("seed checkpoint");

        let err = ReplayPipeline::new(64, 16)
            .with_checkpoint_store(Arc::new(FileReplayCheckpointStore::new(path)))
            .err()
            .expect("mismatched replay config must fail closed");
        assert!(matches!(
            err,
            ReplayPipelineError::Checkpoint(ReplayCheckpointError::ConfigMismatch { .. })
        ));
    }

    #[test]
    fn replay_pipeline_rejects_checkpoint_with_unsorted_recent_hashes() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("replay-checkpoint.json");
        let store = FileReplayCheckpointStore::new(path.clone());
        store
            .save(&ReplayCheckpoint {
                version: REPLAY_CHECKPOINT_VERSION,
                replay_window: 8,
                max_replay_per_poll: 8,
                next_external_block: Some(101),
                expected_parent_hash: Some("0x100".to_string()),
                next_local_block: 3,
                reorg_events: 0,
                recent_hashes: vec![
                    ReplayHashWindowEntry {
                        external_block_number: 100,
                        block_hash: "0x100".to_string(),
                    },
                    ReplayHashWindowEntry {
                        external_block_number: 99,
                        block_hash: "0x99".to_string(),
                    },
                ],
            })
            .expect("seed checkpoint");

        let err = ReplayPipeline::new(8, 8)
            .with_checkpoint_store(Arc::new(FileReplayCheckpointStore::new(path)))
            .err()
            .expect("unsorted checkpoint hash window must fail closed");
        assert!(matches!(
            err,
            ReplayPipelineError::Checkpoint(ReplayCheckpointError::InvalidCheckpoint(message))
                if message.contains("strictly increasing")
        ));
    }

    #[test]
    fn replay_pipeline_rejects_checkpoint_with_parent_hash_mismatch() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("replay-checkpoint.json");
        let store = FileReplayCheckpointStore::new(path.clone());
        store
            .save(&ReplayCheckpoint {
                version: REPLAY_CHECKPOINT_VERSION,
                replay_window: 8,
                max_replay_per_poll: 8,
                next_external_block: Some(101),
                expected_parent_hash: Some("0xbeef".to_string()),
                next_local_block: 3,
                reorg_events: 0,
                recent_hashes: vec![ReplayHashWindowEntry {
                    external_block_number: 100,
                    block_hash: "0x100".to_string(),
                }],
            })
            .expect("seed checkpoint");

        let err = ReplayPipeline::new(8, 8)
            .with_checkpoint_store(Arc::new(FileReplayCheckpointStore::new(path)))
            .err()
            .expect("inconsistent checkpoint parent hash must fail closed");
        assert!(matches!(
            err,
            ReplayPipelineError::Checkpoint(ReplayCheckpointError::InvalidCheckpoint(message))
                if message.contains("expected_parent_hash")
        ));
    }
}
