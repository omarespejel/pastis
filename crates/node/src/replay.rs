#![forbid(unsafe_code)]

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

    pub fn plan(&mut self, external_head_block: u64) -> Vec<u64> {
        self.cursor.plan(external_head_block)
    }

    pub fn replay_lag_blocks(&self, external_head_block: u64) -> u64 {
        self.cursor.replay_lag_blocks(external_head_block)
    }

    pub fn check_block(
        &mut self,
        external_block_number: u64,
        observed_parent_hash: &str,
    ) -> ReplayCheck {
        if !self.cursor.parent_hash_matches(observed_parent_hash) {
            self.cursor.reset_for_reorg(external_block_number);
            self.next_local_block = 1;
            self.reorg_events = self.reorg_events.saturating_add(1);
            return ReplayCheck::Reorg {
                conflicting_external_block: external_block_number,
                restart_external_block: self
                    .cursor
                    .next_external_block()
                    .unwrap_or(external_block_number),
            };
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

    pub fn reorg_events(&self) -> u64 {
        self.reorg_events
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplayCheck, ReplayEngine};

    #[test]
    fn replay_engine_plans_bootstrap_window_and_lag() {
        let mut engine = ReplayEngine::new(4, 3);
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
        let mut engine = ReplayEngine::new(5, 2);
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
        let mut engine = ReplayEngine::new(8, 4);
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
}
