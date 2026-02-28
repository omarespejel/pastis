use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedStateRoot {
    pub l2_block_number: u64,
    pub state_root: String,
    pub eth_block_number: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FinalityError {
    #[error(
        "conflicting state root for L2 block {l2_block_number}: existing {existing_root}, new {new_root}"
    )]
    ConflictingPostedRoot {
        l2_block_number: u64,
        existing_root: String,
        new_root: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct L1FinalityTracker {
    finalized_eth_block: Option<u64>,
    posted_by_l2_block: BTreeMap<u64, PostedStateRoot>,
}

impl L1FinalityTracker {
    /// Returns the latest finalized Ethereum block observed by this tracker.
    pub fn finalized_eth_block(&self) -> Option<u64> {
        self.finalized_eth_block
    }

    pub fn record_state_root_posted(
        &mut self,
        entry: PostedStateRoot,
    ) -> Result<(), FinalityError> {
        if let Some(existing) = self.posted_by_l2_block.get(&entry.l2_block_number) {
            if existing.state_root != entry.state_root {
                return Err(FinalityError::ConflictingPostedRoot {
                    l2_block_number: entry.l2_block_number,
                    existing_root: existing.state_root.clone(),
                    new_root: entry.state_root.clone(),
                });
            }
            // Preserve the earliest observed posting metadata for idempotent reposts.
            return Ok(());
        }
        self.posted_by_l2_block.insert(entry.l2_block_number, entry);
        Ok(())
    }

    pub fn update_finalized_eth_block(&mut self, block_number: u64) {
        self.finalized_eth_block = Some(
            self.finalized_eth_block
                .map_or(block_number, |current| current.max(block_number)),
        );
    }

    /// Removes state roots that are not finalized and were posted at or after the given
    /// Ethereum block number.
    ///
    /// `observed_finalized_eth_block` lets callers atomically advance finality and apply
    /// reorg invalidation using one mutable operation on this tracker.
    pub fn invalidate_unfinalized_from_eth_block(
        &mut self,
        from_eth_block: u64,
        observed_finalized_eth_block: Option<u64>,
    ) {
        if let Some(observed_finalized) = observed_finalized_eth_block {
            self.update_finalized_eth_block(observed_finalized);
        }
        let finalized = self.finalized_eth_block.unwrap_or_default();
        self.posted_by_l2_block.retain(|_, entry| {
            !(entry.eth_block_number >= from_eth_block && entry.eth_block_number > finalized)
        });
    }

    pub fn latest_verified_block(&self) -> Option<u64> {
        let finalized = self.finalized_eth_block?;
        let mut latest_verified = None;
        let mut expected_next = None;
        for (l2_block, entry) in &self.posted_by_l2_block {
            if entry.eth_block_number > finalized {
                continue;
            }
            match expected_next {
                None => {
                    latest_verified = Some(*l2_block);
                    expected_next = l2_block.checked_add(1);
                }
                Some(expected) if *l2_block == expected => {
                    latest_verified = Some(*l2_block);
                    expected_next = l2_block.checked_add(1);
                }
                Some(expected) if *l2_block > expected => break,
                Some(_) => {}
            }
        }
        latest_verified
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(l2: u64, eth: u64) -> PostedStateRoot {
        PostedStateRoot {
            l2_block_number: l2,
            state_root: format!("0x{l2:x}"),
            eth_block_number: eth,
        }
    }

    #[test]
    fn does_not_confirm_until_eth_finalized() {
        let mut tracker = L1FinalityTracker::default();
        tracker
            .record_state_root_posted(root(120, 10_000))
            .expect("record root");
        assert_eq!(tracker.latest_verified_block(), None);

        tracker.update_finalized_eth_block(9_999);
        assert_eq!(tracker.latest_verified_block(), None);

        tracker.update_finalized_eth_block(10_000);
        assert_eq!(tracker.latest_verified_block(), Some(120));
    }

    #[test]
    fn only_confirms_roots_up_to_finalized_checkpoint() {
        let mut tracker = L1FinalityTracker::default();
        tracker
            .record_state_root_posted(root(120, 10_000))
            .expect("record root 120");
        tracker
            .record_state_root_posted(root(121, 10_008))
            .expect("record root 121");
        tracker.update_finalized_eth_block(10_003);

        assert_eq!(tracker.latest_verified_block(), Some(120));
    }

    #[test]
    fn drops_unfinalized_roots_on_reorg() {
        let mut tracker = L1FinalityTracker::default();
        tracker
            .record_state_root_posted(root(120, 10_000))
            .expect("record root 120");
        tracker
            .record_state_root_posted(root(121, 10_008))
            .expect("record root 121");
        tracker.update_finalized_eth_block(10_001);
        assert_eq!(tracker.latest_verified_block(), Some(120));

        tracker.invalidate_unfinalized_from_eth_block(10_005, None);
        tracker.update_finalized_eth_block(10_100);
        assert_eq!(tracker.latest_verified_block(), Some(120));
    }

    #[test]
    fn reorg_invalidation_can_apply_finalized_hint_atomically() {
        let mut tracker = L1FinalityTracker::default();
        tracker
            .record_state_root_posted(root(120, 10_000))
            .expect("record root 120");
        tracker
            .record_state_root_posted(root(121, 10_008))
            .expect("record root 121");
        tracker
            .record_state_root_posted(root(122, 10_012))
            .expect("record root 122");
        tracker.update_finalized_eth_block(10_001);

        tracker.invalidate_unfinalized_from_eth_block(10_005, Some(10_010));

        tracker.update_finalized_eth_block(10_100);
        assert_eq!(tracker.latest_verified_block(), Some(121));
    }

    #[test]
    fn finalized_eth_block_updates_are_monotonic() {
        let mut tracker = L1FinalityTracker::default();
        assert_eq!(tracker.finalized_eth_block(), None);

        tracker.update_finalized_eth_block(100);
        assert_eq!(tracker.finalized_eth_block(), Some(100));

        tracker.update_finalized_eth_block(95);
        assert_eq!(tracker.finalized_eth_block(), Some(100));

        tracker.update_finalized_eth_block(250);
        assert_eq!(tracker.finalized_eth_block(), Some(250));
    }

    #[test]
    fn allows_idempotent_reposts_for_same_l2_state_root() {
        let mut tracker = L1FinalityTracker::default();
        tracker
            .record_state_root_posted(root(120, 10_000))
            .expect("first post");
        tracker
            .record_state_root_posted(root(120, 10_010))
            .expect("idempotent repost");

        tracker.update_finalized_eth_block(10_005);
        assert_eq!(tracker.latest_verified_block(), Some(120));
    }

    #[test]
    fn rejects_conflicting_duplicate_l2_block_posts() {
        let mut tracker = L1FinalityTracker::default();
        tracker
            .record_state_root_posted(root(120, 10_000))
            .expect("first insert");
        let err = tracker
            .record_state_root_posted(PostedStateRoot {
                l2_block_number: 120,
                state_root: "0xdead".to_string(),
                eth_block_number: 10_050,
            })
            .expect_err("conflicting duplicate should fail");
        assert_eq!(
            err,
            FinalityError::ConflictingPostedRoot {
                l2_block_number: 120,
                existing_root: "0x78".to_string(),
                new_root: "0xdead".to_string(),
            }
        );
    }

    #[test]
    fn does_not_advance_past_gaps_in_finalized_l2_sequence() {
        let mut tracker = L1FinalityTracker::default();
        tracker
            .record_state_root_posted(root(120, 10_000))
            .expect("record 120");
        tracker
            .record_state_root_posted(root(122, 10_001))
            .expect("record 122");
        tracker.update_finalized_eth_block(10_100);

        assert_eq!(tracker.latest_verified_block(), Some(120));
    }
}
