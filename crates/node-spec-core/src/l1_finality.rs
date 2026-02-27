use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostedStateRoot {
    pub l2_block_number: u64,
    pub state_root: String,
    pub eth_block_number: u64,
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

    pub fn record_state_root_posted(&mut self, entry: PostedStateRoot) {
        self.posted_by_l2_block.insert(entry.l2_block_number, entry);
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
    /// Callers must synchronize concurrent mutation externally if multiple threads update
    /// finality and reorg state.
    pub fn invalidate_unfinalized_from_eth_block(&mut self, from_eth_block: u64) {
        let finalized = self.finalized_eth_block.unwrap_or_default();
        self.posted_by_l2_block.retain(|_, entry| {
            !(entry.eth_block_number >= from_eth_block && entry.eth_block_number > finalized)
        });
    }

    pub fn latest_verified_block(&self) -> Option<u64> {
        let finalized = self.finalized_eth_block?;
        self.posted_by_l2_block
            .iter()
            .rev()
            .find_map(|(l2_block, entry)| {
                if entry.eth_block_number <= finalized {
                    Some(*l2_block)
                } else {
                    None
                }
            })
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
        tracker.record_state_root_posted(root(120, 10_000));
        assert_eq!(tracker.latest_verified_block(), None);

        tracker.update_finalized_eth_block(9_999);
        assert_eq!(tracker.latest_verified_block(), None);

        tracker.update_finalized_eth_block(10_000);
        assert_eq!(tracker.latest_verified_block(), Some(120));
    }

    #[test]
    fn only_confirms_roots_up_to_finalized_checkpoint() {
        let mut tracker = L1FinalityTracker::default();
        tracker.record_state_root_posted(root(120, 10_000));
        tracker.record_state_root_posted(root(121, 10_008));
        tracker.update_finalized_eth_block(10_003);

        assert_eq!(tracker.latest_verified_block(), Some(120));
    }

    #[test]
    fn drops_unfinalized_roots_on_reorg() {
        let mut tracker = L1FinalityTracker::default();
        tracker.record_state_root_posted(root(120, 10_000));
        tracker.record_state_root_posted(root(121, 10_008));
        tracker.update_finalized_eth_block(10_001);
        assert_eq!(tracker.latest_verified_block(), Some(120));

        tracker.invalidate_unfinalized_from_eth_block(10_005);
        tracker.update_finalized_eth_block(10_100);
        assert_eq!(tracker.latest_verified_block(), Some(120));
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
}
