use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use starknet_node::replay::{ReplayCheck, ReplayEngine};

#[derive(Debug, Deserialize)]
struct ReplayFixture {
    replay_window: u64,
    max_replay_per_poll: u64,
    polls: Vec<PollFixture>,
    expected: ReplayFixtureExpected,
}

#[derive(Debug, Deserialize)]
struct PollFixture {
    external_head: u64,
    expected_plan: Vec<u64>,
    blocks: Vec<BlockFixture>,
}

#[derive(Debug, Deserialize)]
struct BlockFixture {
    number: u64,
    parent_hash: String,
    block_hash: String,
    expected: String,
    expected_local: Option<u64>,
    restart_external_block: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ReplayFixtureExpected {
    reorg_events: u64,
    next_local_block: u64,
    next_external_block: u64,
    replay_lag_blocks_at_final_head: u64,
}

fn load_fixture(name: &str) -> ReplayFixture {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("replay")
        .join(name);
    let raw = fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("failed to read replay fixture {}: {error}", path.display());
    });
    serde_json::from_str(&raw).unwrap_or_else(|error| {
        panic!(
            "failed to decode replay fixture {}: {error}",
            path.display()
        );
    })
}

fn assert_fixture(name: &str) {
    let fixture = load_fixture(name);
    let mut replay = ReplayEngine::new(fixture.replay_window, fixture.max_replay_per_poll);

    for poll in &fixture.polls {
        let plan = replay.plan(poll.external_head);
        assert_eq!(
            plan, poll.expected_plan,
            "planned replay range mismatch in fixture {name} for head {}",
            poll.external_head
        );

        for block in &poll.blocks {
            let check = replay.check_block(block.number, &block.parent_hash);
            match block.expected.as_str() {
                "apply" => {
                    let expected_local = block.expected_local.unwrap_or_else(|| {
                        panic!(
                            "fixture {name} block {} expected apply but missing expected_local",
                            block.number
                        )
                    });
                    assert_eq!(
                        check,
                        ReplayCheck::Continue {
                            local_block_number: expected_local,
                        },
                        "fixture {name} block {} local assignment mismatch",
                        block.number
                    );
                    replay.mark_committed(block.number, &block.block_hash);
                }
                "reorg" => {
                    let expected_restart = block.restart_external_block.unwrap_or_else(|| {
                        panic!(
                            "fixture {name} block {} expected reorg but missing restart_external_block",
                            block.number
                        )
                    });
                    assert_eq!(
                        check,
                        ReplayCheck::Reorg {
                            conflicting_external_block: block.number,
                            restart_external_block: expected_restart,
                        },
                        "fixture {name} block {} reorg decision mismatch",
                        block.number
                    );
                    break;
                }
                other => panic!(
                    "fixture {name} block {} has unsupported expectation `{other}`",
                    block.number
                ),
            }
        }
    }

    let last_head = fixture
        .polls
        .last()
        .map(|poll| poll.external_head)
        .unwrap_or_default();
    assert_eq!(replay.reorg_events(), fixture.expected.reorg_events);
    assert_eq!(replay.next_local_block(), fixture.expected.next_local_block);
    assert_eq!(
        replay.next_external_block(),
        Some(fixture.expected.next_external_block)
    );
    assert_eq!(
        replay.replay_lag_blocks(last_head),
        fixture.expected.replay_lag_blocks_at_final_head
    );
}

#[test]
fn replay_engine_fixture_contiguous_chain() {
    assert_fixture("contiguous.json");
}

#[test]
fn replay_engine_fixture_reorg_chain() {
    assert_fixture("reorg.json");
}
