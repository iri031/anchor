mod harness;
mod scenario_tests;
mod timeout_tests;

// Re-export commonly used items for convenience
pub use harness::{
    ByzantineBehavior, CommitteeSize, ConsensusResult, OperatorBehavior, OperationalStatus,
    QbftTester, TestContext, TEST_TIMEOUT,
};

// Shared test data generator
use ssv_types::consensus::BeaconVote;
use types::Hash256;

use crate::CommitteeInstanceId;
use ssv_types::CommitteeId;

/// Generate unique test data for scenarios
pub(crate) fn generate_test_data(id: usize) -> (BeaconVote, CommitteeInstanceId) {
    let id = CommitteeInstanceId {
        committee: CommitteeId([0; 32]),
        instance_height: id.into(),
    };

    let data = BeaconVote {
        block_root: Hash256::random(),
        source: types::Checkpoint::default(),
        target: types::Checkpoint::default(),
    };

    (data, id)
}
