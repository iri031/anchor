use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ssv_types::{OperatorId, consensus::BeaconVote};
use task_executor::{ShutdownReason, TaskExecutor};
use types::Slot;
use slot_clock::ManualSlotClock;

use super::{CommitteeSize, TestContext, generate_test_data};
use crate::tests::harness::TRACING;
use crate::tests::ByzantineBehavior;
use ssv_types::consensus::QbftMessageType;

/// Keeps the TaskExecutor alive for the duration of the test.
/// When this is dropped, the executor receives a signal to shut down.
type TaskExecutorKeepAlive = async_channel::Sender<()>;

/// Provides test setup
struct Setup {
    executor: TaskExecutor,
    task_executor_keepalive: TaskExecutorKeepAlive,
    _shutdown: futures::channel::mpsc::Sender<ShutdownReason>,
    clock: ManualSlotClock,
    all_data: Vec<(BeaconVote, crate::CommitteeInstanceId)>,
}

/// Setup environment for the test
fn setup_test(num_instances: usize) -> Setup {
    *TRACING;

    // setup the executor
    let handle = tokio::runtime::Handle::current();
    let (signal, exit) = async_channel::bounded(1);
    let (shutdown, _) = futures::channel::mpsc::channel(1);
    let executor = TaskExecutor::new(handle, exit, shutdown.clone(), "qbft_tests".into());

    // setup the slot clock
    let slot_duration = Duration::from_secs(12);
    let genesis_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let clock = ManualSlotClock::new(
        Slot::new(0),
        Duration::from_secs(genesis_time),
        slot_duration,
    );

    let mut all_data = vec![];
    for id in 1..num_instances + 1 {
        all_data.push(generate_test_data(id))
    }

    Setup {
        executor,
        task_executor_keepalive: signal,
        _shutdown: shutdown,
        clock,
        all_data,
    }
}

#[tokio::test]
// Test running a single instance and confirm that it reaches consensus
async fn test_basic_run() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.verify_consensus().await;
}

#[tokio::test]
// Take the leader offline to test a round change
async fn test_round_change() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.set_operators_offline(&[2]);
    context.verify_consensus().await;
}

#[tokio::test]
// Test one offline operator
async fn test_fault_operator() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.set_operators_offline(&[1]);
    context.verify_consensus().await;
}

#[tokio::test]
// Go through all committee sizes and confirm that we can reach consensus with f faulty
async fn test_consensus_f_faulty() {
    let setup = setup_test(1);
    let sizes = vec![
        (CommitteeSize::Four, vec![1]),
        (CommitteeSize::Seven, vec![1, 3]),
        (CommitteeSize::Ten, vec![1, 3, 4]),
        (CommitteeSize::Thirteen, vec![1, 3, 4, 5]),
    ];

    for (size, faulty) in sizes {
        let mut context = TestContext::<BeaconVote>::new(
            setup.clock.clone(),
            setup.executor.clone(),
            size,
            setup.all_data.clone(),
        )
        .await;

        context.set_operators_offline(&faulty);
        context.verify_consensus().await;
    }
}

#[tokio::test]
// Test running concurrent instances and confirm that they reach consensus
async fn test_concurrent_runs() {
    let setup = setup_test(2);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.verify_consensus().await;
}

#[tokio::test(start_paused = true)]
// Start with > f fault and then recover them. This should reach consensus
async fn test_recovery() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.set_operators_offline(&[1, 2]);

    tokio::time::sleep(Duration::from_secs(3)).await;
    context.set_operators_online(&[1, 2]);

    context.verify_consensus().await;
}

#[tokio::test]
// Test commit message suppression for an operator
async fn test_commit_suppression() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.set_operators_byzantine(
        &[1],
        ByzantineBehavior::MessageSuppression(QbftMessageType::Commit),
    );
    context.verify_consensus().await;
}

#[tokio::test]
// Test sending double messages
async fn test_send_double() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.set_operators_byzantine(&[1], ByzantineBehavior::DoubleVote);
    context.verify_consensus().await;
}

#[tokio::test]
// Test one of the nodes sending invalid messages
async fn test_invalid_message() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
    )
    .await;

    context.set_operators_byzantine(&[1], ByzantineBehavior::InvalidMessage);
    context.verify_consensus().await;
}

#[tokio::test(start_paused = true)]
// Test network partition scenarios
// This simulates temporary network partitions by taking nodes offline and bringing them back
async fn test_network_partition() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Ten, // Using larger committee for partition testing
        setup.all_data,
    )
    .await;

    // Initial partition. We have > f offline so we will not be able to reach consensus
    context.set_operators_offline(&[3, 4, 5, 6]);

    // Wait and change partition
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Bring original back online, and then take = f offline. Should be able to reach consensus
    // now
    context.set_operators_online(&[3, 4, 5, 6]);
    context.set_operators_offline(&[6, 7, 8]);

    context.verify_consensus().await;
}

#[tokio::test(start_paused = true)]
// Test two instances starting late.
// If an instance starts late, it needs to catch up properly, including round changes.
// In this test, there are two nodes offline at the start, with one coming back in the second
// round, and the other one coming back in the third round.
// We assert that the instance can finish.
// This is different compared to network partition because here, messages are delayed instead
// of dropped.
async fn test_late_initialization() {
    let setup = setup_test(1);

    let initialization_delays = HashMap::from([
        (OperatorId(2), Duration::from_secs(3)), // Middle of round 2
        (OperatorId(3), Duration::from_secs(5)), // Middle of round 3
    ]);

    let mut context = TestContext::<BeaconVote>::new_with_delays(
        setup.clock,
        setup.executor,
        CommitteeSize::Four,
        setup.all_data,
        initialization_delays,
    )
    .await;

    context.verify_consensus().await;
}
