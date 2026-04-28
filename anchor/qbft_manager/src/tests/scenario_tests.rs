use std::{collections::HashMap, time::Duration};

use ssv_types::{OperatorId, consensus::BeaconVote, consensus::QbftMessageType};

use super::harness::{ByzantineBehavior, CommitteeSize, TestContext, setup_test};

#[tokio::test]
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
async fn test_network_partition() {
    let setup = setup_test(1);
    let mut context = TestContext::<BeaconVote>::new(
        setup.clock,
        setup.executor,
        CommitteeSize::Ten,
        setup.all_data,
    )
    .await;

    context.set_operators_offline(&[3, 4, 5, 6]);

    tokio::time::sleep(Duration::from_secs(3)).await;

    context.set_operators_online(&[3, 4, 5, 6]);
    context.set_operators_offline(&[6, 7, 8]);

    context.verify_consensus().await;
}

#[tokio::test(start_paused = true)]
async fn test_late_initialization() {
    let setup = setup_test(1);

    let initialization_delays = HashMap::from([
        (OperatorId(2), Duration::from_secs(3)),
        (OperatorId(3), Duration::from_secs(5)),
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
