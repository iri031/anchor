use std::time::Duration;

use message_sender::testing::MockMessageSender;
use qbft::InstanceHeight;
use slot_clock::ManualSlotClock;
use ssv_types::{
    IndexSet, OperatorId,
    consensus::{BeaconVote, NoDataValidation},
    domain_type::DomainType,
    msgid::{CommitteeId, DutyExecutor, MessageId, Role},
};
use tokio::{
    sync::{
        mpsc::unbounded_channel,
        oneshot,
    },
    time::Instant,
};
use types::Slot;

use crate::{
    Completed, QbftInitialization, QbftMessage, QbftMessageKind,
    instance::qbft_instance,
};

use super::harness::generate_test_data;

// very important: set paused to true for deterministic timer
#[tokio::test(start_paused = true)]
async fn test_timeouts() {
    for i in 1..=10 {
        test_timeout(i).await;
    }
}

async fn test_timeout(round_timeout_to_test: usize) {
    let (sender_tx, _sender_rx) = unbounded_channel();
    let (message_tx, message_rx) = unbounded_channel();
    let (result_tx, result_rx) = oneshot::channel();
    let message_sender = MockMessageSender::new(sender_tx, OperatorId(1));
    let _handle = tokio::spawn(qbft_instance::<BeaconVote>(
        message_rx,
        std::sync::Arc::new(message_sender),
    ));

    // create a slot clock at slot 0 with a slot duration of 12 seconds
    // we are now at the beginning of the slot and remember that instant
    let slot_clock = ManualSlotClock::new(
        Slot::new(0),
        Duration::from_secs(0),
        Duration::from_secs(12),
    );
    let slot_start_time = Instant::now();

    // start at one third slot duration into the slot
    let qbft_start_time = slot_start_time + slot_clock.slot_duration() / 3;

    message_tx
        .send(QbftMessage {
            kind: QbftMessageKind::Initialize(QbftInitialization {
                initial: generate_test_data(0).0,
                validator: Box::new(NoDataValidation),
                message_id: MessageId::new(
                    &DomainType::default(),
                    Role::Committee,
                    &DutyExecutor::Committee(CommitteeId::default()),
                ),
                start_time: qbft_start_time,
                config: qbft::ConfigBuilder::new(
                    OperatorId(1),
                    InstanceHeight::from(0),
                    IndexSet::from([1, 2, 3, 4].map(OperatorId)),
                )
                // we set the round we want to test as maximum round so that the instance times
                // out at the end of that round
                .with_max_rounds(round_timeout_to_test)
                .build()
                .unwrap(),
                on_completed: result_tx,
            }),
            drop_on_finish: None,
        })
        .unwrap();

    // we now wait for the instance to time out
    assert!(matches!(result_rx.await, Ok(Completed::TimedOut)));

    // we now measure the time it took for the instance to time out
    let timeout = Instant::now() - slot_start_time;

    // Calculate the expected timeout
    let mut expected_timeout = Duration::ZERO;
    // first, the instance should not start until start time, so we add the difference from slot
    // start to qbft start.
    expected_timeout += qbft_start_time - slot_start_time;
    // now, we account for the actual rounds:
    for i in 1..=round_timeout_to_test {
        // check if we use short round timeout or long round timeout for this round
        if i <= 8 {
            expected_timeout += Duration::from_secs(2);
        } else {
            expected_timeout += Duration::from_secs(120);
        }
    }
    assert_eq!(timeout, expected_timeout);
}
