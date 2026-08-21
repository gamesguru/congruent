use std::{
	cmp::Reverse,
	collections::BinaryHeap,
	sync::atomic::Ordering,
	time::{Duration, SystemTime},
};

use conduwuit::{Result, err, utils::stream::TryIgnore};
use futures::StreamExt;
use loole::Receiver;
use tokio::{select, time::sleep};

use super::{DELAY_ID_SIZE, ScheduledDelayedEvent, Service};

pub(crate) struct SubmissionQueue {
	receiver: Receiver<(SystemTime, String)>,
	queue: BinaryHeap<(Reverse<SystemTime>, String)>,
}

impl SubmissionQueue {
	pub(crate) fn new(receiver: Receiver<(SystemTime, String)>) -> Self {
		Self { receiver, queue: BinaryHeap::new() }
	}
}

#[allow(clippy::arithmetic_side_effects)]
pub(crate) async fn worker(service: &Service) -> Result<()> {
	let mut queue = service
		.submission_queue
		.try_lock()
		.map_err(|_| err!(Err("Attempted to launch multiple instances of the worker")))?;
	let receiver = queue.receiver.clone();

	let mut stream = service
		.db
		.delayid_scheduleddelayedevent
		.stream::<'_, String, ScheduledDelayedEvent>()
		.ignore_err();

	let mut loaded_events = Vec::new();

	while let Some((delay_id, event)) = stream.next().await {
		let submission_time = event.running_since + event.delay;
		queue
			.queue
			.push((Reverse(submission_time), delay_id.clone()));
		loaded_events.push(delay_id);
	}

	let mut scheduled_events = service.scheduled_events.lock().await;
	scheduled_events.extend(loaded_events);
	drop(scheduled_events);

	// work loop

	loop {
		if service.interrupt_requested.load(Ordering::Relaxed) {
			break;
		}

		let item_size = DELAY_ID_SIZE.saturating_add(size_of::<SystemTime>());
		let mem_usage = queue.queue.len().saturating_mul(item_size);
		service.mem_usage.store(mem_usage, Ordering::Relaxed);

		// NOTE: If a new event with an earlier submission time is pushed to the
		// queue while the `sleep(sleep_duration).await` is in progress, the worker
		// will not wake early and will continue sleeping for the originally-peeked
		// duration. This behavior is intentional, acceptable, and avoids complex
		// sleep-interrupt mechanisms for this use case.
		let next_submit = async {
			let (time, _) = queue.queue.peek()?;
			if let Ok(sleep_duration) = time.0.duration_since(SystemTime::now()) {
				sleep(sleep_duration).await;
			}
			let (_, delay_id) = queue.queue.pop()?;
			Some(delay_id)
		};

		let next_receive = receiver.recv_async();

		// RescvFuture is cancellation-safe
		select! {
			Some(delay_id) = next_submit => {
				if let Some((time, delay_id)) = Box::pin(service.send_event_if_ready(delay_id)).await {
					queue.queue.push((Reverse(time), delay_id));
				}
			},
			Ok((time, delay_id)) = next_receive => {
				queue.queue.push((Reverse(time), delay_id));
			},
			// Loop regularly to check if the service needs to stop even when there are no in-flight delayed events
			() = sleep(Duration::from_secs(2)) => (),
		}
	}

	Ok(())
}
