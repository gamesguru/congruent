//! Position-aware notification-count recomputation for read receipts.
//!
//! `userroomid_notificationcount`/`userroomid_highlightcount` are plain
//! running counters (see `rooms::user`), incremented once per PDU at append
//! time and otherwise opaque -- they don't record *which* PDUs contributed.
//! A receipt at an arbitrary (not necessarily latest) position therefore
//! can't be satisfied by decrementing; the only way to know how many
//! notifications remain after a given position is to replay push-rule
//! evaluation for the PDUs after it and re-sum. This mirrors
//! `append::evaluate_pdu_for_user` exactly so the two never disagree about
//! what counts as a notification/highlight.

use std::ops::Bound;

use conduwuit_core::{
	implement,
	matrix::{event::Event, pdu::PduCount},
};
use futures::{StreamExt, pin_mut};
use ruma::{
	EventId, RoomId, UserId,
	events::{
		GlobalAccountDataEventType, StateEventType, push_rules::PushRulesEvent,
		receipt::ReceiptThread, room::power_levels::RoomPowerLevelsEventContent,
	},
	push::Ruleset,
};

/// Recompute and persist notification/highlight counts for `user_id` in
/// `room_id` after a read receipt lands at `after`, scoped by `thread`.
///
/// - `Main`: only the room's non-thread bucket is recomputed.
/// - `Thread(root)`: only that thread's bucket is recomputed.
/// - `Unthreaded` (and any other variant): per MSC4102, an unthreaded receipt
///   clears everything at/before its position across the whole room -- the main
///   bucket and every thread bucket this user has notifications tracked in.
#[implement(super::Service)]
pub async fn recompute_notification_counts_for_thread(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread: &ReceiptThread,
	after: PduCount,
) {
	match thread {
		| ReceiptThread::Main => {
			let (notifications, highlights) =
				self.recompute_scope(room_id, user_id, after, None).await;
			self.services.user.put_main_notification_counts(
				user_id,
				room_id,
				notifications,
				highlights,
			);
		},
		| ReceiptThread::Thread(root) => {
			let (notifications, highlights) = self
				.recompute_scope(room_id, user_id, after, Some(root))
				.await;
			self.services.user.put_thread_notification_counts(
				user_id,
				room_id,
				root,
				notifications,
				highlights,
			);
		},
		| _ => {
			let (notifications, highlights) =
				self.recompute_scope(room_id, user_id, after, None).await;
			self.services.user.put_main_notification_counts(
				user_id,
				room_id,
				notifications,
				highlights,
			);

			let thread_roots = self
				.services
				.user
				.known_thread_roots(user_id, room_id)
				.await;
			for root in thread_roots {
				let (notifications, highlights) = self
					.recompute_scope(room_id, user_id, after, Some(&root))
					.await;
				self.services.user.put_thread_notification_counts(
					user_id,
					room_id,
					&root,
					notifications,
					highlights,
				);
			}
		},
	}
}

/// Sum notifications/highlights for `user_id` among PDUs strictly after
/// `after`, restricted to `thread_filter` (`None` = main timeline only,
/// `Some(root)` = that thread only) -- matching the same partitioning
/// `append::append_pdu` used when it originally incremented the counters.
#[implement(super::Service)]
async fn recompute_scope(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	after: PduCount,
	thread_filter: Option<&EventId>,
) -> (u64, u64) {
	let power_levels: RoomPowerLevelsEventContent = self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomPowerLevels, "")
		.await
		.unwrap_or_default();

	let rules_for_user = self
		.services
		.account_data
		.get_global(user_id, GlobalAccountDataEventType::PushRules)
		.await
		.map_or_else(
			|_| Ruleset::server_default(user_id),
			|ev: PushRulesEvent| ev.content.global,
		);

	let now = conduwuit_core::utils::millis_since_unix_epoch();

	let mut notifications = 0_u64;
	let mut highlights = 0_u64;

	let stream = self.pdus(room_id, Bound::Excluded(after));
	pin_mut!(stream);
	while let Some(Ok((_, pdu))) = stream.next().await {
		if pdu.sender() == user_id {
			continue;
		}

		if self
			.services
			.users
			.user_is_ignored(pdu.sender(), user_id)
			.await
		{
			continue;
		}

		// Historical/backfilled PDUs never generated a notification at append
		// time (see `append::append_pdu`'s `is_historical` gate); keep that in
		// sync so recompute can't manufacture notifications that never existed.
		let is_historical = now.saturating_sub(pdu.origin_server_ts().0.into()) > 10 * 60 * 1000;
		if is_historical {
			continue;
		}

		let thread_root = self.services.threads.get_thread_id(&pdu).await;
		let in_scope = match thread_filter {
			| None => thread_root.is_none(),
			| Some(root) => thread_root.as_deref() == Some(root),
		};
		if !in_scope {
			continue;
		}

		let serialized = pdu.to_format();
		let (notify, highlight) = self
			.evaluate_pdu_for_user(user_id, &serialized, room_id, &rules_for_user, &power_levels)
			.await;

		if notify {
			notifications = notifications.saturating_add(1);
		}
		if highlight {
			highlights = highlights.saturating_add(1);
		}
	}

	(notifications, highlights)
}
