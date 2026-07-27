use std::collections::BTreeMap;

use axum::extract::State;
use axum_client_ip::ClientIp;
use conduwuit::{Err, PduCount, Result};
use ruma::{
	MilliSecondsSinceUnixEpoch,
	api::client::{read_marker::set_read_marker, receipt::create_receipt},
	events::{
		RoomAccountDataEventType,
		receipt::{ReceiptThread, ReceiptType},
	},
};

use crate::Ruma;

/// # `POST /_matrix/client/r0/rooms/{roomId}/read_markers`
///
/// Sets different types of read markers.
///
/// - Updates fully-read account data event to `fully_read`
/// - If `read_receipt` is set: Update private marker and public read receipt
///   EDU
pub(crate) async fn set_read_marker_route(
	State(services): State<crate::State>,
	body: Ruma<set_read_marker::v3::Request>,
) -> Result<set_read_marker::v3::Response> {
	let sender_user = body.sender_user();

	if let Some(event) = &body.fully_read {
		let fully_read_event = ruma::events::fully_read::FullyReadEvent {
			content: ruma::events::fully_read::FullyReadEventContent { event_id: event.clone() },
		};

		services
			.account_data
			.update(
				Some(&body.room_id),
				sender_user,
				RoomAccountDataEventType::FullyRead,
				&serde_json::to_value(fully_read_event)?,
			)
			.await?;
	}

	// ping presence
	if services.config.allow_local_presence {
		services
			.presence
			.ping_presence(sender_user, &ruma::presence::PresenceState::Online)
			.await?;
	}

	if let Some(event) = &body.read_receipt {
		if services.config.allow_local_read_receipts
			&& !services.users.is_suspended(sender_user).await?
		{
			update_read_receipt(
				&services,
				sender_user,
				&body.room_id,
				event,
				ReceiptThread::Unthreaded,
			)
			.await?;
		}
	}

	if let Some(event) = &body.private_read_receipt {
		update_private_read_receipt(
			&services,
			sender_user,
			&body.room_id,
			event,
			ReceiptThread::Unthreaded,
		)
		.await?;
	}

	Ok(set_read_marker::v3::Response {})
}

/// # `POST /_matrix/client/r0/rooms/{roomId}/receipt/{receiptType}/{eventId}`
///
/// Sets private read marker and public read receipt EDU.
pub(crate) async fn create_receipt_route(
	State(services): State<crate::State>,
	ClientIp(client_ip): ClientIp,
	body: Ruma<create_receipt::v3::Request>,
) -> Result<create_receipt::v3::Response> {
	let sender_user = body.sender_user();
	services
		.users
		.update_device_last_seen(sender_user, body.sender_device.as_deref(), client_ip)
		.await;

	// ping presence
	if services.config.allow_local_presence {
		services
			.presence
			.ping_presence(sender_user, &ruma::presence::PresenceState::Online)
			.await?;
	}

	match body.receipt_type {
		| create_receipt::v3::ReceiptType::FullyRead => {
			let fully_read_event = ruma::events::fully_read::FullyReadEvent {
				content: ruma::events::fully_read::FullyReadEventContent {
					event_id: body.event_id.clone(),
				},
			};
			services
				.account_data
				.update(
					Some(&body.room_id),
					sender_user,
					RoomAccountDataEventType::FullyRead,
					&serde_json::to_value(fully_read_event)?,
				)
				.await?;
		},
		| create_receipt::v3::ReceiptType::Read => {
			if services.config.allow_local_read_receipts
				&& !services.users.is_suspended(sender_user).await?
			{
				update_read_receipt(
					&services,
					sender_user,
					&body.room_id,
					&body.event_id,
					body.body.thread.clone(),
				)
				.await?;
			}
		},
		| create_receipt::v3::ReceiptType::ReadPrivate => {
			update_private_read_receipt(
				&services,
				sender_user,
				&body.room_id,
				&body.event_id,
				body.body.thread.clone(),
			)
			.await?;
		},
		| _ => {
			return Err!(Request(InvalidParam(warn!(
				"Received unknown read receipt type: {}",
				&body.receipt_type
			))));
		},
	}

	Ok(create_receipt::v3::Response {})
}

async fn update_read_receipt(
	services: &crate::State,
	sender_user: &ruma::UserId,
	room_id: &ruma::RoomId,
	event_id: &ruma::EventId,
	thread: ReceiptThread,
) -> Result<()> {
	// Spec: server SHOULD NOT allow read receipts to move backwards
	services
		.rooms
		.timeline
		.get_pdu_in_room(Some(room_id), event_id)
		.await
		.map_err(|_| conduwuit::err!(Request(NotFound("Event not found in room."))))?;
	let new_count = services
		.rooms
		.timeline
		.get_pdu_count(event_id)
		.await
		.map_err(|_| conduwuit::err!(Request(NotFound("Event not found in room."))))?;

	let mut ignore_receipt = false;
	if let PduCount::Normal(new_count) = new_count {
		if let Some(old_event_id) = services
			.rooms
			.read_receipt
			.readreceipt_get(room_id, sender_user, Some(&thread))
			.await
		{
			if let Ok(PduCount::Normal(old_count)) =
				services.rooms.timeline.get_pdu_count(&old_event_id).await
			{
				if new_count <= old_count {
					conduwuit::info!(
						target: "read_receipt_debug",
						"Ignoring read receipt for {} from {} because it moves backwards from {} to {}",
						room_id, sender_user, old_count, new_count
					);
					ignore_receipt = true;
				}
			}
		}
	} else {
		conduwuit::info!(
			target: "read_receipt_debug",
			"Event {} not found in timeline, ignoring read receipt", event_id
		);
		ignore_receipt = true;
	}

	if !ignore_receipt {
		let receipt_content = BTreeMap::from_iter([(
			event_id.to_owned(),
			BTreeMap::from_iter([(
				ReceiptType::Read,
				BTreeMap::from_iter([(sender_user.to_owned(), ruma::events::receipt::Receipt {
					ts: Some(MilliSecondsSinceUnixEpoch::now()),
					thread,
				})]),
			)]),
		)]);

		services
			.rooms
			.read_receipt
			.readreceipt_update(sender_user, room_id, &ruma::events::receipt::ReceiptEvent {
				content: ruma::events::receipt::ReceiptEventContent(receipt_content),
				room_id: room_id.to_owned(),
			})
			.await;

		services
			.rooms
			.user
			.reset_notification_counts(sender_user, room_id);

		conduwuit::info!(
			target: "read_receipt_debug",
			"Accepted read receipt for {} from {}", event_id, sender_user
		);
	}

	Ok(())
}

async fn update_private_read_receipt(
	services: &crate::State,
	sender_user: &ruma::UserId,
	room_id: &ruma::RoomId,
	event_id: &ruma::EventId,
	thread: ReceiptThread,
) -> Result<()> {
	services
		.rooms
		.timeline
		.get_pdu_in_room(Some(room_id), event_id)
		.await
		.map_err(|_| conduwuit::err!(Request(NotFound("Event not found in room."))))?;
	let count = services
		.rooms
		.timeline
		.get_pdu_count(event_id)
		.await
		.map_err(|_| conduwuit::err!(Request(NotFound("Event not found in room."))))?;

	let PduCount::Normal(new_count) = count else {
		return Err!(Request(InvalidParam(
			"Event is a backfilled PDU and cannot be marked as read."
		)));
	};

	let is_unthreaded = thread == ReceiptThread::Unthreaded;
	let receipt_content = BTreeMap::from_iter([(
		event_id.to_owned(),
		BTreeMap::from_iter([(
			ReceiptType::ReadPrivate,
			BTreeMap::from_iter([(sender_user.to_owned(), ruma::events::receipt::Receipt {
				ts: Some(MilliSecondsSinceUnixEpoch::now()),
				thread,
			})]),
		)]),
	)]);

	let receipt_event = ruma::events::receipt::ReceiptEvent {
		content: ruma::events::receipt::ReceiptEventContent(receipt_content),
		room_id: room_id.to_owned(),
	};

	// The backwards-move check happens inside `private_read_set`, atomically with
	// the write, so a racing older receipt can't be applied after a newer one.
	let applied = services.rooms.read_receipt.private_read_set(
		room_id,
		sender_user,
		new_count,
		&receipt_event,
	)?;

	if applied {
		if is_unthreaded {
			services
				.rooms
				.user
				.reset_notification_counts(sender_user, room_id);
		}

		conduwuit::debug!("Accepted private read receipt for {} from {}", event_id, sender_user);
	} else {
		conduwuit::info!(
			target: "read_receipt_debug",
			"Ignoring private read receipt for {} from {} because it moves backwards or is \
			 stale",
			room_id,
			sender_user,
		);
	}

	Ok(())
}
