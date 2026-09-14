use axum::{extract::State, response::IntoResponse};
use axum_client_ip::ClientIp;
use conduwuit::{Err, Result, err, utils};
use ruma::{OwnedEventId, api::client::message::send_message_event};

use crate::{Ruma, RumaResponse};

const SEND_TXN_EVENT_ID_PREFIX: &[u8] = b"\xFFevent_id:";
const SEND_TXN_DELAY_ID_PREFIX: &[u8] = b"\xFFdelay_id:";

enum CachedSendTxnResponse {
	EventId(OwnedEventId),
	DelayId(String),
}

fn encode_cached_send_txn_response(prefix: &[u8], value: &[u8]) -> Vec<u8> {
	let mut response = Vec::with_capacity(prefix.len().saturating_add(value.len()));
	response.extend_from_slice(prefix);
	response.extend_from_slice(value);
	response
}

fn parse_cached_send_event_id(data: &[u8]) -> Result<OwnedEventId> {
	utils::string_from_bytes(data)
		.map(TryInto::try_into)
		.map_err(|e| err!(Database("Invalid event_id in txnid data: {e:?}")))?
		.map_err(|e| err!(Database("Invalid event_id in txnid data: {e:?}")))
}

fn parse_cached_send_txn_response(
	data: &[u8],
	legacy_is_delay_id: bool,
) -> Result<CachedSendTxnResponse> {
	if let Some(event_id) = data.strip_prefix(SEND_TXN_EVENT_ID_PREFIX) {
		return parse_cached_send_event_id(event_id).map(CachedSendTxnResponse::EventId);
	}

	if let Some(delay_id) = data.strip_prefix(SEND_TXN_DELAY_ID_PREFIX) {
		return utils::string_from_bytes(delay_id).map(CachedSendTxnResponse::DelayId);
	}

	if legacy_is_delay_id {
		// Legacy cache may have an event_id; prefer parsing as event_id if possible.
		if let Ok(event_id) = parse_cached_send_event_id(data) {
			Ok(CachedSendTxnResponse::EventId(event_id))
		} else {
			utils::string_from_bytes(data).map(CachedSendTxnResponse::DelayId)
		}
	} else {
		parse_cached_send_event_id(data).map(CachedSendTxnResponse::EventId)
	}
}

fn cached_send_txn_response(
	data: &[u8],
	legacy_is_delay_id: bool,
) -> Result<axum::response::Response> {
	match parse_cached_send_txn_response(data, legacy_is_delay_id)? {
		| CachedSendTxnResponse::EventId(event_id) =>
			Ok(RumaResponse(send_message_event::v3::Response { event_id }).into_response()),
		| CachedSendTxnResponse::DelayId(delay_id) => Ok(delay_id_response(&delay_id)),
	}
}

fn delay_id_response(delay_id: &str) -> axum::response::Response {
	axum::Json(serde_json::json!({
		"delay_id": delay_id,
	}))
	.into_response()
}

/// # `PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}`
///
/// Send a message event into the room.
///
/// - Is a NOOP if the txn id was already used before and returns the same event
///   id again
/// - The only requirement for the content is that it has to be valid json
/// - Tries to send the event into the room, auth rules will determine if it is
///   allowed
pub(crate) async fn send_message_event_route(
	State(services): State<crate::State>,
	ClientIp(client_ip): ClientIp,
	body: Ruma<send_message_event::v3::Request>,
) -> Result<axum::response::Response> {
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();
	let appservice_info = body.appservice_info.as_ref();
	if services.users.is_suspended(sender_user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	services
		.users
		.update_device_last_seen(sender_user, body.sender_device.as_deref(), client_ip)
		.await;

	if let Some(delay) = body.delay {
		// Check if this is a new transaction id
		if let Ok(response) = services
			.transactions
			.get_room_txn(sender_user, sender_device, &body.room_id, &body.txn_id)
			.await
		{
			if response.is_empty() {
				return Err!(Request(InvalidParam(
					"Tried to use txn id already used for an incompatible endpoint."
				)));
			}
			return cached_send_txn_response(&response, true);
		}

		let txn_lock = services
			.transactions
			.lock_room_txn(sender_user, sender_device, &body.room_id, &body.txn_id)
			.await;

		// Re-check after acquiring the transaction lock in case a concurrent request
		// with the same txn id populated the cache while this request was waiting.
		if let Ok(response) = services
			.transactions
			.get_room_txn(sender_user, sender_device, &body.room_id, &body.txn_id)
			.await
		{
			if response.is_empty() {
				return Err!(Request(InvalidParam(
					"Tried to use txn id already used for an incompatible endpoint."
				)));
			}
			return cached_send_txn_response(&response, true);
		}

		let event = service::rooms::delayed_events::ScheduledDelayedEvent {
			event_type: body.event_type.clone().into(),
			state_key: None,
			content: body.body.body.cast_ref().clone(),
			user_id: sender_user.to_owned(),
			room_id: body.room_id.clone(),
			running_since: std::time::SystemTime::now(),
			delay,
		};
		let delay_id = services
			.rooms
			.delayed_events
			.queue_delayed_event(event)
			.await?;

		services.transactions.add_room_txnid(
			sender_user,
			sender_device,
			&body.room_id,
			&body.txn_id,
			&encode_cached_send_txn_response(SEND_TXN_DELAY_ID_PREFIX, delay_id.as_bytes()),
		);

		drop(txn_lock);

		return Ok(delay_id_response(&delay_id));
	}

	// Check if this is a new transaction id
	if let Ok(response) = services
		.transactions
		.get_room_txn(sender_user, sender_device, &body.room_id, &body.txn_id)
		.await
	{
		// The client might have sent a txnid of the /sendToDevice endpoint
		// This txnid has no response associated with it
		if response.is_empty() {
			return Err!(Request(InvalidParam(
				"Tried to use txn id already used for an incompatible endpoint."
			)));
		}

		return cached_send_txn_response(&response, false);
	}

	let txn_lock = services
		.transactions
		.lock_room_txn(sender_user, sender_device, &body.room_id, &body.txn_id)
		.await;

	// Re-check after acquiring the transaction lock in case a concurrent request
	// with the same txn id populated the cache while this request was waiting.
	if let Ok(response) = services
		.transactions
		.get_room_txn(sender_user, sender_device, &body.room_id, &body.txn_id)
		.await
	{
		// The client might have sent a txnid of the /sendToDevice endpoint
		// This txnid has no response associated with it
		if response.is_empty() {
			return Err!(Request(InvalidParam(
				"Tried to use txn id already used for an incompatible endpoint."
			)));
		}

		return cached_send_txn_response(&response, false);
	}

	let state_lock = services.rooms.state.mutex.lock(&body.room_id).await;

	// Re-check after acquiring the room lock in case a concurrent request with the
	// same txn id populated the cache while this request was waiting.
	if let Ok(response) = services
		.transactions
		.get_room_txn(sender_user, sender_device, &body.room_id, &body.txn_id)
		.await
	{
		// The client might have sent a txnid of the /sendToDevice endpoint
		// This txnid has no response associated with it
		if response.is_empty() {
			return Err!(Request(InvalidParam(
				"Tried to use txn id already used for an incompatible endpoint."
			)));
		}

		return cached_send_txn_response(&response, false);
	}

	let event_id = Box::pin(services.rooms.timeline.send_message_event_helper(
		sender_user,
		&body.room_id,
		&state_lock,
		&body.event_type,
		&body.body.body,
		Some(&body.txn_id),
		if appservice_info.is_some() {
			body.timestamp
		} else {
			None
		},
		None,
	))
	.await?;

	services.transactions.add_room_txnid(
		sender_user,
		sender_device,
		&body.room_id,
		&body.txn_id,
		&encode_cached_send_txn_response(SEND_TXN_EVENT_ID_PREFIX, event_id.as_bytes()),
	);

	drop(state_lock);
	drop(txn_lock);

	Ok(RumaResponse(send_message_event::v3::Response { event_id }).into_response())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn cached_send_txn_response_decodes_typed_event_id_in_delay_branch() -> Result<()> {
		let event_id = b"$event:example.com";
		let cached = encode_cached_send_txn_response(SEND_TXN_EVENT_ID_PREFIX, event_id);

		match parse_cached_send_txn_response(&cached, true)? {
			| CachedSendTxnResponse::EventId(parsed) => {
				assert_eq!(parsed.as_str(), "$event:example.com")
			},
			| CachedSendTxnResponse::DelayId(_) => panic!("expected event id"),
		}

		Ok(())
	}

	#[test]
	fn cached_send_txn_response_decodes_typed_delay_id_in_event_branch() -> Result<()> {
		let cached = encode_cached_send_txn_response(SEND_TXN_DELAY_ID_PREFIX, b"delay-id");

		match parse_cached_send_txn_response(&cached, false)? {
			| CachedSendTxnResponse::DelayId(parsed) => assert_eq!(parsed, "delay-id"),
			| CachedSendTxnResponse::EventId(_) => panic!("expected delay id"),
		}

		Ok(())
	}

	#[test]
	fn cached_send_txn_response_keeps_legacy_branch_specific_decoding() -> Result<()> {
		match parse_cached_send_txn_response(b"legacy-delay-id", true)? {
			| CachedSendTxnResponse::DelayId(parsed) => assert_eq!(parsed, "legacy-delay-id"),
			| CachedSendTxnResponse::EventId(_) => panic!("expected delay id"),
		}

		match parse_cached_send_txn_response(b"$legacy:example.com", false)? {
			| CachedSendTxnResponse::EventId(parsed) => {
				assert_eq!(parsed.as_str(), "$legacy:example.com")
			},
			| CachedSendTxnResponse::DelayId(_) => panic!("expected event id"),
		}

		Ok(())
	}

	#[test]
	fn cached_send_txn_response_prefers_legacy_event_id_in_delay_branch() -> Result<()> {
		match parse_cached_send_txn_response(b"$legacy:example.com", true)? {
			| CachedSendTxnResponse::EventId(parsed) => {
				assert_eq!(parsed.as_str(), "$legacy:example.com")
			},
			| CachedSendTxnResponse::DelayId(_) => panic!("expected event id"),
		}

		Ok(())
	}
}
