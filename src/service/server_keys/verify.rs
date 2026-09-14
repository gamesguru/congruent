use conduwuit::{
	Err, Result, debug_warn, implement, matrix::event::gen_event_id_canonical_json, trace,
};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, OwnedServerName, RoomVersionId,
	ServerName, UserId, signatures::Verified,
};
use serde_json::value::RawValue as RawJsonValue;

/// Extract the origin server(s) from an event and strip all non-origin
/// signatures. Per the Matrix spec, only the origin server's signature is
/// required for verification. Extra signatures added by relay/policy servers
/// (e.g. `asgard.chat`) can cause spurious verification failures when we don't
/// have their signing keys.
///
/// For room versions V1/V2, the "origin" includes both the sender's server and
/// the event_id's server (since event_ids were server-assigned). For V3+,
/// event_ids are content-hash-derived, so only the sender's server matters.
fn isolate_origin_signatures(
	event: &CanonicalJsonObject,
	room_version: &RoomVersionId,
) -> CanonicalJsonObject {
	// Extract the sender's server name
	let sender_server: Option<OwnedServerName> = event
		.get("sender")
		.and_then(|v| match v {
			| CanonicalJsonValue::String(s) => UserId::parse(s.as_str()).ok(),
			| _ => None,
		})
		.map(|user_id| user_id.server_name().to_owned());

	// For V1/V2, event_id is server-assigned (e.g. "$abc:example.com"),
	// so that server's signature is also authoritative.
	let event_id_server: Option<OwnedServerName> = match room_version {
		| RoomVersionId::V1 | RoomVersionId::V2 => event
			.get("event_id")
			.and_then(|v| match v {
				| CanonicalJsonValue::String(s) => {
					// V1/V2 event_ids look like "$opaque:server.name"
					s.split_once(':')
						.and_then(|(_, srv)| ServerName::parse(srv).ok())
				},
				| _ => None,
			})
			.map(ToOwned::to_owned),
		| _ => None,
	};

	let Some(ref sender) = sender_server else {
		// Can't determine origin — return as-is, let ruma handle the failure
		return event.clone();
	};

	// The `origin` field identifies the server that created/signed the event.
	// For restricted joins, this differs from the sender (the resident server
	// signs the join event on behalf of the joining user).
	let origin_field_server: Option<OwnedServerName> = event
		.get("origin")
		.and_then(|v| match v {
			| CanonicalJsonValue::String(s) => ServerName::parse(s.as_str()).ok(),
			| _ => None,
		})
		.map(ToOwned::to_owned);

	// Build the set of origin servers to retain
	let mut origin_servers: Vec<&ServerName> = vec![sender.as_ref()];
	if let Some(ref eid_server) = event_id_server {
		if eid_server != sender {
			origin_servers.push(eid_server.as_ref());
		}
	}
	if let Some(ref origin_server) = origin_field_server {
		if !origin_servers.iter().any(|s| *s == origin_server.as_str()) {
			origin_servers.push(origin_server.as_ref());
		}
	}

	// For V8+ restricted joins, ruma requires a signature from the server of
	// the user in `join_authorised_via_users_server`. We must retain it.
	let authorized_server: Option<OwnedServerName> = match room_version {
		| RoomVersionId::V1
		| RoomVersionId::V2
		| RoomVersionId::V3
		| RoomVersionId::V4
		| RoomVersionId::V5
		| RoomVersionId::V6
		| RoomVersionId::V7 => None,
		| _ => event
			.get("content")
			.and_then(|c| c.as_object())
			.and_then(|c| c.get("join_authorised_via_users_server"))
			.and_then(|v| v.as_str())
			.and_then(|s| UserId::parse(s).ok())
			.map(|u| u.server_name().to_owned()),
	};
	if let Some(ref auth_server) = authorized_server {
		if !origin_servers.iter().any(|s| *s == auth_server.as_str()) {
			origin_servers.push(auth_server.as_ref());
		}
	}

	let mut filtered = event.clone();
	if let Some(CanonicalJsonValue::Object(sigs)) = filtered.get_mut("signatures") {
		let orig_count = sigs.len();
		sigs.retain(|server_name, _| {
			origin_servers
				.iter()
				.any(|origin| server_name == origin.as_str())
		});
		let removed = orig_count.saturating_sub(sigs.len());
		if removed > 0 {
			trace!(
				sender = sender.as_str(),
				removed, "Stripped non-origin signatures before verification"
			);
		}
	}

	filtered
}

#[implement(super::Service)]
pub async fn validate_and_add_event_id(
	&self,
	pdu: &RawJsonValue,
	room_version: &RoomVersionId,
) -> Result<(OwnedEventId, CanonicalJsonObject)> {
	let (event_id, mut value) = gen_event_id_canonical_json(pdu, room_version)?;

	if self
		.services
		.server
		.config
		.bypassed_signature_events
		.contains(&event_id)
	{
		conduwuit::info!(
			"Bypassing signature verification for configured exception event: {event_id}"
		);
	} else if let Err(e) = self
		.verify_event_at(&value, Some(room_version), "validate_and_add_event_id")
		.await
	{
		return Err!(BadServerResponse(debug_error!(
			"Event {event_id} failed verification: {e:?}"
		)));
	}

	value.insert("event_id".into(), CanonicalJsonValue::String(event_id.as_str().into()));

	Ok((event_id, value))
}

#[implement(super::Service)]
pub async fn validate_and_add_event_id_no_fetch(
	&self,
	pdu: &RawJsonValue,
	room_version: &RoomVersionId,
) -> Result<(OwnedEventId, CanonicalJsonObject)> {
	trace!(?pdu, "Validating PDU without fetching keys");
	let (event_id, mut value) = gen_event_id_canonical_json(pdu, room_version)?;
	trace!(event_id = event_id.as_str(), "Generated event ID, checking required keys");
	if !self.required_keys_exist(&value, room_version).await {
		debug_warn!(
			"Event {event_id} is missing required keys, cannot verify without fetching keys"
		);
		return Err!(BadServerResponse(debug_warn!(
			"Event {event_id} cannot be verified: missing keys."
		)));
	}
	trace!("All required keys exist, verifying event");
	if self
		.services
		.server
		.config
		.bypassed_signature_events
		.contains(&event_id)
	{
		conduwuit::info!(
			"Bypassing signature verification for configured exception event: {event_id}"
		);
	} else if let Err(e) = self
		.verify_event_at(&value, Some(room_version), "validate_and_add_event_id_no_fetch")
		.await
	{
		debug_warn!("Event verification failed");
		return Err!(BadServerResponse(debug_error!(
			"Event {event_id} failed verification: {e:?}"
		)));
	}
	trace!("Event verified successfully");

	value.insert("event_id".into(), CanonicalJsonValue::String(event_id.as_str().into()));

	Ok((event_id, value))
}

#[implement(super::Service)]
pub async fn verify_event(
	&self,
	event: &CanonicalJsonObject,
	room_version: Option<&RoomVersionId>,
) -> Result<Verified> {
	self.verify_event_at(event, room_version, "verify_event")
		.await
}

#[implement(super::Service)]
pub async fn verify_event_at(
	&self,
	event: &CanonicalJsonObject,
	room_version: Option<&RoomVersionId>,
	context: &'static str,
) -> Result<Verified> {
	let room_version = room_version.unwrap_or(&RoomVersionId::V12);

	// Per spec, only the origin server's signature is required.
	// Relay/policy servers (e.g. asgard.chat) may add extra signatures
	// that we don't have keys for, causing spurious verification failures.
	// Strip non-origin signatures before verification.
	let event = isolate_origin_signatures(event, room_version);

	let keys = self.get_event_keys(&event, room_version).await?;

	let result = ruma::signatures::verify_event(&keys, &event, room_version);
	if let Err(ref e) = result {
		let event_id = event
			.get("event_id")
			.and_then(|v| v.as_str())
			.unwrap_or("unknown");
		let signatures = event
			.get("signatures")
			.and_then(|v| v.as_object())
			.map(|v| serde_json::to_string(v).unwrap_or_default())
			.unwrap_or_default();
		// Canonical JSON actually fed into the signature check (post
		// isolate_origin_signatures, pre our own event_id re-insertion) --
		// needed to diff against what the origin actually signed when
		// tracking down canonicalization-mismatch verification failures.
		// Remove once that's resolved; this is deliberately temporary.
		let canonical_json = serde_json::to_string(&event).unwrap_or_default();
		conduwuit::warn!(
			"Signature verification failed for event {event_id} in {context}. Error: {e:?}. \
			 Available keys: {keys:?}. Event signatures: {signatures}. Canonical JSON verified: \
			 {canonical_json}"
		);
	}

	result.map_err(Into::into)
}

#[implement(super::Service)]
pub async fn verify_json(
	&self,
	event: &CanonicalJsonObject,
	room_version: Option<&RoomVersionId>,
) -> Result {
	let room_version = room_version.unwrap_or(&RoomVersionId::V12);
	let keys = self.get_event_keys(event, room_version).await?;
	ruma::signatures::verify_json(&keys, event.clone()).map_err(Into::into)
}

use std::sync::Arc;

use futures::{Stream, StreamExt};

#[implement(super::Service)]
pub fn concurrent_validate_and_add_events<'a, I>(
	self: &Arc<Self>,
	events: I,
	room_version: &'a RoomVersionId,
) -> impl Stream<Item = Result<(OwnedEventId, CanonicalJsonObject)>> + Send + 'a
where
	I: IntoIterator<Item = Box<RawJsonValue>> + Send + 'a,
	<I as IntoIterator>::IntoIter: Send,
{
	let server_keys_outer = self.clone();
	futures::stream::iter(events)
		.map(move |pdu: Box<RawJsonValue>| {
			let server_keys = server_keys_outer.clone();
			let room_version_id = room_version.clone();
			let runtime = server_keys.services.server.runtime().clone();
			runtime.spawn(async move {
				server_keys
					.validate_and_add_event_id(&pdu, &room_version_id)
					.await
			})
		})
		.buffer_unordered(self.services.server.concurrency_scaled(200))
		.map(
			|res: Result<Result<(OwnedEventId, CanonicalJsonObject)>, tokio::task::JoinError>| {
				res.unwrap_or_else(|e| Err(conduwuit::err!(Database("Join task failed: {e}"))))
			},
		)
}
