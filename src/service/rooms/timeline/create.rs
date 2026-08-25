use std::{cmp, collections::HashMap};

use conduwuit::{info, smallstr::SmallString, trace};
use conduwuit_core::{
	Err, Error, Result, err, implement,
	matrix::{
		event::{Event, gen_event_id},
		pdu::{EventHash, PduBuilder, PduEvent},
		state_res::RoomVersion,
	},
	utils::{self, IterStream, ReadyExt, stream::TryIgnore},
	warn,
};
use futures::{StreamExt, TryStreamExt, future};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, OwnedRoomId, RoomId, RoomVersionId,
	UserId,
	events::{StateEventType, TimelineEventType, room::create::RoomCreateEventContent},
	uint,
};
use serde_json::value::{RawValue, to_raw_value};

use super::RoomMutexGuard;

pub fn pdu_fits(owned_obj: &mut CanonicalJsonObject) -> bool {
	// room IDs, event IDs, senders, types, and state keys should ideally be <= 255
	// bytes but legacy DAGs (e.g. matrix.org) contain events with much larger
	// fields (e.g. 320+ bytes). We relax these to 1024 to prevent DAG splits,
	// relying on the overall 64KiB limit.
	if let Some(CanonicalJsonValue::String(room_id)) = owned_obj.get("room_id") {
		if room_id.len() > 1024 {
			return false;
		}
	}
	if let Some(CanonicalJsonValue::String(event_id)) = owned_obj.get("event_id") {
		if event_id.len() > 1024 {
			return false;
		}
	}
	if let Some(CanonicalJsonValue::String(sender)) = owned_obj.get("sender") {
		if sender.len() > 1024 {
			return false;
		}
	}
	if let Some(CanonicalJsonValue::String(kind)) = owned_obj.get("type") {
		if kind.len() > 1024 {
			return false;
		}
	}
	if let Some(CanonicalJsonValue::String(state_key)) = owned_obj.get("state_key") {
		if state_key.len() > 1024 {
			return false;
		}
	}
	// Now check the full PDU size
	match serde_json::to_string(owned_obj) {
		| Ok(s) => s.len() <= 65535,
		| Err(_) => false,
	}
}

/// Pulls the room version ID out of the given (create) event.
fn room_version_from_event(
	room_id: OwnedRoomId,
	event_type: &TimelineEventType,
	content: &RawValue,
) -> Result<RoomVersionId> {
	if event_type == &TimelineEventType::RoomCreate {
		let content: RoomCreateEventContent = serde_json::from_str(content.get())?;
		Ok(content.room_version)
	} else {
		Err(Error::InconsistentRoomState(
			"non-create event for room of unknown version",
			room_id,
		))
	}
}

/// Creates, hashes, signs, and auth-checks an event.
///
/// The returned PDU has its real event_id (computed from the content hash),
/// not a placeholder.
#[implement(super::Service)]
pub async fn create_event(
	&self,
	pdu_builder: PduBuilder,
	sender: &UserId,
	room_id: Option<&RoomId>,
	_mutex_lock: &RoomMutexGuard,
) -> Result<(PduEvent, CanonicalJsonObject)> {
	let PduBuilder {
		event_type,
		content,
		unsigned,
		state_key,
		redacts,
		timestamp,
	} = pdu_builder;
	// If there was no create event yet, assume we are creating a room
	trace!(
		"Creating event of type {} in room {}",
		event_type,
		room_id.as_ref().map_or("None", |id| id.as_str())
	);
	let room_version_id = match room_id {
		| Some(room_id) => {
			trace!(%room_id, "Looking up existing room ID");
			self.services
				.state
				.get_room_version(room_id)
				.await
				.or_else(|_| {
					room_version_from_event(
						room_id.to_owned(),
						&event_type.clone(),
						&content.clone(),
					)
				})?
		},
		| None => {
			trace!("No room ID, assuming room creation");
			room_version_from_event(
				RoomId::new(self.services.globals.server_name()),
				&event_type.clone(),
				&content.clone(),
			)?
		},
	};

	let room_version = RoomVersion::new(&room_version_id).expect("room version is supported");

	let prev_events: Vec<OwnedEventId> = match room_id {
		| Some(room_id) =>
			self.services
				.state
				.get_forward_extremities(room_id)
				.take(20)
				.collect()
				.await,
		| None => Vec::new(),
	};

	let mut auth_events: HashMap<(StateEventType, SmallString<[u8; 48]>), PduEvent> =
		match room_id {
			| Some(room_id) =>
				self.services
					.state
					.get_auth_events(
						room_id,
						&event_type,
						sender,
						state_key.as_deref(),
						&content,
						&room_version,
					)
					.await?,
			| None => HashMap::new(),
		};

	// When a room has diverged (multiple forward extremities), we MUST resolve the
	// state across all forks to determine the true current state for this new
	// event. Relying solely on `roomid_shortstatehash` (which `get_auth_events`
	// uses) is vulnerable to a TOCTOU race: if federation is concurrently
	// processing a fork, it may have temporarily forced the room state to that
	// fork's state, causing local users to fail auth against missing memberships.
	if let Some(room_id) = room_id {
		if prev_events.len() > 1 {
			info!(
				"DAG fork detected ({} extremities). Resolving state for new local event.",
				prev_events.len()
			);
			if let Ok(Some(compressed_state)) = self
				.services
				.event_handler
				.resolve_extremities(
					prev_events.iter().map(AsRef::as_ref),
					room_id,
					&room_version_id,
					None,
				)
				.await
			{
				let content_val: serde_json::Value =
					serde_json::from_str(content.get()).unwrap_or_default();
				let auth_types = rezzy::auth::auth_types_for_event(
					&event_type.to_string(),
					sender.as_str(),
					state_key.as_deref(),
					&content_val,
					crate::rooms::auth_adapter::to_state_res_version(&room_version_id),
					room_version_id.as_str(),
				);

				let mut new_auth_events = HashMap::new();
				for (ty, sk) in &auth_types {
					let state_ty: StateEventType = ty.as_str().into();
					let state_key = conduwuit_core::matrix::StateKey::from(sk.as_str());
					if let Some(pdu) = self
						.services
						.event_handler
						.find_pdu_in_compressed_state(&state_ty, &state_key, &compressed_state)
						.await
					{
						new_auth_events.insert((state_ty, state_key), pdu);
					}
				}
				auth_events = new_auth_events;
			}
		}
	}
	// Our depth is the maximum depth of prev_events + 1
	let depth = match room_id {
		| Some(_) => prev_events
			.iter()
			.stream()
			.map(Ok)
			.and_then(|event_id| self.get_pdu(event_id))
			.and_then(|pdu| future::ok(pdu.depth))
			.ignore_err()
			.ready_fold(uint!(0), cmp::max)
			.await
			.saturating_add(uint!(1)),
		| None => uint!(1),
	};

	let mut unsigned = unsigned.unwrap_or_default();

	if let Some(room_id) = room_id {
		if let Some(state_key) = &state_key {
			if let Ok(prev_pdu) = self
				.services
				.state_accessor
				.room_state_get(room_id, &event_type.clone().to_string().into(), state_key)
				.await
			{
				unsigned.insert("prev_content".to_owned(), prev_pdu.get_content_as_value());
				unsigned
					.insert("prev_sender".to_owned(), serde_json::to_value(prev_pdu.sender())?);
				unsigned.insert(
					"replaces_state".to_owned(),
					serde_json::to_value(prev_pdu.event_id())?,
				);
			}
		}
	}

	let mut pdu = PduEvent {
		event_id: ruma::event_id!("$thiswillbefilledinlater").into(),
		room_id: room_id.map(ToOwned::to_owned),
		sender: sender.to_owned(),
		origin: None,
		origin_server_ts: timestamp.map_or_else(
			|| {
				utils::millis_since_unix_epoch()
					.try_into()
					.expect("u64 fits into UInt")
			},
			|ts| ts.get(),
		),
		kind: event_type,
		content,
		prev_state_events: if room_version.state_dags {
			// MSC4242: populate with state DAG forward extremities
			let extremities: Vec<OwnedEventId> = match room_id {
				| Some(room_id) =>
					self.services
						.state
						.get_state_forward_extremities(room_id)
						.map(Into::into)
						.collect()
						.await,
				| None => Vec::new(), // create event has no prev_state_events
			};
			Some(extremities)
		} else {
			None
		},
		state_key,
		prev_events,
		depth,
		auth_events: auth_events
			.values()
			.map(|pdu| pdu.event_id.clone())
			.collect(),
		redacts,
		unsigned: if unsigned.is_empty() {
			None
		} else {
			Some(to_raw_value(&unsigned)?)
		},
		hashes: EventHash { sha256: String::new() },
		signatures: None,
		rejected: false,
	};

	// Hash, sign, and compute the real event_id immediately so all
	// downstream logging and auth checks see the real ID, not the
	// placeholder.
	let pdu_json = self.hash_sign_and_finalize(&mut pdu, &room_version_id)?;

	info!(
		"auth_events keys for new {} at PDU {}: {:?}",
		pdu.kind,
		pdu.event_id,
		auth_events.keys().collect::<Vec<_>>()
	);

	let create_event = if let Some(room_id) = room_id {
		self.services
			.state_accessor
			.room_state_get(room_id, &StateEventType::RoomCreate, "")
			.await
			.ok()
	} else {
		None
	};

	let state_provider =
		crate::rooms::auth_adapter::PduStateProvider::from_smallstr_map(&auth_events)
			.with_create_event(create_event.as_ref());
	if !crate::rooms::auth_adapter::rezzy_auth_check(
		&pdu,
		&state_provider,
		crate::rooms::auth_adapter::to_state_res_version(&room_version_id),
	) {
		return Err!(Request(Forbidden("Event is not authorized.")));
	}
	trace!(
		"Event {} in room {} is authorized",
		pdu.event_id,
		pdu.room_id.as_ref().map_or("None", |id| id.as_str())
	);

	// MSC4291: ensure `room_id` in internal representation even for v12+
	if pdu.room_id.is_none() {
		let internal_room_id = pdu
			.room_id_or_hash()
			.ok_or_else(|| err!(Request(Forbidden("Event has no room_id"))))?;
		pdu.room_id = Some(internal_room_id);
	}

	Ok((pdu, pdu_json))
}

#[implement(super::Service)]
pub async fn create_hash_and_sign_event(
	&self,
	pdu_builder: PduBuilder,
	sender: &UserId,
	room_id: Option<&RoomId>,
	mutex_lock: &RoomMutexGuard, /* Take mutex guard to make sure users get the room
	                              * state mutex */
) -> Result<(PduEvent, CanonicalJsonObject)> {
	if !self.services.globals.user_is_local(sender) {
		return Err!(Request(Forbidden("Sender must be a local user")));
	}
	let (pdu, mut pdu_json) = self
		.create_event(pdu_builder, sender, room_id, mutex_lock)
		.await?;

	// Verify that the *full* PDU isn't over 64KiB.
	// Ruma only validates that it's under 64KiB before signing and hashing.
	// Has to be cloned to prevent mutating pdu_json itself :(
	if !pdu_fits(&mut pdu_json.clone()) {
		// feckin huge PDU mate
		return Err!(Request(TooLarge("Message/PDU is too long (exceeds 65535 bytes)")));
	}

	// Check with the policy server
	if pdu.room_id.is_some() {
		trace!(
			"Checking event in room {} with policy server",
			pdu.room_id.as_ref().map_or("None", |id| id.as_str())
		);
		let policy_room_id = pdu.room_id_or_hash().expect("has room ID");
		match self
			.services
			.event_handler
			.ask_policy_server(&pdu, &mut pdu_json, &policy_room_id, false)
			.await
		{
			| Ok(true) => {},
			| Ok(false) => {
				return Err!(Request(Forbidden(debug_warn!(
					"Policy server marked this event as spam"
				))));
			},
			| Err(e) => {
				// fail open
				warn!("Failed to check event with policy server: {e}");
			},
		}
	}

	// Generate short event id
	trace!(
		"Generating short event ID for {} in room {}",
		pdu.event_id,
		pdu.room_id.as_ref().map_or("None", |id| id.as_str())
	);
	let _shorteventid = self
		.services
		.short
		.get_or_create_shorteventid(&pdu.event_id)
		.await;

	trace!("New PDU created: {pdu:?}");
	Ok((pdu, pdu_json))
}

/// Serialize a PDU to canonical JSON, hash and sign it, then compute and
/// set the real `event_id`.  Returns the signed canonical JSON object.
#[implement(super::Service)]
pub fn hash_sign_and_finalize(
	&self,
	pdu: &mut PduEvent,
	room_version_id: &RoomVersionId,
) -> Result<CanonicalJsonObject> {
	// Sort keys canonically and purge "placeholder" `event_id`
	let mut pdu_json = utils::to_canonical_object(&*pdu).map_err(|e| {
		err!(Request(BadJson(warn!("Failed to convert PDU to canonical JSON: {e}"))))
	})?;
	pdu_json.remove("event_id");
	if let Ok(rv) = RoomVersion::new(room_version_id) {
		if rv.state_dags {
			pdu_json.remove("auth_events");
		}
	}

	// Sign
	if let Err(e) = self
		.services
		.server_keys
		.hash_and_sign_event(&mut pdu_json, room_version_id)
	{
		return match e {
			| Error::Signatures(ruma::signatures::Error::PduSize) => {
				Err!(Request(TooLarge("Message/PDU is too long (exceeds 65535 bytes)")))
			},
			| _ => Err!(Request(BadJson(warn!("Signing event failed: {e}")))),
		};
	}

	pdu.event_id = gen_event_id(&pdu_json, room_version_id)?;
	pdu_json.insert("event_id".into(), CanonicalJsonValue::String(pdu.event_id.clone().into()));

	// Rehydrate the persisted event from the finalized JSON so the stored PDU
	// carries the same hashes/signatures as the canonical representation.
	*pdu = PduEvent::from_id_val(&pdu.event_id, pdu_json.clone(), pdu.room_id.as_deref())?;

	Ok(pdu_json)
}

#[cfg(test)]
mod tests {
	use ruma::{CanonicalJsonObject, CanonicalJsonValue};

	use super::*;

	#[test]
	fn test_pdu_fits() {
		let mut obj = CanonicalJsonObject::new();
		obj.insert("type".into(), CanonicalJsonValue::String("m.room.message".into()));
		assert!(pdu_fits(&mut obj));

		// Test exact size limit boundary (65535 bytes)
		let large_string = "a".repeat(65400);
		obj.insert("content".into(), CanonicalJsonValue::String(large_string));
		assert!(pdu_fits(&mut obj));

		// Test oversized PDU (>65535 bytes)
		let huge_string = "a".repeat(66000);
		obj.insert("content".into(), CanonicalJsonValue::String(huge_string));
		assert!(!pdu_fits(&mut obj));

		// Test oversized individual field (>1024 chars for room_id)
		let mut obj2 = CanonicalJsonObject::new();
		obj2.insert("room_id".into(), CanonicalJsonValue::String("a".repeat(1025)));
		assert!(!pdu_fits(&mut obj2));
	}
}
