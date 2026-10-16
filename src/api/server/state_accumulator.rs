use conduwuit::{Err, Event, Result, err, info};
use conduwuit_core::utils::hash::lthash::serialize_lthash;
use conduwuit_service::rooms::state_accessor::PRIMARY_ALGORITHM;
use futures::TryStreamExt;
use slipstream::{OwnedEventId, api::federation::reconciliation::get_state_accumulator};

use super::AccessCheck;
use crate::{Ruma, router::extract::State};

/// # `GET /_matrix/federation/unstable/tk.nutra.msc4500/state_accumulator/{roomId}`
///
/// Returns the LtHash lattice over the state at `event_id`.
pub(crate) async fn get_state_accumulator_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_accumulator::unstable::Request>,
) -> Result<get_state_accumulator::unstable::Response> {
	let room_id = &body.room_id;
	let event_id = &body.event_id;

	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id,
		event_id: None,
	}
	.check()
	.await?;

	info!(
		origin = body.origin().as_str(),
		room_id = %room_id,
		event_id = %event_id,
		"Serving MSC4500 state accumulator request"
	);

	// Verify the event belongs to the requested room
	let pdu = services
		.rooms
		.timeline
		.get_pdu(event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	if pdu.room_id_or_hash().as_ref() != Some(room_id) {
		return Err!(Request(NotFound("Event does not belong to the requested room.")));
	}

	let shorteventid = services
		.rooms
		.short
		.get_or_create_shorteventid(event_id)
		.await;

	let root_handle = services
		.rooms
		.state
		.get_roothandle(shorteventid)
		.await
		.map_err(|_| err!(Request(NotFound("Root handle not found for event."))))?;

	// Build the LtHash lattice over the event's post-event state. Any entry
	// that cannot be resolved fails the request rather than yielding a
	// digest over partial state.
	let entries = state_tuples(&services, room_id, &root_handle).await?;

	let mut lattice = rezzy::incremental::LtHash::default();
	let n_state_events = u64::try_from(entries.len()).unwrap_or_default();
	for (ty, sk, id) in &entries {
		lattice.insert(ty, sk, id.as_str());
	}
	let (lattice, digest) = serialize_lthash(&lattice);

	Ok(get_state_accumulator::unstable::Response {
		event_id: event_id.clone(),
		algorithm: PRIMARY_ALGORITHM.to_owned(),
		lattice,
		n_state_events,
		digest,
	})
}

/// Resolves every `(type, state_key, event_id)` tuple beneath a HAMT root
/// directly from the HAMT and short-ID mappings, without loading PDUs.
///
/// Fails closed: a traversal error or an unresolvable short ID aborts rather
/// than returning an incomplete state, so callers never digest partial state.
pub(super) async fn state_tuples(
	services: &crate::State,
	room_id: &slipstream::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
) -> Result<Vec<(String, String, OwnedEventId)>> {
	use conduwuit::utils::stream::IterStream;

	let shorts: Vec<_> = services
		.rooms
		.state_accessor
		.state_full_shortids_hamt(room_id, root_handle.clone())
		.try_collect()
		.await?;

	let state_keys: Vec<_> = services
		.rooms
		.short
		.multi_get_statekey_from_short(shorts.iter().map(|(ssk, _)| *ssk).stream())
		.try_collect()
		.await?;

	let event_ids: Vec<OwnedEventId> = services
		.rooms
		.short
		.multi_get_eventid_from_short::<OwnedEventId, _>(
			shorts.iter().map(|(_, seid)| *seid).stream(),
		)
		.try_collect()
		.await?;

	Ok(state_keys
		.into_iter()
		.zip(event_ids)
		.map(|((ty, sk), id)| (ty.to_string(), sk.to_string(), id))
		.collect())
}

#[cfg(test)]
mod tests {
	use conduwuit_core::utils::hash::lthash::serialize_lthash;
	use slipstream::OwnedEventId;

	#[test]
	fn test_serialize_empty_lthash() {
		let empty_lthash = rezzy::incremental::LtHash::ZERO;
		let (lattice, digest): (String, String) = serialize_lthash(&empty_lthash);

		// The lattice for an empty LtHash is 2048 null bytes.
		// 2048 bytes of 0s encoded in base64url without padding:
		let expected_lattice = "A".repeat(2731);
		assert_eq!(
			lattice, expected_lattice,
			"Lattice encoding must be deterministic URL-safe base64"
		);

		// Checksum format must be 43-character base64url (32 bytes)
		assert_eq!(digest.len(), 43);
		assert!(
			digest
				.chars()
				.all(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_')
		);
	}

	#[test]
	fn test_serialize_populated_lthash() {
		let mut lthash = rezzy::incremental::LtHash::ZERO;
		// Add some dummy data to manipulate the lthash state
		let event_id1: OwnedEventId = "$abc:example.com".try_into().unwrap();
		let event_id2: OwnedEventId = "$def:example.com".try_into().unwrap();
		lthash.insert("m.room.name", "", &event_id1);
		lthash.insert("m.room.topic", "", &event_id2);

		let (lattice, digest): (String, String) = serialize_lthash(&lthash);

		// Lattice must remain exactly 2731 base64url-encoded characters long (2048
		// bytes without padding)
		assert_eq!(lattice.len(), 2731);

		// Ensure checksum is 43-character base64url format
		assert_eq!(digest.len(), 43);
		assert!(
			digest
				.chars()
				.all(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_')
		);

		// The digest and lattice should no longer be the empty one
		let empty_lthash = rezzy::incremental::LtHash::ZERO;
		let (empty_lattice, empty_digest): (String, String) = serialize_lthash(&empty_lthash);
		assert_ne!(lattice, empty_lattice);
		assert_ne!(digest, empty_digest);
	}
}
