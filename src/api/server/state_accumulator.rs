use conduwuit::{Err, Event, Result, err, info};
use conduwuit_core::utils::hash::lthash::serialize_lthash;
use conduwuit_service::{
	rooms::state_accessor::PRIMARY_ALGORITHM,
	server_keys::{PubKeyMap, PubKeys},
};
use futures::TryStreamExt;
use serde::Deserialize;
use slipstream::{OwnedEventId, OwnedRoomId, api::federation::authentication::XMatrix};

use super::AccessCheck;
use crate::router::{
	ApiError,
	extract::{State, TypedHeader, headers::Authorization},
};

#[derive(Deserialize)]
pub(crate) struct StateAccumulatorQuery {
	pub event_id: String,
}

pub(crate) async fn get_state_accumulator_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	crate::router::extract::Path(room_id_str): crate::router::extract::Path<String>,
	crate::router::extract::Query(query): crate::router::extract::Query<StateAccumulatorQuery>,
	uri: http::Uri,
) -> std::result::Result<impl crate::router::response::IntoResponse, ApiError> {
	let signature_uri = uri
		.path_and_query()
		.map_or("/", http::uri::PathAndQuery::as_str)
		.to_owned();

	let room_id = OwnedRoomId::parse(room_id_str)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let event_id = OwnedEventId::parse(query.event_id.as_str())
		.map_err(|_| err!(Request(InvalidParam("Invalid event ID."))))?;

	verify_federation_request(&services, &x_matrix, &signature_uri).await?;

	AccessCheck {
		services: &services,
		origin: &x_matrix.origin,
		room_id: &room_id,
		event_id: None,
	}
	.check()
	.await?;

	info!(
		origin = x_matrix.origin.as_str(),
		room_id = %room_id,
		event_id = %event_id,
		"Serving MSC4500 state accumulator request"
	);

	// Verify the event belongs to the requested room
	let pdu = services
		.rooms
		.timeline
		.get_pdu(&event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	if pdu.room_id_or_hash().as_ref() != Some(&room_id) {
		return Err!(Request(NotFound("Event does not belong to the requested room.")))
			.map_err(Into::into);
	}

	let shorteventid = services
		.rooms
		.short
		.get_or_create_shorteventid(&event_id)
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
	let entries = state_tuples(&services, &root_handle).await?;

	let mut lattice = rezzy::state::LtHash::default();
	let n_state_events = u64::try_from(entries.len()).unwrap_or_default();
	for (ty, sk, id) in &entries {
		lattice.insert(ty, sk, id.as_str());
	}
	let (lattice_b64, digest) = serialize_lthash(&lattice);

	let mut response = slipstream::ObjectBuilder::new();
	response.field("event_id", &event_id);
	response.field("algorithm", &PRIMARY_ALGORITHM);
	response.field("lattice", &lattice_b64);
	response.field("n_state_events", &n_state_events);
	response.field("digest", &digest);

	Ok(crate::json_util::json_response(response.finish()))
}

async fn verify_federation_request(
	services: &crate::State,
	x_matrix: &XMatrix,
	signature_uri: &str,
) -> Result<()> {
	type Member = (String, slipstream::CanonicalJsonValue);
	type Object = slipstream::CanonicalJsonObject;
	type Value = slipstream::CanonicalJsonValue;

	let destination = services.globals.server_name();
	if let Some(dest) = x_matrix.destination.as_deref() {
		if dest != destination {
			return Err!(Request(Forbidden(warn!(
				"Invalid destination. Expected: {}, Got: {}",
				destination, dest
			))));
		}
	}

	if services
		.moderation
		.is_remote_server_forbidden(&x_matrix.origin)
	{
		return Err!(Request(Forbidden(warn!(
			"Federation requests from {} denied.",
			x_matrix.origin
		))));
	}

	let signature: [Member; 1] =
		[(x_matrix.key.as_str().into(), Value::String(x_matrix.sig.clone()))];
	let signatures: [Member; 1] =
		[(x_matrix.origin.as_str().into(), Value::Object(signature.into()))];
	let authorization: Object = [
		("destination".into(), Value::String(destination.into())),
		("method".into(), Value::String(http::Method::GET.as_str().into())),
		("origin".into(), Value::String(x_matrix.origin.as_str().into())),
		("signatures".into(), Value::Object(signatures.into())),
		("uri".into(), Value::String(signature_uri.to_owned())),
	]
	.into();

	let key = services
		.server_keys
		.get_active_verify_key(&x_matrix.origin, &x_matrix.key)
		.await
		.map_err(|e| err!(Request(Forbidden(warn!("Failed to fetch signing keys: {e}")))))?;

	let keys: PubKeys = [(x_matrix.key.clone(), key.key)].into();
	let keys: PubKeyMap = [(x_matrix.origin.clone(), keys)].into();
	slipstream::signatures::verify_json(&keys, authorization).map_err(|e| {
		err!(Request(Forbidden(warn!(
			"Failed to verify X-Matrix signatures from {}: {e}",
			x_matrix.origin
		))))
	})?;

	Ok(())
}

/// Resolves every `(type, state_key, event_id)` tuple beneath a HAMT root
/// directly from the HAMT and short-ID mappings, without loading PDUs.
///
/// Fails closed: a traversal error or an unresolvable short ID aborts rather
/// than returning an incomplete state, so callers never digest partial state.
pub(super) async fn state_tuples(
	services: &crate::State,
	root_handle: &rezzy::hamt::RootHandle,
) -> Result<Vec<(String, String, OwnedEventId)>> {
	use conduwuit::utils::stream::IterStream;

	let shorts: Vec<_> = services
		.rooms
		.state_accessor
		.state_full_shortids_hamt(root_handle.clone())
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
		let empty_lthash = rezzy::LtHash::ZERO;
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
		let mut lthash = rezzy::LtHash::ZERO;
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
		let empty_lthash = rezzy::LtHash::ZERO;
		let (empty_lattice, empty_digest): (String, String) = serialize_lthash(&empty_lthash);
		assert_ne!(lattice, empty_lattice);
		assert_ne!(digest, empty_digest);
	}
}
