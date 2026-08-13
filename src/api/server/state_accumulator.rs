use axum::extract::State;
use axum_extra::{TypedHeader, headers::Authorization};
use conduwuit::{Err, Event, Result, err, info};
use conduwuit_service::server_keys::{PubKeyMap, PubKeys};
use ruma::{OwnedEventId, OwnedRoomId, api::federation::authentication::XMatrix};
use serde::Deserialize;

use super::AccessCheck;

#[derive(Deserialize)]
pub(crate) struct StateAccumulatorQuery {
	pub event_id: OwnedEventId,
}

pub(crate) async fn get_state_accumulator_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	axum::extract::Path(room_id_str): axum::extract::Path<String>,
	axum::extract::Query(query): axum::extract::Query<StateAccumulatorQuery>,
	uri: http::Uri,
) -> Result<()> {
	let signature_uri = uri
		.path_and_query()
		.map_or("/", http::uri::PathAndQuery::as_str)
		.to_owned();

	let room_id = OwnedRoomId::try_from(room_id_str)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;

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
		event_id = %query.event_id,
		"Serving MSC4500 state accumulator request"
	);

	// Verify the event belongs to the requested room
	let pdu = services
		.rooms
		.timeline
		.get_pdu(&query.event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	if pdu.room_id_or_hash().as_ref() != Some(&room_id) {
		return Err!(Request(NotFound("Event does not belong to the requested room.")));
	}

	// TODO(MSC00DC/HAMT): derive LtHash from the HAMT store once it exposes
	// LtHash lookup by shortstatehash. state_compressor has been removed.
	Err(err!(Request(NotFound(
		"State accumulator endpoint is not yet available during HAMT migration."
	))))
}

async fn verify_federation_request(
	services: &crate::State,
	x_matrix: &XMatrix,
	signature_uri: &str,
) -> Result<()> {
	type Member = (String, ruma::CanonicalJsonValue);
	type Object = ruma::CanonicalJsonObject;
	type Value = ruma::CanonicalJsonValue;

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
		[(x_matrix.key.as_str().into(), Value::String(x_matrix.sig.to_string()))];
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
		.get_verify_key(&x_matrix.origin, &x_matrix.key)
		.await
		.map_err(|e| err!(Request(Forbidden(warn!("Failed to fetch signing keys: {e}")))))?;

	let keys: PubKeys = [(x_matrix.key.to_string(), key.key)].into();
	let keys: PubKeyMap = [(x_matrix.origin.as_str().into(), keys)].into();
	ruma::signatures::verify_json(&keys, authorization).map_err(|e| {
		err!(Request(Forbidden(warn!(
			"Failed to verify X-Matrix signatures from {}: {e}",
			x_matrix.origin
		))))
	})?;

	Ok(())
}

#[cfg(test)]
use conduwuit_core::utils::hash::lthash::serialize_lthash;
mod tests {
	#[test]
	fn test_serialize_empty_lthash() {
		let empty_lthash = rezzy::LtHash::ZERO;
		let (lattice, digest) = serialize_lthash(&empty_lthash);

		// The lattice for an empty LtHash is 2048 null bytes.
		// 2048 bytes of 0s encoded in base64url without padding:
		let expected_lattice = "A".repeat(2731);
		assert_eq!(
			lattice, expected_lattice,
			"Lattice encoding must be deterministic URL-safe base64"
		);

		// Checksum format must be 64-character lowercase hex (32 bytes)
		assert_eq!(digest.len(), 64);
		assert!(
			digest
				.chars()
				.all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
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

		let (lattice, digest) = serialize_lthash(&lthash);

		// Lattice must remain exactly 2731 base64url-encoded characters long (2048
		// bytes without padding)
		assert_eq!(lattice.len(), 2731);

		// Ensure checksum is also stable length and lowercase hex format
		assert_eq!(digest.len(), 64);
		assert!(
			digest
				.chars()
				.all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
		);

		// The digest and lattice should no longer be the empty one
		let empty_lthash = rezzy::LtHash::ZERO;
		let (empty_lattice, empty_digest) = serialize_lthash(&empty_lthash);
		assert_ne!(lattice, empty_lattice);
		assert_ne!(digest, empty_digest);
	}
}
