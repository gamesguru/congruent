//! MSC4521: algebraic digest set reconciliation.
//!
//! * `GET  .../room_digest/{roomId}`: the responder's level-0 digest, strata
//!   and frame.
//! * `POST .../room_diff/{roomId}`: sketch mode. The requester sends one sketch
//!   per bucket; the responder subtracts its own and decodes the difference.
//!
//! Every response is served from one snapshot of the room (see
//! `conduwuit_service::rooms::timeline::RoomReconciliationState`), so the
//! digest, strata, frame and sketches in a response always agree.

use std::{collections::BTreeSet, sync::Arc};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use conduwuit::{Err, Result, err, utils::hash::sha256};
use conduwuit_service::rooms::timeline::RoomReconciliationState;
use http_body_util::{BodyExt, Limited};
use rezzy_recon::{
	AlgebraicError, BucketRequest, MAX_BUCKETS_PER_ROUND, RoomAccumulator, STRATUM_CAPACITY,
	SyndromeSketch, build_bucket_sketches,
};
use slipstream::{
	CanonicalJsonObject, ObjectBuilder, OwnedEventId, OwnedRoomId,
	api::federation::authentication::XMatrix, json::Value,
};

use super::{AccessCheck, state_accumulator::verify_federation_request};
use crate::router::{
	ApiError,
	extract::{Path, State, TypedHeader, headers::Authorization},
};

const DIGEST_TYPE: &str = "algebraic_v1";

// --- encoding -------------------------------------------------------------

fn encode_stratum(stratum: &[u64; STRATUM_CAPACITY]) -> String {
	let mut bytes = Vec::with_capacity(STRATUM_CAPACITY * 8);
	for coordinate in stratum {
		bytes.extend_from_slice(&coordinate.to_le_bytes());
	}
	URL_SAFE_NO_PAD.encode(bytes)
}

fn encode_short_identifier(h64: u64) -> String { URL_SAFE_NO_PAD.encode(h64.to_be_bytes()) }

fn encode_digest(digest: u128) -> String {
	RoomAccumulator::from_parts(digest, 0).encode_digest()
}

/// The frame ID is the SHA-256 of the canonical JSON array of the sorted,
/// deduplicated frame event IDs.
fn frame_id_for(event_ids: &[OwnedEventId]) -> String {
	let ids: BTreeSet<&OwnedEventId> = event_ids.iter().collect();
	let array = Value::Array(
		ids.into_iter()
			.map(|id| Value::String(id.to_string()))
			.collect(),
	);
	URL_SAFE_NO_PAD.encode(sha256::hash(slipstream::codec::to_string(&array)))
}

// --- request parsing ------------------------------------------------------

fn bad_param(what: &str) -> conduwuit::Error {
	err!(Request(InvalidParam("Invalid or missing `{what}`.")))
}

fn event_id_list(body: &CanonicalJsonObject, key: &str) -> Result<Vec<OwnedEventId>> {
	let Some(value) = body.get(key) else {
		return Ok(Vec::new());
	};
	value
		.as_array()
		.ok_or_else(|| bad_param(key))?
		.iter()
		.map(|item| {
			item.as_str()
				.and_then(|id| OwnedEventId::parse(id).ok())
				.ok_or_else(|| bad_param(key))
		})
		.collect()
}

fn opt_str<'a>(body: &'a CanonicalJsonObject, key: &str) -> Result<Option<&'a str>> {
	body.get(key)
		.map(|value| value.as_str().ok_or_else(|| bad_param(key)))
		.transpose()
}

fn opt_u64(body: &CanonicalJsonObject, key: &str) -> Result<Option<u64>> {
	body.get(key)
		.map(|value| value.as_u64().ok_or_else(|| bad_param(key)))
		.transpose()
}

struct SketchRequest {
	bucket: BucketRequest,
	local_sketch: SyndromeSketch,
}

struct RoomDiffRequest {
	frame_negotiation: bool,
	frame_event_ids: Vec<OwnedEventId>,
	frame_id: String,
	local_digest: u128,
	scope: String,
	state_at: Option<OwnedEventId>,
	requests: Vec<SketchRequest>,
}

fn parse_room_diff_request(body: &CanonicalJsonObject) -> Result<RoomDiffRequest> {
	if opt_str(body, "mode")? != Some("sketch") {
		return Err!(Request(InvalidParam("Only `sketch` mode is supported.")));
	}
	if opt_str(body, "digest_type")? != Some(DIGEST_TYPE) {
		return Err!(Request(InvalidParam("Unsupported `digest_type`.")));
	}

	let scope = opt_str(body, "scope")?.unwrap_or("event_set").to_owned();
	match scope.as_str() {
		| "event_set" => {},
		| "resolved_state" =>
			return Err!(Request(InvalidParam("`resolved_state` scope is not implemented."))),
		| _ => return Err!(Request(InvalidParam("Unknown `scope`."))),
	}

	let frame_negotiation = body
		.get("frame_negotiation")
		.map(|value| {
			value
				.as_bool()
				.ok_or_else(|| bad_param("frame_negotiation"))
		})
		.transpose()?
		.unwrap_or(false);
	let frame_event_ids = event_id_list(body, "frame_event_ids")?;
	if frame_negotiation && frame_event_ids.is_empty() {
		return Err!(Request(InvalidParam(
			"`frame_event_ids` is required when `frame_negotiation` is true."
		)));
	}

	let frame_id = opt_str(body, "frame_id")?
		.ok_or_else(|| bad_param("frame_id"))?
		.to_owned();
	let local_digest = RoomAccumulator::decode_digest(
		opt_str(body, "local_digest")?.ok_or_else(|| bad_param("local_digest"))?,
	)
	.map_err(|_| bad_param("local_digest"))?;
	// Required by the wire format; the responder derives everything it needs
	// from the digest and sketches.
	opt_u64(body, "local_known_event_count")?
		.ok_or_else(|| bad_param("local_known_event_count"))?;

	let state_at = opt_str(body, "state_at")?
		.map(|id| OwnedEventId::parse(id).map_err(|_| bad_param("state_at")))
		.transpose()?;

	let buckets = body
		.get("requests")
		.and_then(Value::as_array)
		.ok_or_else(|| bad_param("requests"))?;
	let sketches = body
		.get("local_sketches")
		.and_then(Value::as_array)
		.ok_or_else(|| bad_param("local_sketches"))?;
	if buckets.len() != sketches.len() {
		return Err!(Request(InvalidParam(
			"`local_sketches` length must match `requests` length."
		)));
	}
	if buckets.len() > MAX_BUCKETS_PER_ROUND {
		return Err!(Request(InvalidParam("Too many bucket requests.")));
	}

	let requests = buckets
		.iter()
		.zip(sketches)
		.map(|(bucket, sketch)| {
			let bucket = bucket.as_object().ok_or_else(|| bad_param("requests"))?;
			let depth = opt_u64(bucket, "depth")?
				.and_then(|depth| u8::try_from(depth).ok())
				.ok_or_else(|| bad_param("depth"))?;
			let prefix = opt_u64(bucket, "prefix")?.ok_or_else(|| bad_param("prefix"))?;
			let capacity = opt_u64(bucket, "capacity")?
				.and_then(|capacity| usize::try_from(capacity).ok())
				.ok_or_else(|| bad_param("capacity"))?;
			let local_sketch = SyndromeSketch::decode(
				capacity,
				sketch.as_str().ok_or_else(|| bad_param("local_sketches"))?,
			)
			.map_err(|_| bad_param("local_sketches"))?;

			Ok(SketchRequest {
				bucket: BucketRequest::new(depth, prefix, capacity),
				local_sketch,
			})
		})
		.collect::<Result<Vec<_>>>()?;

	Ok(RoomDiffRequest {
		frame_negotiation,
		frame_event_ids,
		frame_id,
		local_digest,
		scope,
		state_at,
		requests,
	})
}

/// Reads and canonicalizes a request body, honouring `max_request_size`.
async fn read_json_object(
	services: &crate::State,
	request: hyper::Request<hyper::body::Incoming>,
) -> Result<CanonicalJsonObject> {
	let max = services.server.config.max_request_size;
	let body = Limited::new(request.into_body(), max)
		.collect()
		.await
		.map_err(|e| err!(Request(TooLarge("Request body too large: {e}"))))?
		.to_bytes();

	let text = std::str::from_utf8(&body)
		.map_err(|_| err!(Request(BadJson("Request body is not valid UTF-8."))))?;
	let value = slipstream::canonical_json::from_json_str(text)
		.map_err(|e| err!(Request(BadJson("Invalid request body: {e}"))))?;
	value
		.as_object()
		.cloned()
		.ok_or_else(|| err!(Request(BadJson("Request body must be a JSON object."))))
}

// --- digest ---------------------------------------------------------------

fn digest_response(state: &RoomReconciliationState, with_strata: bool) -> Value {
	let mut response = ObjectBuilder::new();
	response.field("digest", &state.resident.accumulator().encode_digest());
	response.field("digest_type", &DIGEST_TYPE);
	response.field("known_event_count", &state.known_event_count());
	response.field("frame_id", &frame_id_for(&state.frame_event_ids));
	if with_strata {
		let strata: Vec<String> = state.resident.strata().iter().map(encode_stratum).collect();
		response.field("strata", &strata);
	}
	response.field("frame_event_ids", &state.frame_event_ids);
	response.field("extremity_event_ids", &state.frame_event_ids);
	response.field("depth_range", &state.depth_range.to_vec());
	response.field("origin_server_ts_range", &state.origin_server_ts_range.to_vec());
	response.finish()
}

/// Authenticates the origin and checks it may see `room_id`.
async fn authorize(
	services: &crate::State,
	x_matrix: &XMatrix,
	method: &http::Method,
	uri: &http::Uri,
	room_id: String,
	content: Option<&CanonicalJsonObject>,
) -> Result<OwnedRoomId> {
	let signature_uri = uri
		.path_and_query()
		.map_or("/", http::uri::PathAndQuery::as_str);
	verify_federation_request(services, x_matrix, method, signature_uri, content).await?;

	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	AccessCheck {
		services,
		origin: &x_matrix.origin,
		room_id: &room_id,
		event_id: None,
	}
	.check()
	.await?;

	Ok(room_id)
}

async fn serve_digest(
	services: crate::State,
	x_matrix: XMatrix,
	room_id: String,
	uri: http::Uri,
	with_strata: bool,
) -> std::result::Result<crate::router::response::Response, ApiError> {
	let room_id =
		authorize(&services, &x_matrix, &http::Method::GET, &uri, room_id, None).await?;
	let state = services
		.rooms
		.timeline
		.reconciliation_state(&room_id)
		.await?;

	Ok(crate::json_util::json_response(digest_response(&state, with_strata)))
}

/// # `GET /_matrix/federation/unstable/tk.nutra.msc0501/room_digest/{roomId}`
///
/// The digest without strata.
pub(crate) async fn get_room_reconciliation_digest_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	Path(room_id): Path<String>,
	uri: http::Uri,
) -> std::result::Result<impl crate::router::response::IntoResponse, ApiError> {
	serve_digest(services, x_matrix, room_id, uri, false).await
}

/// # `GET /_matrix/federation/unstable/tk.nutra.msc0501/room_digest/{roomId}/strata`
///
/// The digest plus the 32 strata.
pub(crate) async fn get_room_reconciliation_strata_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	Path(room_id): Path<String>,
	uri: http::Uri,
) -> std::result::Result<impl crate::router::response::IntoResponse, ApiError> {
	serve_digest(services, x_matrix, room_id, uri, true).await
}

// --- diff -----------------------------------------------------------------

struct DiffOutcome {
	missing_event_ids: Vec<OwnedEventId>,
	requester_only_short_ids: Vec<String>,
	expected_requester_side_accumulator: String,
}

enum Sketched {
	Decoded(DiffOutcome),
	/// A bucket held more differences than its capacity.
	CapacityExceeded,
}

/// Subtracts the requester's bucket sketches from ours and decodes each
/// residual into event IDs.
fn diff_buckets(state: &RoomReconciliationState, body: &RoomDiffRequest) -> Result<Sketched> {
	let buckets: Vec<BucketRequest> = body.requests.iter().map(|r| r.bucket).collect();
	let responder_sketches =
		build_bucket_sketches(state.population.h64s(), &buckets).map_err(|e| match e {
			| AlgebraicError::InvalidSketchCapacity | AlgebraicError::InvalidBucketIndex =>
				bad_param("requests"),
			| _ => err!(Request(InvalidParam("Invalid bucket requests."))),
		})?;

	let residual_digest = state.resident.accumulator().digest() ^ body.local_digest;
	let mut responder_side = RoomAccumulator::new();
	let mut outcome = DiffOutcome {
		missing_event_ids: Vec::new(),
		requester_only_short_ids: Vec::new(),
		expected_requester_side_accumulator: String::new(),
	};

	for (request, responder_sketch) in body.requests.iter().zip(&responder_sketches) {
		let residual = responder_sketch
			.subtract(&request.local_sketch)
			.map_err(|_| err!(Request(InvalidParam("Invalid sketch length."))))?;
		let roots = match residual.decode_elements(request.bucket.capacity) {
			| Ok(roots) => roots,
			| Err(AlgebraicError::DecodeFailure | AlgebraicError::BudgetExhausted) =>
				return Ok(Sketched::CapacityExceeded),
			| Err(_) => return Err!(Request(InvalidParam("Sketch decode failed."))),
		};

		for root in roots {
			if let Some(events) = state.h64_to_events.get(&root) {
				// Every event sharing the short identifier is repaired.
				for (event_id, hash) in events {
					outcome.missing_event_ids.push(event_id.clone());
					responder_side
						.insert(*hash)
						.map_err(|e| err!(Database("repair set accumulator: {e:?}")))?;
				}
			} else {
				outcome
					.requester_only_short_ids
					.push(encode_short_identifier(root));
			}
		}
	}

	outcome.expected_requester_side_accumulator =
		encode_digest(residual_digest ^ responder_side.digest());
	Ok(Sketched::Decoded(outcome))
}

/// Intersection of the requester's frame with ours (all of ours if the
/// requester sent none).
fn common_frame(state: &RoomReconciliationState, body: &RoomDiffRequest) -> Vec<OwnedEventId> {
	if body.frame_event_ids.is_empty() {
		return state.frame_event_ids.clone();
	}
	let requested: BTreeSet<&OwnedEventId> = body.frame_event_ids.iter().collect();
	state
		.frame_event_ids
		.iter()
		.filter(|id| requested.contains(id))
		.cloned()
		.collect()
}

fn room_diff_response(
	state: &RoomReconciliationState,
	body: &RoomDiffRequest,
	frame_status: &str,
	negotiated: Option<&Vec<OwnedEventId>>,
	sketch_status: &str,
	outcome: Option<&DiffOutcome>,
) -> Value {
	let mut response = ObjectBuilder::new();
	let empty = Vec::new();
	response.field("digest_type", &DIGEST_TYPE);
	response.field("missing_event_ids", outcome.map_or(&empty, |o| &o.missing_event_ids));
	response.field_opt("requester_only_short_ids", outcome.map(|o| &o.requester_only_short_ids));
	response.field_opt(
		"expected_requester_side_accumulator",
		outcome.map(|o| &o.expected_requester_side_accumulator),
	);
	response.field("remote_known_event_count", &state.known_event_count());
	response.field("remote_extremity_event_ids", &state.frame_event_ids);
	response.field("frame_id", &frame_id_for(&state.frame_event_ids));
	response.field("frame_status", &frame_status);
	response.field_opt("negotiated_frame_event_ids", negotiated);
	response.field("scope", &body.scope);
	response.field_opt("state_at", body.state_at.as_ref());
	response.field("sketch_status", &sketch_status);
	response.field("truncated", &outcome.is_none());
	response.finish()
}

/// # `POST /_matrix/federation/unstable/tk.nutra.msc0501/room_diff/{roomId}`
pub(crate) async fn post_room_reconciliation_diff_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	Path(room_id): Path<String>,
	uri: http::Uri,
	request: hyper::Request<hyper::body::Incoming>,
) -> std::result::Result<impl crate::router::response::IntoResponse, ApiError> {
	let content = read_json_object(&services, request).await?;
	let room_id =
		authorize(&services, &x_matrix, &http::Method::POST, &uri, room_id, Some(&content))
			.await?;
	let body = parse_room_diff_request(&content)?;
	let state: Arc<RoomReconciliationState> = services
		.rooms
		.timeline
		.reconciliation_state(&room_id)
		.await?;

	if body.frame_id != frame_id_for(&state.frame_event_ids) {
		return Err!(Request(InvalidParam(
			"Requested `frame_id` does not match the responder's current frame."
		)))
		.map_err(Into::into);
	}

	let common = common_frame(&state, &body);
	let frame_status = match (body.frame_negotiation, common.is_empty()) {
		| (false, _) => "not_requested",
		| (true, false) => "common",
		| (true, true) => "none",
	};
	let negotiated = body.frame_negotiation.then_some(&common);

	if common.is_empty() {
		let response =
			room_diff_response(&state, &body, frame_status, negotiated, "not_applicable", None);
		return Ok(crate::json_util::json_response(response));
	}

	let response = match diff_buckets(&state, &body)? {
		| Sketched::Decoded(outcome) =>
			room_diff_response(&state, &body, frame_status, negotiated, "decoded", Some(&outcome)),
		| Sketched::CapacityExceeded =>
			room_diff_response(&state, &body, frame_status, negotiated, "capacity_exceeded", None),
	};

	Ok(crate::json_util::json_response(response))
}

#[cfg(test)]
mod tests {
	use slipstream::OwnedEventId;

	use super::{encode_digest, frame_id_for};

	fn eid(s: &str) -> OwnedEventId { format!("${s}:example.com").try_into().unwrap() }

	#[test]
	fn frame_id_is_order_insensitive() {
		let a = frame_id_for(&[eid("a"), eid("b")]);
		let b = frame_id_for(&[eid("b"), eid("a")]);
		assert_eq!(a, b);
	}

	#[test]
	fn frame_id_deduplicates() {
		assert_eq!(frame_id_for(&[eid("a")]), frame_id_for(&[eid("a"), eid("a")]));
	}

	#[test]
	fn frame_id_distinguishes_frames() {
		assert_ne!(frame_id_for(&[eid("a")]), frame_id_for(&[eid("b")]));
	}

	#[test]
	fn digest_encoding_is_22_chars() {
		assert_eq!(encode_digest(0x0123_4567_89AB_CDEF).len(), 22);
	}
}
