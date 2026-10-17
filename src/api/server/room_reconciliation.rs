//! MSC4521: algebraic digest set reconciliation.
//!
//! * `GET  .../room_digest/{roomId}`: the responder's level-0 digest and frame.
//! * `GET  .../room_digest/{roomId}/strata`: the same, plus the 32 strata.
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
use rezzy_recon::{
	AlgebraicError, BucketRequest, MAX_BUCKETS_PER_ROUND, RoomAccumulator, SyndromeSketch,
	build_bucket_sketches, encode_strata, triage::validate_bucket_requests,
};
use slipstream::{
	OwnedEventId, RoomId, ServerName,
	api::federation::reconciliation::{get_room_digest, get_room_digest_strata, post_room_diff},
	json::Value,
};

use super::AccessCheck;
use crate::{Ruma, router::extract::State};

const DIGEST_TYPE: &str = "algebraic_v1";

// --- encoding -------------------------------------------------------------

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

// --- digest ---------------------------------------------------------------

/// The fields shared by the digest and strata responses.
struct DigestFields {
	digest: String,
	known_event_count: u64,
	frame_id: String,
	frame_event_ids: Vec<OwnedEventId>,
	depth_range: Vec<u64>,
	origin_server_ts_range: Vec<u64>,
}

fn digest_fields(state: &RoomReconciliationState) -> DigestFields {
	DigestFields {
		digest: state.resident.accumulator().encode_digest(),
		known_event_count: state.known_event_count(),
		frame_id: frame_id_for(&state.frame_event_ids),
		frame_event_ids: state.frame_event_ids.clone(),
		depth_range: state.depth_range.to_vec(),
		origin_server_ts_range: state.origin_server_ts_range.to_vec(),
	}
}

async fn room_snapshot(
	services: &crate::State,
	origin: &ServerName,
	room_id: &RoomId,
) -> Result<Arc<RoomReconciliationState>> {
	AccessCheck {
		services,
		origin,
		room_id,
		event_id: None,
	}
	.check()
	.await?;

	services.rooms.timeline.reconciliation_state(room_id).await
}

/// # `GET /_matrix/federation/unstable/tk.nutra.msc0501/room_digest/{roomId}`
///
/// The digest without strata.
pub(crate) async fn get_room_reconciliation_digest_route(
	State(services): State<crate::State>,
	body: Ruma<get_room_digest::unstable::Request>,
) -> Result<get_room_digest::unstable::Response> {
	let state = room_snapshot(&services, body.origin(), &body.room_id).await?;
	let fields = digest_fields(&state);
	services
		.rooms
		.timeline
		.advertised_frames
		.insert((body.room_id.clone(), fields.frame_id.clone()), ());

	Ok(get_room_digest::unstable::Response {
		digest: fields.digest,
		digest_type: DIGEST_TYPE.to_owned(),
		known_event_count: fields.known_event_count,
		frame_id: fields.frame_id,
		extremity_event_ids: fields.frame_event_ids.clone(),
		frame_event_ids: fields.frame_event_ids,
		depth_range: fields.depth_range,
		origin_server_ts_range: fields.origin_server_ts_range,
	})
}

/// # `GET /_matrix/federation/unstable/tk.nutra.msc0501/room_digest/{roomId}/strata`
///
/// The digest plus the 32 strata.
pub(crate) async fn get_room_reconciliation_strata_route(
	State(services): State<crate::State>,
	body: Ruma<get_room_digest_strata::unstable::Request>,
) -> Result<get_room_digest_strata::unstable::Response> {
	let state = room_snapshot(&services, body.origin(), &body.room_id).await?;
	let fields = digest_fields(&state);
	services
		.rooms
		.timeline
		.advertised_frames
		.insert((body.room_id.clone(), fields.frame_id.clone()), ());

	Ok(get_room_digest_strata::unstable::Response {
		digest: fields.digest,
		digest_type: DIGEST_TYPE.to_owned(),
		known_event_count: fields.known_event_count,
		frame_id: fields.frame_id,
		extremity_event_ids: fields.frame_event_ids.clone(),
		frame_event_ids: fields.frame_event_ids,
		depth_range: fields.depth_range,
		origin_server_ts_range: fields.origin_server_ts_range,
		strata: encode_strata(state.resident.strata()),
	})
}

// --- diff -----------------------------------------------------------------

fn bad_param(what: &str) -> conduwuit::Error {
	err!(Request(InvalidParam("Invalid or missing `{what}`.")))
}

struct SketchRequest {
	bucket: BucketRequest,
	local_sketch: SyndromeSketch,
}

/// A validated `room_diff` request.
struct RoomDiffRequest {
	frame_negotiation: bool,
	frame_event_ids: Vec<OwnedEventId>,
	frame_id: String,
	local_digest: u128,
	local_known_event_count: u64,
	scope: String,
	state_at: Option<OwnedEventId>,
	requests: Vec<SketchRequest>,
}

fn validate_room_diff_request(
	body: &post_room_diff::unstable::Request,
) -> Result<RoomDiffRequest> {
	if body.mode != "sketch" {
		return Err!(Request(InvalidParam("Only `sketch` mode is supported.")));
	}
	if body.digest_type != DIGEST_TYPE {
		return Err!(Request(InvalidParam("Unsupported `digest_type`.")));
	}

	let scope = body.scope.clone().unwrap_or_else(|| "event_set".to_owned());
	match scope.as_str() {
		| "event_set" => {},
		| "resolved_state" =>
			return Err!(Request(InvalidParam("`resolved_state` scope is not implemented."))),
		| _ => return Err!(Request(InvalidParam("Unknown `scope`."))),
	}

	if body.frame_negotiation && body.frame_event_ids.is_empty() {
		return Err!(Request(InvalidParam(
			"`frame_event_ids` is required when `frame_negotiation` is true."
		)));
	}

	let local_digest = RoomAccumulator::decode_digest(&body.local_digest)
		.map_err(|_| bad_param("local_digest"))?;

	if body.requests.len() != body.local_sketches.len() {
		return Err!(Request(InvalidParam(
			"`local_sketches` length must match `requests` length."
		)));
	}
	if body.requests.len() > MAX_BUCKETS_PER_ROUND {
		return Err!(Request(InvalidParam("Too many bucket requests.")));
	}

	// Validate the bucket metadata before decoding sketches. Apart from making
	// malformed requests cheaper to reject, this ensures an invalid capacity,
	// depth or antichain is reported as a bad `requests` field rather than as a
	// secondary sketch-decoding error.
	let buckets = body
		.requests
		.iter()
		.map(|bucket| {
			let depth = u8::try_from(bucket.depth).map_err(|_| bad_param("requests"))?;
			let capacity = usize::try_from(bucket.capacity).map_err(|_| bad_param("requests"))?;
			Ok(BucketRequest::new(depth, bucket.prefix, capacity))
		})
		.collect::<Result<Vec<_>>>()?;
	validate_bucket_requests(&buckets).map_err(|_| bad_param("requests"))?;

	let requests = buckets
		.iter()
		.zip(&body.local_sketches)
		.map(|(bucket, sketch)| {
			let local_sketch = SyndromeSketch::decode(bucket.capacity, sketch)
				.map_err(|_| bad_param("local_sketches"))?;

			Ok(SketchRequest { bucket: *bucket, local_sketch })
		})
		.collect::<Result<Vec<_>>>()?;

	Ok(RoomDiffRequest {
		frame_negotiation: body.frame_negotiation,
		frame_event_ids: body.frame_event_ids.clone(),
		frame_id: body.frame_id.clone(),
		local_digest,
		local_known_event_count: body.local_known_event_count,
		scope,
		state_at: body.state_at.clone(),
		requests,
	})
}

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

	let residual_digest = state.resident.accumulator().digest() ^ body.local_digest;

	// Cheapest path first: the same digest and the same count means the sets
	// agree, so there is nothing to subtract or decode. This skips building
	// every responder sketch, which would otherwise scan the whole room.
	if residual_digest == 0
		&& body.local_known_event_count == state.resident.accumulator().known_event_count()
	{
		return Ok(Sketched::Decoded(DiffOutcome {
			missing_event_ids: Vec::new(),
			requester_only_short_ids: Vec::new(),
			expected_requester_side_accumulator: encode_digest(0),
		}));
	}

	let responder_sketches =
		build_bucket_sketches(state.population.h64s(), &buckets).map_err(|e| match e {
			| AlgebraicError::InvalidSketchCapacity | AlgebraicError::InvalidBucketIndex =>
				bad_param("requests"),
			| _ => err!(Request(InvalidParam("Invalid bucket requests."))),
		})?;

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
		// A zero residual is an empty difference: skip Berlekamp-Massey,
		// root finding and the re-encode check.
		if residual
			.coordinates()
			.iter()
			.all(|&coordinate| coordinate == 0)
		{
			continue;
		}
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
	body: RoomDiffRequest,
	frame_status: &str,
	negotiated: Option<Vec<OwnedEventId>>,
	sketch_status: &str,
	outcome: Option<DiffOutcome>,
) -> post_room_diff::unstable::Response {
	let truncated = outcome.is_none();
	let (missing_event_ids, requester_only_short_ids, expected) = match outcome {
		| Some(outcome) => (
			outcome.missing_event_ids,
			Some(outcome.requester_only_short_ids),
			Some(outcome.expected_requester_side_accumulator),
		),
		| None => (Vec::new(), None, None),
	};

	post_room_diff::unstable::Response {
		digest_type: DIGEST_TYPE.to_owned(),
		missing_event_ids,
		requester_only_short_ids,
		expected_requester_side_accumulator: expected,
		remote_known_event_count: state.known_event_count(),
		remote_extremity_event_ids: state.frame_event_ids.clone(),
		frame_id: frame_id_for(&state.frame_event_ids),
		frame_status: frame_status.to_owned(),
		negotiated_frame_event_ids: negotiated,
		scope: body.scope,
		state_at: body.state_at,
		sketch_status: sketch_status.to_owned(),
		truncated,
	}
}

/// # `POST /_matrix/federation/unstable/tk.nutra.msc0501/room_diff/{roomId}`
pub(crate) async fn post_room_reconciliation_diff_route(
	State(services): State<crate::State>,
	body: Ruma<post_room_diff::unstable::Request>,
) -> Result<post_room_diff::unstable::Response> {
	let state = room_snapshot(&services, body.origin(), &body.room_id).await?;
	let room_id = body.room_id.clone();
	let body = validate_room_diff_request(&body)?;

	// A frame we advertised recently is accepted even if it has since moved on:
	// the sketch covers our current set, so anything newer is one more
	// difference, and the response names the current frame.
	let current = body.frame_id == frame_id_for(&state.frame_event_ids);
	if !current
		&& !services
			.rooms
			.timeline
			.advertised_frames
			.contains_key(&(room_id, body.frame_id.clone()))
	{
		return Err!(Request(InvalidParam(
			"Requested `frame_id` does not match the responder's current frame."
		)));
	}

	let common = if current {
		common_frame(&state, &body)
	} else {
		state.frame_event_ids.clone()
	};
	let frame_status = match (body.frame_negotiation, common.is_empty()) {
		| (false, _) => "not_requested",
		| (true, false) => "common",
		| (true, true) => "none",
	};

	if common.is_empty() {
		let negotiated = body.frame_negotiation.then_some(common);
		return Ok(room_diff_response(
			&state,
			body,
			frame_status,
			negotiated,
			"not_applicable",
			None,
		));
	}

	let sketched = diff_buckets(&state, &body)?;
	let negotiated = body.frame_negotiation.then_some(common);
	Ok(match sketched {
		| Sketched::Decoded(outcome) =>
			room_diff_response(&state, body, frame_status, negotiated, "decoded", Some(outcome)),
		| Sketched::CapacityExceeded =>
			room_diff_response(&state, body, frame_status, negotiated, "capacity_exceeded", None),
	})
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
