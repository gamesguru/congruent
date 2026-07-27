use std::collections::{HashSet, VecDeque};

use axum::extract::{Path, State};
use axum_extra::{TypedHeader, headers::Authorization};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use conduwuit::{Err, Result, err, matrix::dag::sort_topologically};
use futures::StreamExt;
use ruma::{OwnedEventId, OwnedRoomId, api::federation::authentication::XMatrix};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;

use super::AccessCheck;

#[derive(Serialize)]
pub(crate) struct RoomDigestResponse {
	pub digest: String,
	pub digest_type: String,
	pub known_event_count: u64,
	pub frame_id: String,
	pub strata: Vec<String>,
	pub frame_event_ids: Vec<OwnedEventId>,
	pub extremity_event_ids: Vec<OwnedEventId>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub depth_range: Option<[u64; 2]>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub origin_server_ts_range: Option<[u64; 2]>,
}

#[derive(Debug, PartialEq, Eq)]
struct RoomDiffValidation {
	normalized_frame_event_ids: Vec<OwnedEventId>,
	common_frame_event_ids: Vec<OwnedEventId>,
	frame_status: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
pub(crate) struct RoomDiffRequest {
	mode: String,
	#[serde(default)]
	frame_negotiation: bool,
	#[serde(default)]
	frame_event_ids: Vec<OwnedEventId>,
	#[serde(default)]
	local_extremity_event_ids: Vec<OwnedEventId>,
	#[serde(default)]
	have_event_ids: Vec<OwnedEventId>,
	#[serde(default)]
	digest_type: Option<String>,
	#[serde(default)]
	local_digest: Option<String>,
	#[serde(default)]
	local_known_event_count: Option<u64>,
	#[serde(default)]
	requests: Vec<SketchRequest>,
	#[serde(default)]
	local_sketches: Vec<String>,
	#[serde(default)]
	limit: Option<u64>,
	#[serde(default)]
	max_depth_delta: Option<u64>,
	#[serde(default)]
	max_events: Option<u64>,
	#[serde(default)]
	scope: Option<String>,
	#[serde(default)]
	state_at: Option<OwnedEventId>,
	#[serde(default)]
	frame_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
pub(crate) struct SketchRequest {
	depth: u64,
	prefix: u64,
	capacity: u64,
}

#[derive(Serialize)]
struct RoomDiffResponse {
	missing_event_ids: Vec<OwnedEventId>,
	#[serde(skip_serializing_if = "Option::is_none")]
	requester_only_short_ids: Option<Vec<String>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	expected_requester_side_accumulator: Option<String>,
	remote_known_event_count: u64,
	remote_extremity_event_ids: Vec<OwnedEventId>,
	frame_id: String,
	frame_status: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	negotiated_frame_event_ids: Option<Vec<OwnedEventId>>,
	scope: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	state_at: Option<OwnedEventId>,
	#[serde(skip_serializing_if = "Option::is_none")]
	sketch_status: Option<String>,
	truncated: bool,
}

#[derive(Deserialize)]
pub(crate) struct RoomEventsRequest {
	event_ids: Vec<OwnedEventId>,
	#[serde(default = "default_true")]
	include_auth_chain: bool,
	#[serde(default)]
	known_event_ids: Vec<OwnedEventId>,
}

#[derive(Serialize)]
struct RoomEventsResponse {
	events: Vec<Box<RawJsonValue>>,
	auth_chain_events: Vec<Box<RawJsonValue>>,
	missing_event_ids: Vec<OwnedEventId>,
	rejected_tombstones: Vec<serde_json::Value>,
}

fn default_true() -> bool { true }

fn encode_stratum_entry(stratum: &[u64; rezzy::reconcile::STRATUM_CAPACITY]) -> String {
	let mut bytes = Vec::with_capacity(rezzy::reconcile::STRATUM_CAPACITY * 8);
	for coordinate in stratum {
		bytes.extend_from_slice(&coordinate.to_le_bytes());
	}
	URL_SAFE_NO_PAD.encode(bytes)
}

fn encode_short_identifier(h64: u64) -> String { URL_SAFE_NO_PAD.encode(h64.to_be_bytes()) }

fn encode_accumulator_digest(digest: u128) -> String {
	URL_SAFE_NO_PAD.encode(digest.to_be_bytes())
}

fn rejected_tombstone(event_id: &OwnedEventId, reason: &str) -> serde_json::Value {
	serde_json::json!({
		"event_id": event_id,
		"reason": reason,
	})
}

fn frame_id_for(event_ids: &[OwnedEventId]) -> String {
	let mut ids = event_ids.to_vec();
	ids.sort();
	ids.dedup();
	let json = serde_json::to_vec(&ids).expect("frame ids serialize");
	let digest = conduwuit::utils::hash::sha256::hash(json);
	URL_SAFE_NO_PAD.encode(digest)
}

fn validate_room_diff_request(
	body: &RoomDiffRequest,
	digest: &RoomDigestResponse,
) -> Result<RoomDiffValidation> {
	if matches!(body.scope.as_deref(), Some("resolved_state")) {
		return Err!(Request(InvalidParam("resolved_state scope is not yet implemented.")));
	}
	if body.frame_negotiation && body.frame_event_ids.is_empty() {
		return Err!(Request(InvalidParam(
			"frame_event_ids is required when frame_negotiation is true."
		)));
	}
	if let Some(frame_id) = body.frame_id.as_deref() {
		if frame_id != digest.frame_id {
			return Err!(Request(InvalidParam(
				"Requested frame_id does not match the responder's current frame."
			)));
		}
	} else if body.mode == "sketch" {
		return Err!(Request(InvalidParam("frame_id is required in sketch mode.")));
	}

	let mut normalized_frame_event_ids = body.frame_event_ids.clone();
	normalized_frame_event_ids.sort();
	normalized_frame_event_ids.dedup();
	let common_frame_event_ids = if normalized_frame_event_ids.is_empty() {
		digest.frame_event_ids.clone()
	} else {
		normalized_frame_event_ids
			.iter()
			.filter(|event_id| digest.frame_event_ids.binary_search(event_id).is_ok())
			.cloned()
			.collect()
	};
	let frame_matches = !common_frame_event_ids.is_empty();
	let frame_status = if body.frame_negotiation {
		if frame_matches { "common" } else { "none" }
	} else {
		"not_requested"
	};

	Ok(RoomDiffValidation {
		normalized_frame_event_ids,
		common_frame_event_ids,
		frame_status: frame_status.to_owned(),
	})
}

async fn verify_federation_request(
	services: &crate::State,
	x_matrix: &XMatrix,
	signature_uri: &str,
	method: http::Method,
	content: Option<&serde_json::Value>,
) -> Result {
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
	let authorization: Object = if let Some(content) = content {
		let content = conduwuit::utils::to_canonical_object(content).map_err(|e| {
			err!(Request(Forbidden(warn!("Failed to canonicalize federation request body: {e}"))))
		})?;
		[
			("content".into(), Value::Object(content)),
			("destination".into(), Value::String(destination.into())),
			("method".into(), Value::String(method.as_str().into())),
			("origin".into(), Value::String(x_matrix.origin.as_str().into())),
			("signatures".into(), Value::Object(signatures.into())),
			("uri".into(), Value::String(signature_uri.to_owned())),
		]
		.into()
	} else {
		[
			("destination".into(), Value::String(destination.into())),
			("method".into(), Value::String(method.as_str().into())),
			("origin".into(), Value::String(x_matrix.origin.as_str().into())),
			("signatures".into(), Value::Object(signatures.into())),
			("uri".into(), Value::String(signature_uri.to_owned())),
		]
		.into()
	};

	let key = services
		.server_keys
		.get_verify_key(&x_matrix.origin, &x_matrix.key)
		.await
		.map_err(|e| err!(Request(Forbidden(warn!("Failed to fetch signing keys: {e}")))))?;

	let keys: conduwuit_service::server_keys::PubKeys =
		[(x_matrix.key.to_string(), key.key)].into();
	let keys: conduwuit_service::server_keys::PubKeyMap =
		[(x_matrix.origin.as_str().into(), keys)].into();
	ruma::signatures::verify_json(&keys, authorization).map_err(|e| {
		err!(Request(Forbidden(warn!(
			"Failed to verify X-Matrix signatures from {}: {e}",
			x_matrix.origin
		))))
	})?;

	Ok(())
}

async fn room_digest_inner(
	services: &crate::State,
	room_id: &OwnedRoomId,
) -> Result<RoomDigestResponse> {
	let state = services
		.rooms
		.timeline
		.reconciliation_state(room_id)
		.await?;
	let mut frame_event_ids: Vec<_> = services
		.rooms
		.state
		.get_forward_extremities(room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;
	frame_event_ids.sort();
	frame_event_ids.dedup();

	let extremity_event_ids = frame_event_ids.clone();
	let frame_id = frame_id_for(&frame_event_ids);

	Ok(RoomDigestResponse {
		digest: state.resident.accumulator().encode_digest(),
		digest_type: "algebraic_v1".to_owned(),
		known_event_count: state.known_event_count,
		frame_id,
		strata: state
			.resident
			.strata()
			.iter()
			.map(encode_stratum_entry)
			.collect(),
		frame_event_ids,
		extremity_event_ids,
		depth_range: Some(state.depth_range),
		origin_server_ts_range: Some(state.origin_server_ts_range),
	})
}

/// `GET /_matrix/federation/v1/room_digest/{roomId}`
pub(crate) async fn get_room_digest_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	Path(room_id_str): Path<String>,
	uri: http::Uri,
) -> Result<impl axum::response::IntoResponse> {
	let signature_uri = uri
		.path_and_query()
		.map_or("/", http::uri::PathAndQuery::as_str)
		.to_owned();

	verify_federation_request(&services, &x_matrix, &signature_uri, http::Method::GET, None)
		.await?;

	let room_id = OwnedRoomId::try_from(room_id_str)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;

	AccessCheck {
		services: &services,
		origin: &x_matrix.origin,
		room_id: &room_id,
		event_id: None,
	}
	.check()
	.await?;

	let response = room_digest_inner(&services, &room_id).await?;
	Ok(axum::Json(response))
}

async fn walk_extremity_diff(
	services: &crate::State,
	room_id: &OwnedRoomId,
	have: &HashSet<OwnedEventId>,
	limit: usize,
	max_events: usize,
	max_depth_delta: u64,
) -> Result<(Vec<OwnedEventId>, bool)> {
	let mut wanted: VecDeque<_> = services
		.rooms
		.state
		.get_forward_extremities(room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let mut seen = HashSet::new();
	let mut missing = Vec::new();
	let mut truncated = false;
	let mut local_extremity_depth = 0_u64;
	for event_id in &wanted {
		if let Ok(pdu) = services.rooms.timeline.get_pdu(event_id).await {
			local_extremity_depth = local_extremity_depth.max(u64::from(pdu.depth));
		}
	}

	while let Some(event_id) = wanted.pop_front() {
		if missing.len() >= limit || seen.len() >= max_events {
			truncated = true;
			break;
		}
		if !seen.insert(event_id.clone()) {
			continue;
		}
		if have.contains(&event_id) {
			continue;
		}
		let Ok(pdu) = services.rooms.timeline.get_pdu(&event_id).await else {
			truncated = true;
			continue;
		};
		let depth = u64::from(pdu.depth);
		if local_extremity_depth.saturating_sub(depth) > max_depth_delta {
			truncated = true;
			continue;
		}

		missing.push(event_id.clone());

		for prev in &pdu.prev_events {
			if !have.contains(prev) {
				wanted.push_back(prev.to_owned());
			}
		}
	}

	Ok((missing, truncated))
}

/// `POST /_matrix/federation/v1/room_diff/{roomId}`
pub(crate) async fn post_room_diff_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	Path(room_id_str): Path<String>,
	uri: http::Uri,
	axum::Json(body): axum::Json<serde_json::Value>,
) -> Result<impl axum::response::IntoResponse> {
	let signature_uri = uri
		.path_and_query()
		.map_or("/", http::uri::PathAndQuery::as_str)
		.to_owned();

	verify_federation_request(
		&services,
		&x_matrix,
		&signature_uri,
		http::Method::POST,
		Some(&body),
	)
	.await?;

	let room_id = OwnedRoomId::try_from(room_id_str)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;

	AccessCheck {
		services: &services,
		origin: &x_matrix.origin,
		room_id: &room_id,
		event_id: None,
	}
	.check()
	.await?;

	let body: RoomDiffRequest = serde_json::from_value(body)
		.map_err(|_| err!(Request(InvalidParam("Invalid room diff body."))))?;

	let digest = room_digest_inner(&services, &room_id).await?;
	let state = services
		.rooms
		.timeline
		.reconciliation_state(&room_id)
		.await?;
	let validation = validate_room_diff_request(&body, &digest)?;
	let frame_status = validation.frame_status;
	let frame_matches = !validation.common_frame_event_ids.is_empty();
	let mut have: HashSet<OwnedEventId> = body.local_extremity_event_ids.into_iter().collect();
	have.extend(body.have_event_ids);

	if body.mode == "sketch" {
		if !frame_matches {
			let response = RoomDiffResponse {
				missing_event_ids: Vec::new(),
				requester_only_short_ids: None,
				expected_requester_side_accumulator: None,
				remote_known_event_count: digest.known_event_count,
				remote_extremity_event_ids: digest.extremity_event_ids,
				frame_id: digest.frame_id,
				frame_status,
				negotiated_frame_event_ids: None,
				scope: body.scope.unwrap_or_else(|| "event_set".to_owned()),
				state_at: body.state_at,
				sketch_status: Some("not_applicable".to_owned()),
				truncated: true,
			};
			return Ok(axum::Json(response));
		}

		let local_digest = body.local_digest.as_deref().ok_or_else(|| {
			err!(Request(InvalidParam("Missing local_digest for sketch mode.")))
		})?;
		let _local_known_event_count = body.local_known_event_count.ok_or_else(|| {
			err!(Request(InvalidParam("Missing local_known_event_count for sketch mode.")))
		})?;
		if body.digest_type.as_deref() != Some("algebraic_v1") {
			return Err!(Request(InvalidParam("Unsupported digest_type.")));
		}
		if body.local_sketches.len() != body.requests.len() {
			return Err!(Request(InvalidParam(
				"local_sketches length must match requests length."
			)));
		}

		let bucket_requests: Vec<rezzy::reconcile::BucketRequest> = body
			.requests
			.iter()
			.map(|request| {
				Ok(rezzy::reconcile::BucketRequest {
					depth: u8::try_from(request.depth)
						.map_err(|_| err!(Request(InvalidParam("Invalid bucket depth."))))?,
					prefix: u32::try_from(request.prefix)
						.map_err(|_| err!(Request(InvalidParam("Invalid bucket prefix."))))?,
					capacity: usize::try_from(request.capacity)
						.map_err(|_| err!(Request(InvalidParam("Invalid sketch capacity."))))?,
				})
			})
			.collect::<Result<_>>()?;

		let requester_digest = rezzy::reconcile::RoomAccumulator::decode_digest(local_digest)
			.map_err(|_| err!(Request(InvalidParam("Invalid local_digest for sketch mode."))))?;
		let residual_digest = state.resident.accumulator().digest() ^ requester_digest;

		let local_sketches: Vec<_> = body
			.local_sketches
			.iter()
			.zip(&body.requests)
			.map(|(encoded, request)| {
				let capacity = usize::try_from(request.capacity)
					.map_err(|_| err!(Request(InvalidParam("Invalid sketch capacity."))))?;
				rezzy::reconcile::SyndromeSketch::decode(capacity, encoded)
					.map_err(|_| err!(Request(InvalidParam("Invalid local sketch."))))
			})
			.collect::<Result<_>>()?;

		let responder_sketches =
			rezzy::reconcile::build_bucket_sketches(&state.sorted_h64, &bucket_requests)
				.map_err(|_| err!(Request(InvalidParam("Invalid bucket requests."))))?;

		let mut missing_event_ids = Vec::new();
		let mut requester_only_short_ids = Vec::new();
		let mut responder_side_accumulator = rezzy::reconcile::RoomAccumulator::new();

		for ((_, bucket_request), (requester_sketch, responder_sketch)) in body
			.requests
			.iter()
			.zip(bucket_requests.iter())
			.zip(local_sketches.iter().zip(responder_sketches.iter()))
		{
			let residual = responder_sketch
				.subtract(requester_sketch)
				.map_err(|_| err!(Request(InvalidParam("Invalid sketch length."))))?;
			let roots = match residual.decode_elements(bucket_request.capacity) {
				| Ok(roots) => roots,
				| Err(rezzy::reconcile::AlgebraicError::DecodeFailure) => {
					let response = RoomDiffResponse {
						missing_event_ids: Vec::new(),
						requester_only_short_ids: None,
						expected_requester_side_accumulator: None,
						remote_known_event_count: digest.known_event_count,
						remote_extremity_event_ids: digest.extremity_event_ids,
						frame_id: digest.frame_id,
						frame_status,
						negotiated_frame_event_ids: if body.frame_negotiation {
							Some(validation.common_frame_event_ids.clone())
						} else {
							None
						},
						scope: body.scope.unwrap_or_else(|| "event_set".to_owned()),
						state_at: body.state_at,
						sketch_status: Some("capacity_exceeded".to_owned()),
						truncated: true,
					};
					return Ok(axum::Json(response));
				},
				| Err(_) => {
					return Err!(Request(InvalidParam("Sketch decode failed.")));
				},
			};

			for root in roots {
				let entries: Option<&Vec<(OwnedEventId, rezzy::reconcile::ElementHash)>> =
					state.h64_to_event_hashes.get(&root);
				if let Some(entries) = entries {
					let (event_id, hash) = entries
						.first()
						.ok_or_else(|| err!(Database("missing event mapping for short id.")))?;
					missing_event_ids.push(event_id.clone());
					responder_side_accumulator.insert(*hash).map_err(|e| {
						err!(Database("failed to accumulate responder-side repair set: {e:?}"))
					})?;
				} else {
					requester_only_short_ids.push(encode_short_identifier(root));
				}
			}
		}

		let expected_requester_side_accumulator =
			encode_accumulator_digest(residual_digest ^ responder_side_accumulator.digest());

		let response = RoomDiffResponse {
			missing_event_ids,
			requester_only_short_ids: Some(requester_only_short_ids),
			expected_requester_side_accumulator: Some(expected_requester_side_accumulator),
			remote_known_event_count: digest.known_event_count,
			remote_extremity_event_ids: digest.extremity_event_ids,
			frame_id: digest.frame_id,
			frame_status,
			negotiated_frame_event_ids: if body.frame_negotiation {
				Some(validation.common_frame_event_ids.clone())
			} else {
				None
			},
			scope: body.scope.unwrap_or_else(|| "event_set".to_owned()),
			state_at: body.state_at,
			sketch_status: Some("decoded".to_owned()),
			truncated: false,
		};
		return Ok(axum::Json(response));
	}

	let limit =
		usize::try_from(body.limit.unwrap_or(1000).clamp(1, 10_000)).unwrap_or(usize::MAX);
	let max_events =
		usize::try_from(body.max_events.unwrap_or(10_000).clamp(1, 50_000)).unwrap_or(usize::MAX);
	let max_depth_delta = body.max_depth_delta.unwrap_or(5_000);
	let (missing_event_ids, truncated) =
		walk_extremity_diff(&services, &room_id, &have, limit, max_events, max_depth_delta)
			.await?;

	Ok(axum::Json(RoomDiffResponse {
		missing_event_ids,
		requester_only_short_ids: None,
		expected_requester_side_accumulator: None,
		remote_known_event_count: digest.known_event_count,
		remote_extremity_event_ids: digest.extremity_event_ids,
		frame_id: digest.frame_id,
		frame_status,
		negotiated_frame_event_ids: if body.frame_negotiation {
			Some(validation.common_frame_event_ids.clone())
		} else {
			None
		},
		scope: body.scope.unwrap_or_else(|| "event_set".to_owned()),
		state_at: body.state_at,
		sketch_status: None,
		truncated,
	}))
}

/// `POST /_matrix/federation/v1/room_events/{roomId}`
pub(crate) async fn post_room_events_route(
	State(services): State<crate::State>,
	TypedHeader(Authorization(x_matrix)): TypedHeader<Authorization<XMatrix>>,
	Path(room_id_str): Path<String>,
	uri: http::Uri,
	axum::Json(body): axum::Json<serde_json::Value>,
) -> Result<impl axum::response::IntoResponse> {
	let signature_uri = uri
		.path_and_query()
		.map_or("/", http::uri::PathAndQuery::as_str)
		.to_owned();

	verify_federation_request(
		&services,
		&x_matrix,
		&signature_uri,
		http::Method::POST,
		Some(&body),
	)
	.await?;

	let room_id = OwnedRoomId::try_from(room_id_str)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;

	AccessCheck {
		services: &services,
		origin: &x_matrix.origin,
		room_id: &room_id,
		event_id: None,
	}
	.check()
	.await?;

	let body: RoomEventsRequest = serde_json::from_value(body)
		.map_err(|_| err!(Request(InvalidParam("Invalid room events body."))))?;

	let known: HashSet<_> = body.known_event_ids.into_iter().collect();
	let requested: HashSet<_> = body.event_ids.iter().cloned().collect();
	let mut added: HashSet<OwnedEventId> = requested.iter().cloned().collect();

	let mut missing_event_ids = Vec::new();
	let mut rejected_tombstones = Vec::new();
	let mut rejected_tombstone_ids = HashSet::new();
	let mut combined = Vec::new();

	for event_id in &body.event_ids {
		match services
			.rooms
			.timeline
			.get_pdu_in_room(Some(&room_id), event_id)
			.await
		{
			| Ok(pdu) => {
				if services
					.rooms
					.state_accessor
					.server_can_see_event(&x_matrix.origin, &room_id, event_id.as_ref())
					.await
				{
					combined.push(pdu);
				} else {
					missing_event_ids.push(event_id.clone());
				}
			},
			| Err(_) => missing_event_ids.push(event_id.clone()),
		}
	}

	if body.include_auth_chain {
		for event_id in &body.event_ids {
			let Ok(chain) = services
				.rooms
				.auth_chain
				.get_auth_chain(&room_id, std::iter::once(event_id.as_ref()))
				.await
			else {
				continue;
			};
			for short in chain {
				let Ok(auth_event_id) = services
					.rooms
					.short
					.get_eventid_from_short::<OwnedEventId>(short)
					.await
				else {
					continue;
				};
				if let Some(reason) = services
					.rooms
					.pdu_metadata
					.tombstone_reason(&room_id, &auth_event_id)
					.await
				{
					if rejected_tombstone_ids.insert(auth_event_id.clone()) {
						rejected_tombstones.push(rejected_tombstone(&auth_event_id, &reason));
					}
					continue;
				}
				if !added.insert(auth_event_id.clone()) || known.contains(&auth_event_id) {
					continue;
				}
				if let Ok(pdu) = services
					.rooms
					.timeline
					.get_pdu_in_room(Some(&room_id), &auth_event_id)
					.await
				{
					combined.push(pdu);
				}
			}
		}
	}

	let sorted = sort_topologically(combined);
	let mut events = Vec::new();
	let mut auth_chain_events = Vec::new();

	for pdu in sorted {
		let json = services
			.sending
			.convert_to_outgoing_federation_event(
				services.rooms.timeline.get_pdu_json(&pdu.event_id).await?,
			)
			.await;
		if requested.contains(&pdu.event_id) {
			events.push(json);
		} else {
			auth_chain_events.push(json);
		}
	}

	Ok(axum::Json(RoomEventsResponse {
		events,
		auth_chain_events,
		missing_event_ids,
		rejected_tombstones,
	}))
}

#[cfg(test)]
mod tests {
	use ruma::OwnedEventId;

	use super::{RoomDiffRequest, RoomDigestResponse, frame_id_for, validate_room_diff_request};

	fn eid(s: &str) -> OwnedEventId { format!("${s}:example.com").try_into().unwrap() }

	fn digest(frame_event_ids: Vec<OwnedEventId>) -> RoomDigestResponse {
		RoomDigestResponse {
			digest: "digest".to_owned(),
			digest_type: "algebraic_v1".to_owned(),
			known_event_count: 1,
			frame_id: frame_id_for(&frame_event_ids),
			strata: vec![],
			frame_event_ids,
			extremity_event_ids: vec![],
			depth_range: None,
			origin_server_ts_range: None,
		}
	}

	#[test]
	fn frame_id_is_order_insensitive() {
		let ordered = vec![eid("a"), eid("b"), eid("c")];
		let shuffled = vec![eid("c"), eid("a"), eid("b")];

		assert_eq!(frame_id_for(&ordered), frame_id_for(&shuffled));
	}

	#[test]
	fn frame_id_deduplicates_anchor_ids() {
		let unique = vec![eid("a"), eid("b"), eid("c")];
		let duplicated = vec![eid("b"), eid("a"), eid("c"), eid("b"), eid("a")];

		assert_eq!(frame_id_for(&unique), frame_id_for(&duplicated));
	}

	#[test]
	fn resolved_state_scope_is_rejected() {
		let body = RoomDiffRequest {
			mode: "extremity".to_owned(),
			frame_negotiation: false,
			frame_event_ids: vec![],
			local_extremity_event_ids: vec![],
			have_event_ids: vec![],
			digest_type: None,
			local_digest: None,
			local_known_event_count: None,
			requests: vec![],
			local_sketches: vec![],
			limit: None,
			max_depth_delta: None,
			max_events: None,
			scope: Some("resolved_state".to_owned()),
			state_at: None,
			frame_id: None,
		};

		let err = validate_room_diff_request(&body, &digest(vec![eid("a")])).unwrap_err();
		assert!(
			err.to_string()
				.contains("resolved_state scope is not yet implemented")
		);
	}

	#[test]
	fn sketch_mode_accepts_canonicalized_frame() {
		let frame_event_ids = vec![eid("a"), eid("b")];
		let digest = digest(frame_event_ids.clone());
		let body = RoomDiffRequest {
			mode: "sketch".to_owned(),
			frame_negotiation: true,
			frame_event_ids: vec![eid("b"), eid("a"), eid("a")],
			local_extremity_event_ids: vec![],
			have_event_ids: vec![],
			digest_type: Some("algebraic_v1".to_owned()),
			local_digest: Some("AQ".to_owned()),
			local_known_event_count: Some(1),
			requests: vec![],
			local_sketches: vec![],
			limit: None,
			max_depth_delta: None,
			max_events: None,
			scope: None,
			state_at: None,
			frame_id: Some(digest.frame_id.clone()),
		};

		let validation = validate_room_diff_request(&body, &digest).expect("validation");
		assert!(!validation.common_frame_event_ids.is_empty());
		assert_eq!(validation.frame_status, "common");
		assert_eq!(validation.normalized_frame_event_ids, frame_event_ids);
	}

	#[test]
	fn sketch_mode_rejects_frame_mismatch() {
		let digest = digest(vec![eid("a"), eid("b")]);
		let body = RoomDiffRequest {
			mode: "sketch".to_owned(),
			frame_negotiation: true,
			frame_event_ids: vec![eid("c")],
			local_extremity_event_ids: vec![],
			have_event_ids: vec![],
			digest_type: Some("algebraic_v1".to_owned()),
			local_digest: Some("AQ".to_owned()),
			local_known_event_count: Some(1),
			requests: vec![],
			local_sketches: vec![],
			limit: None,
			max_depth_delta: None,
			max_events: None,
			scope: None,
			state_at: None,
			frame_id: Some(digest.frame_id.clone()),
		};

		let validation = validate_room_diff_request(&body, &digest).expect("validation");
		assert!(validation.common_frame_event_ids.is_empty());
		assert_eq!(validation.frame_status, "none");
	}

	#[test]
	fn sketch_mode_negotiates_common_subset() {
		let digest = digest(vec![eid("a"), eid("b"), eid("c")]);
		let body = RoomDiffRequest {
			mode: "sketch".to_owned(),
			frame_negotiation: true,
			frame_event_ids: vec![eid("b"), eid("c"), eid("d")],
			local_extremity_event_ids: vec![],
			have_event_ids: vec![],
			digest_type: Some("algebraic_v1".to_owned()),
			local_digest: Some("AQ".to_owned()),
			local_known_event_count: Some(1),
			requests: vec![],
			local_sketches: vec![],
			limit: None,
			max_depth_delta: None,
			max_events: None,
			scope: None,
			state_at: None,
			frame_id: Some(digest.frame_id.clone()),
		};

		let validation = validate_room_diff_request(&body, &digest).expect("validation");
		assert_eq!(validation.common_frame_event_ids, vec![eid("b"), eid("c")]);
		assert_eq!(validation.frame_status, "common");
	}
}
