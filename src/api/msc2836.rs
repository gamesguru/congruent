//! Shared walk logic for [MSC2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836)
//! (`/event_relationships`), used by both the client and federation handlers.
//!
//! Relationships are read from `content.m.relationship = { rel_type, event_id
//! }` (the parent pointer), and the reverse (child) edges are indexed at write
//! time into `pdu_metadata::msc2836_children`. See that service module for
//! the storage layer and the "reported children" bookkeeping this module
//! relies on to decide when a federated re-fetch is worthwhile.

use std::collections::{BTreeMap, HashSet, VecDeque};

use conduwuit::{Err, Event, PduEvent, Result, err};
use conduwuit_service::Services;
use futures::StreamExt;
use ruma::{
	OwnedEventId, OwnedRoomId, RoomId, ServerName, UserId,
	api::federation::event::event_relationships as federation_event_relationships,
};
use serde::Deserialize;
use serde_json::value::RawValue as RawJsonValue;

pub(crate) enum Requester<'a> {
	Client(&'a UserId),
	Federation(&'a ServerName),
}

pub(crate) struct Params {
	pub event_id: OwnedEventId,
	pub room_id: Option<OwnedRoomId>,
	pub max_depth: i64,
	pub max_breadth: i64,
	pub limit: usize,
	pub depth_first: bool,
	pub recent_first: bool,
	pub include_parent: bool,
	pub include_children: bool,
	pub direction_up: bool,
}

pub(crate) struct DefaultedParams {
	pub event_id: OwnedEventId,
	pub room_id: Option<OwnedRoomId>,
	pub max_depth: Option<i64>,
	pub max_breadth: Option<i64>,
	pub limit: Option<i64>,
	pub depth_first: Option<bool>,
	pub recent_first: Option<bool>,
	pub include_parent: Option<bool>,
	pub include_children: Option<bool>,
	pub direction: Option<String>,
}

impl Params {
	pub(crate) fn defaulted(params: DefaultedParams) -> Self {
		Self {
			event_id: params.event_id,
			room_id: params.room_id,
			max_depth: params.max_depth.unwrap_or(3),
			max_breadth: params.max_breadth.unwrap_or(10),
			limit: params
				.limit
				.unwrap_or(100)
				.max(0)
				.try_into()
				.unwrap_or(100)
				.min(100),
			depth_first: params.depth_first.unwrap_or(false),
			recent_first: params.recent_first.unwrap_or(true),
			include_parent: params.include_parent.unwrap_or(false),
			include_children: params.include_children.unwrap_or(false),
			direction_up: params.direction.as_deref() == Some("up"),
		}
	}
}

#[derive(Deserialize)]
struct RelationshipContent {
	rel_type: String,
	event_id: OwnedEventId,
}

#[derive(Deserialize)]
struct ExtractRelationship {
	#[serde(rename = "m.relationship")]
	relationship: Option<RelationshipContent>,
}

#[derive(Deserialize, Default)]
struct ExtractChildrenUnsigned {
	#[serde(default)]
	children: BTreeMap<String, u64>,
	#[serde(default)]
	children_hash: Option<String>,
}

fn parent_of(pdu: &PduEvent) -> Option<(OwnedEventId, String)> {
	pdu.get_content::<ExtractRelationship>()
		.ok()?
		.relationship
		.map(|r| (r.event_id, r.rel_type))
}

async fn can_see(
	services: &Services,
	requester: &Requester<'_>,
	room_id: &RoomId,
	event_id: &ruma::EventId,
) -> bool {
	match requester {
		| Requester::Client(user_id) =>
			services
				.rooms
				.state_accessor
				.user_can_see_event(user_id, room_id, event_id)
				.await,
		| Requester::Federation(server_name) =>
			services
				.rooms
				.state_accessor
				.server_can_see_event(
					(*server_name).to_owned(),
					room_id.to_owned(),
					event_id.to_owned(),
				)
				.await,
	}
}

async fn pick_server(services: &Services, room_id: &RoomId) -> Option<ruma::OwnedServerName> {
	let mut servers = std::pin::pin!(
		services
			.rooms
			.state_cache
			.room_servers(room_id)
			.filter(|s| { futures::future::ready(!services.globals.server_is_ours(s)) })
	);
	servers.next().await.map(ToOwned::to_owned)
}

/// Persists events fetched via a federated `/event_relationships` response
/// (auth chain first, then the events themselves), indexing each one's
/// `m.relationship` parent edge and remembering any `unsigned.children`/
/// `children_hash` it carried. Uses a small fixed-point loop so ordering
/// within the batch doesn't matter (auth events resolve as earlier passes
/// succeed).
async fn persist_federation_events(
	services: &Services,
	origin: &ServerName,
	room_id: &RoomId,
	raws: Vec<Box<RawJsonValue>>,
) -> Vec<PduEvent> {
	let Ok(room_version) = services.rooms.state.get_room_version(room_id).await else {
		return Vec::new();
	};

	let mut pending: Vec<(OwnedEventId, ruma::CanonicalJsonObject)> = Vec::new();
	for raw in &raws {
		if let Ok((event_id, value)) =
			conduwuit::matrix::event::gen_event_id_canonical_json(raw, &room_version)
		{
			pending.push((event_id, value));
		}
	}

	let mut persisted = Vec::new();
	for _pass in 0..8 {
		if pending.is_empty() {
			break;
		}
		let mut still_pending = Vec::new();
		let mut made_progress = false;
		for (event_id, value) in pending {
			match services
				.rooms
				.event_handler
				.handle_outlier_pdu(
					origin,
					None::<&PduEvent>,
					&event_id,
					room_id,
					value.clone(),
					false,
					false,
					Some(&room_version),
				)
				.await
			{
				| Ok((pdu, _)) => {
					if let Some((parent_id, rel_type)) = parent_of(&pdu) {
						services.rooms.pdu_metadata.msc2836_add_child(
							&parent_id,
							pdu.event_id(),
							&rel_type,
						);
					}
					if let Ok(unsigned) = pdu.get_unsigned::<ExtractChildrenUnsigned>() {
						if let Some(hash) = unsigned.children_hash {
							if !unsigned.children.is_empty() {
								services.rooms.pdu_metadata.msc2836_set_reported_children(
									pdu.event_id(),
									&unsigned.children,
									&hash,
								);
							}
						}
					}
					persisted.push(pdu);
					made_progress = true;
				},
				| Err(_) => {
					still_pending.push((event_id, value));
				},
			}
		}
		pending = still_pending;
		if !made_progress {
			break;
		}
	}
	persisted
}

/// Fetches `event_id` (and whatever else the remote server chooses to
/// include, e.g. its ancestor chain) via a minimal federated
/// `/event_relationships` request, persisting anything returned.
async fn fetch_missing(services: &Services, room_id: &RoomId, event_id: &ruma::EventId) {
	let Some(dest) = pick_server(services, room_id).await else {
		return;
	};
	let request = federation_event_relationships::unstable::Request {
		event_id: event_id.to_owned(),
		room_id: Some(room_id.to_owned()),
		max_depth: None,
		max_breadth: None,
		limit: None,
		depth_first: None,
		recent_first: None,
		include_parent: None,
		include_children: None,
		direction: Some("up".to_owned()),
		batch: None,
	};
	if let Ok(response) = services
		.sending
		.send_federation_request(&dest, request)
		.await
	{
		let raws: Vec<_> = response
			.auth_chain
			.into_iter()
			.chain(response.events)
			.collect();
		persist_federation_events(services, &dest, room_id, raws).await;
	}
}

/// Fetches with the full original request parameters mirrored through,
/// used to resolve/explore the anchor event itself.
async fn fetch_full(services: &Services, room_id: &RoomId, params: &Params) {
	let Some(dest) = pick_server(services, room_id).await else {
		return;
	};
	let request = federation_event_relationships::unstable::Request {
		event_id: params.event_id.clone(),
		room_id: Some(room_id.to_owned()),
		max_depth: Some(params.max_depth),
		max_breadth: Some(params.max_breadth),
		limit: i64::try_from(params.limit).ok(),
		depth_first: Some(params.depth_first),
		recent_first: Some(params.recent_first),
		include_parent: Some(params.include_parent),
		include_children: Some(params.include_children),
		direction: Some(if params.direction_up { "up" } else { "down" }.to_owned()),
		batch: None,
	};
	if let Ok(response) = services
		.sending
		.send_federation_request(&dest, request)
		.await
	{
		let raws: Vec<_> = response
			.auth_chain
			.into_iter()
			.chain(response.events)
			.collect();
		persist_federation_events(services, &dest, room_id, raws).await;
	}
}

/// Breadth/depth-first local walk of `anchor`'s descendants (down the DAG),
/// appending newly-discovered, visible events to `results` in traversal
/// order. Purely local -- children of a node are only ever what we already
/// know about (the anchor-level federated fetch is what populates that
/// index; see [`resolve`]).
#[allow(clippy::too_many_arguments)]
async fn walk_down(
	services: &Services,
	requester: &Requester<'_>,
	room_id: &RoomId,
	anchor_id: &ruma::EventId,
	max_depth: i64,
	max_breadth: i64,
	depth_first: bool,
	recent_first: bool,
	limit: usize,
	results: &mut Vec<PduEvent>,
	seen: &mut HashSet<OwnedEventId>,
) {
	let mut frontier: VecDeque<(OwnedEventId, i64)> = VecDeque::new();
	frontier.push_back((anchor_id.to_owned(), 0));

	while let Some((event_id, depth)) = if depth_first {
		frontier.pop_back()
	} else {
		frontier.pop_front()
	} {
		if results.len() >= limit {
			break;
		}
		if max_depth >= 0 && depth >= max_depth {
			continue;
		}

		let children = services
			.rooms
			.pdu_metadata
			.msc2836_get_children(&event_id)
			.await;
		let mut child_pdus = Vec::new();
		for (child_id, _rel_type) in children {
			if seen.contains(&child_id) {
				continue;
			}
			if let Ok(pdu) = services.rooms.timeline.get_pdu(&child_id).await {
				child_pdus.push(pdu);
			}
		}
		child_pdus.sort_by_key(|p| p.origin_server_ts);
		if recent_first {
			child_pdus.reverse();
		}
		if max_breadth >= 0 {
			if let Ok(max_breadth) = usize::try_from(max_breadth) {
				child_pdus.truncate(max_breadth);
			}
		}

		for pdu in child_pdus {
			if results.len() >= limit {
				break;
			}
			if !seen.insert(pdu.event_id().to_owned()) {
				continue;
			}
			if !can_see(services, requester, room_id, pdu.event_id()).await {
				continue;
			}
			let child_id = pdu.event_id().to_owned();
			results.push(pdu);
			frontier.push_back((child_id, depth.saturating_add(1)));
		}
	}
}

/// Walks up from `anchor` following each event's `m.relationship` parent
/// pointer, fetching a missing parent over federation (client requests
/// only) one hop at a time.
#[allow(clippy::too_many_arguments)]
async fn walk_up(
	services: &Services,
	requester: &Requester<'_>,
	can_fetch: bool,
	room_id: &RoomId,
	anchor: &PduEvent,
	max_depth: i64,
	limit: usize,
	results: &mut Vec<PduEvent>,
	seen: &mut HashSet<OwnedEventId>,
) {
	let mut current = anchor.clone();
	let mut depth = 0_i64;
	loop {
		if results.len() >= limit {
			break;
		}
		if max_depth >= 0 && depth >= max_depth {
			break;
		}
		let Some((parent_id, _rel_type)) = parent_of(&current) else {
			break;
		};
		if seen.contains(&parent_id) {
			break;
		}

		let mut parent = services.rooms.timeline.get_pdu(&parent_id).await.ok();
		if parent.is_none() && can_fetch {
			fetch_missing(services, room_id, &parent_id).await;
			parent = services.rooms.timeline.get_pdu(&parent_id).await.ok();
		}
		let Some(parent) = parent else {
			break;
		};
		if !can_see(services, requester, room_id, parent.event_id()).await {
			break;
		}

		seen.insert(parent.event_id().to_owned());
		results.push(parent.clone());
		current = parent;
		depth = depth.saturating_add(1);
	}
}

/// Resolves an `/event_relationships` request (client or federation) into
/// the ordered list of events to return, and whether the response was
/// truncated by `limit`.
pub(crate) async fn resolve(
	services: &Services,
	requester: Requester<'_>,
	params: Params,
) -> Result<(Vec<PduEvent>, bool)> {
	let can_fetch = matches!(requester, Requester::Client(_));

	let mut anchor = services.rooms.timeline.get_pdu(&params.event_id).await.ok();

	if can_fetch {
		let needs_fetch = match &anchor {
			| None => true,
			| Some(pdu) =>
				services
					.rooms
					.pdu_metadata
					.msc2836_needs_explore(pdu.event_id())
					.await,
		};
		if needs_fetch {
			let room_id = anchor
				.as_ref()
				.and_then(Event::room_id_or_hash)
				.or_else(|| params.room_id.clone());
			if let Some(room_id) = room_id {
				fetch_full(services, &room_id, &params).await;
				anchor = services.rooms.timeline.get_pdu(&params.event_id).await.ok();
			}
		}
	}

	let Some(anchor) = anchor else {
		return Err!(Request(NotFound(
			"event not found, and could not be fetched over federation"
		)));
	};

	let room_id = anchor
		.room_id_or_hash()
		.ok_or_else(|| err!(Request(NotFound("event has no room"))))?;

	if !can_see(services, &requester, &room_id, anchor.event_id()).await {
		return Err!(Request(Forbidden("not allowed to see this event")));
	}

	let mut results = vec![anchor.clone()];
	let mut seen: HashSet<OwnedEventId> = HashSet::new();
	seen.insert(anchor.event_id().to_owned());

	if params.include_parent {
		if let Some((parent_id, _rel_type)) = parent_of(&anchor) {
			let mut parent = services.rooms.timeline.get_pdu(&parent_id).await.ok();
			if parent.is_none() && can_fetch {
				fetch_missing(services, &room_id, &parent_id).await;
				parent = services.rooms.timeline.get_pdu(&parent_id).await.ok();
			}
			if let Some(parent) = parent {
				if seen.insert(parent.event_id().to_owned())
					&& can_see(services, &requester, &room_id, parent.event_id()).await
				{
					results.push(parent);
				}
			}
		}
	}

	if params.direction_up {
		Box::pin(walk_up(
			services,
			&requester,
			can_fetch,
			&room_id,
			&anchor,
			params.max_depth,
			params.limit,
			&mut results,
			&mut seen,
		))
		.await;
	} else {
		walk_down(
			services,
			&requester,
			&room_id,
			anchor.event_id(),
			params.max_depth,
			params.max_breadth,
			params.depth_first,
			params.recent_first,
			params.limit,
			&mut results,
			&mut seen,
		)
		.await;
	}

	if params.include_children && params.direction_up {
		walk_down(
			services,
			&requester,
			&room_id,
			anchor.event_id(),
			1,
			params.max_breadth,
			params.depth_first,
			params.recent_first,
			params.limit,
			&mut results,
			&mut seen,
		)
		.await;
	}

	let limited = results.len() > params.limit;
	results.truncate(params.limit);
	Ok((results, limited))
}

/// Builds an event's outgoing JSON with `unsigned.children`/
/// `unsigned.children_hash` injected, per MSC2836.
pub(crate) async fn to_raw_json_with_children(
	services: &Services,
	pdu: &PduEvent,
) -> Box<RawJsonValue> {
	let (counts, hash) = services
		.rooms
		.pdu_metadata
		.msc2836_children_unsigned(pdu.event_id())
		.await;

	let mut value = pdu.to_canonical_object();
	if !counts.is_empty() {
		let unsigned = value.entry("unsigned".to_owned()).or_insert_with(|| {
			ruma::CanonicalJsonValue::Object(ruma::CanonicalJsonObject::new())
		});
		if let ruma::CanonicalJsonValue::Object(unsigned) = unsigned {
			if let Some(counts_value) = serde_json::to_value(&counts)
				.ok()
				.and_then(|v| ruma::CanonicalJsonValue::try_from(v).ok())
			{
				unsigned.insert("children".to_owned(), counts_value);
				unsigned
					.insert("children_hash".to_owned(), ruma::CanonicalJsonValue::String(hash));
			}
		}
	}

	serde_json::value::to_raw_value(&value).unwrap_or_else(|_| {
		serde_json::value::RawValue::from_string("{}".to_owned()).expect("static JSON is valid")
	})
}
