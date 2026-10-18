//! One reconciliation exchange with one peer.
//!
//! The peer's digest is compared with a fresh local snapshot. If the sets
//! differ, sketch rounds narrow the difference to short identifiers, which the
//! peer maps to event IDs. Those IDs are only hints: a sample is fetched first,
//! a peer whose sample mostly fails is abandoned, and every fetched event goes
//! through the normal incoming-PDU pipeline before it is believed.

use std::{
	collections::BTreeSet,
	time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use conduwuit::{debug, warn};
use futures::{StreamExt, stream};
use rand::seq::SliceRandom;
use rezzy_recon::{
	BucketDecodeBatch, BucketDecodeSuccess, BucketRequest, ClientAction, ElementHash,
	EventIdFormat, MAX_BUCKETED_SKETCH_CAPACITY, MAX_LOCAL_SKETCH_DECODE_CAPACITY,
	MAX_RECONCILIATION_ROUNDS, ReconciliationClient, RemoteDigest, build_bucket_sketches,
};
use slipstream::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId, OwnedRoomId, RoomId,
	ServerName,
	api::federation::{
		discovery::get_server_version,
		event::get_event,
		reconciliation::{
			get_room_digest_strata,
			post_room_diff::{self, unstable::SketchBucket},
		},
	},
};

use super::{DIGEST_FEATURE, Key, Service};
/// Events fetched from one peer in one exchange.
const MAX_FETCHES: usize = 100;
/// Events fetched first, before the rest are trusted enough to fetch.
const SAMPLE: usize = 16;
/// Concurrent event fetches.
const FETCH_CONCURRENCY: usize = 4;
/// The share of failed fetches, out of 100, above which a peer's claim is
/// abandoned. Honest peers fail rarely: an event they offered is one they hold.
const MAX_FAILED_PERCENT: usize = 25;
/// How long an event that could not be fetched or used is not asked for again.
const REMEMBER_FOR: Duration = Duration::from_hours(1);
/// How many such events are remembered at once.
const REMEMBER_MAX: usize = 20_000;

#[derive(Debug)]
pub(super) enum Outcome {
	/// Digest and count agree.
	Synchronized,
	/// The sets differ and the client chose an action we do not run yet.
	Differs(&'static str),
	/// Sketch rounds finished and the offered events were fetched.
	Reconciled {
		offered: usize,
		applied: usize,
		unusable: usize,
		/// More was offered than one exchange may fetch.
		truncated: bool,
	},
	/// The peer's claim was not believed; the round was dropped.
	Abandoned(String),
	/// The peer does not serve the endpoints.
	Unsupported,
	Failed(String),
}

impl Outcome {
	pub(super) fn describe(&self) -> String {
		match self {
			| Self::Synchronized => "synchronized".to_owned(),
			| Self::Differs(action) => format!("differs ({action}, not run)"),
			| Self::Reconciled { offered, applied, unusable, .. } => {
				format!("reconciled: {offered} offered, {applied} applied, {unusable} unusable")
			},
			| Self::Abandoned(why) => format!("abandoned: {why}"),
			| Self::Unsupported => "unsupported".to_owned(),
			| Self::Failed(why) => format!("failed: {why}"),
		}
	}
}

fn action_name(action: &ClientAction) -> &'static str {
	match action {
		| ClientAction::Synchronized => "synchronized",
		| ClientAction::ExtremityDiff => "extremity_diff",
		| ClientAction::BucketSketches { .. } => "bucket_sketches",
		| ClientAction::ResolveRoots { .. } => "resolve_roots",
	}
}

/// The bucket a short identifier falls in at `depth`.
fn bucket_prefix(h64: u64, depth: u8) -> u64 {
	if depth == 0 {
		0
	} else {
		h64 >> (64_u32.saturating_sub(u32::from(depth)))
	}
}

/// The short identifier of an event ID, whichever base64 alphabet it uses.
fn short_id(event_id: &EventId) -> Option<u64> {
	[EventIdFormat::V4Plus, EventIdFormat::V3]
		.into_iter()
		.find_map(|format| ElementHash::from_matrix_event_id(event_id.as_str(), format).ok())
		.map(|hash| hash.h64)
}

/// Events expected to arrive on the peer while the exchange runs.
const HEADROOM: usize = 8;

impl Service {
	async fn admission_lapsed(&self, room_id: &RoomId, peer: &ServerName) -> Option<Outcome> {
		if self.services.sending.destination_aborted(peer) {
			return Some(Outcome::Abandoned("sending to the peer was aborted".to_owned()));
		}
		let ours = self.services.globals.server_name();
		let state_cache = &self.services.state_cache;
		if !state_cache.server_in_room(ours, room_id).await
			|| !state_cache.server_in_room(peer, room_id).await
		{
			return Some(Outcome::Abandoned("no longer in the room".to_owned()));
		}
		if self
			.services
			.event_handler
			.acl_check(peer, room_id)
			.await
			.is_err()
		{
			return Some(Outcome::Abandoned("peer denied by room ACL".to_owned()));
		}
		None
	}

	pub(super) async fn exchange(&self, key: &Key) -> Outcome {
		let (room_id, peer) = (key.room_id(), key.peer());

		// Admission was checked when the pair was queued; the situation may
		// have changed while it waited.
		if let Some(outcome) = self.admission_lapsed(room_id, peer).await {
			return outcome;
		}

		let local = match self.services.timeline.reconciliation_state(room_id).await {
			| Ok(local) => local,
			| Err(e) => return Outcome::Failed(format!("local snapshot: {e}")),
		};

		// Snapshot construction may be expensive; re-check admission before
		// any peer request so a queued exchange cannot outlive membership/ACL.
		if let Some(outcome) = self.admission_lapsed(room_id, peer).await {
			return outcome;
		}
		if let Err(outcome) = self.peer_advertises(peer).await {
			return outcome;
		}
		if let Some(outcome) = self.admission_lapsed(room_id, peer).await {
			return outcome;
		}

		let request = get_room_digest_strata::unstable::Request::new(room_id.to_owned());
		let response = match self
			.services
			.sending
			.send_federation_request(peer, request)
			.await
		{
			| Ok(response) => response,
			// The endpoints are optional: a peer that lacks them is fine.
			| Err(e) if e.is_not_found() || e.status_code() == http::StatusCode::BAD_REQUEST => {
				// The advertisement was stale or wrong: read it again next time.
				self.advertised.invalidate(&peer.to_owned());
				return Outcome::Unsupported;
			},
			| Err(e) => return Outcome::Failed(format!("peer digest: {e}")),
		};
		if response.digest_type != "algebraic_v1" {
			return Outcome::Unsupported;
		}

		// The responder sketches its whole event set, so the exchange runs against
		// its frame whether or not we know its extremities: a lagging server
		// by definition does not. Declaring the frames equal and the extremities
		// known keeps the client from stopping at `ExtremityDiff`.
		let remote = match RemoteDigest::from_wire(
			&response.digest,
			response.known_event_count,
			&response.strata,
			&response.frame_event_ids,
			&response.frame_event_ids,
			&response.extremity_event_ids,
			|_| true,
		) {
			| Ok(remote) => remote,
			| Err(e) => return Outcome::Failed(format!("malformed digest: {e:?}")),
		};

		let client = match ReconciliationClient::new(MAX_LOCAL_SKETCH_DECODE_CAPACITY) {
			| Ok(client) => client,
			| Err(e) => return Outcome::Failed(format!("client: {e:?}")),
		};
		let mut action = client.select_action(&local.resident, remote, HEADROOM);
		let mut offered: BTreeSet<OwnedEventId> = BTreeSet::new();

		for _ in 0..MAX_RECONCILIATION_ROUNDS {
			match action {
				| ClientAction::Synchronized if offered.is_empty() => {
					return Outcome::Synchronized;
				},
				| ClientAction::Synchronized | ClientAction::ResolveRoots { .. } => {
					return self.fetch_offered(room_id, peer, offered).await;
				},
				| ClientAction::ExtremityDiff => {
					debug!(target: "reconcile", %room_id, %peer, "sets differ; extremity transfer not implemented");
					return Outcome::Differs(action_name(&ClientAction::ExtremityDiff));
				},
				| ClientAction::BucketSketches { requests, accumulated_roots } => {
					let round = match self
						.sketch_round(room_id, peer, &local, &response, &requests)
						.await
					{
						| Ok(round) => round,
						| Err(outcome) => return outcome,
					};
					offered.extend(round.event_ids);
					action = ReconciliationClient::transition_bucket_batch(
						round.batch,
						&requests,
						accumulated_roots,
						None,
						MAX_BUCKETED_SKETCH_CAPACITY,
					);
				},
			}
		}
		Outcome::Abandoned(format!("no agreement after {MAX_RECONCILIATION_ROUNDS} rounds"))
	}

	/// Whether `peer` advertises the digest profile in its federation
	/// `/version`. A peer that does not is never sent the reconciliation
	/// endpoints, which it would only answer with an error. A yes is cached for
	/// `ADVERTISED_FOR`; a no is not, because the queue already backs an
	/// unsupported peer off.
	async fn peer_advertises(&self, peer: &ServerName) -> Result<(), Outcome> {
		let owned = peer.to_owned();
		if self.advertised.contains_key(&owned) {
			return Ok(());
		}
		let response = match self
			.services
			.sending
			.send_federation_request(peer, get_server_version::v1::Request {})
			.await
		{
			| Ok(response) => response,
			// No `/version` at all: nothing to read the profile from.
			| Err(e) if e.is_not_found() => return Err(Outcome::Unsupported),
			| Err(e) => return Err(Outcome::Failed(format!("peer version: {e}"))),
		};
		let advertised = response
			.unstable_features
			.as_ref()
			.and_then(|features| features.get(DIGEST_FEATURE))
			.copied()
			.unwrap_or(false);
		if !advertised {
			return Err(Outcome::Unsupported);
		}
		self.advertised.insert(owned, ());
		Ok(())
	}

	/// Sends one `room_diff` for `requests` and returns what the peer decoded.
	async fn sketch_round(
		&self,
		room_id: &RoomId,
		peer: &ServerName,
		local: &crate::rooms::timeline::RoomReconciliationState,
		remote: &get_room_digest_strata::unstable::Response,
		requests: &[BucketRequest],
	) -> Result<Round, Outcome> {
		if let Some(outcome) = self.admission_lapsed(room_id, peer).await {
			return Err(outcome);
		}
		let sketches = build_bucket_sketches(local.population.h64s(), requests)
			.map_err(|e| Outcome::Failed(format!("local sketches: {e:?}")))?;

		let buckets = requests
			.iter()
			.map(|r| {
				Ok(SketchBucket {
					depth: u64::from(r.depth),
					prefix: r.prefix,
					capacity: u64::try_from(r.capacity)
						.map_err(|e| Outcome::Failed(format!("bucket capacity: {e}")))?,
				})
			})
			.collect::<Result<Vec<_>, Outcome>>()?;
		let request = post_room_diff::unstable::Request {
			room_id: room_id.to_owned(),
			mode: "sketch".to_owned(),
			digest_type: "algebraic_v1".to_owned(),
			frame_id: remote.frame_id.clone(),
			frame_negotiation: false,
			frame_event_ids: remote.frame_event_ids.clone(),
			local_digest: local.resident.accumulator().encode_digest(),
			local_known_event_count: local.resident.accumulator().known_event_count(),
			requests: buckets,
			local_sketches: sketches
				.iter()
				.map(rezzy_recon::SyndromeSketch::encode)
				.collect(),
			scope: None,
			state_at: None,
		};
		if let Some(outcome) = self.admission_lapsed(room_id, peer).await {
			return Err(outcome);
		}
		let response = self
			.services
			.sending
			.send_federation_request(peer, request)
			.await
			.map_err(|e| Outcome::Failed(format!("peer room_diff: {e}")))?;

		match response.sketch_status.as_str() {
			| "capacity_exceeded" => Ok(Round {
				batch: BucketDecodeBatch {
					successful_buckets: Vec::new(),
					failed_buckets: requests.iter().map(|r| (r.depth, r.prefix)).collect(),
				},
				event_ids: Vec::new(),
			}),
			| "decoded" => {
				let mut roots: Vec<u64> = response
					.missing_event_ids
					.iter()
					.filter_map(short_id)
					.collect();
				for short in response.requester_only_short_ids.iter().flatten() {
					let Ok(raw) = URL_SAFE_NO_PAD.decode(short) else {
						return Err(Outcome::Failed("malformed short identifier".to_owned()));
					};
					let Ok(raw) = <[u8; 8]>::try_from(raw.as_slice()) else {
						return Err(Outcome::Failed("malformed short identifier".to_owned()));
					};
					roots.push(u64::from_be_bytes(raw));
				}

				let successful_buckets = requests
					.iter()
					.map(|r| BucketDecodeSuccess {
						depth: r.depth,
						prefix: r.prefix,
						roots: roots
							.iter()
							.copied()
							.filter(|root| bucket_prefix(*root, r.depth) == r.prefix)
							.collect(),
					})
					.collect();
				Ok(Round {
					batch: BucketDecodeBatch {
						successful_buckets,
						failed_buckets: Vec::new(),
					},
					event_ids: response.missing_event_ids,
				})
			},
			| other => Err(Outcome::Failed(format!("unexpected sketch_status {other:?}"))),
		}
	}

	/// Fetches the events a peer offered, a sample first, and runs the ones
	/// that check out through the incoming-PDU pipeline.
	async fn fetch_offered(
		&self,
		room_id: &RoomId,
		peer: &ServerName,
		offered: BTreeSet<OwnedEventId>,
	) -> Outcome {
		let total = offered.len();
		let mut wanted = Vec::new();
		for id in offered {
			if self.is_remembered(peer, &id)
				|| self.services.timeline.non_outlier_pdu_exists(&id).await
			{
				continue;
			}
			wanted.push(id);
		}
		if wanted.is_empty() {
			return Outcome::Reconciled {
				offered: total,
				applied: 0,
				unusable: 0,
				truncated: false,
			};
		}
		wanted.shuffle(&mut rand::rng());
		let truncated = wanted.len() > MAX_FETCHES;
		wanted.truncate(MAX_FETCHES);

		let mut fetched: Vec<(u64, OwnedRoomId, OwnedEventId, CanonicalJsonObject)> = Vec::new();
		let mut failed = 0_usize;
		let mut attempted = 0_usize;

		// Sample first; a peer whose sample mostly fails is not believed.
		let rest = wanted.split_off(SAMPLE.min(wanted.len()));
		for batch in [wanted, rest] {
			let results: Vec<_> = stream::iter(batch)
				.map(|id| async move {
					let result = self.fetch_event(room_id, peer, &id).await;
					(id, result)
				})
				.buffer_unordered(FETCH_CONCURRENCY)
				.collect()
				.await;
			for (id, result) in results {
				attempted = attempted.saturating_add(1);
				match result {
					| Some(event) => fetched.push(event),
					| None => {
						failed = failed.saturating_add(1);
						self.remember(peer, id);
					},
				}
			}
			if failed.saturating_mul(100) > attempted.saturating_mul(MAX_FAILED_PERCENT) {
				return Outcome::Abandoned(format!(
					"{failed} of {attempted} offered events could not be fetched"
				));
			}
		}

		// Oldest first, so each event's predecessors are already in place.
		fetched.sort_by_key(|(depth, ..)| *depth);
		let room_version = match self.services.state.get_room_version(room_id).await {
			| Ok(version) => version,
			| Err(e) => return Outcome::Failed(format!("room version: {e}")),
		};

		let (mut applied, mut unusable) = (0_usize, 0_usize);
		for (_, parsed_room, event_id, value) in fetched {
			let handled = self
				.services
				.event_handler
				.handle_incoming_pdu(peer, &parsed_room, &event_id, value, true, &room_version)
				.await;
			if let Err(e) = &handled {
				debug!(target: "reconcile", %event_id, "offered event rejected: {e}");
			}
			if self
				.services
				.timeline
				.non_outlier_pdu_exists(&event_id)
				.await
			{
				applied = applied.saturating_add(1);
			} else {
				unusable = unusable.saturating_add(1);
				self.remember(peer, event_id);
			}
		}

		Outcome::Reconciled {
			offered: total,
			applied,
			unusable,
			truncated,
		}
	}

	/// Fetches one event from `peer` and checks it is the event, in the room,
	/// that was asked for.
	async fn fetch_event(
		&self,
		room_id: &RoomId,
		peer: &ServerName,
		event_id: &EventId,
	) -> Option<(u64, OwnedRoomId, OwnedEventId, CanonicalJsonObject)> {
		if self.admission_lapsed(room_id, peer).await.is_some() {
			return None;
		}
		let request = get_event::v1::Request {
			event_id: event_id.to_owned(),
			include_unredacted_content: Some(false),
		};
		let response = self
			.services
			.sending
			.send_federation_request(peer, request)
			.await
			.ok()?;
		let raw = response.pdus.first()?;
		let (parsed_room, parsed_id, value) = self
			.services
			.event_handler
			.parse_incoming_pdu(raw)
			.await
			.map_err(|e| warn!(target: "reconcile", %event_id, "offered event unparsable: {e}"))
			.ok()?;
		if parsed_room != room_id || parsed_id != event_id {
			return None;
		}
		let depth = value
			.get("depth")
			.and_then(CanonicalJsonValue::as_u64)
			.unwrap_or(u64::MAX);
		Some((depth, parsed_room, parsed_id, value))
	}

	/// Memory is per peer: one peer failing to serve an event says nothing
	/// about whether another can.
	fn is_remembered(&self, peer: &ServerName, event_id: &EventId) -> bool {
		self.remembered
			.lock()
			.get(&(peer.to_owned(), event_id.to_owned()))
			.is_some_and(|since| since.elapsed() < REMEMBER_FOR)
	}

	fn remember(&self, peer: &ServerName, event_id: OwnedEventId) {
		let mut remembered = self.remembered.lock();
		if remembered.len() >= REMEMBER_MAX {
			remembered.retain(|_, since| since.elapsed() < REMEMBER_FOR);
		}
		if remembered.len() >= REMEMBER_MAX {
			// Still full of live entries: drop the oldest half.
			let mut ages: Vec<Instant> = remembered.values().copied().collect();
			ages.sort_unstable();
			let cutoff = ages[ages.len() / 2];
			remembered.retain(|_, since| *since > cutoff);
		}
		remembered.insert((peer.to_owned(), event_id), Instant::now());
	}
}

/// What one `room_diff` round produced.
struct Round {
	batch: BucketDecodeBatch,
	/// Event IDs the peer decoded in this round, to be fetched afterwards.
	event_ids: Vec<OwnedEventId>,
}
