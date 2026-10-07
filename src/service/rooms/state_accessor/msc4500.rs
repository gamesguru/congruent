//! MSC4500 digest derivation.
//!
//! This is the data-model layer shared by the outbound `/send` builder, the
//! inbound validator and the `state_accumulator` endpoint. It computes *what*
//! each digest commits to; the wire shapes live with their callers.
//!
//! None of these functions guess. A DAG point that cannot be resolved exactly
//! yields `None`, which senders surface as an explicit `limited` assertion.

use std::{
	collections::{BTreeMap, HashMap, HashSet, VecDeque},
	sync::Arc,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use conduwuit::{Pdu, RoomVersion, implement, matrix::Event};
use futures::TryStreamExt;
use rezzy::state::{LtHash, RedactionOverlay, ResolutionInputRecord, ResolutionInputs};
use ruma::{
	EventId, OwnedEventId, RoomVersionId,
	events::{
		StateEventType, TimelineEventType,
		room::{
			create::RoomCreateEventContent,
			power_levels::{RoomPowerLevels, RoomPowerLevelsEventContent},
		},
	},
};
use serde::Deserialize;

use crate::rooms::short::ShortEventId;

/// Collapsed wire digests of one DAG point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointDigests {
	/// Primary resolved-state digest over `(type, state_key, event_id)`.
	pub primary: String,
	/// Redaction digest over the selected state events that are effectively
	/// redacted at this point.
	pub redactions: String,
}

/// The before/after assertion for one PDU.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PduDigests {
	pub before: PointDigests,
	pub after: PointDigests,
}

/// One node of the resolution-input closure, memoized across a transaction.
#[derive(Clone, Debug)]
pub struct InputNode {
	event_id: OwnedEventId,
	kind: String,
	state_key: String,
	auth_events: Vec<OwnedEventId>,
}

/// Per-transaction memo for the resolution-input closure. `None` records an
/// event that could not be loaded, so a gap is only discovered once.
pub type InputCache = HashMap<OwnedEventId, Option<InputNode>>;

fn encode_digest(digest: [u8; 32]) -> String { URL_SAFE_NO_PAD.encode(digest) }

/// Valid-looking redactions in the causal past of an event, as
/// `redaction event ID -> target event ID`. Shared between events through `Arc`.
///
/// This is deliberately keyed by *event ID*, never by state root: a redaction
/// is a non-state event, so two concurrent events can share a root while only
/// one has the redaction in its past.
pub(super) type RedactionSet = Arc<BTreeMap<OwnedEventId, OwnedEventId>>;

/// One DAG node, reduced to what the redaction walk needs.
#[derive(Clone, Debug)]
pub(super) struct DagNode {
	pub(super) prevs: Vec<OwnedEventId>,
	/// The target, when this node is an `m.room.redaction`.
	pub(super) redaction_of: Option<OwnedEventId>,
}

/// Memo of derived, immutable facts: an event's causal past never changes, and
/// neither does a redaction's authorization at its own state.
#[derive(Default)]
pub(super) struct CausalMemo {
	/// Redactions in `past(e) ∪ {e}`.
	through: HashMap<OwnedEventId, RedactionSet>,
	/// Whether redaction `r` was authorized at the state preceding it.
	effective: HashMap<OwnedEventId, bool>,
}

const MEMO_CAPACITY: usize = 200_000;

/// Maximum events loaded to answer one causal query. Exceeding it makes the
/// point unresolvable (`limited`), never approximated. Progress is memoized, so
/// a retry continues where the last attempt stopped.
const CAUSAL_WALK_LIMIT: usize = 100_000;

/// Folds `nodes` into the redaction sets of `root`'s past, memoizing every
/// event it visits. `None` if any ancestor is neither in `nodes` nor `memo`.
///
/// Pure and synchronous so the causal rule is testable without a database.
#[must_use]
pub(super) fn fold_redaction_sets(
	root: &EventId,
	nodes: &HashMap<OwnedEventId, DagNode>,
	memo: &mut HashMap<OwnedEventId, RedactionSet>,
) -> Option<RedactionSet> {
	let mut stack: Vec<OwnedEventId> = vec![root.to_owned()];
	while let Some(id) = stack.last().cloned() {
		if memo.contains_key(&id) {
			stack.pop();
			continue;
		}
		let node = nodes.get(&id)?;
		let pending: Vec<&OwnedEventId> = node
			.prevs
			.iter()
			.filter(|p| !memo.contains_key(*p))
			.collect();
		if !pending.is_empty() {
			stack.extend(pending.into_iter().cloned());
			continue;
		}

		let parents: Vec<&RedactionSet> = node.prevs.iter().filter_map(|p| memo.get(p)).collect();
		let set = match (parents.as_slice(), &node.redaction_of) {
			// No new information: share the parent's set.
			| ([only], None) => Arc::clone(only),
			| _ => {
				let mut merged: BTreeMap<OwnedEventId, OwnedEventId> = BTreeMap::new();
				for parent in &parents {
					merged.extend(parent.iter().map(|(r, t)| (r.clone(), t.clone())));
				}
				if let Some(target) = &node.redaction_of {
					merged.insert(id.clone(), target.clone());
				}
				Arc::new(merged)
			},
		};
		memo.insert(id, set);
		stack.pop();
	}
	memo.get(root).cloned()
}

/// The overlay digest: `selected` tuples whose event ID is in `redacted`.
#[must_use]
pub(super) fn redaction_overlay_digest(
	selected: &[(String, String, OwnedEventId)],
	redacted: &HashSet<&EventId>,
) -> String {
	let mut overlay = RedactionOverlay::default();
	for (kind, state_key, id) in selected {
		if redacted.contains(&**id) {
			overlay.insert(kind, state_key, id.as_str());
		}
	}
	encode_digest(overlay.digest())
}

/// Redactions in `past(event_id) ∪ {event_id}`, or `None` if the ancestry is not
/// fully available or exceeds the walk limit.
#[implement(super::Service)]
pub async fn msc4500_redactions_through(
	&self,
	event_id: &EventId,
	room_version: &RoomVersionId,
) -> Option<RedactionSet> {
	if let Some(hit) = self.msc4500_memo.lock().through.get(event_id) {
		return Some(Arc::clone(hit));
	}

	let mut nodes: HashMap<OwnedEventId, DagNode> = HashMap::new();
	let mut frontier: Vec<OwnedEventId> = vec![event_id.to_owned()];
	while let Some(id) = frontier.pop() {
		if nodes.contains_key(&id) || self.msc4500_memo.lock().through.contains_key(&id) {
			continue;
		}
		if nodes.len() >= CAUSAL_WALK_LIMIT {
			return None;
		}
		let pdu = self.services.timeline.get_pdu(&id).await.ok()?;
		let redaction_of = if *pdu.kind() == TimelineEventType::RoomRedaction {
			pdu.redacts_id(room_version)
		} else {
			None
		};
		let prevs: Vec<OwnedEventId> = pdu.prev_events().map(ToOwned::to_owned).collect();
		frontier.extend(prevs.iter().cloned());
		nodes.insert(id, DagNode { prevs, redaction_of });
	}

	let mut memo = self.msc4500_memo.lock();
	if memo.through.len() > MEMO_CAPACITY {
		memo.through.clear();
	}
	fold_redaction_sets(event_id, &nodes, &mut memo.through)
}

/// Whether `redaction` was authorized to redact `target` at the state that
/// preceded it (a non-state event's stored root is its resolved parent state).
///
/// This replaces any reading of current room state or of local
/// accepted/soft-failed flags: both would change retroactively. `None` means the
/// state at the redaction cannot be loaded and the caller must fail closed.
#[implement(super::Service)]
async fn msc4500_redaction_effective(
	&self,
	redaction: &Pdu,
	target: &Pdu,
	room_version: &RoomVersionId,
) -> Option<bool> {
	if let Some(hit) = self.msc4500_memo.lock().effective.get(redaction.event_id()) {
		return Some(*hit);
	}

	let room_id = redaction.room_id_or_hash()?;
	let root = self
		.pdu_roothandle_after_event(redaction.event_id())
		.await
		.ok()?;
	let create = self
		.state_get_in_room_hamt(&room_id, &root, &StateEventType::RoomCreate, "")
		.await
		.ok()?;
	let create_content: RoomCreateEventContent = create.get_content().ok()?;
	let features = RoomVersion::new(&create_content.room_version).ok()?;

	let sender = redaction.sender();
	let same_server = target.sender().server_name() == sender.server_name();

	let effective = if matches!(room_version, RoomVersionId::V1 | RoomVersionId::V2) {
		// Rule 11 for v1-v2: PL, or the redacted event ID shares the redaction's domain.
		let same_domain = target.event_id().server_name().is_some()
			&& target.event_id().server_name() == redaction.event_id().server_name();
		same_domain || self.msc4500_redact_power(&room_id, &root, sender).await?
	} else if features.explicitly_privilege_room_creators
		&& (sender == create.sender()
			|| create_content
				.additional_creators
				.as_ref()
				.is_some_and(|c| c.iter().any(|u| u == sender)))
	{
		true
	} else {
		same_server || self.msc4500_redact_power(&room_id, &root, sender).await?
	};

	let mut memo = self.msc4500_memo.lock();
	if memo.effective.len() > MEMO_CAPACITY {
		memo.effective.clear();
	}
	memo.effective
		.insert(redaction.event_id().to_owned(), effective);
	Some(effective)
}

/// Whether `sender`'s power level at `root` reaches the redact level.
#[implement(super::Service)]
async fn msc4500_redact_power(
	&self,
	room_id: &ruma::RoomId,
	root: &rezzy::hamt::RootHandle,
	sender: &ruma::UserId,
) -> Option<bool> {
	match self
		.state_get_content_hamt::<RoomPowerLevelsEventContent>(
			room_id,
			root,
			&StateEventType::RoomPowerLevels,
			"",
		)
		.await
	{
		| Ok(content) => {
			let levels: RoomPowerLevels = content.into();
			Some(levels.user_can_redact_event_of_other(sender))
		},
		// With no power-levels event the room creator holds all power.
		| Err(_) => {
			let create = self
				.state_get_in_room_hamt(room_id, root, &StateEventType::RoomCreate, "")
				.await
				.ok()?;
			Some(create.sender() == sender)
		},
	}
}

/// Digests of the state under `root` as evaluated for `pdu`.
///
/// The redaction digest is derived causally: a selected state event counts as
/// redacted only if an `m.room.redaction` targeting it lies in `past(pdu)`
/// (plus `pdu` itself when `include_self`) and was authorized at the state
/// preceding that redaction. A redaction arriving before its target, becoming
/// authorized later, or being soft-failed locally therefore never changes the
/// answer retroactively.
///
/// Fails closed: an unresolvable state entry, ancestry gap or redaction state
/// returns `None` instead of a digest over partial information.
#[implement(super::Service)]
pub async fn msc4500_point_digests(
	&self,
	root: &rezzy::hamt::RootHandle,
	pdu: &Pdu,
	include_self: bool,
) -> Option<PointDigests> {
	let room_id = pdu.room_id_or_hash()?;
	let room_version = self.services.state.get_room_version(&room_id).await.ok()?;

	let state: Vec<Pdu> = self
		.state_full_pdus_hamt_strict(root.clone())
		.try_collect()
		.await
		.ok()?;

	let mut primary = LtHash::default();
	let mut selected: Vec<(String, String, OwnedEventId)> = Vec::new();
	let mut by_id: HashMap<&EventId, &Pdu> = HashMap::new();
	for state_pdu in &state {
		let Some(state_key) = state_pdu.state_key() else {
			continue;
		};
		let kind = state_pdu.kind().to_string();
		primary.insert(&kind, state_key, state_pdu.event_id().as_str());
		selected.push((kind, state_key.to_owned(), state_pdu.event_id().to_owned()));
		by_id.insert(state_pdu.event_id(), state_pdu);
	}

	let mut past: BTreeMap<OwnedEventId, OwnedEventId> = BTreeMap::new();
	for prev in pdu.prev_events() {
		let set = self.msc4500_redactions_through(prev, &room_version).await?;
		past.extend(set.iter().map(|(r, t)| (r.clone(), t.clone())));
	}
	if include_self && *pdu.kind() == TimelineEventType::RoomRedaction {
		if let Some(target) = pdu.redacts_id(&room_version) {
			past.insert(pdu.event_id().to_owned(), target);
		}
	}

	let mut redacted: HashSet<&EventId> = HashSet::new();
	for (redaction_id, target_id) in &past {
		// Only a target selected at this point can contribute.
		let Some(target) = by_id.get(&**target_id) else {
			continue;
		};
		let redaction = if &**redaction_id == pdu.event_id() {
			pdu.clone()
		} else {
			self.services.timeline.get_pdu(redaction_id).await.ok()?
		};
		if self
			.msc4500_redaction_effective(&redaction, target, &room_version)
			.await?
		{
			redacted.insert(target.event_id());
		}
	}

	Some(PointDigests {
		primary: encode_digest(primary.digest()),
		redactions: redaction_overlay_digest(&selected, &redacted),
	})
}

/// The state immediately before `pdu`, or `None` when it cannot be resolved
/// exactly.
///
/// Unlike [`Self::pdu_roothandle_before_event`], this never falls back to the
/// post-event root: that would present a guessed value as an assertion. A state
/// event with several `prev_events` needs a state-resolution result that is not
/// stored per event, so it is reported as unresolvable.
#[implement(super::Service)]
pub async fn msc4500_before_root(&self, pdu: &Pdu) -> Option<rezzy::hamt::RootHandle> {
	// A non-state event leaves state untouched, so its stored root is the
	// resolved state of all its parents.
	if pdu.state_key().is_none() {
		return self.pdu_roothandle_after_event(pdu.event_id()).await.ok();
	}

	let mut prevs = pdu.prev_events();
	let (Some(prev), None) = (prevs.next(), prevs.next()) else {
		// No predecessors: preceded by the empty state. Several: unresolvable.
		if pdu.prev_events().next().is_some() {
			return None;
		}
		let room_id = pdu.room_id_or_hash()?;
		let structural_key = crate::rooms::state_hamt::room_structural_key(
			&self.services.globals.server_secret,
			&room_id,
		);
		let (root, node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &LtHash::default(), Vec::new())
				.ok()?;
		self.services.state_hamt.store.persist_node_recursive(node);
		return Some(root);
	};

	self.pdu_roothandle_after_event(prev).await.ok()
}

/// The primary and redaction digests before and after `event_id`.
///
/// `None` means the sender cannot assert this PDU and must mark it `limited`.
#[implement(super::Service)]
pub async fn msc4500_pdu_digests(&self, event_id: &EventId) -> Option<PduDigests> {
	let pdu = self.services.timeline.get_pdu(event_id).await.ok()?;
	let after_root = self.pdu_roothandle_after_event(event_id).await.ok()?;
	let before_root = self.msc4500_before_root(&pdu).await?;

	let before = self
		.msc4500_point_digests(&before_root, &pdu, false)
		.await?;
	let after = self.msc4500_point_digests(&after_root, &pdu, true).await?;

	Some(PduDigests { before, after })
}

/// Digest of the labelled resolution-input set `I(P)` for `pdu`.
///
/// `I(P)` is every event in the state at each of `pdu`'s `prev_events` plus
/// everything reachable from those through `auth_events`. This server's room
/// versions define no `prev_state_events`, so the state-predecessor edge is
/// empty and `prev_events` is deliberately not used in its place.
///
/// Returns `None` when a referenced event is unavailable. The caller reports
/// that as no assertion for this component only (JSON `null`); the PDU's other
/// digests are unaffected. No placeholder is ever substituted for a gap.
#[implement(super::Service)]
pub async fn msc4500_resolution_inputs_digest(
	&self,
	pdu: &Pdu,
	cache: &mut InputCache,
) -> Option<String> {
	let mut frontier: VecDeque<OwnedEventId> = VecDeque::new();
	let mut seen: HashSet<OwnedEventId> = HashSet::new();

	for prev in pdu.prev_events() {
		let short: ShortEventId = self.services.short.get_shorteventid(prev).await.ok()?;
		let root = self.services.state.get_roothandle(short).await.ok()?;
		let ids: Vec<OwnedEventId> = self
			.state_full_ids_hamt(&root)
			.map_ok(|(_, id)| id)
			.try_collect()
			.await
			.ok()?;
		for id in ids {
			if seen.insert(id.clone()) {
				frontier.push_back(id);
			}
		}
	}

	let mut inputs = ResolutionInputs::default();
	while let Some(id) = frontier.pop_front() {
		if !cache.contains_key(&id) {
			let node = self
				.services
				.timeline
				.get_pdu(&id)
				.await
				.ok()
				.map(|p| InputNode {
					event_id: id.clone(),
					kind: p.kind().to_string(),
					state_key: p.state_key().unwrap_or_default().to_owned(),
					auth_events: p.auth_events().map(ToOwned::to_owned).collect(),
				});
			cache.insert(id.clone(), node);
		}
		let node = cache.get(&id)?.as_ref()?;

		let auth: Vec<&str> = node.auth_events.iter().map(|e| e.as_str()).collect();
		inputs.insert(&ResolutionInputRecord {
			event_id: node.event_id.as_str(),
			event_type: &node.kind,
			state_key: &node.state_key,
			auth_events: &auth,
			state_predecessors: &[],
		});

		for auth_id in &node.auth_events {
			if seen.insert(auth_id.clone()) {
				frontier.push_back(auth_id.clone());
			}
		}
	}

	Some(encode_digest(inputs.digest()))
}

/// Algorithm identifier for the primary and redaction digests.
pub const ALGORITHM: &str = "lthash16-blake3-v1+redactions-blake3-v1";

/// Algorithm identifier that additionally commits the resolution-input set.
pub const ALGORITHM_WITH_INPUTS: &str =
	"lthash16-blake3-v1+redactions-blake3-v1+resolution-inputs-blake3-v1";

/// The `state_hashes` object of a `/send` transaction.
///
/// One `algorithm` governs every entry; `entries` is keyed by PDU ID.
#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct StateHashes {
	pub algorithm: String,
	pub entries: BTreeMap<OwnedEventId, StateHashEntry>,
}

impl StateHashes {
	/// Whether this server understands the transaction's algorithm. An unknown
	/// algorithm defers validation of the whole transaction.
	#[must_use]
	pub fn is_known_algorithm(&self) -> bool {
		self.algorithm == ALGORITHM || self.algorithm == ALGORITHM_WITH_INPUTS
	}

	/// Whether entries may carry a resolution-input digest.
	#[must_use]
	pub fn has_resolution_inputs(&self) -> bool { self.algorithm == ALGORITHM_WITH_INPUTS }
}

/// One PDU's assertion. See the MSC4500 transaction payload.
///
/// A `limited` entry serializes `before` and `redactions_before` as `null` and
/// omits both `after` fields. `resolution_inputs_before` is omitted under the
/// base algorithm; under the input algorithm it is a string, or `null` when
/// the sender has no assertion for that component.
#[derive(Clone, Debug, Default, Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct StateHashEntry {
	#[serde(default)]
	pub before: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub after: Option<String>,
	#[serde(default)]
	pub redactions_before: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub redactions_after: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resolution_inputs_before: Option<Option<String>>,
	#[serde(default, skip_serializing_if = "is_false")]
	pub limited: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool { !*b }

impl StateHashEntry {
	/// An explicit deferral: the sender cannot resolve this DAG point.
	#[must_use]
	pub fn limited(with_inputs: bool) -> Self {
		Self {
			limited: true,
			resolution_inputs_before: with_inputs.then_some(None),
			..Self::default()
		}
	}

	/// A full assertion from locally derived digests. Under the input
	/// algorithm `inputs` of `None` is sent as `null`.
	#[must_use]
	pub fn asserting(digests: PduDigests, inputs: Option<Option<String>>) -> Self {
		Self {
			before: Some(digests.before.primary),
			after: Some(digests.after.primary),
			redactions_before: Some(digests.before.redactions),
			redactions_after: Some(digests.after.redactions),
			resolution_inputs_before: inputs,
			limited: false,
		}
	}

	/// The four digests every non-limited entry must carry, or `None` when
	/// the entry is limited or malformed. Receivers defer in both cases.
	#[must_use]
	pub fn required_digests(&self) -> Option<(&str, &str, &str, &str)> {
		if self.limited {
			return None;
		}
		Some((
			self.before.as_deref()?,
			self.after.as_deref()?,
			self.redactions_before.as_deref()?,
			self.redactions_after.as_deref()?,
		))
	}

	/// The resolution-input digest, flattening "absent" and `null`: both mean
	/// no assertion for this component.
	#[must_use]
	pub fn resolution_inputs(&self) -> Option<&str> {
		self.resolution_inputs_before.as_ref()?.as_deref()
	}
}

#[cfg(test)]
mod wire_tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn limited_entry_nulls_before_and_omits_after() {
		let base = serde_json::to_value(StateHashEntry::limited(false)).unwrap();
		assert_eq!(base, json!({"before": null, "redactions_before": null, "limited": true}));

		let with_inputs = serde_json::to_value(StateHashEntry::limited(true)).unwrap();
		assert_eq!(
			with_inputs,
			json!({
				"before": null,
				"redactions_before": null,
				"resolution_inputs_before": null,
				"limited": true
			})
		);
	}

	#[test]
	fn full_entry_omits_limited_and_optional_inputs() {
		let point = |p: &str, r: &str| PointDigests { primary: p.into(), redactions: r.into() };
		let entry = StateHashEntry::asserting(
			PduDigests {
				before: point("b", "rb"),
				after: point("a", "ra"),
			},
			None,
		);
		assert_eq!(
			serde_json::to_value(&entry).unwrap(),
			json!({"before": "b", "after": "a", "redactions_before": "rb", "redactions_after": "ra"})
		);
		assert!(entry.required_digests().is_some());
		assert_eq!(entry.resolution_inputs(), None);
	}

	#[test]
	fn inputs_null_absent_and_present_all_parse() {
		let parse = |v: serde_json::Value| serde_json::from_value::<StateHashEntry>(v).unwrap();
		let base =
			json!({"before":"b","after":"a","redactions_before":"rb","redactions_after":"ra"});

		assert_eq!(parse(base.clone()).resolution_inputs(), None);

		let mut with_null = base.clone();
		with_null["resolution_inputs_before"] = json!(null);
		assert_eq!(parse(with_null).resolution_inputs(), None);

		let mut with_value = base;
		with_value["resolution_inputs_before"] = json!("i");
		assert_eq!(parse(with_value).resolution_inputs(), Some("i"));
	}

	#[test]
	fn malformed_or_limited_entries_defer() {
		let parse = |v: serde_json::Value| serde_json::from_value::<StateHashEntry>(v).unwrap();
		// Omitting a redaction digest is malformed, not an empty overlay.
		assert!(
			parse(json!({"before":"b","after":"a","redactions_before":"rb"}))
				.required_digests()
				.is_none()
		);
		assert!(
			parse(json!({"before":null,"redactions_before":null,"limited":true}))
				.required_digests()
				.is_none()
		);
	}

	#[test]
	fn only_known_algorithms_are_validated() {
		let with = |a: &str| StateHashes {
			algorithm: a.into(),
			entries: Default::default(),
		};
		assert!(with(ALGORITHM).is_known_algorithm());
		assert!(with(ALGORITHM_WITH_INPUTS).is_known_algorithm());
		assert!(with(ALGORITHM_WITH_INPUTS).has_resolution_inputs());
		assert!(!with("lthash16-blake3-v1").is_known_algorithm());
	}
}

#[cfg(test)]
mod causal_tests {
	use super::*;

	fn id(s: &str) -> OwnedEventId { OwnedEventId::try_from(format!("${s}")).unwrap() }

	fn node(prevs: &[&str], redaction_of: Option<&str>) -> DagNode {
		DagNode {
			prevs: prevs.iter().map(|p| id(p)).collect(),
			redaction_of: redaction_of.map(id),
		}
	}

	/// ```text
	///        c ── r(redacts T) ── x
	///        └──── y
	/// ```
	/// `x` and `y` resolve the same state, so they share a state root, yet only
	/// `x` has the redaction in its past. A cache keyed by root would give both
	/// the same redaction digest.
	fn concurrent_branches() -> HashMap<OwnedEventId, DagNode> {
		HashMap::from([
			(id("c"), node(&[], None)),
			(id("r"), node(&["c"], Some("T"))),
			(id("x"), node(&["r"], None)),
			(id("y"), node(&["c"], None)),
		])
	}

	#[test]
	fn concurrent_events_with_one_state_root_get_different_redaction_sets() {
		let nodes = concurrent_branches();
		let mut memo = HashMap::new();
		let x = fold_redaction_sets(&id("x"), &nodes, &mut memo).unwrap();
		let y = fold_redaction_sets(&id("y"), &nodes, &mut memo).unwrap();
		assert_eq!(x.get(&id("r")), Some(&id("T")));
		assert!(y.is_empty());

		// The same selected state, evaluated for each branch.
		let selected = vec![("m.room.name".to_owned(), String::new(), id("T"))];
		let digest = |set: &RedactionSet| {
			let targets: HashSet<&EventId> = set.values().map(AsRef::as_ref).collect();
			redaction_overlay_digest(&selected, &targets)
		};
		assert_ne!(digest(&x), digest(&y));
		// An unredacted selection is the empty overlay.
		assert_eq!(digest(&y), "viqN49z0bJTOhc3I4HrDCPTYqVSQ2VbDjXgP1hDbCBM");
	}

	#[test]
	fn merges_union_both_parents_and_share_unchanged_sets() {
		let mut nodes = concurrent_branches();
		nodes.insert(id("m"), node(&["x", "y"], None));
		nodes.insert(id("z"), node(&["y"], None));
		let mut memo = HashMap::new();

		let merged = fold_redaction_sets(&id("m"), &nodes, &mut memo).unwrap();
		assert_eq!(merged.get(&id("r")), Some(&id("T")));

		// A node that adds nothing reuses its sole parent's set.
		let y = fold_redaction_sets(&id("y"), &nodes, &mut memo).unwrap();
		let z = fold_redaction_sets(&id("z"), &nodes, &mut memo).unwrap();
		assert!(Arc::ptr_eq(&y, &z));
	}

	#[test]
	fn missing_ancestry_is_unresolvable_not_empty() {
		let mut nodes = concurrent_branches();
		nodes.remove(&id("c"));
		assert!(fold_redaction_sets(&id("x"), &nodes, &mut HashMap::new()).is_none());
	}

	#[test]
	fn redaction_before_its_target_arrives_still_counts_once_selected() {
		// The set records the redaction regardless of whether the target was
		// known when it arrived; selection at assembly time decides relevance.
		let nodes =
			HashMap::from([(id("r"), node(&[], Some("T"))), (id("x"), node(&["r"], None))]);
		let set = fold_redaction_sets(&id("x"), &nodes, &mut HashMap::new()).unwrap();
		assert_eq!(set.get(&id("r")), Some(&id("T")));
	}
}
