//! Adapter layer bridging continuwuity's `PduEvent` types to rezzy's
//! `LeanEvent` and `StateProvider` interfaces.
//!
//! This enables using `rezzy::auth::check_auth` as a drop-in replacement
//! for ruma's `state_res::event_auth::auth_check` throughout the codebase.
//!
//! `Pdu` implements `rezzy::RawEvent` directly (in `pdu.rs`), so
//! `ParsedEvent::new(&pdu)` gives you `DagNode + EventLike` with zero
//! boilerplate.

use std::collections::HashMap;

use conduwuit_core::matrix::{
	Event, PduEvent,
	state_key::StateKey,
	state_res::{RoomVersion, StateResolutionVersion},
};
use rezzy::{LeanEvent, StateResVersion, auth::StateProvider};
use ruma::{RoomVersionId, events::StateEventType};

/// Map a ruma `RoomVersionId` to rezzy's `StateResVersion`.
///
/// Delegates to `RoomVersion::new()` which already has the canonical
/// version→state_res mapping, then converts to rezzy's enum.
///
/// # Panics
///
/// Panics on unrecognized room versions (via `RoomVersion::new()`).
#[must_use]
pub fn to_state_res_version(room_version_id: &RoomVersionId) -> StateResVersion {
	let rv = RoomVersion::new(room_version_id).expect("unsupported room version");

	match rv.state_res {
		| StateResolutionVersion::V1 => StateResVersion::V1,
		| StateResolutionVersion::V2 => StateResVersion::V2,
		| StateResolutionVersion::V2_1 => StateResVersion::V2_1,
		| StateResolutionVersion::V2_1_1 => StateResVersion::V2_1_1,
		| StateResolutionVersion::V2_2 | _ => StateResVersion::V2_2,
	}
}

/// Convert any `Event` into a `LeanEvent<String>` suitable for rezzy's
/// auth checking and state resolution APIs.
#[must_use]
pub fn pdu_to_lean<E: Event>(pdu: &E) -> LeanEvent<String> {
	LeanEvent {
		event_id: pdu.event_id().to_string(),
		event_type: pdu.kind().to_string(),
		sender: pdu.sender().to_string(),
		state_key: pdu.state_key().map(str::to_owned),
		content: pdu.get_content_as_value(),
		prev_events: pdu.prev_events().map(|id| format!("{id}")).collect(),
		auth_events: pdu.auth_events().map(|id| format!("{id}")).collect(),
		origin_server_ts: pdu.origin_server_ts().get().into(),
		depth: pdu.depth().into(),
		..Default::default()
	}
}

/// A `StateProvider` backed by a pre-built HashMap of auth events,
/// keyed by `(event_type, state_key)`.
///
/// This is the primary adapter for all auth check callsites that already
/// have their auth events collected (handle_outlier_pdu, upgrade_outlier_pdu,
/// create.rs).
pub struct PduStateProvider {
	/// Auth events converted to LeanEvent, keyed by (type, state_key).
	events: HashMap<(String, Option<String>), LeanEvent<String>>,
}

impl PduStateProvider {
	/// Build a state provider from a HashMap keyed by `(StateEventType,
	/// StateKey)`.
	///
	/// This is the format used by ruma's auth_check callsites.
	#[must_use]
	pub fn from_ruma_map(auth_events: &HashMap<(StateEventType, StateKey), PduEvent>) -> Self {
		let events = auth_events
			.iter()
			.map(|((ty, sk), pdu)| {
				let key = (ty.to_string(), Some(sk.to_string()));
				(key, pdu_to_lean(pdu))
			})
			.collect();

		Self { events }
	}

	/// Build a state provider from a HashMap keyed by
	/// `(StateEventType, SmallString)` (the format used in create.rs).
	#[must_use]
	pub fn from_smallstr_map(
		auth_events: &HashMap<
			(StateEventType, conduwuit_core::smallstr::SmallString<[u8; 48]>),
			PduEvent,
		>,
	) -> Self {
		let events = auth_events
			.iter()
			.map(|((ty, sk), pdu)| {
				let key = (ty.to_string(), Some(sk.to_string()));
				(key, pdu_to_lean(pdu))
			})
			.collect();

		Self { events }
	}

	/// Add the `m.room.create` event explicitly to the state provider.
	///
	/// This is necessary for Room Versions 11/12 (v12+), where `m.room.create`
	/// is omitted from the event's `auth_events` list but is still required by
	/// state resolution and auth checks.
	#[must_use]
	pub fn with_create_event<E: Event>(mut self, create_event: Option<&E>) -> Self {
		if let Some(pdu) = create_event {
			let key = (StateEventType::RoomCreate.to_string(), Some(String::new()));
			self.events.insert(key, pdu_to_lean(pdu));
		}
		self
	}

	/// Build a state provider from the current room state by fetching key
	/// auth events from the database.
	///
	/// Fetches `m.room.create`, `m.room.power_levels`, and `m.room.join_rules`
	/// from the room's current state snapshot. This is useful for query-style
	/// checks (e.g. `rezzy::auth::user::user_can_invite`) where a pre-built
	/// auth events map is not available.
	pub async fn from_room_state(
		room_id: &ruma::RoomId,
		state_accessor: &crate::rooms::state_accessor::Service,
	) -> Self {
		let mut events = HashMap::new();

		// Fetch power levels (most important for PL queries)
		if let Ok(pdu) = state_accessor
			.room_state_get(room_id, &StateEventType::RoomPowerLevels, "")
			.await
		{
			let key = (StateEventType::RoomPowerLevels.to_string(), Some(String::new()));
			events.insert(key, pdu_to_lean(&pdu));
		}

		// Fetch create event (needed for V12+ implicit creator PL)
		if let Ok(pdu) = state_accessor
			.room_state_get(room_id, &StateEventType::RoomCreate, "")
			.await
		{
			let key = (StateEventType::RoomCreate.to_string(), Some(String::new()));
			events.insert(key, pdu_to_lean(&pdu));
		}

		// Fetch join rules (needed for restricted join checks)
		if let Ok(pdu) = state_accessor
			.room_state_get(room_id, &StateEventType::RoomJoinRules, "")
			.await
		{
			let key = (StateEventType::RoomJoinRules.to_string(), Some(String::new()));
			events.insert(key, pdu_to_lean(&pdu));
		}

		Self { events }
	}
}

impl StateProvider<String> for PduStateProvider {
	fn get_event(&self, event_type: &str, state_key: &str) -> Option<&LeanEvent<String>> {
		let key_owned = (event_type.to_owned(), Some(state_key.to_owned()));
		self.events.get(&key_owned)
	}
}

/// Run rezzy's auth check on a PduEvent, converting the result to a
/// continuwuity-compatible `Result<bool>`.
///
/// Returns `Ok(true)` if the event passes auth, `Ok(false)` if it fails.
/// This matches ruma's `auth_check` return signature for drop-in
/// compatibility.
pub fn rezzy_auth_check<S: StateProvider<String>>(
	pdu: &PduEvent,
	state: &S,
	version: StateResVersion,
) -> bool {
	let lean = pdu_to_lean(pdu);
	match rezzy::auth::check_auth(&lean, state, version, None) {
		| Ok(()) => true,
		| Err(e) => {
			tracing::error!("rezzy auth check failed: {e}");
			false
		},
	}
}

/// Convenience wrapper bundling a [`PduStateProvider`] with the resolved
/// [`StateResVersion`] for a room.  Eliminates boilerplate at call sites
/// that need both.
pub struct RoomStateProvider {
	pub provider: PduStateProvider,
	pub version: StateResVersion,
}

impl RoomStateProvider {
	/// Build from the current room state.
	///
	/// # Errors
	///
	/// Returns an error if the room version cannot be determined (missing
	/// create event, DB corruption, or nonexistent room).
	pub async fn new(
		room_id: &ruma::RoomId,
		state_accessor: &crate::rooms::state_accessor::Service,
	) -> conduwuit_core::Result<Self> {
		let provider = PduStateProvider::from_room_state(room_id, state_accessor).await;

		let version = state_accessor.get_state_res_version(room_id).await?;

		Ok(Self { provider, version })
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use rezzy::{LeanEvent, StateResVersion, auth::StateProvider};

	use super::*;

	/// Build a minimal LeanEvent for testing.
	fn make_lean(event_type: &str, state_key: Option<&str>, sender: &str) -> LeanEvent<String> {
		LeanEvent {
			event_id: format!("$test_{event_type}:example.org"),
			event_type: event_type.to_owned(),
			sender: sender.to_owned(),
			state_key: state_key.map(str::to_owned),
			content: serde_json::json!({}),
			prev_events: vec![],
			auth_events: vec![],
			origin_server_ts: 1_000_000,
			depth: 1,
			..Default::default()
		}
	}

	// -----------------------------------------------------------------
	// Regression: PduStateProvider lookups must not panic on missing keys
	// -----------------------------------------------------------------
	// The `check_current_state_auth` bug (fixed in the `.unwrap_or(false)`
	// change) was caused by error propagation turning auth lookup failures
	// into hard rejections. This test verifies the StateProvider impl
	// gracefully returns None for missing state keys instead of panicking.
	#[test]
	fn state_provider_returns_none_for_missing_keys() {
		let provider = PduStateProvider { events: HashMap::new() };

		assert!(
			provider.get_event("m.room.create", "").is_none(),
			"empty state provider must return None, not panic"
		);
		assert!(
			provider.get_event("m.room.power_levels", "").is_none(),
			"missing power levels must return None"
		);
		assert!(
			provider
				.get_event("m.room.member", "@alice:example.org")
				.is_none(),
			"missing member must return None"
		);
	}

	// -----------------------------------------------------------------
	// Regression: rezzy_auth_check must return false (not panic) on
	// completely empty state — matching the old ruma .unwrap_or(false)
	// -----------------------------------------------------------------
	// When check_current_state_auth encounters an error building the
	// state provider (e.g. room version lookup fails during federation
	// join setup), the contract is: return false → soft-fail. The auth
	// check itself must also handle degenerate inputs gracefully.
	#[test]
	fn auth_check_with_empty_state_returns_false() {
		let provider = PduStateProvider { events: HashMap::new() };

		// A non-create event with no auth state should fail auth, not panic
		let lean = make_lean("m.room.message", None, "@alice:example.org");
		let _pdu_json = serde_json::to_string(&serde_json::json!({
			"event_id": lean.event_id,
			"type": lean.event_type,
			"sender": lean.sender,
			"room_id": "!test:example.org",
			"origin_server_ts": lean.origin_server_ts,
			"depth": lean.depth,
			"prev_events": lean.prev_events,
			"auth_events": lean.auth_events,
			"content": lean.content,
			"hashes": {"sha256": "test"},
			"signatures": {},
		}))
		.unwrap();

		// We can't easily construct a PduEvent here without the full
		// deserialization pipeline, but we CAN verify the StateProvider +
		// rezzy contract directly:
		let result = rezzy::auth::check_auth(&lean, &provider, StateResVersion::V2, None);

		// Must be Err (auth fails), not panic
		assert!(result.is_err(), "auth check with no state must fail gracefully, got Ok");
	}

	// -----------------------------------------------------------------
	// Verify state_key mapping: Some("") must NOT become None
	// -----------------------------------------------------------------
	// The rezzy migration had a bug where empty state_keys were mapped
	// to None instead of Some(""). This test ensures the PduStateProvider
	// always wraps state_keys as Some().
	#[test]
	fn state_provider_maps_empty_state_key_as_some() {
		let mut events = HashMap::new();
		let lean = make_lean("m.room.create", Some(""), "@alice:example.org");
		events.insert(("m.room.create".to_owned(), Some(String::new())), lean);

		let provider = PduStateProvider { events };

		// Lookup with empty string must find the event
		assert!(
			provider.get_event("m.room.create", "").is_some(),
			"empty state_key must be stored as Some(\"\"), not None"
		);
	}
}
