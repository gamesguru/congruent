use std::sync::Arc;

use conduwuit::{Result, err, matrix::StateKey};
use slipstream::{EventId, OwnedEventId, events::StateEventType};

pub mod orphan_scan;
pub mod store;

pub struct Service {
	pub store: store::Store,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			store: store::Store::new(args.mtxdb.clone()),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// A state-HAMT leaf key: the `(event_type, state_key)` strings.
///
/// Leaves hold the canonical strings rather than this server's short IDs, so
/// the encoded nodes, and therefore their structural hashes, are identical on
/// every server holding the same state of the same room.
pub type StateHamtKey = (String, String);

/// A state-HAMT leaf value: the event ID string.
pub type StateHamtValue = String;

/// A node of a room's state HAMT.
pub type StateHamtNode = rezzy::hamt::HamtNode<StateHamtKey, StateHamtValue>;

/// The leaf key for `(event_type, state_key)`.
#[must_use]
pub fn hamt_key(event_type: &StateEventType, state_key: &str) -> StateHamtKey {
	(event_type.to_string(), state_key.to_owned())
}

/// The leaf value for `event_id`.
#[must_use]
pub fn hamt_value(event_id: &EventId) -> StateHamtValue { event_id.as_str().to_owned() }

/// Parses a leaf back into its typed `(event_type, state_key)` and event ID.
pub fn parse_hamt_entry(
	key: &StateHamtKey,
	value: &StateHamtValue,
) -> Result<((StateEventType, StateKey), OwnedEventId)> {
	let event_id = OwnedEventId::try_from(value.as_str())
		.map_err(|e| err!(Database(error!("Invalid event ID in state HAMT leaf: {e}"))))?;
	Ok(((key.0.as_str().into(), key.1.as_str().into()), event_id))
}

/// Computes the structural key for a room's state HAMT.
///
/// It depends only on the room ID, so every server derives the same key and
/// builds identical nodes for identical state. The key is public: it separates
/// rooms' hash domains but does not stop a sender from choosing state keys
/// whose routing hashes share a prefix. That costs about 2^(5d) hashes per key
/// to share `d` levels, and only deepens the paths it targets; a full routing
/// collision (256 bits) is infeasible.
#[must_use]
pub fn room_structural_key(room_id: &slipstream::RoomId) -> [u8; 32] {
	use sha2::{Digest, Sha256};

	let mut hasher = Sha256::new();
	hasher.update(b"congruent state hamt structural key\0");
	hasher.update(room_id.as_bytes());
	hasher.finalize().into()
}
