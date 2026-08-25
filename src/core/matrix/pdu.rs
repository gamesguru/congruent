mod builder;
mod count;
mod id;
mod raw_id;
mod redact;
#[cfg(test)]
mod tests;
mod topo;
mod unsigned;

use std::cmp::Ordering;

use ruma::{
	CanonicalJsonObject, EventId, MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId,
	OwnedServerName, OwnedUserId, RoomId, UInt, UserId, events::TimelineEventType,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as RawJsonValue;

pub use self::{
	Count as PduCount, Id as PduId, Pdu as PduEvent, RawId as RawPduId,
	builder::{Builder, Builder as PduBuilder},
	count::{Count, TimelineKey},
	id::{ShortId, *},
	raw_id::*,
	topo::TopoToken,
};
use super::{Event, StateKey};
use crate::Result;

/// Persistent Data Unit (Event)
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Pdu {
	pub event_id: OwnedEventId,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub room_id: Option<OwnedRoomId>,

	pub sender: OwnedUserId,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub origin: Option<OwnedServerName>,

	pub origin_server_ts: UInt,

	#[serde(rename = "type")]
	pub kind: TimelineEventType,

	pub content: Box<RawJsonValue>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub state_key: Option<StateKey>,

	pub prev_events: Vec<OwnedEventId>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub prev_state_events: Option<Vec<OwnedEventId>>,

	pub depth: UInt,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub auth_events: Vec<OwnedEventId>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub redacts: Option<OwnedEventId>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub unsigned: Option<Box<RawJsonValue>>,

	pub hashes: EventHash,

	// BTreeMap<Box<ServerName>, BTreeMap<ServerSigningKeyId, String>>
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub signatures: Option<Box<RawJsonValue>>,

	/// Whether this event has been rejected (by auth check, soft-fail, or
	/// admin action). Populated at fetch time from pdu_metadata DB;
	/// not persisted in the event JSON itself.
	#[serde(skip)]
	pub rejected: bool,
}

/// Content hashes of a PDU.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventHash {
	/// The SHA-256 hash.
	pub sha256: String,
}

impl Pdu {
	pub fn from_id_val(
		event_id: &EventId,
		mut json: CanonicalJsonObject,
		room_id: Option<&RoomId>,
	) -> Result<Self> {
		json.insert(
			"event_id".into(),
			ruma::CanonicalJsonValue::String(event_id.as_str().to_owned()),
		);
		let mut pdu: Self = serde_json::from_value(serde_json::to_value(json)?)?;
		pdu.event_id = event_id.to_owned();

		if pdu.kind.to_string().chars().count() > 255 {
			return Err(crate::err!(Request(InvalidParam("Event type is too long"))));
		}

		if let Some(state_key) = &pdu.state_key {
			if state_key.chars().count() > 255 {
				return Err(crate::err!(Request(InvalidParam("State key is too long"))));
			}
		}

		if pdu.room_id.is_none() {
			if pdu.kind == TimelineEventType::RoomCreate {
				// V12+: room_id is omitted from the signed content. Derive it
				// deterministically from the event_id hash ($ -> !) to prevent
				// a malicious server from spoofing creator privileges.
				let constructed_hash = event_id.as_str().replacen('$', "!", 1);
				let constructed_room_id = RoomId::parse(&constructed_hash).map_err(|_| {
					crate::err!(Request(InvalidParam(
						"Invalid event_id for room hash derivation"
					)))
				})?;
				pdu.room_id = Some(constructed_room_id.into());
			} else if let Some(room_id) = room_id {
				pdu.room_id = Some(room_id.to_owned());
			} else {
				return Err(crate::err!(Request(InvalidParam("Event is missing room_id"))));
			}
		}

		// Validate the PDU belongs to the expected room if one is specified
		if let Some(expected_room) = room_id {
			if pdu.room_id_or_hash().as_deref() != Some(expected_room) {
				return Err(crate::err!(Request(InvalidParam(
					"PDU {event_id} does not belong to room {expected_room}"
				))));
			}
		}

		Ok(pdu)
	}
}

/// Maps Pdu fields to rezzy's [`RawEvent`](rezzy::RawEvent) trait,
/// enabling `rezzy::ParsedEvent::new(&pdu)` for zero-copy auth checks
/// and state resolution without intermediate `LeanEvent` conversion.
impl rezzy::RawEvent for Pdu {
	type Id = OwnedEventId;

	/// `Pdu::event_id` → `$event_id:server.name`
	#[inline]
	fn raw_event_id(&self) -> &OwnedEventId { &self.event_id }

	/// `Pdu::kind` (`TimelineEventType` enum) → `"m.room.member"` etc.
	#[inline]
	fn raw_event_type(&self) -> std::borrow::Cow<'_, str> {
		std::borrow::Cow::Owned(self.kind.to_string())
	}

	/// `Pdu::sender` (`OwnedUserId`) → `"@user:server"`
	#[inline]
	fn raw_sender(&self) -> &str { self.sender.as_str() }

	/// `Pdu::state_key` (`Option<StateKey>`) → `Option<&str>`
	#[inline]
	fn raw_state_key(&self) -> Option<&str> { self.state_key.as_deref() }

	/// `Pdu::content` (`Box<RawJsonValue>`) → raw JSON string
	#[inline]
	fn raw_content_json(&self) -> &str { self.content.get() }

	/// `Pdu::prev_events` → DAG parent references
	#[inline]
	fn raw_prev_events(&self) -> &[OwnedEventId] { &self.prev_events }

	/// `Pdu::auth_events` → auth chain references
	#[inline]
	fn raw_auth_events(&self) -> &[OwnedEventId] { &self.auth_events }

	/// `Pdu::depth` (`UInt`) → `u64`
	#[inline]
	fn raw_depth(&self) -> u64 { self.depth.into() }

	/// `Pdu::origin_server_ts` (`UInt`) → milliseconds since epoch
	#[inline]
	fn raw_origin_server_ts(&self) -> u64 { self.origin_server_ts.into() }

	#[inline]
	fn raw_rejected(&self) -> bool { self.rejected }

	#[inline]
	fn raw_soft_fail(&self) -> bool { false }
}

/// Direct [`DagNode`](rezzy::DagNode) implementation on `Pdu`, enabling
/// rezzy's traversal and topological functions (`find_backward_extremities`,
/// `compute_topo_positions`, `reverse_topological_order`, etc.) to operate
/// on `HashMap<OwnedEventId, Pdu>` with zero conversion overhead.
///
/// For auth checking and content-aware operations, use
/// `rezzy::ParsedEvent::new(&pdu)` which provides the full `EventLike` trait
/// via the [`RawEvent`](rezzy::RawEvent) impl above.
impl rezzy::DagNode for Pdu {
	type Id = OwnedEventId;

	#[inline]
	fn event_id(&self) -> &OwnedEventId { &self.event_id }

	#[inline]
	fn depth(&self) -> u64 { self.depth.into() }

	#[inline]
	fn prev_events(&self) -> &[OwnedEventId] { &self.prev_events }

	#[inline]
	fn auth_events(&self) -> &[OwnedEventId] { &self.auth_events }
}

macro_rules! impl_event_delegates {
	() => {
		#[inline]
		fn auth_events(
			&self,
		) -> impl DoubleEndedIterator<Item = &EventId>
		+ ExactSizeIterator
		+ Clone
		+ Send
		+ std::fmt::Debug
		+ '_ {
			self.as_pdu().auth_events.iter().map(AsRef::as_ref)
		}

		#[inline]
		fn content(&self) -> &RawJsonValue { &self.as_pdu().content }

		#[inline]
		fn event_id(&self) -> &EventId { &self.as_pdu().event_id }

		#[inline]
		fn origin_server_ts(&self) -> MilliSecondsSinceUnixEpoch {
			MilliSecondsSinceUnixEpoch(self.as_pdu().origin_server_ts)
		}

		#[inline]
		fn depth(&self) -> UInt { self.as_pdu().depth }

		#[inline]
		fn prev_events(&self) -> impl DoubleEndedIterator<Item = &EventId> + Clone + Send + '_ {
			self.as_pdu().prev_events.iter().map(AsRef::as_ref)
		}

		#[inline]
		fn prev_state_events(
			&self,
		) -> Option<impl DoubleEndedIterator<Item = &EventId> + Clone + Send + '_> {
			self.as_pdu()
				.prev_state_events
				.as_ref()
				.map(|v| v.iter().map(AsRef::as_ref))
		}

		#[inline]
		fn redacts(&self) -> Option<&EventId> { self.as_pdu().redacts.as_deref() }

		#[inline]
		fn room_id(&self) -> Option<&RoomId> { self.as_pdu().room_id.as_deref() }

		#[inline]
		fn room_id_or_hash(&self) -> Option<OwnedRoomId> {
			if let Some(room_id) = &self.as_pdu().room_id {
				return Some(room_id.clone());
			}
			if *self.as_pdu().event_type() == TimelineEventType::RoomCreate {
				let constructed_hash = self.as_pdu().event_id.as_str().replace('$', "!");
				return RoomId::parse(&constructed_hash).ok().map(ToOwned::to_owned);
			}
			None
		}

		#[inline]
		fn sender(&self) -> &UserId { &self.as_pdu().sender }

		#[inline]
		fn state_key(&self) -> Option<&str> { self.as_pdu().state_key.as_deref() }

		#[inline]
		fn kind(&self) -> &TimelineEventType { &self.as_pdu().kind }

		#[inline]
		fn unsigned(&self) -> Option<&RawJsonValue> { self.as_pdu().unsigned.as_deref() }

		#[inline]
		fn rejected(&self) -> bool { self.as_pdu().rejected }
	};
}

impl Event for Pdu {
	impl_event_delegates!();

	#[inline]
	fn as_mut_pdu(&mut self) -> &mut Pdu { self }

	#[inline]
	fn as_pdu(&self) -> &Pdu { self }

	#[inline]
	fn into_pdu(self) -> Pdu { self }

	#[inline]
	fn is_owned(&self) -> bool { true }
}

impl Event for std::sync::Arc<Pdu> {
	impl_event_delegates!();

	#[inline]
	fn as_mut_pdu(&mut self) -> &mut Pdu { Self::make_mut(self) }

	#[inline]
	fn as_pdu(&self) -> &Pdu { self }

	#[inline]
	fn into_pdu(self) -> Pdu { Self::try_unwrap(self).unwrap_or_else(|arc| (*arc).clone()) }

	#[inline]
	fn is_owned(&self) -> bool { true }
}

impl Event for &std::sync::Arc<Pdu> {
	impl_event_delegates!();

	#[inline]
	fn as_mut_pdu(&mut self) -> &mut Pdu { panic!("Cannot mutate shared reference") }

	#[inline]
	fn as_pdu(&self) -> &Pdu { self }

	#[inline]
	fn into_pdu(self) -> Pdu { (**self).clone() }

	#[inline]
	fn is_owned(&self) -> bool { false }
}

impl Event for &Pdu {
	impl_event_delegates!();

	#[inline]
	fn as_mut_pdu(&mut self) -> &mut Pdu { panic!("Cannot mutate shared reference") }

	#[inline]
	fn as_pdu(&self) -> &Pdu { self }

	#[inline]
	fn into_pdu(self) -> Pdu { (*self).clone() }

	#[inline]
	fn is_owned(&self) -> bool { false }
}

/// Prevent derived equality which wouldn't limit itself to event_id
impl Eq for Pdu {}

/// Equality determined by the Pdu's ID, not the memory representations.
impl PartialEq for Pdu {
	fn eq(&self, other: &Self) -> bool { self.event_id == other.event_id }
}

/// Ordering determined by the Pdu's ID, not the memory representations.
impl Ord for Pdu {
	fn cmp(&self, other: &Self) -> Ordering { self.event_id.cmp(&other.event_id) }
}

/// Ordering determined by the Pdu's ID, not the memory representations.
impl PartialOrd for Pdu {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
