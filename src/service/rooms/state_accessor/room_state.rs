use conduwuit::{
	Pdu, Result, err, implement,
	matrix::{Event, StateKey},
};
use futures::{Stream, StreamExt, TryFutureExt};
use slipstream::{OwnedEventId, RoomId, codec::Deserialize, events::StateEventType};

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
#[implement(super::Service)]
pub async fn room_state_get_content<T>(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<T>
where
	T: Deserialize,
{
	self.room_state_get(room_id, event_type, state_key)
		.await
		.and_then(|event| event.get_content())
}

/// Returns the full room state.
#[implement(super::Service)]
pub fn room_state_full<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = Result<((StateEventType, StateKey), impl Event)>> + Send + 'a {
	self.services
		.state
		.get_room_state_hamt(room_id)
		.map_ok(|root_handle| self.state_full_hamt(room_id, root_handle).map(Ok).boxed())
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
}

/// Returns the full room state pdus
#[implement(super::Service)]
pub fn room_state_full_pdus<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = Result<impl Event>> + Send + 'a {
	self.services
		.state
		.get_room_state_hamt(room_id)
		.map_ok(|root_handle| {
			self.state_full_pdus_hamt(room_id, root_handle)
				.map(Ok)
				.boxed()
		})
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
}

/// Strict variant of [`Self::room_state_full_pdus`]: missing PDUs and HAMT
/// traversal errors are emitted as `Err` items instead of being dropped.
#[implement(super::Service)]
pub fn room_state_full_pdus_strict<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = Result<Pdu>> + Send + 'a {
	self.services
		.state
		.get_room_state_hamt(room_id)
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.map_ok(move |root_handle| {
			self.state_full_pdus_hamt_strict(room_id, root_handle)
				.boxed()
		})
		.try_flatten_stream()
}

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn room_state_get_id(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<OwnedEventId> {
	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;
	self.state_get_event_id_hamt(room_id, &root_handle, event_type, state_key)
		.await
}

/// Returns a single PDU from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn room_state_get(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Pdu> {
	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;
	self.state_get_in_room_hamt(room_id, &root_handle, event_type, state_key)
		.await
}

/// Returns a single PDU from `room_id` at the given HAMT root with key
/// (`event_type`, `state_key`).
#[implement(super::Service)]
pub async fn room_state_get_hamt_at_root(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Pdu> {
	self.state_get_in_room_hamt(room_id, root_handle, event_type, state_key)
		.await
}

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`)
/// via the current HAMT root.
#[implement(super::Service)]
pub async fn room_state_get_hamt(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Pdu> {
	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;
	self.room_state_get_hamt_at_root(room_id, &root_handle, event_type, state_key)
		.await
}

/// Returns all state keys for the given `room_id` and `event_type`.
#[implement(super::Service)]
pub async fn room_state_keys(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
) -> Result<Vec<String>> {
	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;

	let state_keys = self
		.load_full_state_hamt(room_id, &root_handle)?
		.into_keys()
		.filter(|(ty, _)| ty == event_type)
		.map(|(_, key)| key.to_string())
		.collect();

	Ok(state_keys)
}
