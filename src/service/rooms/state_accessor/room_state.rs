use std::borrow::Borrow;

use conduwuit::{
	Pdu, Result, err, implement,
	matrix::{Event, StateKey},
	utils::stream::ReadyExt,
};
use futures::{Stream, StreamExt, TryFutureExt};
use ruma::{EventId, RoomId, events::StateEventType};
use serde::Deserialize;

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
#[implement(super::Service)]
pub async fn room_state_get_content<T>(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	self.room_state_get(room_id, event_type, state_key)
		.await
		.and_then(|event| event.get_content())
}

/// Returns the full room state.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_state_full<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = Result<((StateEventType, StateKey), impl Event)>> + Send + 'a {
	self.services
		.state
		.get_room_state_hamt(room_id)
		.map_ok(|root_handle| self.state_full_hamt(root_handle).map(Ok).boxed())
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
}

/// Returns the full room state pdus
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_state_full_pdus<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = Result<impl Event>> + Send + 'a {
	self.services
		.state
		.get_room_state_hamt(room_id)
		.map_ok(|root_handle| self.state_full_pdus_hamt(root_handle).map(Ok).boxed())
		.map_err(move |e| err!(Database("Missing state for {room_id:?}: {e:?}")))
		.try_flatten_stream()
}

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn room_state_get_id<Id>(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Id>
where
	Id: for<'de> Deserialize<'de> + Sized + ToOwned,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;
	let shorteventid = self
		.state_get_shortid_hamt(room_id, &root_handle, event_type, state_key)
		.await?;
	self.services
		.short
		.get_eventid_from_short(shorteventid)
		.await
}

/// Returns a single PDU from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
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
#[tracing::instrument(skip(self), level = "debug")]
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
#[tracing::instrument(skip(self), level = "debug")]
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
#[tracing::instrument(skip(self), level = "debug")]
pub async fn room_state_keys(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
) -> Result<Vec<String>> {
	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;

	let full_state = self.load_full_state_hamt(&root_handle).await?;

	// Batch-resolve the short state keys through the caching key store rather
	// than awaiting a database lookup for each entry serially.
	let shortstatekeys = futures::stream::iter(full_state.into_keys());
	let state_keys = self
		.services
		.short
		.multi_get_statekey_from_short(shortstatekeys)
		.ready_filter_map(Result::ok)
		.ready_filter_map(move |(ty, key)| (ty == *event_type).then_some(key.to_string()))
		.collect()
		.await;

	Ok(state_keys)
}
