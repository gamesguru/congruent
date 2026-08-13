use std::{borrow::Borrow, mem::size_of};

use conduwuit::{
	Result, at, err, implement,
	matrix::{Event, StateKey},
	pair_of,
	utils::stream::{BroadbandExt, IterStream, ReadyExt, TryIgnore},
};
use database::Deserialized;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, pin_mut};
use ruma::{
	EventId, OwnedEventId, UserId,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};
use serde::Deserialize;

use crate::rooms::short::{ShortEventId, ShortStateHash, ShortStateKey};

/// The user was a joined member at this state (potentially in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_joined(&self, shortstatehash: ShortStateHash, user_id: &UserId) -> bool {
	self.user_membership(shortstatehash, user_id).await == MembershipState::Join
}

/// The user was an invited or joined room member at this state (potentially
/// in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_invited(&self, shortstatehash: ShortStateHash, user_id: &UserId) -> bool {
	let s = self.user_membership(shortstatehash, user_id).await;
	s == MembershipState::Join || s == MembershipState::Invite
}

/// Get membership for given user in state
#[implement(super::Service)]
pub async fn user_membership(
	&self,
	shortstatehash: ShortStateHash,
	user_id: &UserId,
) -> MembershipState {
	self.state_get_content(shortstatehash, &StateEventType::RoomMember, user_id.as_str())
		.await
		.map_or(MembershipState::Leave, |c: RoomMemberEventContent| c.membership)
}

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
#[implement(super::Service)]
pub async fn state_get_content<T>(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	self.state_get(shortstatehash, event_type, state_key)
		.await
		.and_then(|event| event.get_content())
}

#[implement(super::Service)]
pub async fn state_contains(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<bool> {
	let Ok(shortstatekey) = self
		.services
		.short
		.get_shortstatekey(event_type, state_key)
		.await
	else {
		return Ok(false);
	};

	self.state_contains_shortstatekey(shortstatehash, shortstatekey)
		.await
}

#[implement(super::Service)]
pub async fn state_contains_type(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
) -> bool {
	let state_keys = self.state_keys(shortstatehash, event_type);

	pin_mut!(state_keys);
	state_keys.next().await.is_some()
}

#[implement(super::Service)]
pub async fn state_contains_shortstatekey(
	&self,
	shortstatehash: ShortStateHash,
	shortstatekey: ShortStateKey,
) -> Result<bool> {
	self.load_full_state(shortstatehash)
		.await
		.map(|full_state| full_state.contains_key(&shortstatekey))
}

#[implement(super::Service)]
pub async fn state_contains_shortstatekey_hamt(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	shortstatekey: ShortStateKey,
) -> Result<bool> {
	let structural_key = crate::rooms::state_hamt::room_structural_key(
		&self.services.globals.server_secret,
		room_id,
	);

	let res = tokio::task::block_in_place(|| {
		let root_node = self
			.services
			.state_hamt
			.store
			.get_node_blocking(&root_handle.structural_hash)?;

		let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
		root_node.search(&structural_key, &shortstatekey, &mut resolver)
	})
	.map_err(|e| err!(error!("HAMT lookup failed: {e:?}")))?;

	Ok(res.is_some())
}

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn state_get_id<Id>(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Id>
where
	Id: for<'de> Deserialize<'de> + Sized + ToOwned,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	let shorteventid = self
		.state_get_shortid(shortstatehash, event_type, state_key)
		.await?;

	self.services
		.short
		.get_eventid_from_short(shorteventid)
		.await
}

/// Returns a single EventId from `room_id` with key (`event_type`,
/// `state_key`).
#[implement(super::Service)]
pub async fn state_get_shortid(
	&self,
	shortstatehash: ShortStateHash,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<ShortEventId> {
	let shortstatekey = self
		.services
		.short
		.get_shortstatekey(event_type, state_key)
		.await?;

	self.load_full_state(shortstatehash)
		.await?
		.get(&shortstatekey)
		.copied()
		.ok_or(err!(Request(NotFound("Not found in room state"))))
}

#[implement(super::Service)]
pub async fn state_get_shortid_hamt(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<ShortEventId> {
	let shortstatekey = self
		.services
		.short
		.get_shortstatekey(event_type, state_key)
		.await?;

	let structural_key = crate::rooms::state_hamt::room_structural_key(
		&self.services.globals.server_secret,
		room_id,
	);

	tokio::task::block_in_place(|| {
		let root_node = self
			.services
			.state_hamt
			.store
			.get_node_blocking(&root_handle.structural_hash)?;

		let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
		root_node
			.search(&structural_key, &shortstatekey, &mut resolver)
			.map_err(|e| err!(error!("HAMT lookup failed: {e:?}")))?
			.ok_or_else(|| err!(Request(NotFound("Not found in room state"))))
	})
}

/// Returns a PDU from `room_id` with key `(event_type, state_key)` via HAMT.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
#[allow(unused_variables)]
pub async fn room_state_get_hamt(
	&self,
	room_id: &ruma::RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<std::sync::Arc<conduwuit::PduEvent>> {
	let shortstatekey = self
		.services
		.short
		.get_shortstatekey(event_type, state_key)
		.await?;

	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;

	let shorteventid = tokio::task::block_in_place(|| {
		let root_node = self
			.services
			.state_hamt
			.store
			.get_node_blocking(&root_handle.structural_hash)?;

		let structural_key = crate::rooms::state_hamt::room_structural_key(
			&self.services.globals.server_secret,
			room_id,
		);

		let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
		root_node
			.search(&structural_key, &shortstatekey, &mut resolver)
			.map_err(|e| err!(error!("HAMT lookup failed: {e:?}")))?
			.ok_or_else(|| err!(Request(NotFound("Not found in room state"))))
	})?;

	let event_id = self
		.services
		.short
		.get_eventid_from_short::<OwnedEventId>(shorteventid)
		.await?;
	self.services
		.timeline
		.get_pdu(&event_id)
		.await
		.map(std::sync::Arc::new)
}

/// Returns a Stream of all the full state for a given RootHandle.
#[implement(super::Service)]
#[allow(unused_variables)]
pub fn state_full_ids_hamt<'a>(
	&'a self,
	root_handle: &rezzy::hamt::RootHandle,
) -> futures::stream::BoxStream<'a, Result<(StateEventType, String, OwnedEventId)>> {
	let structural_hash = root_handle.structural_hash;
	let short_states_result =
		tokio::task::block_in_place(|| -> Result<Vec<(ShortStateKey, ShortEventId)>> {
			let root_node = self
				.services
				.state_hamt
				.store
				.get_node_blocking(&structural_hash)?;
			let mut short_states = Vec::new();
			let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
			root_node
				.visit_entries(&mut resolver, &mut |&k, &v| {
					short_states.push((k, v));
					Ok(())
				})
				.map_err(|e| err!(error!("HAMT visit failed: {e:?}")))?;
			Ok(short_states)
		});

	match short_states_result {
		| Ok(short_states) => {
			let stream =
				futures::stream::iter(short_states).then(move |(ssk, seid)| async move {
					let (ty, key) = self.services.short.get_statekey_from_short(ssk).await?;
					let event_id = self
						.services
						.short
						.get_eventid_from_short::<OwnedEventId>(seid)
						.await?;
					Ok((ty, key.to_string(), event_id))
				});
			stream.boxed()
		},
		| Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
	}
}

/// Iterates the state_keys for an event_type in the state; current state
/// event_id included.
#[implement(super::Service)]
pub fn state_keys_with_ids<'a, Id>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = (StateKey, Id)> + Send + 'a
where
	Id: for<'de> Deserialize<'de> + Send + Sized + ToOwned + 'a,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	let state_keys_with_short_ids = self
		.state_keys_with_shortids(shortstatehash, event_type)
		.unzip()
		.map(|(ssks, sids): (Vec<StateKey>, Vec<u64>)| (ssks, sids))
		.shared();

	let state_keys = state_keys_with_short_ids
		.clone()
		.map(at!(0))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	let shorteventids = state_keys_with_short_ids
		.map(at!(1))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	self.services
		.short
		.multi_get_eventid_from_short(shorteventids)
		.zip(state_keys)
		.ready_filter_map(|(eid, sk)| eid.map(move |eid| (sk, eid)).ok())
}

/// Iterates the state_keys for an event_type in the state; current state
/// event_id included.
#[implement(super::Service)]
pub fn state_keys_with_shortids<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = (StateKey, ShortEventId)> + Send + 'a {
	let short_ids = self
		.state_full_shortids(shortstatehash)
		.ignore_err()
		.unzip()
		.map(|(ssks, sids): (Vec<u64>, Vec<u64>)| (ssks, sids))
		.boxed()
		.shared();

	let shortstatekeys = short_ids
		.clone()
		.map(at!(0))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	let shorteventids = short_ids
		.map(at!(1))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	self.services
		.short
		.multi_get_statekey_from_short(shortstatekeys)
		.zip(shorteventids)
		.ready_filter_map(|(res, id)| res.map(|res| (res, id)).ok())
		.ready_filter_map(move |((event_type_, state_key), event_id)| {
			event_type_.eq(event_type).then_some((state_key, event_id))
		})
}

/// Iterates the state_keys for an event_type in the state
#[implement(super::Service)]
pub fn state_keys<'a>(
	&'a self,
	shortstatehash: ShortStateHash,
	event_type: &'a StateEventType,
) -> impl Stream<Item = StateKey> + Send + 'a {
	let short_ids = self
		.state_full_shortids(shortstatehash)
		.ignore_err()
		.map(at!(0));

	self.services
		.short
		.multi_get_statekey_from_short(short_ids)
		.ready_filter_map(Result::ok)
		.ready_filter_map(move |(event_type_, state_key)| {
			event_type_.eq(event_type).then_some(state_key)
		})
}

/// Returns the state events removed between the interval (present in .0 but
/// not in .1)
#[implement(super::Service)]
#[inline]
pub async fn state_removed(
	&self,
	shortstatehash: pair_of!(ShortStateHash),
) -> Result<Vec<(ShortStateKey, ShortEventId)>> {
	self.state_added((shortstatehash.1, shortstatehash.0)).await
}

/// Returns the state events added between the interval (present in .1 but
/// not in .0)
#[implement(super::Service)]
pub async fn state_added(
	&self,
	shortstatehash: pair_of!(ShortStateHash),
) -> Result<Vec<(ShortStateKey, ShortEventId)>> {
	let full_state_a = self.load_full_state(shortstatehash.0).await?;
	let full_state_b = self.load_full_state(shortstatehash.1).await?;

	Ok(full_state_b
		.into_iter()
		.filter(|(k, v)| full_state_a.get(k) != Some(v))
		.collect())
}

#[implement(super::Service)]
#[inline]
pub async fn state_removed_hamt(
	&self,
	root_handles: (&rezzy::hamt::RootHandle, &rezzy::hamt::RootHandle),
) -> Result<Vec<(ShortStateKey, ShortEventId)>> {
	self.state_added_hamt((root_handles.1, root_handles.0))
		.await
}

#[implement(super::Service)]
pub async fn state_added_hamt(
	&self,
	root_handles: (&rezzy::hamt::RootHandle, &rezzy::hamt::RootHandle),
) -> Result<Vec<(ShortStateKey, ShortEventId)>> {
	let full_state_a = self.load_full_state_hamt(root_handles.0).await?;
	let full_state_b = self.load_full_state_hamt(root_handles.1).await?;

	Ok(full_state_b
		.into_iter()
		.filter(|(k, v)| full_state_a.get(k) != Some(v))
		.collect())
}

#[implement(super::Service)]
pub fn state_full(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = ((StateEventType, StateKey), impl Event)> + Send + '_ {
	self.state_full_pdus(shortstatehash)
		.ready_filter_map(|pdu| Some(((pdu.kind().clone().into(), pdu.state_key()?.into()), pdu)))
}

#[implement(super::Service)]
pub fn state_full_pdus(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = impl Event> + Send + '_ {
	let short_ids = self
		.state_full_shortids(shortstatehash)
		.ignore_err()
		.map(at!(1));

	self.services
		.short
		.multi_get_eventid_from_short(short_ids)
		.ready_filter_map(Result::ok)
		.broad_filter_map(move |event_id: OwnedEventId| async move {
			self.services.timeline.get_pdu(&event_id).await.ok()
		})
}

/// Builds a StateMap by iterating over all keys that start
/// with state_hash, this gives the full state for the given state_hash.
#[implement(super::Service)]
pub fn state_full_ids<'a, Id>(
	&'a self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = (ShortStateKey, Id)> + Send + 'a
where
	Id: for<'de> Deserialize<'de> + Send + Sized + ToOwned + 'a,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	let shortids = self
		.state_full_shortids(shortstatehash)
		.ignore_err()
		.unzip()
		.shared();

	let shortstatekeys = shortids
		.clone()
		.map(at!(0))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	let shorteventids = shortids
		.map(at!(1))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	self.services
		.short
		.multi_get_eventid_from_short(shorteventids)
		.zip(shortstatekeys)
		.ready_filter_map(|(event_id, shortstatekey)| Some((shortstatekey, event_id.ok()?)))
}

#[implement(super::Service)]
pub fn state_full_shortids(
	&self,
	shortstatehash: ShortStateHash,
) -> impl Stream<Item = Result<(ShortStateKey, ShortEventId)>> + Send + '_ {
	self.load_full_state(shortstatehash)
		.map_ok(|full_state| full_state.into_iter().collect::<Vec<_>>())
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
		.boxed()
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn state_is_empty(&self, shortstatehash: ShortStateHash) -> Result<bool> {
	self.load_full_state(shortstatehash)
		.await
		.map(|s| s.is_empty())
}

#[implement(super::Service)]
pub fn state_full_shortids_hamt<'a>(
	&'a self,
	root_handle: &'a rezzy::hamt::RootHandle,
) -> impl Stream<Item = Result<(ShortStateKey, ShortEventId)>> + Send + 'a {
	self.load_full_state_hamt(root_handle)
		.map_ok(|full_state| full_state.into_iter().collect::<Vec<_>>())
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
		.boxed()
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn state_is_empty_hamt(&self, root_handle: &rezzy::hamt::RootHandle) -> Result<bool> {
	// A new, completely empty HAMT has a specific structural hash (usually 32 zero
	// bytes or the hash of an empty string, depending on the lattice). But to be
	// perfectly safe and consistent, we'll just check if load_full_state_hamt
	// yields an empty map. Optimizing this to an early exit could be a future
	// step.
	self.load_full_state_hamt(root_handle)
		.await
		.map(|s| s.is_empty())
}

#[implement(super::Service)]
#[tracing::instrument(name = "load", level = "debug", skip_all)]
#[allow(clippy::used_underscore_binding)]
async fn load_full_state(
	&self,
	_shortstatehash: ShortStateHash,
) -> Result<std::collections::HashMap<ShortStateKey, ShortEventId>> {
	Err(err!(Request(NotImplemented("TODO: Traverse HAMT to build full state"))))
}

#[implement(super::Service)]
#[tracing::instrument(name = "load_hamt", level = "debug", skip_all)]
pub async fn load_full_state_hamt(
	&self,
	root_handle: &rezzy::hamt::RootHandle,
) -> Result<std::collections::HashMap<ShortStateKey, ShortEventId>> {
	let structural_hash = root_handle.structural_hash;
	tokio::task::block_in_place(
		|| -> Result<std::collections::HashMap<ShortStateKey, ShortEventId>> {
			let root_node = self
				.services
				.state_hamt
				.store
				.get_node_blocking(&structural_hash)?;
			let mut short_states = std::collections::HashMap::new();
			let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
			root_node
				.visit_entries(&mut resolver, &mut |&k, &v| {
					short_states.insert(k, v);
					Ok(())
				})
				.map_err(|e| err!(error!("HAMT visit failed: {e:?}")))?;
			Ok(short_states)
		},
	)
}

/// Returns the state hash for this pdu.
#[implement(super::Service)]
pub async fn pdu_shortstatehash(&self, event_id: &EventId) -> Result<ShortStateHash> {
	const BUFSIZE: usize = size_of::<ShortEventId>();

	self.services
		.short
		.get_shorteventid(event_id)
		.and_then(|shorteventid| {
			self.db
				.shorteventid_shortstatehash
				.aqry::<BUFSIZE, _>(&shorteventid)
		})
		.await
		.deserialized()
}
