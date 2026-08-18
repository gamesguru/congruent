use std::borrow::Borrow;

use conduwuit::{
	Pdu, Result, at, err, implement,
	matrix::{Event, StateKey},
	utils::stream::{BroadbandExt, IterStream, ReadyExt, TryIgnore},
};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, pin_mut};
use ruma::{
	EventId, OwnedEventId, UserId,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};
use serde::Deserialize;

use crate::rooms::short::{ShortEventId, ShortStateKey};

/// The user was a joined member at this state (potentially in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_joined_hamt(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	user_id: &UserId,
) -> bool {
	self.user_membership_hamt(room_id, root_handle, user_id)
		.await == MembershipState::Join
}

/// The user was an invited or joined room member at this state (potentially
/// in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_invited_hamt(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	user_id: &UserId,
) -> bool {
	let s = self
		.user_membership_hamt(room_id, root_handle, user_id)
		.await;
	s == MembershipState::Join || s == MembershipState::Invite
}

/// Get membership for given user in state at a HAMT root.
#[implement(super::Service)]
pub async fn user_membership_hamt(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	user_id: &UserId,
) -> MembershipState {
	self.state_get_content_hamt(
		room_id,
		root_handle,
		&StateEventType::RoomMember,
		user_id.as_str(),
	)
	.await
	.map_or(MembershipState::Leave, |c: RoomMemberEventContent| c.membership)
}

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`)
/// at a HAMT root.
#[implement(super::Service)]
pub async fn state_get_content_hamt<T>(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	self.state_get_in_room_hamt(room_id, root_handle, event_type, state_key)
		.await
		.and_then(|event| event.get_content())
}

#[implement(super::Service)]
pub async fn state_contains_type_hamt(
	&self,
	_room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
) -> bool {
	// Short-circuit iterate this root's state keys and test whether any entry
	// has the requested type. Mirrors the legacy `state_contains_type` (which
	// iterated `state_keys`); here we stream the root's short IDs, resolve each
	// to its `(event_type, state_key)`, and stop at the first match.
	let shortstatekeys = self
		.state_full_shortids_hamt(root_handle.clone())
		.ignore_err()
		.map(at!(0));

	let stream = self
		.services
		.short
		.multi_get_statekey_from_short(shortstatekeys)
		.ready_filter_map(Result::ok);

	pin_mut!(stream);

	while let Some((event_type_, _)) = stream.next().await {
		if event_type_.eq(event_type) {
			return true;
		}
	}

	false
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

	let root_node = self
		.services
		.state_hamt
		.store
		.get_node(&root_handle.structural_hash)?;
	let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
	let res = root_node
		.search(&structural_key, &shortstatekey, &mut resolver)
		.map_err(|e| err!(error!("HAMT lookup failed: {e:?}")))?;

	Ok(res.is_some())
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

	let root_node = self
		.services
		.state_hamt
		.store
		.get_node(&root_handle.structural_hash)?;
	let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
	root_node
		.search(&structural_key, &shortstatekey, &mut resolver)
		.map_err(|e| err!(error!("HAMT lookup failed: {e:?}")))?
		.ok_or_else(|| err!(Request(NotFound("Not found in room state"))))
}

#[implement(super::Service)]
pub async fn state_get_in_room_hamt(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Pdu> {
	let shorteventid = self
		.state_get_shortid_hamt(room_id, root_handle, event_type, state_key)
		.await?;
	let event_id: OwnedEventId = self
		.services
		.short
		.get_eventid_from_short(shorteventid)
		.await?;
	self.services
		.timeline
		.get_pdu_in_room(Some(room_id), &event_id)
		.await
}

/// Returns a single EventId at the given HAMT root with key
/// (`event_type`, `state_key`).
#[implement(super::Service)]
pub async fn state_get_id_hamt<Id>(
	&self,
	room_id: &ruma::RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Id>
where
	Id: serde::de::DeserializeOwned + Sized + ToOwned,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	let shorteventid = self
		.state_get_shortid_hamt(room_id, root_handle, event_type, state_key)
		.await?;

	self.services
		.short
		.get_eventid_from_short(shorteventid)
		.await
}

/// Returns a PDU from `room_id` with key `(event_type, state_key)` via HAMT.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
#[allow(unused_variables)]
pub async fn room_state_get_hamt_legacy(
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

	let shorteventid = self
		.state_get_shortid_hamt(room_id, &root_handle, event_type, state_key)
		.await?;

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

/// Returns a Stream of all `(shortstatekey, event_id)` for a given RootHandle.
#[implement(super::Service)]
#[allow(unused_variables)]
pub fn state_full_ids_hamt<'a>(
	&'a self,
	root_handle: &'a rezzy::hamt::RootHandle,
) -> futures::stream::BoxStream<'a, (ShortStateKey, OwnedEventId)> {
	let structural_hash = root_handle.structural_hash;
	let short_states_result = (|| -> Result<Vec<(ShortStateKey, ShortEventId)>> {
		let root_node = self.services.state_hamt.store.get_node(&structural_hash)?;
		let mut short_states = Vec::new();
		let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
		root_node
			.visit_entries(&mut resolver, &mut |&k, &v| {
				short_states.push((k, v));
				Ok(())
			})
			.map_err(|e| err!(error!("HAMT visit failed: {e:?}")))?;
		Ok(short_states)
	})();

	match short_states_result {
		| Ok(short_states) => {
			let stream =
				futures::stream::iter(short_states).filter_map(move |(ssk, seid)| async move {
					let event_id = self
						.services
						.short
						.get_eventid_from_short::<OwnedEventId>(seid)
						.await
						.ok()?;
					Some((ssk, event_id))
				});
			stream.boxed()
		},
		| Err(_) => futures::stream::empty().boxed(),
	}
}

/// Returns a Stream of all the full state PDUs for a given RootHandle.
#[implement(super::Service)]
pub fn state_full_pdus_hamt(
	&self,
	root_handle: rezzy::hamt::RootHandle,
) -> impl Stream<Item = impl Event> + Send + '_ {
	let short_ids = self
		.state_full_shortids_hamt(root_handle)
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

/// Returns a Stream of all the full state (type, key, event) for a given
/// RootHandle.
#[implement(super::Service)]
pub fn state_full_hamt(
	&self,
	root_handle: rezzy::hamt::RootHandle,
) -> impl Stream<Item = ((StateEventType, StateKey), impl Event)> + Send + '_ {
	self.state_full_pdus_hamt(root_handle)
		.ready_filter_map(|pdu| Some(((pdu.kind().clone().into(), pdu.state_key()?.into()), pdu)))
}

/// Iterates the state_keys for an event_type at a HAMT root; current state
/// event_id included.
#[implement(super::Service)]
pub fn state_keys_with_ids_hamt<'a, Id>(
	&'a self,
	root_handle: rezzy::hamt::RootHandle,
	event_type: &'a StateEventType,
) -> impl Stream<Item = (StateKey, Id)> + Send + 'a
where
	Id: for<'de> Deserialize<'de> + Send + Sized + ToOwned + 'a,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	// Resolve this root's short IDs, filter to the requested event_type, and
	// map each surviving short event id to its full event id. Order is
	// preserved between the filtered state keys and the event ids.
	let short_ids = self
		.state_full_shortids_hamt(root_handle)
		.ignore_err()
		.unzip()
		.map(|(ssks, sids): (Vec<ShortStateKey>, Vec<ShortEventId>)| (ssks, sids))
		.shared();

	let shortstatekeys = short_ids
		.clone()
		.map(at!(0))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	let shorteventids = short_ids
		.clone()
		.map(at!(1))
		.map(Vec::into_iter)
		.map(IterStream::stream)
		.flatten_stream();

	let state_keys = self
		.services
		.short
		.multi_get_statekey_from_short(shortstatekeys)
		.ready_filter_map(Result::ok)
		.ready_filter_map(move |(event_type_, state_key)| {
			event_type_.eq(event_type).then_some(state_key)
		});

	self.services
		.short
		.multi_get_eventid_from_short(shorteventids)
		.zip(state_keys)
		.ready_filter_map(|(eid, sk)| eid.map(move |eid| (sk, eid)).ok())
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
pub fn state_full_shortids_hamt(
	&self,
	root_handle: rezzy::hamt::RootHandle,
) -> impl Stream<Item = Result<(ShortStateKey, ShortEventId)>> + Send + '_ {
	let load = async move { self.load_full_state_hamt(&root_handle).await };
	load.map_ok(|full_state| full_state.into_iter().collect::<Vec<_>>())
		.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
		.boxed()
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn state_is_empty_hamt(&self, root_handle: &rezzy::hamt::RootHandle) -> Result<bool> {
	let root_node = self
		.services
		.state_hamt
		.store
		.get_node(&root_handle.structural_hash)?;

	// O(1) structural check on the root's datamap/nodemap bitmaps, rather than
	// materializing the full tree to test `.is_empty()`.
	Ok(root_node.is_empty())
}

#[implement(super::Service)]
#[tracing::instrument(name = "load_hamt", level = "debug", skip_all)]
pub async fn load_full_state_hamt(
	&self,
	root_handle: &rezzy::hamt::RootHandle,
) -> Result<std::collections::HashMap<ShortStateKey, ShortEventId>> {
	let structural_hash = root_handle.structural_hash;
	let root_node = self.services.state_hamt.store.get_node(&structural_hash)?;
	let mut short_states = std::collections::HashMap::new();
	let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
	root_node
		.visit_entries(&mut resolver, &mut |&k, &v| {
			short_states.insert(k, v);
			Ok(())
		})
		.map_err(|e| err!(error!("HAMT visit failed: {e:?}")))?;
	Ok(short_states)
}

/// Returns the HAMT RootHandle for this pdu.
#[implement(super::Service)]
pub async fn pdu_roothandle(&self, event_id: &EventId) -> Result<rezzy::hamt::RootHandle> {
	let shorteventid = self.services.short.get_shorteventid(event_id).await?;
	self.services.state.get_roothandle(shorteventid).await
}
