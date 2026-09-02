use std::{
	borrow::Borrow,
	collections::{HashMap, HashSet},
};

use conduwuit::{
	Error, Result, err, implement,
	state_res::{self, StateMap},
	trace,
	utils::stream::{IterStream, ReadyExt, TryWidebandExt, WidebandExt},
};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, future::try_join};
use ruma::{OwnedEventId, RoomId, RoomVersionId};

#[implement(super::Service)]
#[tracing::instrument(name = "resolve", level = "debug", skip_all)]
pub async fn resolve_state(
	&self,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
	incomingstate: HashMap<u64, OwnedEventId>,
) -> Result<rezzy::hamt::RootHandle> {
	trace!("Loading current room state ids");
	let current_root_handle = self
		.services
		.state
		.get_room_state_hamt(room_id)
		.map_err(|e| err!(Database(error!("No state for {room_id:?}: {e:?}"))))
		.await?;

	let currentstate_ids: HashMap<_, _> = self
		.services
		.state_accessor
		.state_full_ids_hamt(&current_root_handle)
		.try_collect()
		.await?;

	trace!("Loading fork states");
	let forkstates = [currentstate_ids, incomingstate];
	let auth_chain_sets = forkstates
		.iter()
		.try_stream()
		.wide_and_then(|state| {
			self.services
				.auth_chain
				.event_ids_iter(room_id, state.values().map(Borrow::borrow))
				.try_collect()
		})
		.try_collect::<Vec<HashSet<OwnedEventId>>>();

	let forkstates = forkstates
		.iter()
		.stream()
		.wide_then(|forkstate| {
			let shortstatekeys = forkstate.keys().copied().stream();
			let event_ids = forkstate.values().cloned().stream();
			self.services
				.short
				.multi_get_statekey_from_short(shortstatekeys)
				.zip(event_ids)
				.ready_filter_map(|(ty_sk, id): (Result<_>, _)| Some((ty_sk.ok()?, id)))
				.collect()
		})
		.map(Ok::<_, Error>)
		.try_collect::<Vec<StateMap<OwnedEventId>>>();

	let (forkstates, auth_chain_sets): (Vec<StateMap<OwnedEventId>>, Vec<HashSet<OwnedEventId>>) =
		try_join(forkstates, auth_chain_sets).await?;

	trace!("Resolving state");
	let state: StateMap<OwnedEventId> = self
		.state_resolution(room_id, room_version_id, forkstates.iter(), &auth_chain_sets)
		.boxed()
		.await?;

	trace!("State resolution done.");

	let mut lattice = rezzy::state::LtHash::default();
	let mut entries = Vec::with_capacity(state.len());

	for ((ty, sk), id) in &state {
		lattice.insert(ty.to_string().as_str(), sk.as_str(), id.as_str());

		let shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(ty, sk)
			.await;
		let shorteventid = self.services.short.get_or_create_shorteventid(id).await;
		entries.push((shortstatekey, shorteventid));
	}

	let structural_key = crate::rooms::state_hamt::room_structural_key(
		&self.services.globals.server_secret,
		room_id,
	);
	let (root_handle, root_node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
			.map_err(|e| err!(error!("Failed to build HAMT root: {e:?}")))?;

	self.services.globals.with_cork_and_flush(|| {
		self.services
			.state_hamt
			.store
			.persist_node_recursive(root_node);
	});

	Ok(root_handle)
}

#[implement(super::Service)]
#[tracing::instrument(name = "ruma", level = "debug", skip_all)]
pub async fn state_resolution<'a, StateSets>(
	&'a self,
	room_id: &RoomId,
	room_version: &'a RoomVersionId,
	state_sets: StateSets,
	auth_chain_sets: &'a [HashSet<OwnedEventId>],
) -> Result<StateMap<OwnedEventId>>
where
	StateSets: Iterator<Item = &'a StateMap<OwnedEventId>> + Clone + Send,
{
	let event_fetch = |event_id| self.event_fetch(Some(room_id), event_id);
	state_res::resolve(room_version, state_sets, auth_chain_sets, &event_fetch)
		.map_err(|e| err!(error!("State resolution failed: {e:?}")))
		.await
}
