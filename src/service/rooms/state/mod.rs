use std::{collections::HashMap, fmt::Write, iter::once, mem::size_of, sync::Arc};

use async_trait::async_trait;
use conduwuit::{RoomVersion, debug, matrix::StateKey};
use conduwuit_core::{
	Event, PduEvent, Result, err,
	state_res::{self, StateMap},
	utils::{
		IterStream, MutexMap, MutexMapGuard, ReadyExt,
		stream::{BroadbandExt, TryIgnore},
	},
	warn,
};
use conduwuit_database::{Deserialized, Ignore, Interfix, Map};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::join_all};
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, RoomId, RoomVersionId, UserId,
	events::{
		AnyStrippedStateEvent, StateEventType, TimelineEventType,
		room::create::RoomCreateEventContent,
	},
	serde::Raw,
};

use crate::{
	Dep, rooms,
	rooms::short::{ShortEventId, ShortStateHash, ShortStateKey},
};

pub struct Service {
	pub mutex: RoomMutexMap,
	services: Services,
	db: Data,
}

struct Services {
	short: Dep<rooms::short::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	state_hamt: Dep<rooms::state_hamt::Service>,
	timeline: Dep<rooms::timeline::Service>,
}

struct Data {
	shorteventid_shortstatehash: Arc<Map>,
	roomid_shortstatehash: Arc<Map>,
	roomid_pduleaves: Arc<Map>,
	roomid_roothandle: Arc<Map>,
}

type RoomMutexMap = MutexMap<OwnedRoomId, ()>;
pub type RoomMutexGuard = MutexMapGuard<OwnedRoomId, ()>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			mutex: RoomMutexMap::new(),
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				state_hamt: args.depend::<rooms::state_hamt::Service>("rooms::state_hamt"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
			db: Data {
				shorteventid_shortstatehash: args.db["shorteventid_shortstatehash"].clone(),
				roomid_shortstatehash: args.db["roomid_shortstatehash"].clone(),
				roomid_pduleaves: args.db["roomid_pduleaves"].clone(),
				roomid_roothandle: args.db["roomid_roothandle"].clone(),
			},
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let mutex = self.mutex.len();
		writeln!(out, "state_mutex: {mutex}")?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Set the room to the given statehash and update caches.
	pub async fn force_state(
		&self,
		_room_id: &RoomId,
		_shortstatehash: u64,
		_added: Vec<(ShortStateKey, ShortEventId)>,
		_removed: Vec<(ShortStateKey, ShortEventId)>,
		_state_lock: &RoomMutexGuard,
	) -> Result {
		Err(err!(Request(NotImplemented("TODO: HAMT traversal for force_state"))))
	}

	/// Generates a new StateHash and associates it with the incoming event.
	///
	/// This adds all current state events (not including the incoming event)
	/// to `stateid_pduid` and adds the incoming event to `eventid_statehash`.
	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn set_event_state(
		&self,
		_event_id: &EventId,
		_room_id: &RoomId,
		_state_ids: Vec<(ShortStateKey, ShortEventId)>,
	) -> Result<ShortStateHash> {
		Err(err!(Request(NotImplemented("TODO: Generate HAMT root for set_event_state"))))
	}

	/// Generates a new StateHash and associates it with the incoming event.
	///
	/// This adds all current state events (not including the incoming event)
	/// to `stateid_pduid` and adds the incoming event to `eventid_statehash`.
	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn append_to_state(
		&self,
		new_pdu: &PduEvent,
		room_id: &RoomId,
		state_lock: &RoomMutexGuard,
	) -> Result<u64> {
		let Some(state_key) = new_pdu.state_key() else {
			// Non-state events do not change the room state
			return Ok(0);
		};

		let event_type: StateEventType = new_pdu.kind().to_string().into();
		let new_shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(&event_type, state_key)
			.await;

		// Collect the current state, dropping the slot we are about to replace
		let mut current: HashMap<ShortStateKey, OwnedEventId> =
			if let Ok(sstatehash) = self.get_room_shortstatehash(room_id).await {
				self.services
					.state_accessor
					.state_full_ids(sstatehash)
					.ready_filter_map(|(ssk, eid): (ShortStateKey, OwnedEventId)| {
						(ssk != new_shortstatekey).then_some((ssk, eid))
					})
					.collect()
					.await
			} else {
				HashMap::new()
			};

		// Insert/replace the new event
		current.insert(new_shortstatekey, new_pdu.event_id().to_owned());

		// Unzip in a single pass so ordering is guaranteed consistent
		let (short_state_keys, event_ids): (Vec<ShortStateKey>, Vec<OwnedEventId>) =
			current.into_iter().unzip();

		// Resolve ShortStateKey → (StateEventType, StateKey) for lattice hashing
		let string_keys: Vec<Result<(StateEventType, StateKey)>> = self
			.services
			.short
			.multi_get_statekey_from_short(short_state_keys.iter().copied().stream())
			.collect()
			.await;

		let mut lattice = rezzy::state::LtHash::default();
		let mut entries: Vec<(ShortStateKey, ShortEventId)> =
			Vec::with_capacity(short_state_keys.len());

		for ((ssk, event_id), key_result) in short_state_keys
			.into_iter()
			.zip(event_ids.into_iter())
			.zip(string_keys.into_iter())
		{
			let shorteventid = self
				.services
				.short
				.get_or_create_shorteventid(&event_id)
				.await;
			entries.push((ssk, shorteventid));

			if let Ok((ty, sk)) = key_result {
				lattice.insert(ty.to_string().as_str(), sk.as_str(), event_id.as_str());
			}
		}

		let structural_key = room_id.as_bytes();
		let (root_handle, root_node) =
			rezzy::hamt::build_hamt_root_handle(structural_key, &lattice, entries)
				.map_err(|e| err!(error!("Failed to build HAMT in append_to_state: {e:?}")))?;

		self.services
			.state_hamt
			.store
			.persist_node_recursive(root_node);
		self.set_room_state_hamt(room_id, &root_handle, state_lock);

		Ok(0)
	}

	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn summary_stripped<'a, E>(
		&self,
		event: &'a E,
		room_id: &RoomId,
	) -> Vec<Raw<AnyStrippedStateEvent>>
	where
		E: Event + Send + Sync,
		&'a E: Event + Send,
	{
		let cells = [
			(&StateEventType::RoomCreate, ""),
			(&StateEventType::RoomJoinRules, ""),
			(&StateEventType::RoomCanonicalAlias, ""),
			(&StateEventType::RoomName, ""),
			(&StateEventType::RoomAvatar, ""),
			(&StateEventType::RoomMember, event.sender().as_str()), // Add recommended events
			(&StateEventType::RoomEncryption, ""),
			(&StateEventType::RoomTopic, ""),
		];

		let fetches = cells.into_iter().map(|(event_type, state_key)| {
			self.services
				.state_accessor
				.room_state_get(room_id, event_type, state_key)
		});

		join_all(fetches)
			.await
			.into_iter()
			.filter_map(Result::ok)
			.map(Event::into_format)
			.chain(once(event.to_format()))
			.collect()
	}

	/// Set the state hash to a new version, but does not update state_cache.
	#[tracing::instrument(skip(self, _mutex_lock), level = "debug")]
	pub fn set_room_state(
		&self,
		room_id: &RoomId,
		shortstatehash: u64,
		// Take mutex guard to make sure users get the room state mutex
		_mutex_lock: &RoomMutexGuard,
	) {
		const BUFSIZE: usize = size_of::<u64>();

		self.db
			.roomid_shortstatehash
			.raw_aput::<BUFSIZE, _, _>(room_id, shortstatehash);
	}

	/// Set the state HAMT RootHandle to a new version.
	#[tracing::instrument(skip(self, _mutex_lock), level = "debug")]
	pub fn set_room_state_hamt(
		&self,
		room_id: &RoomId,
		root_handle: &rezzy::hamt::RootHandle,
		// Take mutex guard to make sure users get the room state mutex
		_mutex_lock: &RoomMutexGuard,
	) {
		const BUFSIZE: usize = 48;

		let data = (root_handle.structural_hash, root_handle.state_group_id);
		self.db
			.roomid_roothandle
			.raw_aput::<BUFSIZE, _, _>(room_id, data);
	}

	/// Returns the room's current HAMT RootHandle.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn get_room_state_hamt(&self, room_id: &RoomId) -> Result<rezzy::hamt::RootHandle> {
		let data: (rezzy::hamt::StructuralHash, rezzy::hamt::StateGroupId) = self
			.db
			.roomid_roothandle
			.get(room_id)
			.await
			.deserialized()?;

		Ok(rezzy::hamt::RootHandle {
			structural_hash: data.0,
			state_group_id: data.1,
		})
	}

	/// Returns the room's version.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn get_room_version(&self, room_id: &RoomId) -> Result<RoomVersionId> {
		if let Ok(version) = self.services.short.get_room_version(room_id).await {
			return Ok(version);
		}

		let version = self
			.services
			.state_accessor
			.room_state_get_content(room_id, &StateEventType::RoomCreate, "")
			.await
			.map(|content: RoomCreateEventContent| content.room_version)
			.map_err(|e| err!(Request(NotFound("No create event found: {e:?}"))))?;

		self.services.short.set_room_version(room_id, &version);
		Ok(version)
	}

	pub async fn get_shortstatehash(&self, shorteventid: ShortEventId) -> Result<ShortStateHash> {
		self.db
			.shorteventid_shortstatehash
			.qry(&shorteventid)
			.await
			.deserialized()
	}

	pub async fn get_room_shortstatehash(&self, room_id: &RoomId) -> Result<ShortStateHash> {
		self.db
			.roomid_shortstatehash
			.get(room_id)
			.await
			.deserialized()
	}

	pub fn get_forward_extremities<'a>(
		&'a self,
		room_id: &'a RoomId,
	) -> impl Stream<Item = &'a EventId> + Send + 'a {
		let prefix = (room_id, Interfix);

		self.db
			.roomid_pduleaves
			.keys_prefix(&prefix)
			.map_ok(|(_, event_id): (Ignore, &EventId)| event_id)
			.ignore_err()
	}

	pub async fn set_forward_extremities<'a, I>(
		&'a self,
		room_id: &'a RoomId,
		event_ids: I,
		_state_lock: &'a RoomMutexGuard,
	) where
		I: Iterator<Item = &'a EventId> + Send + 'a,
	{
		let prefix = (room_id, Interfix);
		self.db
			.roomid_pduleaves
			.keys_prefix_raw(&prefix)
			.ignore_err()
			.ready_for_each(|key| self.db.roomid_pduleaves.remove(key))
			.await;

		for event_id in event_ids {
			let key = (room_id, event_id);
			self.db.roomid_pduleaves.put_raw(key, event_id);
		}
	}

	/// This fetches auth events from the current state.
	#[tracing::instrument(skip(self, content, room_version), level = "trace")]
	pub async fn get_auth_events(
		&self,
		room_id: &RoomId,
		kind: &TimelineEventType,
		sender: &UserId,
		state_key: Option<&str>,
		content: &serde_json::value::RawValue,
		room_version: &RoomVersion,
	) -> Result<StateMap<PduEvent>> {
		let Ok(shortstatehash) = self.get_room_shortstatehash(room_id).await else {
			return Ok(HashMap::new());
		};

		let auth_types =
			state_res::auth_types_for_event(kind, sender, state_key, content, room_version)?;
		debug!(?auth_types, "Auth types for event");
		let sauthevents: HashMap<_, _> = auth_types
			.iter()
			.stream()
			.broad_filter_map(|(event_type, state_key)| {
				self.services
					.short
					.get_shortstatekey(event_type, state_key)
					.map_ok(move |ssk| (ssk, (event_type, state_key)))
					.map(Result::ok)
			})
			.collect()
			.await;
		debug!(?sauthevents, "Auth events to fetch");

		let (state_keys, event_ids): (Vec<_>, Vec<_>) = self
			.services
			.state_accessor
			.state_full_shortids(shortstatehash)
			.ready_filter_map(Result::ok)
			.ready_filter_map(|(shortstatekey, shorteventid)| {
				sauthevents
					.get(&shortstatekey)
					.map(|(ty, sk)| ((ty, sk), shorteventid))
			})
			.unzip()
			.await;
		debug!(?state_keys, ?event_ids, "Auth events found in state");
		self.services
			.short
			.multi_get_eventid_from_short(event_ids.into_iter().stream())
			.zip(state_keys.into_iter().stream())
			.ready_filter_map(|(event_id, (ty, sk))| Some(((ty, sk), event_id.ok()?)))
			.broad_filter_map(|((ty, sk), event_id): (_, OwnedEventId)| async move {
				self.services
					.timeline
					.get_pdu(&event_id)
					.await
					.map(move |pdu| (((*ty).clone(), (*sk).clone()), pdu))
					.inspect_err(|e| warn!("Failed to get auth event {event_id}: {e:?}"))
					.ok()
			})
			.collect()
			.map(Ok)
			.await
	}
}
