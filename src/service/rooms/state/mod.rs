use std::{collections::HashMap, fmt::Write, iter::once, sync::Arc};

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
use conduwuit_database::{Ignore, Interfix, Map};

/// A (add, rem) pair of `(shortstatekey, shorteventid)` from a HAMT delta.
type HamtDelta = (Vec<(u64, u64)>, Vec<(u64, u64)>);

/// A raw `RootHandle` value persisted in `roomid_roothandle` /
/// `shorteventid_roothandle`: 16-byte structural hash followed by the 32-byte
/// state-group ID, with no per-field serde separators. The database serde
/// format cannot represent `[u8; N]` arrays (nested-tuple separator assert and
/// `deserialize_u8` is unimplemented), so these maps are stored as flat bytes.
pub(crate) fn root_handle_to_bytes(handle: &rezzy::hamt::RootHandle) -> Vec<u8> {
	let mut out = Vec::with_capacity(ROOT_HANDLE_LEN);
	out.extend_from_slice(&handle.structural_hash);
	out.extend_from_slice(&handle.state_group_id);
	out
}

pub(crate) fn root_handle_from_bytes(bytes: &[u8]) -> Result<rezzy::hamt::RootHandle> {
	if bytes.len() != ROOT_HANDLE_LEN {
		return Err(err!(error!(
			"RootHandle value invalid length: expected {ROOT_HANDLE_LEN} bytes, got {}",
			bytes.len()
		)));
	}

	Ok(rezzy::hamt::RootHandle {
		codec_version: rezzy::hamt::HAMT_CODEC_VERSION_V1,
		routing_version: rezzy::hamt::HAMT_ROUTING_VERSION_V1,
		routing_params: [0; 4],
		structural_hash: bytes[0..16]
			.try_into()
			.expect("fixed 16-byte structural hash slice"),
		state_group_id: bytes[16..48]
			.try_into()
			.expect("fixed 32-byte state-group ID slice"),
	})
}

pub(crate) const ROOT_HANDLE_LEN: usize = 16 + 32;

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
	rooms::short::{ShortEventId, ShortStateKey},
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
	globals: Dep<crate::globals::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
}

struct Data {
	roomid_pduleaves: Arc<Map>,
	roomid_roothandle: Arc<Map>,
	shorteventid_roothandle: Arc<Map>,
	state_hamt_root_lattices: Arc<Map>,
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
				globals: args.depend::<crate::globals::Service>("globals"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
			},
			db: Data {
				roomid_pduleaves: args.db["roomid_pduleaves"].clone(),
				roomid_roothandle: args.db["roomid_roothandle"].clone(),
				shorteventid_roothandle: args.db["shorteventid_roothandle"].clone(),
				state_hamt_root_lattices: args.db["state_hamt_root_lattices"].clone(),
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
	/// Set the room to the given state root and update caches.
	pub async fn force_state(
		&self,
		room_id: &RoomId,
		new_root_handle: &rezzy::hamt::RootHandle,
		state_lock: &RoomMutexGuard,
	) -> Result<()> {
		let current_root = self.get_room_state_hamt(room_id).await.ok();

		self.update_caches_for_state_delta_between(
			room_id,
			current_root.as_ref(),
			new_root_handle,
		)
		.await?;

		self.set_room_state_hamt(room_id, new_root_handle, state_lock);

		Ok(())
	}

	/// Computes the HAMT delta between `from_root` (default: empty state) and
	/// `to_root`, resolves the added/removed PDUs, and updates the derived
	/// membership and participation caches (`roomserverids` etc.).
	///
	/// This is the cache-update half of the legacy `force_state`. It must be
	/// run whenever the room state transitions to a new root so that joined
	/// members and their servers are registered for outbound federation
	/// fan-out. The caller is responsible for committing the new root to the
	/// room's current-state pointer (via `set_room_state_hamt` /
	/// `set_event_state_with_root`).
	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn update_caches_for_state_delta_between(
		&self,
		room_id: &RoomId,
		from_root: Option<&rezzy::hamt::RootHandle>,
		to_root: &rezzy::hamt::RootHandle,
	) -> Result<()> {
		let old_node = match from_root {
			| Some(root) => self
				.services
				.state_hamt
				.store
				.get_node(&root.structural_hash)?,
			| None => Arc::new(rezzy::hamt::HamtNode {
				datamap: 0,
				nodemap: 0,
				leaves: vec![],
				children: vec![],
				structural_hash: rezzy::hamt::StructuralHash::default(),
			}),
		};
		let new_node = if to_root.structural_hash == rezzy::hamt::StructuralHash::default() {
			let empty_node = Arc::new(rezzy::hamt::HamtNode {
				datamap: 0,
				nodemap: 0,
				leaves: vec![],
				children: vec![],
				structural_hash: rezzy::hamt::StructuralHash::default(),
			});
			self.services.state_hamt.store.put_node(empty_node.clone());
			empty_node
		} else {
			self.services
				.state_hamt
				.store
				.get_node(&to_root.structural_hash)?
		};

		let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
		let lattice = rezzy::state::LtHash::default();
		let (added, removed): HamtDelta =
			rezzy::hamt::delta::isolate_delta::<u64, u64, _, conduwuit::Error>(
				&old_node,
				&lattice,
				&new_node,
				&lattice,
				&mut resolver,
			)
			.map_err(|e| match e {
				| rezzy::hamt::delta::HamtTraversalError::Resolve(inner) => inner,
				| rezzy::hamt::delta::HamtTraversalError::MaxDepthExceeded { depth } => {
					err!(error!("HAMT diff exceeded max depth at {depth}"))
				},
			})?;

		// resolve PDUs
		let mut added_pdus = Vec::with_capacity(added.len());
		for (_k, event_id) in added {
			let event_id_obj = self
				.services
				.short
				.get_eventid_from_short::<OwnedEventId>(event_id)
				.await?;
			let pdu = self
				.services
				.timeline
				.get_pdu_in_room(Some(room_id), &event_id_obj)
				.await?;
			added_pdus.push(Arc::new(pdu));
		}

		let mut removed_pdus = Vec::with_capacity(removed.len());
		for (_k, event_id) in removed {
			let event_id_obj = self
				.services
				.short
				.get_eventid_from_short::<OwnedEventId>(event_id)
				.await?;
			let pdu = self
				.services
				.timeline
				.get_pdu_in_room(Some(room_id), &event_id_obj)
				.await?;
			removed_pdus.push(Arc::new(pdu));
		}

		self.services
			.state_cache
			.update_caches_for_state_delta(room_id, to_root, removed_pdus, added_pdus)
			.await?;

		Ok(())
	}

	/// Generates a new HAMT RootHandle for the incoming event's state.
	///
	/// Appends the incoming event to the room's current HAMT state (if it is a
	/// state event) and returns the resulting root handle.
	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn set_event_state(
		&self,
		room_id: &RoomId,
		new_pdu: &PduEvent,
		state_lock: &RoomMutexGuard,
	) -> Result<rezzy::hamt::RootHandle> {
		self.set_event_state_with_root(room_id, new_pdu, state_lock, None, None)
			.await
	}

	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn set_event_state_with_root(
		&self,
		room_id: &RoomId,
		new_pdu: &PduEvent,
		state_lock: &RoomMutexGuard,
		state_root_handle: Option<&rezzy::hamt::RootHandle>,
		prev_root_handle: Option<&rezzy::hamt::RootHandle>,
	) -> Result<rezzy::hamt::RootHandle> {
		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(new_pdu.event_id())
			.await;

		let is_state = new_pdu.state_key().is_some();
		let (root_handle, new_node) = if is_state {
			let (handle, node) = self
				.append_to_state(new_pdu, room_id, state_lock, state_root_handle)
				.await?;
			(handle, Some(node))
		} else {
			let root = match state_root_handle {
				| Some(root) => root.clone(),
				| None => self.get_room_state_hamt(room_id).await?,
			};
			(root, None)
		};

		let mut batch = conduwuit_database::Batch::new(&self.db.shorteventid_roothandle);

		if let Some(node) = new_node {
			self.services
				.state_hamt
				.store
				.persist_node_recursive_batch(node, &mut batch);
		}

		let serialized = root_handle_to_bytes(&root_handle);

		// Atomically map the new PDU's shortevent ID to its RootHandle,
		// and for state events, advance the room's current-state pointer.
		batch.insert(&self.db.shorteventid_roothandle, shorteventid.to_be_bytes(), &serialized);
		if is_state {
			batch.insert(&self.db.roomid_roothandle, room_id.as_bytes(), &serialized);
		}

		batch.commit();

		// Update the derived membership/participation caches for the state
		// transition. `state_root_handle` is the *post*-event root, so the delta
		// must be computed against `prev_root_handle` (the state before this
		// event was applied), otherwise the diff is empty and joined members /
		// their servers are never registered for outbound federation fan-out.
		if is_state {
			if let Some(prev_root) = prev_root_handle {
				self.update_caches_for_state_delta_between(
					room_id,
					Some(prev_root),
					&root_handle,
				)
				.await?;
			}
		}

		Ok(root_handle)
	}

	/// Appends a state event to the room's HAMT state and returns the new root.
	///
	/// Builds a new HAMT root handle (and its root node) representing the
	/// room's current state plus the incoming state event. Only state events
	/// may be appended; non-state events are rejected.
	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn append_to_state(
		&self,
		new_pdu: &PduEvent,
		room_id: &RoomId,
		_state_lock: &RoomMutexGuard,
		state_root_handle: Option<&rezzy::hamt::RootHandle>,
	) -> Result<(rezzy::hamt::RootHandle, Arc<rezzy::hamt::HamtNode<u64, u64>>)> {
		let Some(state_key) = new_pdu.state_key() else {
			return Err(err!(Request(InvalidParam("append_to_state called on non-state event"))));
		};

		let event_type: StateEventType = new_pdu.kind().to_string().into();
		let new_shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(&event_type, state_key)
			.await;

		let base = match state_root_handle {
			| Some(root) => Some(root.clone()),
			| None => self.get_room_state_hamt(room_id).await.ok(),
		};
		if let Some(base) = base {
			if let Ok(raw) = self
				.db
				.state_hamt_root_lattices
				.get(&base.structural_hash)
				.await
			{
				if raw.len() == 2048 {
					let mut lattice = rezzy::state::LtHash::default();
					for (v, b) in lattice.0.iter_mut().zip(raw.chunks_exact(2)) {
						*v = u16::from_le_bytes([b[0], b[1]]);
					}
					let old = self
						.services
						.state_hamt
						.store
						.get_node(&base.structural_hash)?;
					let mut resolver = self.services.state_hamt.store.get_blocking_resolver();
					let value = self
						.services
						.short
						.get_or_create_shorteventid(new_pdu.event_id())
						.await;
					let (new_node, displaced, created) = rezzy::hamt::persist_mutation(
						&old,
						&rooms::state_hamt::room_structural_key(
							&self.services.globals.server_secret,
							room_id,
						),
						new_shortstatekey,
						Some(value),
						&mut resolver,
					)
					.map_err(|e| err!(error!("HAMT mutation failed: {e:?}")))?;
					if let Some(old) = displaced {
						let old_id = self
							.services
							.short
							.get_eventid_from_short::<OwnedEventId>(old)
							.await?;
						lattice.replace(
							&event_type.to_string(),
							state_key,
							old_id.as_str(),
							new_pdu.event_id().as_str(),
						);
					} else {
						lattice.insert(
							&event_type.to_string(),
							state_key,
							new_pdu.event_id().as_str(),
						);
					}
					for (hash, bytes) in created {
						self.services
							.state_hamt
							.store
							.put_encoded_node(hash, &bytes);
					}
					let handle = self
						.services
						.state_hamt
						.store
						.root_handle(new_node.structural_hash, &lattice);
					let mut encoded = Vec::with_capacity(2048);
					for v in lattice.0 {
						encoded.extend_from_slice(&v.to_le_bytes());
					}
					self.db
						.state_hamt_root_lattices
						.insert(&handle.structural_hash, &encoded);
					return Ok((handle, new_node));
				}
			}
		}

		let mut current: HashMap<ShortStateKey, OwnedEventId> =
			if let Some(root_handle) = state_root_handle {
				self.load_state_map_from_root_handle(root_handle, new_shortstatekey)
					.await?
			} else {
				match self.get_room_state_hamt(room_id).await {
					| Ok(root_handle) =>
						self.load_state_map_from_root_handle(&root_handle, new_shortstatekey)
							.await?,
					| Err(e) if e.is_not_found() => HashMap::new(),
					| Err(e) => return Err(e),
				}
			};

		current.insert(new_shortstatekey, new_pdu.event_id().to_owned());

		let (short_state_keys, event_ids): (Vec<ShortStateKey>, Vec<OwnedEventId>) =
			current.into_iter().unzip();

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

		let structural_key =
			rooms::state_hamt::room_structural_key(&self.services.globals.server_secret, room_id);
		let (root_handle, root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
				.map_err(|e| err!(error!("Failed to build HAMT in append_to_state: {e:?}")))?;
		let mut encoded_lattice = Vec::with_capacity(2048);
		for value in lattice.0 {
			encoded_lattice.extend_from_slice(&value.to_le_bytes());
		}
		self.db
			.state_hamt_root_lattices
			.insert(&root_handle.structural_hash, &encoded_lattice);

		Ok((root_handle, root_node))
	}

	async fn load_state_map_from_root_handle(
		&self,
		root_handle: &rezzy::hamt::RootHandle,
		skip_shortstatekey: ShortStateKey,
	) -> Result<HashMap<ShortStateKey, OwnedEventId>> {
		let node = self
			.services
			.state_hamt
			.store
			.get_node(&root_handle.structural_hash)?;

		let mut short_events = Vec::new();
		node.visit_entries(
			&mut self.services.state_hamt.store.get_blocking_resolver(),
			&mut |k, v| {
				short_events.push((*k, *v));
				Ok::<(), conduwuit::Error>(())
			},
		)?;

		let mut map = HashMap::new();
		for (sk, se) in short_events {
			if sk != skip_shortstatekey {
				let eid = self
					.services
					.short
					.get_eventid_from_short::<OwnedEventId>(se)
					.await?;
				map.insert(sk, eid);
			}
		}

		Ok(map)
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

	/// Set the state HAMT RootHandle to a new version.
	#[tracing::instrument(skip(self, _mutex_lock), level = "debug")]
	pub fn set_room_state_hamt(
		&self,
		room_id: &RoomId,
		root_handle: &rezzy::hamt::RootHandle,
		// Take mutex guard to make sure users get the room state mutex
		_mutex_lock: &RoomMutexGuard,
	) {
		let data = root_handle_to_bytes(root_handle);
		self.db.roomid_roothandle.insert(room_id.as_bytes(), &data);
	}

	/// Returns the room's current HAMT RootHandle.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn get_room_state_hamt(&self, room_id: &RoomId) -> Result<rezzy::hamt::RootHandle> {
		let data = self.db.roomid_roothandle.get(room_id).await?;
		root_handle_from_bytes(&data)
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

	pub async fn get_roothandle(
		&self,
		shorteventid: ShortEventId,
	) -> Result<rezzy::hamt::RootHandle> {
		let data = self.db.shorteventid_roothandle.qry(&shorteventid).await?;
		root_handle_from_bytes(&data)
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
		let Ok(root_handle) = self.get_room_state_hamt(room_id).await else {
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
			.state_full_shortids_hamt(root_handle)
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

#[cfg(test)]
mod tests;
