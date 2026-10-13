use std::{collections::HashMap, fmt::Write, iter::once, sync::Arc};

use async_trait::async_trait;
use conduwuit::{RoomVersion, debug, error, matrix::StateKey};
use conduwuit_core::{
	Event, PduEvent, Result, err, info,
	state_res::StateMap,
	utils::{
		IterStream, MutexMap, MutexMapGuard, ReadyExt,
		stream::{BroadbandExt, TryIgnore},
	},
	warn,
};
use conduwuit_database::{Batch, Ignore, Interfix, Map, serialize_to_vec};

/// A (add, rem) pair of `(shortstatekey, shorteventid)` from a HAMT delta.
type HamtDelta = (Vec<(u64, u64)>, Vec<(u64, u64)>);

const CODEC_VERSION_LEN: usize = 1;
const ROUTING_VERSION_LEN: usize = 1;
const ROUTING_PARAMS_LEN: usize = 4;
const STRUCTURAL_HASH_LEN: usize = size_of::<rezzy::hamt::StructuralHash>();
const STATE_GROUP_ID_LEN: usize = size_of::<rezzy::hamt::StateGroupId>();

const CODEC_VERSION_AT: usize = 0;
const ROUTING_VERSION_AT: usize = CODEC_VERSION_AT + CODEC_VERSION_LEN;
const ROUTING_PARAMS_AT: usize = ROUTING_VERSION_AT + ROUTING_VERSION_LEN;
const STRUCTURAL_HASH_AT: usize = ROUTING_PARAMS_AT + ROUTING_PARAMS_LEN;
const STATE_GROUP_ID_AT: usize = STRUCTURAL_HASH_AT + STRUCTURAL_HASH_LEN;

/// Byte length of the persisted [`rezzy::hamt::RootHandle`] encoding.
pub(crate) const ROOT_HANDLE_LEN: usize = STATE_GROUP_ID_AT + STATE_GROUP_ID_LEN;

/// Copies the fixed-width array `N` starting at `offset`.
///
/// The caller has already checked `bytes.len() == ROOT_HANDLE_LEN`, so every
/// offset above is in range and this cannot fail.
fn fixed<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
	let end = offset
		.checked_add(N)
		.expect("fixed-width RootHandle field offset overflow");
	bytes[offset..end]
		.try_into()
		.expect("length-checked fixed-width RootHandle field")
}

/// Serializes a `RootHandle` for `roomid_roothandle` / `shorteventid_roothandle`.
///
/// These maps hold flat bytes because the database serde format cannot represent
/// `[u8; N]` arrays (nested-tuple separator assert and `deserialize_u8` is
/// unimplemented).
///
/// Every field of the handle is written, including the codec and routing
/// versions. Persisting only the two hashes left the decoder filling those in
/// with the *running* build's values, so a handle read back under a different
/// codec would claim a version it was never written with.
pub(crate) fn root_handle_to_bytes(handle: &rezzy::hamt::RootHandle) -> Vec<u8> {
	let mut out = Vec::with_capacity(ROOT_HANDLE_LEN);
	out.push(handle.codec_version);
	out.push(handle.routing_version);
	out.extend_from_slice(&handle.routing_params);
	out.extend_from_slice(&handle.structural_hash);
	out.extend_from_slice(&handle.state_group_id);
	out
}

/// Returns a compact diagnostic fingerprint for a HAMT root.
#[must_use]
pub fn root_handle_fingerprint(handle: &rezzy::hamt::RootHandle) -> u64 {
	u64::from_be_bytes(
		handle.structural_hash[..8]
			.try_into()
			.expect("structural hash is at least 8 bytes"),
	)
}

/// Parses a [`rezzy::hamt::RootHandle`] written by [`root_handle_to_bytes`].
pub(crate) fn root_handle_from_bytes(bytes: &[u8]) -> Result<rezzy::hamt::RootHandle> {
	if bytes.len() != ROOT_HANDLE_LEN {
		return Err(err!(error!(
			"RootHandle value invalid length: expected {ROOT_HANDLE_LEN} bytes, got {}",
			bytes.len()
		)));
	}

	Ok(rezzy::hamt::RootHandle {
		codec_version: bytes[CODEC_VERSION_AT],
		routing_version: bytes[ROUTING_VERSION_AT],
		routing_params: fixed(bytes, ROUTING_PARAMS_AT),
		structural_hash: fixed(bytes, STRUCTURAL_HASH_AT),
		state_group_id: fixed(bytes, STATE_GROUP_ID_AT),
	})
}

/// True when [`Service::set_event_state_with_root`] advances the room's
/// current-state pointer (`roomid_roothandle`) inside its atomic write batch.
///
/// Callers must not follow that call with a standalone
/// [`Service::set_room_state_hamt`] for the same PDU when this holds: the batch
/// has already committed the byte-identical pointer, so the second write is pure
/// amplification outside the batch the timeline write is committed with.
#[must_use]
pub(crate) fn is_state_event(pdu: &PduEvent) -> bool { pdu.state_key().is_some() }

use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::join_all};
use slipstream::{
	EventId, OwnedEventId, OwnedRoomId, RoomId, RoomVersionId, UserId,
	events::{
		AnyStrippedStateEvent, StateEventType, TimelineEventType,
		room::create::RoomCreateEventContent,
	},
	sswire::Raw,
};

use crate::{
	Dep, globals, rooms,
	rooms::short::{ShortEventId, ShortStateKey},
};

pub struct Service {
	pub mutex: RoomMutexMap,
	services: Services,
	db: Data,
}

struct Services {
	globals: Dep<globals::Service>,
	short: Dep<rooms::short::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	state_hamt: Dep<rooms::state_hamt::Service>,
	timeline: Dep<rooms::timeline::Service>,
	pdu_metadata: Dep<rooms::pdu_metadata::Service>,
	outlier: Dep<rooms::outlier::Service>,
}

struct Data {
	roomid_pduleaves: Arc<Map>,
	roomid_roothandle: Arc<Map>,
	shorteventid_roothandle: Arc<Map>,
	state_hamt_root_lattices: Arc<Map>,
	/// Bounded cache of encoded root lattices, keyed by structural hash.
	/// Lattices are immutable and content-addressed, so entries never go
	/// stale.
	lattice_cache: moka::sync::Cache<[u8; 32], Arc<[u8]>>,
}

/// Encoded length of a persisted `LtHash` lattice.
const LATTICE_LEN: usize = 2048;

type RoomMutexMap = MutexMap<OwnedRoomId, ()>;
pub type RoomMutexGuard = MutexMapGuard<OwnedRoomId, ()>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			mutex: RoomMutexMap::new(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				state_hamt: args.depend::<rooms::state_hamt::Service>("rooms::state_hamt"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				pdu_metadata: args.depend::<rooms::pdu_metadata::Service>("rooms::pdu_metadata"),
				outlier: args.depend::<rooms::outlier::Service>("rooms::outlier"),
			},
			db: Data {
				roomid_pduleaves: args.db["roomid_pduleaves"].clone(),
				roomid_roothandle: args.db["roomid_roothandle"].clone(),
				shorteventid_roothandle: args.db["shorteventid_roothandle"].clone(),
				state_hamt_root_lattices: args.db["state_hamt_root_lattices"].clone(),
				lattice_cache: moka::sync::Cache::builder()
					.max_capacity(u64::from(args.server.config.lthash_cache_capacity))
					.build(),
			},
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let mutex = self.mutex.len();
		writeln!(out, "state_mutex: {mutex}")?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(module_path!()) }
}

impl Service {
	/// Set the room to the given state root and update caches.
	pub async fn force_state(
		&self,
		room_id: &RoomId,
		new_root_handle: &rezzy::hamt::RootHandle,
		state_lock: &RoomMutexGuard,
	) -> Result<()> {
		let current_root = match self.get_room_state_hamt(room_id).await {
			| Ok(root) => Some(root),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => return Err(error),
		};

		Box::pin(self.update_caches_for_state_delta_between(
			room_id,
			current_root.as_ref(),
			new_root_handle,
		))
		.await?;

		self.set_room_state_hamt(room_id, new_root_handle, state_lock)
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
	pub async fn update_caches_for_state_delta_between(
		&self,
		room_id: &RoomId,
		from_root: Option<&rezzy::hamt::RootHandle>,
		to_root: &rezzy::hamt::RootHandle,
	) -> Result<()> {
		let structural_key =
			rooms::state_hamt::room_structural_key(&self.services.globals.server_secret, room_id);
		let store = &self.services.state_hamt.store;
		let old_node = match from_root {
			| Some(root) =>
				store.get_node(room_id.as_bytes(), &structural_key, &root.structural_hash)?,
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
			self.services
				.state_hamt
				.store
				.put_node(room_id.as_bytes(), empty_node.clone())?;
			empty_node
		} else {
			store.get_node(room_id.as_bytes(), &structural_key, &to_root.structural_hash)?
		};

		let mut resolver = store.get_room_resolver(room_id.as_bytes(), &structural_key);
		let lattice = rezzy::incremental::LtHash::default();
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
		let mut added_pdus = Vec::new();
		for (shortstatekey, event_id) in added {
			if let Ok((event_type, _)) = self
				.services
				.short
				.get_statekey_from_short(shortstatekey)
				.await && !matches!(
				event_type,
				StateEventType::RoomMember
					| StateEventType::RoomEncryption
					| StateEventType::SpaceChild
			) {
				continue;
			}
			let Ok(event_id_obj) = self
				.services
				.short
				.get_eventid_from_short::<OwnedEventId>(event_id)
				.await
			else {
				continue;
			};
			let Ok(pdu) = self
				.services
				.timeline
				.get_pdu_in_room(Some(room_id), &event_id_obj)
				.await
			else {
				continue;
			};
			added_pdus.push(Arc::new(pdu));
		}

		let mut removed_pdus = Vec::new();
		for (shortstatekey, event_id) in removed {
			if let Ok((event_type, _)) = self
				.services
				.short
				.get_statekey_from_short(shortstatekey)
				.await && !matches!(
				event_type,
				StateEventType::RoomMember
					| StateEventType::RoomEncryption
					| StateEventType::SpaceChild
			) {
				continue;
			}
			let Ok(event_id_obj) = self
				.services
				.short
				.get_eventid_from_short::<OwnedEventId>(event_id)
				.await
			else {
				continue;
			};
			let Ok(pdu) = self
				.services
				.timeline
				.get_pdu_in_room(Some(room_id), &event_id_obj)
				.await
			else {
				continue;
			};
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
	pub async fn set_event_state(
		&self,
		room_id: &RoomId,
		new_pdu: &PduEvent,
		state_lock: &RoomMutexGuard,
	) -> Result<rezzy::hamt::RootHandle> {
		let previous_root = match self.get_room_state_hamt(room_id).await {
			| Ok(root) => Some(root),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => return Err(error),
		};
		Box::pin(self.set_event_state_with_root(
			room_id,
			new_pdu,
			state_lock,
			None,
			previous_root.as_ref(),
			&[],
		))
		.await
	}

	pub async fn set_event_state_with_root(
		&self,
		room_id: &RoomId,
		new_pdu: &PduEvent,
		state_lock: &RoomMutexGuard,
		state_root_handle: Option<&rezzy::hamt::RootHandle>,
		prev_root_handle: Option<&rezzy::hamt::RootHandle>,
		pre_persisted_nodes: &[rooms::state_hamt::store::PendingNode],
	) -> Result<rezzy::hamt::RootHandle> {
		let root_handle = self
			.persist_event_state_with_root(
				room_id,
				new_pdu,
				state_lock,
				state_root_handle,
				pre_persisted_nodes,
			)
			.await?;

		self.update_event_state_caches(room_id, new_pdu, prev_root_handle, &root_handle)
			.await?;

		Ok(root_handle)
	}

	/// Persists the event's state association (its HAMT root and, for state
	/// events, the room's current-state pointer) without touching the derived
	/// membership caches. These writes are keyed by event id, so they may run
	/// before the event's timeline row is visible. A caller that does so must
	/// call `update_event_state_caches` once the row is visible, because that
	/// update allocates stream counts (e.g. a left marker) which sync compares
	/// against the event's own count.
	pub async fn persist_event_state_with_root(
		&self,
		room_id: &RoomId,
		new_pdu: &PduEvent,
		state_lock: &RoomMutexGuard,
		state_root_handle: Option<&rezzy::hamt::RootHandle>,
		pre_persisted_nodes: &[rooms::state_hamt::store::PendingNode],
	) -> Result<rezzy::hamt::RootHandle> {
		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(new_pdu.event_id())
			.await;

		let is_state = is_state_event(new_pdu);
		// A supplied `state_root_handle` is the *post*-event root: callers have
		// already applied the event (and any state resolution) and persisted its
		// nodes. Appending the event again would clobber a resolution winner, so
		// only append when no post-event root was provided.
		let (root_handle, mut deferred_nodes) = if is_state && state_root_handle.is_none() {
			let (handle, _node, nodes) = self
				.append_to_state(new_pdu, room_id, state_lock, None)
				.await?;
			(handle, nodes)
		} else if let Some(root) = state_root_handle {
			(root.clone(), Vec::new())
		} else {
			let root = self.get_room_state_hamt(room_id).await?;
			(root, Vec::new())
		};

		deferred_nodes.extend_from_slice(pre_persisted_nodes);

		let serialized = root_handle_to_bytes(&root_handle);

		// One mtxdb transaction for the new nodes and both roots, committed
		// before the RocksDB index batch below, so a failure leaves at worst
		// unreferenced nodes and never an index entry pointing at nothing.
		self.services.state_hamt.store.publish(
			room_id.as_bytes(),
			new_pdu.event_id().as_bytes(),
			&deferred_nodes,
			&serialized,
			is_state,
		)?;

		let mut batch = Batch::new();

		// Atomically map the new PDU's shortevent ID to its RootHandle,
		// and for state events, advance the room's current-state pointer.
		self.db.shorteventid_roothandle.batch_put(
			&mut batch,
			&shorteventid.to_be_bytes(),
			serialized.as_slice(),
		);
		if is_state {
			self.db.roomid_roothandle.batch_put(
				&mut batch,
				room_id.as_bytes(),
				serialized.as_slice(),
			);
		}

		self.db.shorteventid_roothandle.apply_batch(batch);

		Ok(root_handle)
	}

	/// Updates the derived membership/participation caches for the state
	/// transition made by `new_pdu`. `root_handle` is the *post*-event root, so
	/// the delta must be computed against `prev_root_handle` (the state before
	/// this event was applied), otherwise the diff is empty and joined members /
	/// their servers are never registered for outbound federation fan-out.
	///
	/// This must run after the event's timeline row is visible: it allocates
	/// stream counts for membership markers (e.g. `mark_as_left`), and sync only
	/// delivers a leave event if its marker count is greater than the event's
	/// count.
	pub async fn update_event_state_caches(
		&self,
		room_id: &RoomId,
		new_pdu: &PduEvent,
		prev_root_handle: Option<&rezzy::hamt::RootHandle>,
		root_handle: &rezzy::hamt::RootHandle,
	) -> Result<()> {
		if is_state_event(new_pdu) {
			if let Some(prev_root) = prev_root_handle {
				Box::pin(self.update_caches_for_state_delta_between(
					room_id,
					Some(prev_root),
					root_handle,
				))
				.await?;
			}
		}

		Ok(())
	}

	/// Appends a state event to the room's HAMT state and returns the new root.
	///
	/// Builds a new HAMT root handle (and its root node) representing the
	/// room's current state plus the incoming state event. Only state events
	/// may be appended; non-state events are rejected.
	pub async fn append_to_state(
		&self,
		new_pdu: &PduEvent,
		room_id: &RoomId,
		_state_lock: &RoomMutexGuard,
		state_root_handle: Option<&rezzy::hamt::RootHandle>,
	) -> Result<(
		rezzy::hamt::RootHandle,
		Arc<rezzy::hamt::HamtNode<u64, u64>>,
		Vec<rooms::state_hamt::store::PendingNode>,
	)> {
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
			if let Some(mut lattice) = self.get_root_lattice(&base).await {
				let structural_key = rooms::state_hamt::room_structural_key(
					&self.services.globals.server_secret,
					room_id,
				);
				let store = &self.services.state_hamt.store;
				let old =
					store.get_node(room_id.as_bytes(), &structural_key, &base.structural_hash)?;
				let mut resolver = store.get_room_resolver(room_id.as_bytes(), &structural_key);
				let value = self
					.services
					.short
					.get_or_create_shorteventid(new_pdu.event_id())
					.await;
				let (new_node, displaced, created) = rezzy::hamt::persist_mutation(
					&old,
					&structural_key,
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
						event_type.as_ref(),
						state_key,
						old_id.as_str(),
						new_pdu.event_id().as_str(),
					);
				} else {
					lattice.insert(event_type.as_ref(), state_key, new_pdu.event_id().as_str());
				}
				let mut deferred_nodes = Vec::with_capacity(created.len());
				for (hash, bytes) in created {
					deferred_nodes.extend(
						self.services.state_hamt.store.put_encoded_node_deferred(
							room_id.as_bytes(),
							hash,
							&bytes,
						)?,
					);
				}
				let handle = self
					.services
					.state_hamt
					.store
					.root_handle(new_node.structural_hash, &lattice);
				self.persist_root_lattice(&handle, &lattice);
				return Ok((handle, new_node, deferred_nodes));
			}
		}

		let mut current: HashMap<ShortStateKey, OwnedEventId> = if let Some(root_handle) =
			state_root_handle
		{
			self.load_state_map_from_root_handle(room_id, root_handle, new_shortstatekey)
				.await?
		} else {
			match self.get_room_state_hamt(room_id).await {
				| Ok(root_handle) =>
					self.load_state_map_from_root_handle(room_id, &root_handle, new_shortstatekey)
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

		let mut lattice = rezzy::incremental::LtHash::default();
		let mut entries: Vec<(ShortStateKey, ShortEventId)> =
			Vec::with_capacity(short_state_keys.len());

		for ((ssk, event_id), key_result) in
			short_state_keys.into_iter().zip(event_ids).zip(string_keys)
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
		self.persist_root_lattice(&root_handle, &lattice);

		let deferred_nodes = self
			.services
			.state_hamt
			.store
			.persist_node_recursive_deferred(room_id.as_bytes(), root_node.clone())?;

		Ok((root_handle, root_node, deferred_nodes))
	}

	/// Applies `mutations` on top of `prev` with copy-on-write path copying and
	/// persists only the nodes along the changed spines, returning the new root
	/// handle.
	///
	/// This is the bulk counterpart to the single-key fast path in
	/// [`Self::append_to_state`]: `K` changed state keys cost `O(K log₃₂ S)`
	/// node writes instead of the `O(S)` of materializing and rewriting the
	/// whole tree. `lattice` must already describe the post-mutation state, and
	/// is persisted alongside the nodes so a root never references a node that
	/// is not durable yet.
	pub fn persist_state_hamt_mutations(
		&self,
		room_id: &RoomId,
		structural_key: &[u8],
		prev: &rezzy::hamt::RootHandle,
		mutations: Vec<(ShortStateKey, Option<ShortEventId>)>,
		lattice: &rezzy::incremental::LtHash,
	) -> Result<(rezzy::hamt::RootHandle, Vec<rooms::state_hamt::store::PendingNode>)> {
		let store = &self.services.state_hamt.store;
		let old = store.get_node(room_id.as_bytes(), structural_key, &prev.structural_hash)?;
		let mut resolver = store.get_room_resolver(room_id.as_bytes(), structural_key);

		// Displaced values are unused: callers pass a lattice already computed
		// from the resolved post-mutation state, so there is no need to replay
		// per-key insert/replace arithmetic here.
		let (new_node, _displaced, created) =
			rezzy::hamt::persist_mutations(&old, structural_key, mutations, &mut resolver)
				.map_err(|e| err!(error!("HAMT batch mutation failed: {e:?}")))?;

		let mut deferred_nodes = Vec::with_capacity(created.len());
		for (hash, bytes) in created {
			deferred_nodes.extend(self.services.state_hamt.store.put_encoded_node_deferred(
				room_id.as_bytes(),
				hash,
				&bytes,
			)?);
		}

		let handle = self
			.services
			.state_hamt
			.store
			.root_handle(new_node.structural_hash, lattice);
		self.persist_root_lattice(&handle, lattice);

		Ok((handle, deferred_nodes))
	}

	/// Persists the lattice for a root and populates the lattice cache.
	pub fn persist_root_lattice(
		&self,
		root_handle: &rezzy::hamt::RootHandle,
		lattice: &rezzy::incremental::LtHash,
	) {
		let encoded = lattice.to_bytes();
		self.db
			.state_hamt_root_lattices
			.insert(&root_handle.structural_hash, &encoded);
		self.db
			.lattice_cache
			.insert(root_handle.structural_hash, Arc::from(&*encoded));
	}

	/// Returns the lattice persisted for a root, if any. Served from the
	/// bounded cache, falling back to the database on a miss.
	pub async fn get_root_lattice(
		&self,
		root_handle: &rezzy::hamt::RootHandle,
	) -> Option<rezzy::incremental::LtHash> {
		if let Some(raw) = self.db.lattice_cache.get(&root_handle.structural_hash) {
			return rezzy::incremental::LtHash::from_bytes(&raw);
		}

		let raw = self
			.db
			.state_hamt_root_lattices
			.get(&root_handle.structural_hash)
			.await
			.ok()?;
		if raw.len() != LATTICE_LEN {
			return None;
		}

		let lattice = rezzy::incremental::LtHash::from_bytes(&raw)?;
		self.db
			.lattice_cache
			.insert(root_handle.structural_hash, Arc::from(&*raw));
		Some(lattice)
	}

	async fn load_state_map_from_root_handle(
		&self,
		room_id: &RoomId,
		root_handle: &rezzy::hamt::RootHandle,
		skip_shortstatekey: ShortStateKey,
	) -> Result<HashMap<ShortStateKey, OwnedEventId>> {
		let structural_key =
			rooms::state_hamt::room_structural_key(&self.services.globals.server_secret, room_id);
		let store = &self.services.state_hamt.store;
		let node =
			store.get_node(room_id.as_bytes(), &structural_key, &root_handle.structural_hash)?;

		let mut short_events = Vec::new();
		node.visit_entries(
			&mut store.get_room_resolver(room_id.as_bytes(), &structural_key),
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
	pub fn set_room_state_hamt(
		&self,
		room_id: &RoomId,
		root_handle: &rezzy::hamt::RootHandle,
		// Take mutex guard to make sure users get the room state mutex
		_mutex_lock: &RoomMutexGuard,
	) -> Result<()> {
		self.set_room_state_hamt_with_nodes(room_id, root_handle, &[])
	}

	/// Set the current room root, publishing any nodes created while building it
	/// in the same mtxdb transaction.
	pub fn set_room_state_hamt_with_nodes(
		&self,
		room_id: &RoomId,
		root_handle: &rezzy::hamt::RootHandle,
		nodes: &[rooms::state_hamt::store::PendingNode],
	) -> Result<()> {
		let data = root_handle_to_bytes(root_handle);
		self.services.state_hamt.store.publish_roots(
			room_id.as_bytes(),
			nodes,
			&[],
			Some(&data),
		)?;
		self.db
			.roomid_roothandle
			.insert(room_id.as_str().as_bytes(), &data);
		Ok(())
	}

	/// Returns the room's current HAMT RootHandle.
	pub async fn get_room_state_hamt(&self, room_id: &RoomId) -> Result<rezzy::hamt::RootHandle> {
		let data = self.db.roomid_roothandle.get(room_id).await?;
		root_handle_from_bytes(&data)
	}

	/// Returns the room's version.
	pub async fn get_room_version(&self, room_id: &RoomId) -> Result<RoomVersionId> {
		if let Ok(version) = self.services.short.get_room_version(room_id).await {
			return Ok(version);
		}

		info!(target: "rooms", "Could not get room_version by direct lookup for {}", room_id);
		// Try the current room state snapshot first.
		if let Ok(content) = self
			.services
			.state_accessor
			.room_state_get_content::<RoomCreateEventContent>(
				room_id,
				&StateEventType::RoomCreate,
				"",
			)
			.await
		{
			let version = content.room_version;
			self.services.short.set_room_version(room_id, &version);
			return Ok(version);
		}

		warn!(target: "rooms", "Could not get room_version via state for {}", room_id);
		// Fallback: the create event might be an outlier (not in the state
		// snapshot). Scan outliers for this room to find it.
		let mut outlier_stream = Box::pin(self.services.timeline.room_outlier_stream(room_id));
		while let Some((_eid, pdu)) = outlier_stream.next().await {
			if pdu.kind == TimelineEventType::RoomCreate {
				if let Ok(content) = pdu.get_content::<RoomCreateEventContent>() {
					let version = content.room_version;
					self.services.short.set_room_version(room_id, &version);
					return Ok(version);
				}
			}
		}

		Err(err!(Request(NotFound(
			"No create event found for room (checked db, state, and outliers)"
		))))
	}

	pub async fn get_roothandle(
		&self,
		shorteventid: ShortEventId,
	) -> Result<rezzy::hamt::RootHandle> {
		let data = self.db.shorteventid_roothandle.qry(&shorteventid).await?;
		root_handle_from_bytes(&data)
	}

	/// Associates an event with a HAMT `RootHandle` without advancing the
	/// room's current-state pointer (`roomid_roothandle`).
	///
	/// Used when attaching a resolved historical state snapshot to a
	/// backfilled event: the event's own state association must be recorded,
	/// but the room's live current state must not be touched.
	pub async fn set_event_roothandle(
		&self,
		room_id: &RoomId,
		event_id: &EventId,
		root_handle: &rezzy::hamt::RootHandle,
	) -> Result<()> {
		self.set_event_roothandle_with_nodes(room_id, event_id, root_handle, &[])
			.await
	}

	/// Record an event's root, publishing the nodes created while building it in
	/// the same mtxdb transaction.
	pub async fn set_event_roothandle_with_nodes(
		&self,
		room_id: &RoomId,
		event_id: &EventId,
		root_handle: &rezzy::hamt::RootHandle,
		nodes: &[rooms::state_hamt::store::PendingNode],
	) -> Result<()> {
		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(event_id)
			.await;
		let data = root_handle_to_bytes(root_handle);
		self.services.state_hamt.store.publish_roots(
			room_id.as_bytes(),
			nodes,
			&[(event_id.as_bytes(), data.as_slice())],
			None,
		)?;
		self.db
			.shorteventid_roothandle
			.insert(&shorteventid.to_be_bytes(), &data);
		Ok(())
	}

	/// Atomically publish one event root, the current room root, and their
	/// deferred nodes to mtxdb.
	pub async fn set_event_and_room_roothandles_with_nodes(
		&self,
		room_id: &RoomId,
		event_id: &EventId,
		root_handle: &rezzy::hamt::RootHandle,
		nodes: &[rooms::state_hamt::store::PendingNode],
	) -> Result<()> {
		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(event_id)
			.await;
		let data = root_handle_to_bytes(root_handle);
		self.services.state_hamt.store.publish_roots(
			room_id.as_bytes(),
			nodes,
			&[(event_id.as_bytes(), data.as_slice())],
			Some(&data),
		)?;
		let mut batch = Batch::new();
		self.db
			.shorteventid_roothandle
			.batch_put(&mut batch, &shorteventid.to_be_bytes(), &data);
		self.db
			.roomid_roothandle
			.batch_put(&mut batch, room_id.as_bytes(), &data);
		self.db.shorteventid_roothandle.apply_batch(batch);
		Ok(())
	}

	/// Returns every recorded root handle in the database.
	///
	/// This is the complete set of roots recorded in the RocksDB index: both the
	/// current per-room `roomid_roothandle` pointers and every per-event
	/// `shorteventid_roothandle` snapshot.
	///
	/// Reads both maps through `raw_stream` rather than any prefix scan, because
	/// the two maps are keyed differently (room ID bytes and a big-endian
	/// short-event ID) and neither has a prefix that isolates its own entries
	/// from keys of the other shape. A key that does not decode to a
	/// [`ROOT_HANDLE_LEN`]-byte value is skipped rather than treated as an error:
	/// the map is not expected to hold anything else, and refusing to sweep
	/// because of one unrecognised entry would leave orphans alive forever.
	pub async fn live_root_handles(&self) -> Result<Vec<rezzy::hamt::RootHandle>> {
		let mut handles = Vec::new();

		for map in [&self.db.roomid_roothandle, &self.db.shorteventid_roothandle] {
			let mut stream = map.raw_stream();
			while let Some(kv) = stream.next().await {
				let (_, value): database::KeyVal<'_> = kv?;
				if value.len() != ROOT_HANDLE_LEN {
					warn!(target: "state_hamt", "skipping unrecognized root handle record");
					continue;
				}
				handles.push(root_handle_from_bytes(value)?);
			}
		}

		Ok(handles)
	}

	/// Returns rooms with a persisted current-state root.
	pub async fn rooms_with_roots(&self) -> Result<Vec<OwnedRoomId>> {
		let mut rooms = Vec::new();
		let mut stream = self.db.roomid_roothandle.raw_stream();
		while let Some(kv) = stream.next().await {
			let (key, _): database::KeyVal<'_> = kv?;
			if let Ok(text) = std::str::from_utf8(key) {
				if let Ok(room_id) = OwnedRoomId::parse(text) {
					rooms.push(room_id);
				}
			}
		}
		Ok(rooms)
	}

	/// Dry run of an orphan sweep over mtxdb: lists, per room, the HAMT nodes no
	/// root of that room reaches. Reads and reports only; nothing is deleted
	/// because standalone node writes can still land before their roots.
	///
	/// Rooms come from `roomid_roothandle`, whose key is the room id mtxdb is
	/// also keyed by.
	pub async fn scan_hamt_orphans(
		&self,
	) -> Result<rooms::state_hamt::orphan_scan::OrphanReport> {
		let mut rooms = Vec::new();
		let mut bad_room_ids = 0_usize;
		let mut stream = self.db.roomid_roothandle.raw_stream();
		while let Some(kv) = stream.next().await {
			let (key, _): database::KeyVal<'_> = kv?;
			let Some(room_id) = std::str::from_utf8(key)
				.ok()
				.and_then(|text| OwnedRoomId::parse(text).ok())
			else {
				bad_room_ids = bad_room_ids.saturating_add(1);
				continue;
			};
			rooms.push(rooms::state_hamt::orphan_scan::RoomInput {
				room_id: key.to_vec(),
				structural_key: rooms::state_hamt::room_structural_key(
					&self.services.globals.server_secret,
					&room_id,
				),
			});
		}

		Ok(self
			.services
			.state_hamt
			.store
			.orphan_scan_dry_run(&rooms, bad_room_ids))
	}

	pub fn get_forward_extremities<'a>(
		&'a self,
		room_id: &'a RoomId,
	) -> impl Stream<Item = OwnedEventId> + Send + 'a {
		let prefix = (room_id, Interfix);

		self.db
			.roomid_pduleaves
			.keys_prefix(&prefix)
			.map_ok(|(_, event_id): (Ignore, OwnedEventId)| event_id)
			.ignore_err()
	}

	/// Returns true if the given event_id is a current forward extremity
	/// (DAG tip) for the room.
	pub async fn is_forward_extremity(&self, room_id: &RoomId, event_id: &EventId) -> bool {
		self.get_forward_extremities(room_id)
			.any(|eid| futures::future::ready(eid == *event_id))
			.await
	}

	pub async fn set_forward_extremities<'a, I>(
		&'a self,
		room_id: &'a RoomId,
		event_ids: I,
		trusted_new_event: Option<&'a EventId>,
		_state_lock: &'a RoomMutexGuard,
	) -> Result<()>
	where
		I: Iterator<Item = OwnedEventId> + Send + 'a,
	{
		// Only events that are actually accepted into the timeline may become
		// citable forward extremities. Outliers, rejected, and soft-failed
		// events cannot be relied upon to ever converge, so admitting them
		// here means every future event/state-res pass in this room pays to
		// re-walk their dependencies indefinitely. This is the single write
		// path to `roomid_pduleaves`, so enforcing eligibility here covers
		// all callers (including `recalculate_extremities` and reorder).
		//
		// `trusted_new_event`, if given, is the one event currently being
		// appended by this same operation: its `eventid_metadata` entry is
		// written moments after this call returns (see
		// `timeline::append_pdu`), so a metadata lookup for it here would
		// always miss. It is exempted from the DB check and trusted
		// directly, since by construction it is being newly accepted into
		// the timeline right now, not an outlier/rejected/soft-failed event.
		let mut eligible: Vec<OwnedEventId> = Vec::new();
		for event_id in event_ids {
			if trusted_new_event.is_some_and(|trusted| trusted.as_str() == event_id.as_str()) {
				eligible.push(event_id);
				continue;
			}

			// Only events actually accepted into the timeline may become citable
			// forward extremities. A non-outlier event is not automatically
			// acceptable: its rejection/soft-fail verdict lives in mtxdb
			// (`pdu_metadata`), not in `EventMetadata`, and a non-outlier event
			// can carry one (mirroring the checks `recalculate_extremities`
			// performs). Admit it only if it is not an outlier, rejected, or
			// soft-failed. A read error aborts the update; a missing metadata
			// row means the event is unknown and is skipped.
			let verdicts = self
				.services
				.pdu_metadata
				.is_event_rejected(&event_id)
				.and_then(|rejected| {
					self.services
						.pdu_metadata
						.is_event_soft_failed(&event_id)
						.map(|soft_failed| rejected || soft_failed)
				});
			let flagged = match verdicts {
				| Ok(flagged) => flagged,
				| Err(error) => {
					error!(
						%room_id, %event_id, %error,
						"set_forward_extremities: could not read verdicts",
					);
					return Err(err!(Database(
						"Failed to read verdicts for forward extremity {event_id}: {error}"
					)));
				},
			};
			if flagged {
				debug!(
					%room_id, %event_id,
					"Refusing to persist rejected/soft-failed event as a forward extremity",
				);
				continue;
			}

			let metadata = match self.services.timeline.get_event_metadata(&event_id).await {
				| Ok(metadata) => metadata,
				| Err(error) if error.is_not_found() => {
					warn!(
						%room_id, %event_id,
						"Refusing to persist forward extremity with unknown event metadata",
					);
					continue;
				},
				| Err(error) => {
					error!(
						?error, %room_id, %event_id,
						"Aborting forward-extremity update because event metadata is unreadable",
					);
					return Err(err!(Database(
						"Failed to read metadata for forward extremity {event_id}: {error}"
					)));
				},
			};
			if metadata.is_outlier {
				debug!(
					%room_id, %event_id,
					"Refusing to persist outlier event as a forward extremity",
				);
			} else {
				eligible.push(event_id);
			}
		}

		if eligible.is_empty() {
			warn!(
				%room_id,
				"set_forward_extremities: all candidate tips were ineligible \
				 (outlier/rejected/soft-failed/unknown); leaving existing forward \
				 extremities unchanged",
			);
			return Ok(());
		}

		// Replace the stored set in one batch so a crash cannot leave the room
		// with no forward extremities between the delete and the puts.
		let leaves = &self.db.roomid_pduleaves;
		let mut batch = Batch::new();
		let prefix = (room_id, Interfix);
		leaves
			.keys_prefix_raw(&prefix)
			.ignore_err()
			.ready_for_each(|key| leaves.batch_delete(&mut batch, key))
			.await;

		// Enforce a hard cap at the DB writer level. Callers may pass more
		// tips than this (e.g. from recalculate_extremities),
		// Keeping the newest tips is preferred since they are most likely to
		// be merged by future events.
		let max_extremities = self.services.globals.max_forward_extremities();
		let start = eligible.len().saturating_sub(max_extremities);
		for event_id in &eligible[start..] {
			let key = serialize_to_vec((room_id, event_id))?;
			leaves.batch_put(&mut batch, &key, event_id.as_bytes());
		}
		leaves.apply_batch(batch);

		Ok(())
	}

	/// This fetches auth events from the current state.
	#[allow(clippy::too_many_arguments)]
	pub async fn get_auth_events(
		&self,
		room_id: &RoomId,
		kind: &TimelineEventType,
		sender: &UserId,
		state_key: Option<&str>,
		content: &conduwuit::matrix::pdu::RawJson,
		room_version: &RoomVersion,
		room_version_id: &RoomVersionId,
	) -> Result<StateMap<PduEvent>> {
		let Ok(root_handle) = self.get_room_state_hamt(room_id).await else {
			return Ok(HashMap::new());
		};

		let content_val =
			rezzy::JsonValue::parse(content.get()).expect("PDU content must be valid JSON");
		// For auth_types_for_event, V2 vs V2_1+ is the only distinction
		// (whether `m.room.create` is included). V2_1_1 and V2_2 behave the same.
		let version = if room_version.room_ids_as_hashes {
			rezzy::StateResVersion::V2_1
		} else {
			// TODO: what about StateRes V1? V1 rooms?
			rezzy::StateResVersion::V2
		};
		let auth_types_raw = rezzy::auth::auth_types_for_event(
			kind.as_ref(),
			sender.as_str(),
			state_key,
			&content_val,
			// MSC4291 (v12+): auth_events must NOT reference m.room.create
			version,
			room_version_id.as_str(),
		);
		let auth_types: Vec<(StateEventType, StateKey)> = auth_types_raw
			.into_iter()
			.map(|(ty, sk)| (ty.into(), sk.into()))
			.collect();
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
			.state_full_shortids_hamt(room_id, root_handle)
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

mod export;
pub use export::{EXPORT_FORMAT, ExportCounts};

#[cfg(test)]
mod tests;
