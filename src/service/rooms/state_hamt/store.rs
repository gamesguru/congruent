//! HAMT node store: the mtxdb STAT collections, fronted by a content-addressed
//! memory cache.
//!
//! mtxdb is the only store for HAMT nodes. They live per room, so every read and
//! write names its room, and a node read back is verified against its full
//! structural hash because mtxdb keys carry only the first 16 bytes of it.
use std::{
	collections::HashSet,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use conduwuit::{Result, err, warn};
use rezzy::{
	hamt::{HamtNode, PersistedInternalNode, RootHandle, StructuralHash},
	incremental::LtHash,
};

use super::orphan_scan::{NodePrefix, OrphanReport, RoomInput, RoomOrphans, SkipReason};

/// A node's structural hash and encoded bytes, held back so it is published to
/// mtxdb in the same transaction as the roots that reference it.
pub type PendingNode = (StructuralHash, Vec<u8>);
type NodeCache = moka::sync::Cache<(Vec<u8>, StructuralHash), Arc<HamtNode<u64, u64>>>;

/// Counters describing how much redundant node writing the content-addressed
/// elision in [`Store`] avoided.
///
/// These exist because write amplification here is otherwise invisible: every
/// elided write is a value that is byte-identical to what the store already
/// holds, so nothing downstream can observe it except the write volume itself.
#[derive(Debug, Default)]
pub struct WriteStats {
	/// Nodes handed to mtxdb for writing.
	pub written: AtomicU64,
	/// Node writes skipped because the content-addressed node was already in
	/// mtxdb.
	pub elided: AtomicU64,
}

impl WriteStats {
	/// A consistent-enough snapshot of the counters for assertions.
	#[must_use]
	pub fn snapshot(&self) -> (u64, u64) {
		(self.written.load(Ordering::Relaxed), self.elided.load(Ordering::Relaxed))
	}
}

/// Adapter mapping the rezzy HAMT nodes into mtxdb and a memory cache.
pub struct Store {
	mtxdb: conduwuit_mtxdb::state::StateAdapter,

	/// Content-addressed cache of parsed HamtNodes in memory.
	/// Room is part of the key because mtxdb scopes node records by room.
	///
	/// Note: `u64, u64` is currently a placeholder for `ShortStateKey` and
	/// `ShortEventId` which are the actual domain types.
	node_cache: NodeCache,

	/// Redundant-write counters for the content-addressed elision below.
	write_stats: WriteStats,

	/// Candidate set from the previous scheduled dry-run invocation.
	previous_candidates: std::sync::Mutex<HashSet<super::orphan_scan::Candidate>>,
}

impl Store {
	/// Creates a HAMT node store backed by mtxdb.
	#[must_use]
	pub fn new(mtxdb: Arc<conduwuit_mtxdb::store::MtxdbStore>) -> Self {
		// Use a generic capacity for now. In a full production setup, this
		// could be wired to a config value like other caches.
		let node_cache = moka::sync::Cache::builder().max_capacity(100_000).build();

		Self {
			mtxdb: conduwuit_mtxdb::state::StateAdapter::new(mtxdb),
			node_cache,
			write_stats: WriteStats::default(),
			previous_candidates: std::sync::Mutex::new(HashSet::new()),
		}
	}

	/// Returns the redundant-write counters for this store.
	#[must_use]
	pub fn write_stats(&self) -> &WriteStats {
		&self.write_stats
	}

	/// Returns true when `hash` is provably already stored in `room_id`.
	///
	/// Proof means a committed mtxdb record, never a cache hit. Deferred nodes
	/// are not cached before publication, so an uncommitted node cannot be
	/// mistaken for durable data.
	///
	/// The probe costs a point read and saves a write of bytes already there.
	fn is_durable(&self, room_id: &[u8], hash: &StructuralHash) -> Result<bool> {
		let nodes = self.mtxdb.get_nodes(room_id, &[*hash]).map_err(|error| {
			err!(Database(error!("Failed to check HAMT node durability in mtxdb: {error}")))
		})?;
		Ok(nodes.into_iter().next().flatten().is_some())
	}

	/// Resolves a node of `room_id`: from the memory cache, else from mtxdb,
	/// verified against its full hash. A node mtxdb lacks is an error.
	pub fn get_node(
		&self,
		room_id: &[u8],
		structural_key: &[u8],
		hash: &StructuralHash,
	) -> Result<Arc<HamtNode<u64, u64>>> {
		let cache_key = (room_id.to_vec(), *hash);
		if let Some(node) = self.node_cache.get(&cache_key) {
			return Ok(node);
		}

		let node = self
			.read_node_verified(room_id, structural_key, hash)?
			.ok_or_else(|| err!(Database(error!("State HAMT node not found in mtxdb."))))?;
		self.node_cache.insert(cache_key, node.clone());
		Ok(node)
	}

	/// A resolver for [`rezzy::hamt`] traversals in `room_id`.
	pub fn get_room_resolver<'a>(
		&'a self,
		room_id: &'a [u8],
		structural_key: &'a [u8],
	) -> impl FnMut(&StructuralHash) -> Result<Arc<HamtNode<u64, u64>>, conduwuit::Error> + 'a {
		move |hash: &StructuralHash| self.get_node(room_id, structural_key, hash)
	}

	/// Read a node from mtxdb and verify the full structural hash before
	/// exposing it. mtxdb keys use only the first 16 hash bytes, so the stored
	/// value must be checked against the caller's full hash. Bypasses the cache.
	pub fn read_node_verified(
		&self,
		room_id: &[u8],
		structural_key: &[u8],
		hash: &StructuralHash,
	) -> Result<Option<Arc<HamtNode<u64, u64>>>> {
		let nodes = self
			.mtxdb
			.get_nodes(room_id, &[*hash])
			.map_err(|error| err!(Database(error!("{error}"))))?;
		let Some(bytes) = nodes.into_iter().next().flatten() else {
			return Ok(None);
		};

		let node =
			PersistedInternalNode::<u64, u64>::decode_v1_verified(&bytes, structural_key, *hash)
				.map_err(|error| err!(Database(error!("{error}"))))?;
		Ok(Some(Arc::new(node)))
	}

	/// Validates and parses an encoded node body into an in-memory `HamtNode`
	/// whose children are left lazy for the room resolver to pull.
	///
	/// Shared by the read and write paths so a node cached right after being
	/// written is held under exactly the same representation a later read would
	/// have produced.
	fn decode_node(hash: &StructuralHash, bytes: &[u8]) -> Result<Arc<HamtNode<u64, u64>>> {
		let persisted = PersistedInternalNode::<u64, u64>::decode_v1_unverified(bytes)
			.map_err(|e| err!(Database(error!("{e}"))))?;

		if (persisted.datamap & persisted.nodemap) != 0 {
			return Err(err!(Database(error!(
				"PersistedInternalNode datamap and nodemap overlap"
			))));
		}
		if persisted.leaves.len() != usize::try_from(persisted.datamap.count_ones()).unwrap_or(0)
		{
			return Err(err!(Database(error!(
				"PersistedInternalNode leaf count does not match datamap"
			))));
		}
		if persisted.child_hashes.len()
			!= usize::try_from(persisted.nodemap.count_ones()).unwrap_or(0)
		{
			return Err(err!(Database(error!(
				"PersistedInternalNode child count does not match nodemap"
			))));
		}

		let node = Arc::new(HamtNode {
			datamap: persisted.datamap,
			nodemap: persisted.nodemap,
			leaves: persisted.leaves,
			children: persisted
				.child_hashes
				.into_iter()
				.map(rezzy::hamt::NodeRef::Lazy)
				.collect(),
			structural_hash: *hash,
		});

		Ok(node)
	}

	/// Stages a node for publication, returning an empty vector when mtxdb
	/// already holds it.
	///
	/// Content addressing makes this idempotent: a node reached here under an
	/// already-stored hash is byte-identical to what mtxdb holds, so the write is
	/// skipped. That matters because callers legitimately re-emit unchanged nodes
	/// whenever they re-materialize a tree.
	///
	/// The returned node is not stored until it is published, with the roots
	/// that reference it, by [`Self::publish_roots`] or [`Self::publish_nodes`].
	pub fn put_node_deferred(
		&self,
		room_id: &[u8],
		node: Arc<HamtNode<u64, u64>>,
	) -> Result<Vec<PendingNode>> {
		let hash = node.structural_hash;
		if self.is_durable(room_id, &hash)? {
			self.write_stats.elided.fetch_add(1, Ordering::Relaxed);
			// Refresh the cache entry: the caller may hold a freshly parsed node
			// that is worth keeping resident even though it needs no write.
			self.node_cache.insert((room_id.to_vec(), hash), node);
			return Ok(Vec::new());
		}

		let persisted: PersistedInternalNode<u64, u64> = node.as_ref().into();
		let bytes = persisted.encode_v1();

		self.write_stats.written.fetch_add(1, Ordering::Relaxed);
		Ok(vec![(hash, bytes)])
	}

	/// Stores a node on its own, without a root.
	pub fn put_node(&self, room_id: &[u8], node: Arc<HamtNode<u64, u64>>) -> Result<()> {
		let pending = self.put_node_deferred(room_id, node)?;
		self.publish_nodes(room_id, &pending)
	}

	/// Stages a node and all of its resolved children recursively.
	pub fn persist_node_recursive_deferred(
		&self,
		room_id: &[u8],
		node: Arc<HamtNode<u64, u64>>,
	) -> Result<Vec<PendingNode>> {
		let mut deferred = Vec::new();
		for child in &node.children {
			if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
				deferred
					.extend(self.persist_node_recursive_deferred(room_id, child_node.clone())?);
			}
		}
		deferred.extend(self.put_node_deferred(room_id, node)?);
		Ok(deferred)
	}

	/// Stores a node and all of its resolved children recursively, in one
	/// transaction and without a root.
	pub fn persist_node_recursive(
		&self,
		room_id: &[u8],
		node: Arc<HamtNode<u64, u64>>,
	) -> Result<()> {
		let pending = self.persist_node_recursive_deferred(room_id, node)?;
		self.publish_nodes(room_id, &pending)
	}

	/// Stages an already-encoded node. Returns an empty vector when mtxdb
	/// already holds it.
	pub fn put_encoded_node_deferred(
		&self,
		room_id: &[u8],
		hash: StructuralHash,
		bytes: &[u8],
	) -> Result<Vec<PendingNode>> {
		if self.is_durable(room_id, &hash)? {
			self.write_stats.elided.fetch_add(1, Ordering::Relaxed);
			return Ok(Vec::new());
		}

		let _node = Self::decode_node(&hash, bytes)?;
		self.write_stats.written.fetch_add(1, Ordering::Relaxed);
		Ok(vec![(hash, bytes.to_vec())])
	}

	/// Stores an already-encoded node on its own, without a root.
	pub fn put_encoded_node(
		&self,
		room_id: &[u8],
		hash: StructuralHash,
		bytes: &[u8],
	) -> Result<()> {
		let pending = self.put_encoded_node_deferred(room_id, hash, bytes)?;
		self.publish_nodes(room_id, &pending)
	}

	/// Publishes nodes and roots to mtxdb in one transaction, so a reader can
	/// never observe a root without the nodes it points to. A failure is an
	/// error: mtxdb is the only store, so there is nothing to fall back on.
	pub fn publish(
		&self,
		room_id: &[u8],
		event_id: &[u8],
		nodes: &[PendingNode],
		root_bytes: &[u8],
		is_state_event: bool,
	) -> Result<()> {
		let event_roots = [(event_id, root_bytes)];
		self.publish_roots(room_id, nodes, &event_roots, is_state_event.then_some(root_bytes))
	}

	/// Publishes nodes with one or more event roots and an optional current room
	/// root in one mtxdb transaction.
	pub fn publish_roots(
		&self,
		room_id: &[u8],
		nodes: &[PendingNode],
		event_roots: &[(&[u8], &[u8])],
		room_root: Option<&[u8]>,
	) -> Result<()> {
		let nodes: Vec<([u8; 32], Vec<u8>)> = nodes
			.iter()
			.map(|(hash, bytes)| (*hash, bytes.clone()))
			.collect();
		self.mtxdb
			.publish_roots(room_id, &nodes, event_roots, room_root)
			.map_err(|error| {
				err!(Database(error!("Failed to publish HAMT state to mtxdb: {error}")))
			})
	}

	/// Publishes nodes alone. Does nothing when there are none.
	pub fn publish_nodes(&self, room_id: &[u8], nodes: &[PendingNode]) -> Result<()> {
		if nodes.is_empty() {
			return Ok(());
		}
		self.publish_roots(room_id, nodes, &[], None)
	}

	/// Dry run: list the HAMT nodes in each room's mtxdb store that none of the
	/// room's roots reach.
	///
	/// Never deletes and never writes. Reachability is rezzy's
	/// `walk_reachable_node_hashes`, driven through mtxdb alone (no node
	/// cache), so a node mtxdb lacks makes the room
	/// incomplete and it is skipped rather than reported as garbage. A room
	/// whose roots collection changed during the scan is skipped too, as a live
	/// sweep's version pin would abort on it.
	#[must_use]
	pub fn orphan_scan_dry_run(&self, rooms: &[RoomInput], bad_room_ids: usize) -> OrphanReport {
		let mut report = OrphanReport::default();
		for _ in 0..bad_room_ids {
			report.skipped.count(SkipReason::BadRoomId);
		}

		for room in rooms {
			match self.scan_room(room) {
				| Ok(RoomScan::Skipped(reason)) => report.skipped.count(reason),
				| Ok(RoomScan::Scanned { universe, reachable }) => {
					let mut orphans: Vec<NodePrefix> = universe
						.iter()
						.filter(|prefix| !reachable.contains(*prefix))
						.copied()
						.collect();
					orphans.sort_unstable();
					report.rooms.push(RoomOrphans {
						room_id: room.room_id.clone(),
						nodes: universe.len(),
						orphans,
					});
				},
				| Err(error) => {
					warn!(?error, "orphan scan could not read a room's HAMT store");
					report.skipped.count(SkipReason::Io);
				},
			}
		}

		let candidates: HashSet<super::orphan_scan::Candidate> = report
			.rooms
			.iter()
			.flat_map(|room| {
				room.orphans.iter().map(|node| {
					(conduwuit_mtxdb::keys::room_state_collection_id(&room.room_id), *node)
				})
			})
			.collect();
		let mut previous = self
			.previous_candidates
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		report.persisted_two_passes =
			super::orphan_scan::persisted_candidates(&candidates, &previous);
		*previous = candidates;

		report
	}

	/// Walk one room's roots through mtxdb alone and list its
	/// nodes and the ones its roots reach. Skips the room, with a reason, when
	/// the answer could not be trusted.
	fn scan_room(&self, room: &RoomInput) -> Result<RoomScan> {
		let storage_err =
			|error: conduwuit_mtxdb::StorageError| err!(Database(error!("{error}")));

		// The version is read before the roots, so a publish after this point
		// is visible as a change when it is read again below.
		let snapshot = self.mtxdb.room_roots(&room.room_id).map_err(storage_err)?;
		let roots_version = snapshot.version;
		if snapshot.roots.is_empty() {
			return Ok(RoomScan::Skipped(SkipReason::NoRoots));
		}

		let mut handles = Vec::with_capacity(snapshot.roots.len());
		for bytes in &snapshot.roots {
			match crate::rooms::state::root_handle_from_bytes(bytes) {
				| Ok(handle) => handles.push(handle),
				| Err(_) => return Ok(RoomScan::Skipped(SkipReason::UndecodableRoot)),
			}
		}
		handles.sort_unstable_by_key(|handle| handle.structural_hash);
		handles.dedup_by_key(|handle| handle.structural_hash);

		let mut resolver =
			|hash: &StructuralHash| -> std::result::Result<Arc<HamtNode<u64, u64>>, Missing> {
				match self.read_node_verified(&room.room_id, &room.structural_key, hash) {
					| Ok(Some(node)) => Ok(node),
					| Ok(None) | Err(_) => Err(Missing),
				}
			};

		let mut seen: std::collections::BTreeSet<StructuralHash> =
			std::collections::BTreeSet::new();
		for handle in &handles {
			let Ok(Some(root_node)) = self.read_node_verified(
				&room.room_id,
				&room.structural_key,
				&handle.structural_hash,
			) else {
				return Ok(RoomScan::Skipped(SkipReason::IncompleteWalk));
			};
			if rezzy::hamt::delta::walk_reachable_node_hashes(
				&root_node,
				&mut resolver,
				&mut |hash| seen.insert(hash),
			)
			.is_err()
			{
				return Ok(RoomScan::Skipped(SkipReason::IncompleteWalk));
			}
		}

		let universe = self
			.mtxdb
			.room_node_ids(&room.room_id)
			.map_err(storage_err)?;
		let reachable = seen.iter().map(prefix_of).collect();

		if self
			.mtxdb
			.roots_version(&room.room_id)
			.map_err(storage_err)?
			!= roots_version
		{
			return Ok(RoomScan::Skipped(SkipReason::RootsChanged));
		}

		Ok(RoomScan::Scanned { universe, reachable })
	}

	/// Derives the deterministic `RootHandle` (structural hash + the
	/// cross-server-comparable `StateGroupId`) for a resolved root, from
	/// rezzy's `LtHash` state accumulator.
	///
	/// This is the MSC00DC root: `state_group_id` is `BLAKE2b-256(lattice)`
	/// (rezzy's `LtHash::checksum`), reproducible by any server that
	/// resolves to the same state — unlike `structural_hash`, which is a
	/// local-only cache key and must never be compared across servers.
	#[must_use]
	pub fn root_handle(&self, structural_hash: StructuralHash, lattice: &LtHash) -> RootHandle {
		RootHandle::from_lthash(structural_hash, lattice)
	}
}

/// A node mtxdb could not supply during a walk.
struct Missing;

/// Outcome of scanning one room's HAMT store.
enum RoomScan {
	Skipped(SkipReason),
	Scanned {
		universe: Vec<NodePrefix>,
		reachable: HashSet<NodePrefix>,
	},
}

/// The 16-byte key a node is stored under in mtxdb.
fn prefix_of(hash: &StructuralHash) -> NodePrefix {
	let mut prefix = [0_u8; 16];
	prefix.copy_from_slice(&hash[..16]);
	prefix
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("conduwuit-mtxdb-hamt-{name}-{}", std::process::id()))
	}

	#[test]
	fn cross_shard_atomicity_and_sync() {
		let directory = test_path("cross-shard");
		let _ = fs::remove_dir_all(&directory);
		let collection = conduwuit_mtxdb::keys::derive_id(b"sys", b"test-cross-shard");
		let event_node = conduwuit_mtxdb::keys::derive_id(b"event", b"$event:example.com");
		let state_node = conduwuit_mtxdb::keys::derive_id(b"state", b"root");

		let () = {
			let store = conduwuit_mtxdb::store::MtxdbStore::open(&directory).expect("open mtxdb");
			let tx = store.begin_transaction();
			tx.put(conduwuit_mtxdb::ShardType::EventDag, collection, event_node, b"event")
				.expect("stage event");
			tx.put(conduwuit_mtxdb::ShardType::State, collection, state_node, b"state")
				.expect("stage state");

			assert!(
				store
					.get(conduwuit_mtxdb::ShardType::EventDag, &collection, event_node)
					.expect("read event")
					.is_none()
			);
			assert!(
				store
					.get(conduwuit_mtxdb::ShardType::State, &collection, state_node)
					.expect("read state")
					.is_none()
			);

			tx.commit().expect("commit transaction");
			assert_eq!(
				store
					.get(conduwuit_mtxdb::ShardType::EventDag, &collection, event_node)
					.unwrap()
					.as_deref(),
				Some(&b"event"[..])
			);
			assert_eq!(
				store
					.get(conduwuit_mtxdb::ShardType::State, &collection, state_node)
					.unwrap()
					.as_deref(),
				Some(&b"state"[..])
			);

			store
				.sync(conduwuit_mtxdb::ShardType::EventDag)
				.expect("sync event pool");
			store
				.sync(conduwuit_mtxdb::ShardType::State)
				.expect("sync state pool");
		};

		let reopened =
			conduwuit_mtxdb::store::MtxdbStore::open(&directory).expect("reopen mtxdb");
		assert_eq!(
			reopened
				.get(conduwuit_mtxdb::ShardType::EventDag, &collection, event_node)
				.unwrap()
				.as_deref(),
			Some(&b"event"[..])
		);
		assert_eq!(
			reopened
				.get(conduwuit_mtxdb::ShardType::State, &collection, state_node)
				.unwrap()
				.as_deref(),
			Some(&b"state"[..])
		);
		let _ = fs::remove_dir_all(directory);
	}
}
