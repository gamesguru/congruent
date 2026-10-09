use std::{
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use conduwuit::{Result, err};
use database::{Batch, Map};
use futures::TryStreamExt;
use rezzy::{
	LtHash,
	hamt::{HamtNode, PersistedInternalNode, RootHandle, StructuralHash},
};

/// Report produced by [`Store::sweep`], in either dry-run or live mode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
	/// Number of orphaned node hashes that were eligible for reclamation.
	pub orphaned: usize,
	/// Sum of the sizes of the orphaned node values, in bytes.
	pub bytes: u64,
	/// Whether [`Store::sweep`] was run without actually deleting anything.
	pub dry_run: bool,
}

/// Counters describing how much redundant node writing the content-addressed
/// elision in [`Store`] avoided.
///
/// These exist because write amplification here is otherwise invisible: every
/// elided write is a value that is byte-identical to what the store already
/// holds, so nothing downstream can observe it except the write volume itself.
#[derive(Debug, Default)]
pub struct WriteStats {
	/// Node value writes actually issued to the backing store.
	pub written: AtomicU64,
	/// Node writes skipped because the content-addressed node was already
	/// durable.
	pub elided: AtomicU64,
}

impl WriteStats {
	/// A consistent-enough snapshot of the counters for assertions.
	#[must_use]
	pub fn snapshot(&self) -> (u64, u64) {
		(self.written.load(Ordering::Relaxed), self.elided.load(Ordering::Relaxed))
	}
}

/// Adapter mapping the rezzy HAMT nodes into RocksDB and memory caches.
pub struct Store {
	/// RocksDB column family containing the densely persisted nodes.
	db: Arc<Map>,

	/// RocksDB column family tracking the wall-clock persistence time of each
	/// node, keyed by `StructuralHash`. Used by the orphan sweep's grace window
	/// to avoid deleting a node that a concurrent writer has just persisted but
	/// not yet linked into a durable root handle.
	node_mtimes: Arc<Map>,

	/// Content-addressed cache of parsed HamtNodes in memory.
	/// Deduplicates subtrees across different room states automatically since
	/// the key is purely the StructuralHash.
	///
	/// Note: `u64, u64` is currently a placeholder for `ShortStateKey` and
	/// `ShortEventId` which are the actual domain types.
	node_cache: moka::sync::Cache<StructuralHash, Arc<HamtNode<u64, u64>>>,

	/// Redundant-write counters for the content-addressed elision below.
	write_stats: WriteStats,
}

impl Store {
	/// Creates a HAMT node store backed by the node and mtime maps.
	pub fn new(db: Arc<Map>, node_mtimes: Arc<Map>) -> Self {
		// Use a generic capacity for now. In a full production setup, this
		// could be wired to a config value like other caches.
		let node_cache = moka::sync::Cache::builder().max_capacity(100_000).build();

		Self {
			db,
			node_mtimes,
			node_cache,
			write_stats: WriteStats::default(),
		}
	}

	/// Returns the redundant-write counters for this store.
	#[must_use]
	pub fn write_stats(&self) -> &WriteStats { &self.write_stats }

	/// Returns true when `hash` is provably already durable in the store.
	///
	/// Proof means a committed row on disk, never a cache hit. The node cache
	/// doubles as a read-your-writes buffer for staged-but-unapplied batches --
	/// [`Self::put_node_batch`] publishes into it before `apply_batch` runs -- so
	/// a cached node may have no backing row yet, and may never acquire one if
	/// that batch is dropped. Treating a cache hit as durability would let such a
	/// node be skipped forever instead of being rewritten on the next
	/// materialization, turning transient in-flight state into permanent loss for
	/// any root that ends up referencing it.
	///
	/// The probe costs a point read and saves a value write, an mtime write, and
	/// the compaction pressure of re-storing bytes that are already there.
	fn is_durable(&self, hash: &StructuralHash) -> bool { self.db.exists_blocking(hash).is_ok() }

	/// Resolves a node while avoiding blocking a single-threaded Tokio runtime.
	pub fn get_node(&self, hash: &StructuralHash) -> Result<Arc<HamtNode<u64, u64>>> {
		self.get_node_blocking(hash)
	}

	/// Fetches a node by its structural hash synchronously (for the resolver).
	///
	/// Checks the memory cache first. On miss, reads from RocksDB and
	/// parses the dense format into an in-memory `HamtNode`.
	pub fn get_node_blocking(&self, hash: &StructuralHash) -> Result<Arc<HamtNode<u64, u64>>> {
		if let Some(node) = self.node_cache.get(hash) {
			return Ok(node);
		}

		let bytes = self.db.get_blocking(hash)?;
		if bytes.is_empty() {
			return Err(err!(Database(error!("State HAMT node not found in database."))));
		}

		let node = Self::decode_node(hash, &bytes)?;
		self.node_cache.insert(*hash, node.clone());

		Ok(node)
	}

	/// Validates and parses an encoded node body into an in-memory `HamtNode`
	/// whose children are left lazy for [`Self::get_blocking_resolver`] to pull.
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

	/// Persists a node to RocksDB and populates the cache.
	///
	/// Content addressing makes this idempotent: a node reached here under an
	/// already-durable hash is byte-identical to what the store holds, so the
	/// write is skipped. That matters because callers legitimately re-emit
	/// unchanged nodes whenever they re-materialize a tree.
	pub fn put_node(&self, node: Arc<HamtNode<u64, u64>>) {
		let hash = node.structural_hash;
		if self.is_durable(&hash) {
			self.write_stats.elided.fetch_add(1, Ordering::Relaxed);
			// Refresh the cache entry: the caller may hold a freshly parsed node
			// that is worth keeping resident even though it needs no write.
			self.node_cache.insert(hash, node);
			return;
		}

		let persisted: PersistedInternalNode<u64, u64> = node.as_ref().into();
		let bytes = persisted.encode_v1();

		// Durable value before the cache entry, mirroring the ordering
		// `del_node` relies on: a cache hit must never be able to expose a node
		// that has no backing row.
		self.db.insert(&hash, &bytes);
		self.node_mtimes.insert(&hash, unix_millis().to_be_bytes());
		self.node_cache.insert(hash, node);
		self.write_stats.written.fetch_add(1, Ordering::Relaxed);
	}

	/// Persists a node to RocksDB in the provided WriteBatch and populates the
	/// cache.
	///
	/// The mtime for the node joins the same batch as the node value itself, so
	/// the grace window and the node's durability are updated atomically.
	pub fn put_node_batch<'a>(&'a self, node: Arc<HamtNode<u64, u64>>, batch: &mut Batch<'a>) {
		let hash = node.structural_hash;
		if self.is_durable(&hash) {
			self.write_stats.elided.fetch_add(1, Ordering::Relaxed);
			self.node_cache.insert(hash, node);
			return;
		}

		let persisted: PersistedInternalNode<u64, u64> = node.as_ref().into();
		let bytes = persisted.encode_v1();

		self.node_cache.insert(hash, node);
		self.db.batch_put(batch, hash.as_ref(), bytes.as_slice());
		self.node_mtimes
			.batch_put(batch, hash.as_ref(), unix_millis().to_be_bytes());
		self.write_stats.written.fetch_add(1, Ordering::Relaxed);
	}

	/// Stores an already-encoded node and records its persistence time.
	///
	/// Populates the node cache like [`Self::put_node`]: without this, the node a
	/// caller just wrote is invisible to readers until it is evicted or the
	/// process restarts, and it would be re-emitted on every subsequent
	/// materialization of the same subtree.
	pub fn put_encoded_node(&self, hash: StructuralHash, bytes: &[u8]) -> Result<()> {
		if self.is_durable(&hash) {
			self.write_stats.elided.fetch_add(1, Ordering::Relaxed);
			return Ok(());
		}

		let node = Self::decode_node(&hash, bytes)?;
		self.db.insert(&hash, bytes);
		self.node_mtimes.insert(&hash, unix_millis().to_be_bytes());
		self.node_cache.insert(hash, node);
		self.write_stats.written.fetch_add(1, Ordering::Relaxed);

		Ok(())
	}

	/// Persists a node and all of its resolved children recursively.
	pub fn persist_node_recursive(&self, node: Arc<HamtNode<u64, u64>>) {
		for child in &node.children {
			if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
				self.persist_node_recursive(child_node.clone());
			}
		}
		self.put_node(node);
	}

	/// Persists a node and all of its resolved children recursively into a
	/// batch.
	pub fn persist_node_recursive_batch<'a>(
		&'a self,
		node: Arc<HamtNode<u64, u64>>,
		batch: &mut Batch<'a>,
	) {
		for child in &node.children {
			if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
				self.persist_node_recursive_batch(child_node.clone(), batch);
			}
		}
		self.put_node_batch(node, batch);
	}

	/// Physically deletes a node value and its recorded mtime, and evicts it
	/// from the memory cache.
	///
	/// This is the raw delete primitive: it does *not* consult reachability or
	/// any grace window. Callers are responsible for only deleting nodes that
	/// are confirmed unreachable (e.g. after [`Store::sweep`]).
	pub fn del_node(&self, hash: &StructuralHash) -> Result<()> {
		// Remove the durable value first and evict the cache last. Evicting
		// first would let a concurrent `get_node` repopulate the cache from the
		// not-yet-deleted row, leaving a cached node with no backing row.
		self.db.remove_raw(hash.as_ref());
		self.node_mtimes.remove_raw(hash.as_ref());
		self.node_cache.invalidate(hash);
		Ok(())
	}

	/// Backdates a node's recorded persistence time by `age`.
	///
	/// Test-only. Reclamation eligibility is decided by the grace window, so
	/// exercising the deleting path needs a node that is unambiguously older than
	/// any grace a test would pass -- a node written microseconds ago is
	/// correctly spared as possibly-still-in-flight.
	#[cfg(test)]
	pub fn age_node_for_test(&self, hash: &StructuralHash, age: Duration) {
		let millis = u64::try_from(age.as_millis()).unwrap_or(u64::MAX);
		self.node_mtimes
			.insert(hash, unix_millis().saturating_sub(millis).to_be_bytes());
	}

	/// Sweeps the node store for hashes unreachable from the given live roots,
	/// optionally deleting them.
	///
	/// All recorded roots (`shorteventid_roothandle` per-event handles plus the
	/// current `roomid_roothandle`) are treated as live and permanently pin
	/// their reachable trees; sweep never reclaims anything reachable from
	/// them. It only reclaims nodes that are absent from the union of the live
	/// roots' reachable sets — i.e. nodes orphaned by an actual deletion.
	///
	/// # Correctness: the grace window
	///
	/// A concurrent writer can persist a node a moment before linking it into a
	/// durable root handle. If this sweep snapshots the live roots, walks
	/// reachability, and then deletes in that order, it would classify that
	/// just-persisted node as unreachable and delete it out from under the
	/// in-flight write. To close this race, only nodes whose persistence time
	/// (see [`Self::put_node`]) predates `now - grace` are considered for
	/// reclamation. Nodes whose mtime is missing (written before this feature
	/// landed) are by definition old and treated as eligible.
	///
	/// When `dry_run` is true (the default), nothing is deleted; the report
	/// still reflects exactly which nodes a live run would reclaim.
	///
	/// # Correctness: caller-supplied live roots
	///
	/// `live_roots` must be the *complete, exhaustive* set of currently
	/// recorded roots — **every** `shorteventid_roothandle` entry (and the
	/// current `roomid_roothandle`), not just the room roots a single caller
	/// happens to hold. Anything reachable from a root you fail to pass here
	/// is invisible to the reachability walk and will be classified as an
	/// orphan and deleted even though it is still live. Sweep guarantees it
	/// never reclaims anything reachable from a *supplied* root; the burden
	/// is on the caller to supply all of them. A partial root set is a
	/// silent, persistent data-loss bug — there is no runtime check that
	/// catches it.
	pub async fn sweep(
		&self,
		live_roots: &[&RootHandle],
		grace: Duration,
		dry_run: bool,
	) -> Result<SweepReport> {
		self.sweep_blocking(live_roots, grace, dry_run).await
	}

	/// Blocking portion of [`Store::sweep`]; performs the reachability walk and
	/// deletion on the current thread.
	///
	/// Enumerates every node hash in `state_hamt_nodes` and deletes the ones
	/// not reachable from `live_roots`, subject to the grace window. Only
	/// likely nodes (32-byte keys) are considered; anything else in the column
	/// family is not a node this store manages.
	async fn sweep_blocking(
		&self,
		live_roots: &[&RootHandle],
		grace: Duration,
		dry_run: bool,
	) -> Result<SweepReport> {
		let mut resolver = self.get_blocking_resolver();

		// Shared visited set across every root: most of the tree is shared, so
		// `walk_reachable_node_hashes` only descends into a subtree the first
		// time its hash is seen.
		let mut seen: std::collections::BTreeSet<StructuralHash> =
			std::collections::BTreeSet::new();

		for root in live_roots {
			let root_node = self.get_node_blocking(&root.structural_hash)?;

			rezzy::hamt::delta::walk_reachable_node_hashes(
				&root_node,
				&mut resolver,
				&mut |hash| seen.insert(hash),
			)
			.map_err(|e| err!(Database(error!("{e}"))))?;
		}

		let cutoff =
			unix_millis().saturating_sub(u64::try_from(grace.as_millis()).unwrap_or(u64::MAX));

		let report = SweepReport { dry_run, ..SweepReport::default() };

		// Sweep the raw key stream incrementally rather than materializing
		// every node key into memory at once, keeping only the reachability
		// set and the running report.
		let report = self
			.db
			.raw_stream()
			.try_fold(report, async |mut report, (key, _): database::KeyVal<'_>| {
				// Keys in `state_hamt_nodes` are 32-byte hashes; any
				// other key is not a node we manage and is skipped
				// defensively.
				let Ok(hash) = StructuralHash::try_from(key) else {
					return Ok(report);
				};

				if seen.contains(&hash) {
					return Ok(report);
				}

				// Grace-window filter: a recent mtime means a write is
				// possibly still in flight, so ignore this node this
				// round. Metadata read failures are propagated so the
				// sweep fails closed rather than deleting a node it
				// could not verify.
				if let Some(mtime) = self.node_mtime(&hash)? {
					if mtime >= cutoff {
						return Ok(report);
					}
				}

				let bytes = u64::try_from(self.db.get_blocking(&hash)?.len()).unwrap_or(u64::MAX);
				report.orphaned = report.orphaned.saturating_add(1);
				report.bytes = report.bytes.saturating_add(bytes);

				if !dry_run {
					self.del_node(&hash)?;
				}

				Ok(report)
			})
			.await?;

		Ok(report)
	}

	/// Returns the recorded persistence time (unix milliseconds) of a node, if
	/// any.
	///
	/// `Ok(None)` means the node predates the mtime column (no recorded mtime
	/// and no database error). A *missing* key and a genuine database read
	/// failure are kept distinct so a live sweep fails closed on metadata read
	/// failures instead of treating them as "no mtime" and deleting a
	/// recently-persisted node it could not verify.
	fn node_mtime(&self, hash: &StructuralHash) -> Result<Option<u64>> {
		let bytes = match self.node_mtimes.get_blocking(hash) {
			| Ok(bytes) => bytes,
			| Err(e) if e.is_not_found() => return Ok(None),
			| Err(e) => return Err(e),
		};
		if bytes.is_empty() {
			return Ok(None);
		}
		let arr = <[u8; 8]>::try_from(&*bytes)
			.map_err(|_| err!(Database(error!("Malformed mtime for state HAMT node."))))?;
		Ok(Some(u64::from_be_bytes(arr)))
	}

	/// Provides a synchronous resolver closure for `isolate_delta` and the
	/// reachability walks.
	///
	/// # Important Architecture Note
	/// `isolate_delta` is synchronous and `#![no_std]` in `rezzy`. However,
	/// it triggers lazy node resolutions which require hitting the database.
	/// Because this closure runs in a sync context but must perform blocking
	/// I/O, we use `tokio::task::block_in_place` if running under a
	/// multithreaded runtime, or block the thread directly (via the blocking
	/// RocksDB API) if we're not executing within Tokio's worker pool (e.g.
	/// tests or synchronous spawns).
	pub fn get_blocking_resolver(
		&self,
	) -> impl FnMut(&StructuralHash) -> Result<Arc<HamtNode<u64, u64>>, conduwuit::Error> + '_ {
		move |hash: &StructuralHash| self.get_node_blocking(hash)
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

/// Current unix time in milliseconds.
fn unix_millis() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}
