//! Generic, Matrix-independent access to the mtxdb shard pools.

use std::{
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	thread::JoinHandle,
	time::Duration,
};

use mtxdb::{
	Database, DatabaseTransaction, GroupCommitConfig, ShardType, storage::StorageEngine,
};

use crate::StorageError;

pub type CollectionId = [u8; 16];
pub type NodeId = [u8; 16];
pub type VersionedRead = (Vec<Option<Vec<u8>>>, u64);

/// How often the maintenance thread syncs the pools.
///
/// This is the upper bound on how long a published-but-unflushed journal group
/// can wait before it is durable, and it gives the shared WAL opportunities to
/// reclaim. The background committer below flushes much more frequently.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
/// Group-commit window for the shared WAL. A published event is durable within
/// this window, and bursty writes coalesce into a single fsync.
const GROUP_COMMIT_INTERVAL: Duration = Duration::from_millis(10);
/// Fsync dirty pack bytes in slices instead of one burst at checkpoint time.
const PACK_FSYNC_BUDGET: u64 = 8 << 20;
/// Fsync the index delta log in slices instead of at the coverage barrier.
const DELTA_FSYNC_BUDGET: u64 = 2 << 20;

/// Physical mtxdb boundary shared by event, state, edge, and metadata
/// adapters. Matrix codecs and key derivation do not belong here.
pub struct MtxdbStore {
	db: Arc<Database>,
	maintenance: Mutex<Option<Maintenance>>,
}

struct Maintenance {
	stop: Arc<AtomicBool>,
	handle: Option<JoinHandle<()>>,
}

/// Result of one journal-reclaim attempt. The segment length is reported after
/// the attempt so a bulk writer can back off instead of syncing every batch
/// while some other pool still pins the segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReclaimOutcome {
	/// Whether a sync ran.
	pub synced: bool,
	/// Segment length after the attempt.
	pub segment_len: u64,
	/// Length at which another attempt is worthwhile.
	pub trigger_len: u64,
}

impl ReclaimOutcome {
	/// Whether the segment is back at or below the reclaim trigger.
	#[must_use]
	pub fn reclaimed(&self) -> bool { self.segment_len <= self.trigger_len }
}

impl MtxdbStore {
	#[must_use]
	pub fn new(db: Arc<Database>) -> Self { Self { db, maintenance: Mutex::new(None) } }

	pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, StorageError> {
		let db = Database::open_with_policies(path.into(), mtxdb::matrix_pool_policies())?;
		for shard in ShardType::ALL {
			let pool = db.pool(shard);
			// Hand checkpoint write/fsync tails to a worker instead of making the
			// syncing thread pay the whole burst inline.
			pool.set_background_checkpoint(true);
			pool.set_pack_fsync_budget(PACK_FSYNC_BUDGET);
			pool.set_delta_fsync_budget(DELTA_FSYNC_BUDGET);
		}
		// One shared journal across every pool; the first call starts the
		// committer and the rest are idempotent.
		db.pool(ShardType::EventDag)
			.start_background_commit(GroupCommitConfig::with_interval(GROUP_COMMIT_INTERVAL))?;
		let store = Self::new(Arc::new(db));
		store.start_maintenance(MAINTENANCE_INTERVAL)?;
		Ok(store)
	}

	#[must_use]
	pub fn database(&self) -> &Arc<Database> { &self.db }

	pub fn get(
		&self,
		shard: ShardType,
		collection: &CollectionId,
		node: NodeId,
	) -> Result<Option<Vec<u8>>, StorageError> {
		self.get_many(shard, collection, std::slice::from_ref(&node))
			.map(|mut values| values.pop().flatten())
	}

	/// Read the immutable genesis metadata for a collection, if it has one.
	pub fn get_collection_metadata(
		&self,
		shard: ShardType,
		collection: &CollectionId,
	) -> Result<Option<mtxdb::template::CollectionMetadata>, StorageError> {
		self.db.pool(shard).get_collection_metadata(collection)
	}

	/// Every live record of a collection, in no particular order. A removed
	/// record is not returned.
	pub fn scan(
		&self,
		shard: ShardType,
		collection: &CollectionId,
	) -> Result<Vec<(NodeId, Vec<u8>)>, StorageError> {
		self.db
			.pool(shard)
			.scan_collection(collection)?
			.map(|entry| entry.map(|(node, data)| (node, data.bytes.to_vec())))
			.collect()
	}

	/// Whether a collection has any live records, without materializing them.
	pub fn has_records(
		&self,
		shard: ShardType,
		collection: &CollectionId,
	) -> Result<bool, StorageError> {
		let mut records = self.db.pool(shard).scan_collection(collection)?;
		match records.next() {
			| Some(record) => record.map(|_| true),
			| None => Ok(false),
		}
	}

	/// The collection's logical version, advanced by every committed write to
	/// it. Read it before the data it guards, then pass it to
	/// [`MtxdbTransaction::expect_collection_version`] so the commit fails if
	/// anything was written in between.
	pub fn collection_version(
		&self,
		shard: ShardType,
		collection: &CollectionId,
	) -> Result<u64, StorageError> {
		self.db
			.pool(shard)
			.get_with_collection_version(collection, &[[0; 16]])
			.map(|(_, version)| version)
	}

	pub fn get_many(
		&self,
		shard: ShardType,
		collection: &CollectionId,
		nodes: &[NodeId],
	) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
		Ok(self
			.db
			.pool(shard)
			.get_read_committed(collection, nodes)?
			.into_iter()
			.map(|value| value.map(|node| node.bytes.to_vec()))
			.collect())
	}

	#[must_use]
	pub fn begin_transaction(&self) -> MtxdbTransaction<'_> {
		MtxdbTransaction { tx: self.db.begin_transaction() }
	}

	pub fn sync(&self, shard: ShardType) -> Result<(), StorageError> {
		self.db.pool(shard).sync_all()
	}

	/// Sync `shard` and wait for any checkpoint tail the sync started.
	///
	/// A checkpoint with a coverage claim reports it from the tail, after the
	/// image is durable. A caller that needs the resulting WAL coverage before
	/// it stages more writes (a bulk import, for example) must wait for that
	/// tail rather than only call [`Self::sync`].
	pub fn sync_and_wait(&self, shard: ShardType) -> Result<(), StorageError> {
		self.sync(shard)?;
		self.db.pool(shard).wait_for_checkpoint();
		Ok(())
	}

	/// Reclaim the shared journal when it has reached its trigger, syncing
	/// `shard` and waiting for the resulting coverage.
	///
	/// Below the trigger this does nothing: the segment has headroom, so a
	/// delta append (or no work at all) is enough. At or above it, the sync
	/// takes the coverage checkpoint that lets the journal reclaim; waiting
	/// ensures the next write does not race the checkpoint tail on a nearly
	/// full segment.
	///
	/// The returned length is measured after the sync, so a caller can tell
	/// whether the reclaim actually brought the segment back under the trigger.
	pub fn reclaim_if_needed(&self, shard: ShardType) -> Result<ReclaimOutcome, StorageError> {
		let coordinator = self.db.coordinator();
		let trigger_len = coordinator.reclaim_trigger_len();
		let before = coordinator.segment_len();
		if before <= trigger_len {
			return Ok(ReclaimOutcome {
				synced: false,
				segment_len: before,
				trigger_len,
			});
		}
		self.sync_and_wait(shard)?;
		Ok(ReclaimOutcome {
			synced: true,
			segment_len: coordinator.segment_len(),
			trigger_len,
		})
	}

	/// Current length of the shared journal segment.
	#[must_use]
	pub fn journal_segment_len(&self) -> u64 { self.db.coordinator().segment_len() }

	/// Sync every pool and drain any background checkpoint tail.
	///
	/// The periodic maintenance thread syncs pools individually; shutdown calls
	/// this after the thread is stopped so the final telemetry sees completed tails.
	pub fn checkpoint_all(&self) -> Result<(), StorageError> {
		let mut first_error = None;
		for shard in ShardType::ALL {
			if let Err(error) = self.db.pool(shard).sync_all() {
				first_error.get_or_insert(error);
			}
		}
		for shard in ShardType::ALL {
			self.db.pool(shard).wait_for_checkpoint();
		}
		first_error.map_or(Ok(()), Err)
	}

	/// Start the periodic WAL/checkpoint maintenance thread.
	///
	/// Idempotent: a second call while the thread is running is a no-op. The
	/// thread holds the database (not this adapter), so it keeps working across
	/// clones of the `Arc<MtxdbStore>`.
	#[allow(
		clippy::disallowed_macros,
		reason = "conduwuit_core::warn expands through the log crate"
	)]
	pub fn start_maintenance(&self, interval: Duration) -> Result<(), StorageError> {
		let mut guard = self
			.maintenance
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		if guard.is_some() {
			return Ok(());
		}
		let db = Arc::clone(&self.db);
		let stop = Arc::new(AtomicBool::new(false));
		let thread_stop = Arc::clone(&stop);
		let handle = std::thread::Builder::new()
			.name("mtxdb-maintenance".to_owned())
			.spawn(move || {
				while !thread_stop.load(Ordering::Acquire) {
					std::thread::park_timeout(interval);
					if thread_stop.load(Ordering::Acquire) {
						break;
					}
					for shard in ShardType::ALL {
						if let Err(error) = db.pool(shard).sync_all() {
							conduwuit_core::warn!(
								"mtxdb: periodic {shard:?} checkpoint failed: {error}"
							);
						}
					}
				}
			})
			.map_err(StorageError::Io)?;
		*guard = Some(Maintenance { stop, handle: Some(handle) });
		Ok(())
	}

	/// Stop the periodic maintenance thread. Safe to call more than once.
	pub fn stop_maintenance(&self) {
		let mut guard = self
			.maintenance
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		let Some(mut maintenance) = guard.take() else {
			return;
		};
		maintenance.stop.store(true, Ordering::Release);
		if let Some(handle) = maintenance.handle.take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
	}
}

impl Drop for MtxdbStore {
	fn drop(&mut self) {
		self.stop_maintenance();
		for shard in ShardType::ALL {
			let _ = self.db.pool(shard).stop_background_commit();
		}
	}
}

pub struct MtxdbTransaction<'a> {
	tx: DatabaseTransaction<'a>,
}

impl<'a> MtxdbTransaction<'a> {
	/// Read records through this transaction and capture the collection version
	/// from the same read. Pass that version to [`Self::expect_collection_version`]
	/// before committing any writes based on the result.
	pub fn get_with_collection_version(
		&self,
		shard: ShardType,
		collection: CollectionId,
		nodes: &[NodeId],
	) -> Result<VersionedRead, StorageError> {
		let (values, version) = self
			.tx
			.get_with_collection_version(shard, &collection, nodes)?;
		Ok((
			values
				.into_iter()
				.map(|value| value.map(|node| node.bytes.to_vec()))
				.collect(),
			version,
		))
	}

	/// Read records through this transaction's staged writes first, then the
	/// live store.
	pub fn get_many(
		&self,
		shard: ShardType,
		collection: CollectionId,
		nodes: &[NodeId],
	) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
		Ok(self
			.tx
			.get(shard, &collection, nodes)?
			.into_iter()
			.map(|value| value.map(|node| node.bytes.to_vec()))
			.collect())
	}

	pub fn put(
		&self,
		shard: ShardType,
		collection: CollectionId,
		node: NodeId,
		data: &[u8],
	) -> Result<(), StorageError> {
		self.tx
			.put(shard, collection, node, &mtxdb::storage::NodeData::from_slice(data))?;
		Ok(())
	}

	/// Stage removal of one record. Removing a key that is not live is a
	/// no-op: it writes nothing and does not advance the collection version,
	/// so a sweep that is retried after a crash is idempotent. The removal
	/// commits atomically with every other mutation in this transaction, so
	/// orphaned nodes can be removed in the same transaction that publishes
	/// the new roots.
	pub fn remove(
		&self,
		shard: ShardType,
		collection: CollectionId,
		node: NodeId,
	) -> Result<(), StorageError> {
		self.tx.remove(shard, collection, node)?;
		Ok(())
	}

	/// Stage removal of many records in one collection. Prefer this to a loop
	/// of [`Self::remove`]: the liveness read and the staged-mutation scan
	/// happen once for the whole batch, so a sweep stays linear in the number
	/// of orphans.
	pub fn remove_many(
		&self,
		shard: ShardType,
		collection: CollectionId,
		nodes: &[NodeId],
	) -> Result<(), StorageError> {
		self.tx.remove_many(shard, collection, nodes)?;
		Ok(())
	}

	/// Make the commit conditional on the collection not having been written
	/// since `version` was read. A commit that loses the race fails with
	/// [`StorageError::StaleRead`] and applies nothing.
	pub fn expect_collection_version(
		&self,
		shard: ShardType,
		collection: CollectionId,
		version: u64,
	) -> Result<(), StorageError> {
		self.tx
			.expect_collection_version(shard, collection, version)?;
		Ok(())
	}

	pub fn as_database_transaction(&self) -> &DatabaseTransaction<'a> { &self.tx }

	pub fn commit(self) -> Result<(), StorageError> { self.tx.commit() }
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	use super::*;
	use crate::keys::derive_id;

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("conduwuit-mtxdb-{name}-{}", std::process::id()))
	}

	#[test]
	fn cross_shard_atomicity_and_sync() {
		let directory = test_path("cross-shard");
		let _ = fs::remove_dir_all(&directory);
		let collection = derive_id(b"sys", b"test-cross-shard");
		let event_node = derive_id(b"event", b"$event:example.com");
		let state_node = derive_id(b"state", b"root");

		let () = {
			let store = MtxdbStore::open(&directory).expect("open mtxdb");
			let tx = store.begin_transaction();
			tx.put(ShardType::EventDag, collection, event_node, b"event")
				.expect("stage event");
			tx.put(ShardType::State, collection, state_node, b"state")
				.expect("stage state");

			assert!(
				store
					.get(ShardType::EventDag, &collection, event_node)
					.expect("read event")
					.is_none()
			);
			assert!(
				store
					.get(ShardType::State, &collection, state_node)
					.expect("read state")
					.is_none()
			);

			tx.commit().expect("commit transaction");
			assert_eq!(
				store
					.get(ShardType::EventDag, &collection, event_node)
					.unwrap()
					.as_deref(),
				Some(&b"event"[..])
			);
			assert_eq!(
				store
					.get(ShardType::State, &collection, state_node)
					.unwrap()
					.as_deref(),
				Some(&b"state"[..])
			);

			store.sync(ShardType::EventDag).expect("sync event pool");
			store.sync(ShardType::State).expect("sync state pool");
		};

		let reopened = MtxdbStore::open(&directory).expect("reopen mtxdb");
		assert_eq!(
			reopened
				.get(ShardType::EventDag, &collection, event_node)
				.unwrap()
				.as_deref(),
			Some(&b"event"[..])
		);
		assert_eq!(
			reopened
				.get(ShardType::State, &collection, state_node)
				.unwrap()
				.as_deref(),
			Some(&b"state"[..])
		);
		let _ = fs::remove_dir_all(directory);
	}
	#[test]
	fn removals_commit_atomically_with_the_roots_that_replace_them() {
		let directory = test_path("remove-atomic");
		let _ = fs::remove_dir_all(&directory);
		let nodes = derive_id(b"sys", b"test-remove-nodes");
		let roots = derive_id(b"sys", b"test-remove-roots");
		let orphan = derive_id(b"state", b"orphan");
		let kept = derive_id(b"state", b"kept");
		let root_key = derive_id(b"state", b"root");

		let store = MtxdbStore::open(&directory).expect("open mtxdb");
		let seed = store.begin_transaction();
		seed.put(ShardType::State, nodes, orphan, b"old-node")
			.unwrap();
		seed.put(ShardType::State, nodes, kept, b"kept-node")
			.unwrap();
		seed.put(ShardType::EventDag, roots, root_key, b"root-1")
			.unwrap();
		seed.commit().unwrap();

		// A sweep: publish the new root and drop the orphan in one commit.
		let sweep = store.begin_transaction();
		sweep.remove(ShardType::State, nodes, orphan).unwrap();
		sweep
			.put(ShardType::EventDag, roots, root_key, b"root-2")
			.unwrap();
		assert!(
			store
				.get(ShardType::State, &nodes, orphan)
				.unwrap()
				.is_some(),
			"nothing is visible before commit"
		);
		sweep.commit().unwrap();

		assert_eq!(store.get(ShardType::State, &nodes, orphan).unwrap(), None);
		assert_eq!(
			store
				.get(ShardType::State, &nodes, kept)
				.unwrap()
				.as_deref(),
			Some(&b"kept-node"[..])
		);
		assert_eq!(
			store
				.get(ShardType::EventDag, &roots, root_key)
				.unwrap()
				.as_deref(),
			Some(&b"root-2"[..])
		);
		let _ = fs::remove_dir_all(directory);
	}

	#[test]
	fn a_repeated_sweep_is_idempotent_and_a_removed_node_can_be_re_added() {
		let directory = test_path("remove-idempotent");
		let _ = fs::remove_dir_all(&directory);
		let nodes = derive_id(b"sys", b"test-remove-idempotent");
		let ids: Vec<NodeId> = (0..8_u8)
			.map(|index| derive_id(b"state", &[index]))
			.collect();

		let store = MtxdbStore::open(&directory).expect("open mtxdb");
		let seed = store.begin_transaction();
		for id in &ids {
			seed.put(ShardType::State, nodes, *id, b"node").unwrap();
		}
		seed.commit().unwrap();

		for _ in 0..2 {
			// The second pass finds everything already gone and must succeed.
			let sweep = store.begin_transaction();
			sweep
				.remove_many(ShardType::State, nodes, &ids[..5])
				.unwrap();
			sweep.commit().unwrap();
		}
		for id in &ids[..5] {
			assert_eq!(store.get(ShardType::State, &nodes, *id).unwrap(), None);
		}
		for id in &ids[5..] {
			assert!(store.get(ShardType::State, &nodes, *id).unwrap().is_some());
		}

		// The HAMT re-emits an unchanged node after it was removed.
		let readd = store.begin_transaction();
		readd.put(ShardType::State, nodes, ids[0], b"node").unwrap();
		readd.commit().unwrap();
		assert_eq!(
			store
				.get(ShardType::State, &nodes, ids[0])
				.unwrap()
				.as_deref(),
			Some(&b"node"[..])
		);

		store.sync(ShardType::State).unwrap();
		drop(store);
		let reopened = MtxdbStore::open(&directory).expect("reopen mtxdb");
		assert_eq!(reopened.get(ShardType::State, &nodes, ids[1]).unwrap(), None);
		assert!(
			reopened
				.get(ShardType::State, &nodes, ids[0])
				.unwrap()
				.is_some()
		);
		let _ = fs::remove_dir_all(directory);
	}
}
