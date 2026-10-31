//! Domain adapter for Matrix timeline ordering indexes.
//!
//! Wraps [`mtxdb::timeline::TimelineIndex`] to provide room-scoped
//! timeline sequencing (depth and stream ordering) stored directly in mtxdb.

use std::sync::Arc;

pub use mtxdb::timeline::{TIMELINE_HEAD_ID, event_id_ref, room_scope_id, timeline_collection_id};
use mtxdb::{
	ShardType,
	interop::event_record_id,
	timeline::{TimelineCursor, TimelineEntry, TimelineIndex, TimelinePage},
};

use crate::{
	StorageError,
	store::{MtxdbStore, MtxdbTransaction, NodeId},
};

/// Adapter providing ordered timeline access backed by mtxdb's [`TimelineIndex`].
#[derive(Clone)]
pub struct TimelineAdapter {
	store: Arc<MtxdbStore>,
	shard: ShardType,
}

impl TimelineAdapter {
	/// Open or initialize a timeline adapter using the default `ShardType::EventDag`.
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self::with_shard(store, ShardType::EventDag) }

	/// Open the adapter on a custom shard.
	#[must_use]
	pub fn with_shard(store: Arc<MtxdbStore>, shard: ShardType) -> Self { Self { store, shard } }

	fn room_index<'a>(
		&'a self,
		room_id: &'a [u8],
	) -> TimelineIndex<'a, mtxdb::packfile::storage::PackfileStorage> {
		let room_str = std::str::from_utf8(room_id).unwrap_or("");
		TimelineIndex::open(self.store.database().pool(self.shard).as_ref(), room_str)
	}

	/// Ensure timeline index collection metadata is registered for a room.
	pub fn ensure_room_metadata(&self, room_id: &[u8]) -> Result<(), StorageError> {
		self.room_index(room_id).ensure_metadata()
	}

	/// Append entries to a room's timeline index in one transaction.
	///
	/// Writers in this process serialize on the collection's write lock. A
	/// [`StorageError::StaleRead`] from anything else is returned unchanged so
	/// the application owns the retry policy.
	pub fn append_room(
		&self,
		room_id: &[u8],
		entries: &[TimelineEntry],
	) -> Result<NodeId, StorageError> {
		let room_str = std::str::from_utf8(room_id).unwrap_or("");
		let _write_lock = self
			.store
			.write_lock(self.shard, timeline_collection_id(room_str));
		let room_idx = self.room_index(room_id);
		let tx = self.store.database().begin_transaction();
		let root_id = room_idx.stage_append(&tx, self.shard, entries)?;
		tx.commit()?;
		Ok(root_id)
	}

	/// Stage timeline entries for a room into an in-progress transaction.
	pub fn stage_append_room(
		&self,
		room_id: &[u8],
		tx: &MtxdbTransaction<'_>,
		entries: &[TimelineEntry],
	) -> Result<NodeId, StorageError> {
		self.room_index(room_id)
			.stage_append(tx.as_database_transaction(), self.shard, entries)
	}

	/// Build a [`TimelineEntry`] for an event.
	#[must_use]
	pub fn make_entry(
		room_id: &[u8],
		topological_ordering: i64,
		stream_ordering: i64,
		event_id: &[u8],
	) -> TimelineEntry {
		TimelineEntry::new(
			room_scope_id(room_id),
			topological_ordering,
			stream_ordering,
			event_record_id(event_id),
			event_id_ref(event_id),
		)
	}

	/// Page through a room's timeline in topological/stream order.
	pub fn page_room(
		&self,
		room_id: &[u8],
		cursor: Option<&TimelineCursor>,
		forward: bool,
		limit: usize,
	) -> Result<TimelinePage, StorageError> {
		let scope = room_scope_id(room_id);
		let room_idx = self.room_index(room_id);
		room_idx.page(&scope, cursor, forward, limit)
	}

	/// Retrieve the latest entry for a room, if any exists.
	pub fn latest_entry(&self, room_id: &[u8]) -> Result<Option<TimelineEntry>, StorageError> {
		let page = self.page_room(room_id, None, false, 1)?;
		Ok(page.entries.into_iter().next())
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use super::*;

	#[test]
	fn test_timeline_append_and_paging() {
		let dir =
			std::env::temp_dir().join(format!("conduwuit-mtxdb-timeline-{}", std::process::id()));
		let _ = fs::remove_dir_all(&dir);
		let store = Arc::new(MtxdbStore::open(&dir).expect("open store"));
		let adapter = TimelineAdapter::new(store);

		let room_a = b"!room_a:example.com";
		let room_b = b"!room_b:example.com";

		let entry_a1 = TimelineAdapter::make_entry(room_a, 1, 100, b"$event_a1");
		let entry_a2 = TimelineAdapter::make_entry(room_a, 2, 101, b"$event_a2");
		let entry_b1 = TimelineAdapter::make_entry(room_b, 1, 102, b"$event_b1");

		adapter
			.append_room(room_a, &[entry_a1, entry_a2])
			.expect("append room_a entries");
		adapter
			.append_room(room_b, &[entry_b1])
			.expect("append room_b entry");

		// Page room_a forward
		let page_a = adapter
			.page_room(room_a, None, true, 10)
			.expect("page forward");
		assert_eq!(page_a.entries.len(), 2);
		assert_eq!(page_a.entries[0], entry_a1);
		assert_eq!(page_a.entries[1], entry_a2);

		// Latest entry in room_a
		let latest_a = adapter.latest_entry(room_a).expect("latest entry");
		assert_eq!(latest_a, Some(entry_a2));

		// Page room_b
		let page_b = adapter
			.page_room(room_b, None, true, 10)
			.expect("page room b");
		assert_eq!(page_b.entries.len(), 1);
		assert_eq!(page_b.entries[0], entry_b1);

		let _ = fs::remove_dir_all(&dir);
	}
}
