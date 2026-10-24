//! Domain adapter for Matrix timeline ordering indexes.
//!
//! Wraps [`mtxdb::timeline::TimelineIndex`] to provide room-scoped
//! timeline sequencing (depth and stream ordering) stored directly in mtxdb.

use std::sync::Arc;

use mtxdb::{
	ShardType,
	interop::event_record_id,
	timeline::{EventRef, TimelineCursor, TimelineEntry, TimelineIndex, TimelinePage},
};
use sha2::{Digest, Sha256};

use crate::{
	StorageError,
	store::{MtxdbStore, MtxdbTransaction, NodeId},
};

const DEFAULT_TIMELINE_NAME: &str = "events_order_room";

/// Hash full event ID into a 32-byte reference for [`TimelineEntry`].
#[must_use]
pub fn event_id_ref(event_id: &[u8]) -> EventRef {
	let mut hasher = Sha256::new();
	hasher.update(event_id);
	hasher.finalize().into()
}

/// Truncate or hash a room ID into a 16-byte scope for [`TimelineEntry`].
#[must_use]
pub fn room_scope_id(room_id: &[u8]) -> [u8; 16] {
	crate::keys::derive_id(b"timeline-room-scope", room_id)
}

/// Adapter providing ordered timeline access backed by mtxdb's [`TimelineIndex`].
#[derive(Clone)]
pub struct TimelineAdapter {
	store: Arc<MtxdbStore>,
	shard: ShardType,
	name: Arc<str>,
	collection_id: NodeId,
}

impl TimelineAdapter {
	/// Open or initialize a timeline adapter using the default `ShardType::EventDag`.
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self {
		Self::with_shard_and_name(store, ShardType::EventDag, DEFAULT_TIMELINE_NAME)
	}

	/// Custom shard or index name.
	#[must_use]
	pub fn with_shard_and_name(store: Arc<MtxdbStore>, shard: ShardType, name: &str) -> Self {
		let collection_id = mtxdb::timeline::timeline_collection_id(name);
		Self {
			store,
			shard,
			name: Arc::from(name),
			collection_id,
		}
	}

	/// The pre-computed collection ID for this timeline index.
	#[must_use]
	pub const fn collection_id(&self) -> NodeId { self.collection_id }

	/// The timeline index name.
	#[must_use]
	pub fn name(&self) -> &str { &self.name }

	fn index(&self) -> TimelineIndex<'_, mtxdb::packfile::storage::PackfileStorage> {
		TimelineIndex::with_collection_id(
			self.store.database().pool(self.shard).as_ref(),
			&self.name[..],
			self.collection_id,
		)
	}

	/// Ensure index collection metadata is registered.
	pub fn ensure_metadata(&self) -> Result<(), StorageError> { self.index().ensure_metadata() }

	/// Append entries directly to the timeline index.
	pub fn append(&self, entries: &[TimelineEntry]) -> Result<NodeId, StorageError> {
		self.index()
			.append(self.store.database().as_ref(), self.shard, entries)
	}

	/// Stage timeline entries into an in-progress transaction.
	pub fn stage_append(
		&self,
		tx: &MtxdbTransaction<'_>,
		entries: &[TimelineEntry],
	) -> Result<NodeId, StorageError> {
		self.index()
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
		self.index().page(&scope, cursor, forward, limit)
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
			.append(&[entry_a1, entry_a2, entry_b1])
			.expect("append entries");

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
