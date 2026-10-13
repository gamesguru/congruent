//! The import-complete marker.
//!
//! mtxdb is the only store for events, verdicts and state. A database that
//! predates that has them in RocksDB instead, and must be exported and
//! imported first. The importer writes this marker as the last record of its
//! final transaction; a server refuses to start on a database that has data
//! but no marker, because reading it through mtxdb would silently report no
//! rejections, no soft-fails and no events.

use std::sync::Arc;

pub use mtxdb::interop::{DATA_FORMAT, ImportMarker};
use mtxdb::{
	ShardType,
	interop::{
		decode_import_marker, encode_import_marker, import_marker_collection_id,
		import_marker_node_id,
	},
};

use crate::{
	StorageError,
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

#[derive(Clone)]
pub struct MarkerAdapter {
	store: Arc<MtxdbStore>,
}

impl MarkerAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	fn collection() -> CollectionId { import_marker_collection_id() }

	fn node() -> NodeId { import_marker_node_id() }

	pub fn get(&self) -> Result<Option<ImportMarker>, StorageError> {
		self.store
			.get(ShardType::ServerInfo, &Self::collection(), Self::node())?
			.map_or(Ok(None), |bytes| decode_import_marker(&bytes).map(Some))
	}

	/// Stage the marker in `tx`. The importer calls this last, so the marker
	/// exists only if everything before it committed.
	pub fn put(
		&self,
		tx: &MtxdbTransaction<'_>,
		marker: ImportMarker,
	) -> Result<(), StorageError> {
		tx.put(
			ShardType::ServerInfo,
			Self::collection(),
			Self::node(),
			&encode_import_marker(marker),
		)
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use super::*;

	#[test]
	fn marker_is_absent_until_written_and_survives_restart() {
		let path =
			std::env::temp_dir().join(format!("conduwuit-mtxdb-marker-{}", std::process::id()));
		let _ = fs::remove_dir_all(&path);
		let marker = ImportMarker { data_format: DATA_FORMAT, events: 42 };

		let () = {
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = MarkerAdapter::new(store.clone());
			assert_eq!(adapter.get().unwrap(), None);

			// A staged but uncommitted marker is not visible.
			let tx = store.begin_transaction();
			adapter.put(&tx, marker).unwrap();
			assert_eq!(adapter.get().unwrap(), None);
			tx.commit().unwrap();
			assert_eq!(adapter.get().unwrap(), Some(marker));
			store.sync(ShardType::ServerInfo).unwrap();
		};

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		assert_eq!(MarkerAdapter::new(store).get().unwrap(), Some(marker));
		fs::remove_dir_all(&path).unwrap();
	}

	#[test]
	fn shared_layout_preserves_the_existing_marker_bytes() {
		let marker = ImportMarker { data_format: DATA_FORMAT, events: 42 };
		let existing_bytes = [1_u8, 1, 42, 0, 0, 0, 0, 0, 0, 0];
		assert_eq!(encode_import_marker(marker).as_slice(), existing_bytes.as_slice());
		assert_eq!(decode_import_marker(&existing_bytes).unwrap(), marker);
	}

	#[test]
	fn decode_rejects_truncated_and_unknown_versions() {
		let _error = decode_import_marker(&[]).unwrap_err();
		let _error = decode_import_marker(&[1, 1, 0, 0]).unwrap_err();
		let _error = decode_import_marker(&[9, 1, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap_err();
	}
}
