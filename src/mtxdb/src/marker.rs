//! The import-complete marker.
//!
//! mtxdb is the only store for events, verdicts and state. A database that
//! predates that has them in RocksDB instead, and must be exported and
//! imported first. The importer writes this marker as the last record of its
//! final transaction; a server refuses to start on a database that has data
//! but no marker, because reading it through mtxdb would silently report no
//! rejections, no soft-fails and no events.

use std::sync::Arc;

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::derive_id,
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

/// Version byte of the marker value encoding.
const MARKER_VERSION: u8 = 1;

/// Version of the export/import data contract the marker vouches for. Bump it
/// when the stored layout changes incompatibly, so an older database is
/// refused instead of misread.
pub const DATA_FORMAT: u8 = 1;

/// What an import (or a fresh install) recorded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImportMarker {
	pub data_format: u8,
	/// Events imported. Zero for a fresh install.
	pub events: u64,
}

#[derive(Clone)]
pub struct MarkerAdapter {
	store: Arc<MtxdbStore>,
}

impl MarkerAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	fn collection() -> CollectionId { derive_id(b"sys", b"import-marker") }

	fn node() -> NodeId { derive_id(b"marker", b"import") }

	pub fn get(&self) -> Result<Option<ImportMarker>, StorageError> {
		self.store
			.get(ShardType::ServerInfo, &Self::collection(), Self::node())?
			.map_or(Ok(None), |bytes| Self::decode(&bytes).map(Some))
	}

	/// Stage the marker in `tx`. The importer calls this last, so the marker
	/// exists only if everything before it committed.
	pub fn put(
		&self,
		tx: &MtxdbTransaction<'_>,
		marker: ImportMarker,
	) -> Result<(), StorageError> {
		let mut out = vec![MARKER_VERSION, marker.data_format];
		out.extend_from_slice(&marker.events.to_le_bytes());
		tx.put(ShardType::ServerInfo, Self::collection(), Self::node(), &out)
	}

	fn decode(bytes: &[u8]) -> Result<ImportMarker, StorageError> {
		let malformed = || StorageError::Corrupt("malformed import marker".to_owned());
		let (&version, rest) = bytes.split_first().ok_or_else(malformed)?;
		if version != MARKER_VERSION {
			return Err(StorageError::Corrupt("unknown import marker version".to_owned()));
		}
		let (&data_format, rest) = rest.split_first().ok_or_else(malformed)?;
		let (events, _) = rest.split_first_chunk::<8>().ok_or_else(malformed)?;
		Ok(ImportMarker {
			data_format,
			events: u64::from_le_bytes(*events),
		})
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
	fn decode_rejects_truncated_and_unknown_versions() {
		let _error = MarkerAdapter::decode(&[]).unwrap_err();
		let _error = MarkerAdapter::decode(&[MARKER_VERSION, 1, 0, 0]).unwrap_err();
		let _error = MarkerAdapter::decode(&[9, 1, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap_err();
	}
}
