//! Domain adapter for federation signing-key records.
//!
//! Keys are global, not per-room, so every origin's records share one
//! collection in the server-info shard. Each origin has several independent
//! records, told apart by [`KeyRecordKind`]; the payloads are opaque bytes
//! (JSON, written by the caller). Validity windows (`valid_until_ts`,
//! `expired_ts`) live inside the payloads.

use std::sync::Arc;

pub use mtxdb::interop::KeyRecordKind;
use mtxdb::{
	ShardType,
	interop::{
		decode_server_key, encode_server_key, server_key_record_id, server_keys_collection_id,
	},
};

use crate::{StorageError, store::MtxdbStore};

/// A key record's kind, origin bytes, and serialized payload.
pub type KeyRecord = (KeyRecordKind, Vec<u8>, Vec<u8>);

#[derive(Clone)]
pub struct ServerKeysAdapter {
	store: Arc<MtxdbStore>,
}

impl ServerKeysAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	/// Whether any signing-key record exists, for startup's incomplete-import
	/// check.
	pub fn has_any_records(&self) -> Result<bool, StorageError> {
		self.store
			.has_records(ShardType::ServerInfo, &server_keys_collection_id())
	}

	/// The record's payload. The origin is stored beside it, because the record
	/// id is only 128 bits; a record for a different origin is a miss.
	pub fn get(
		&self,
		kind: KeyRecordKind,
		origin: &[u8],
	) -> Result<Option<Vec<u8>>, StorageError> {
		let node = server_key_record_id(kind, origin);
		self.store
			.get(ShardType::ServerInfo, &server_keys_collection_id(), node)?
			.map_or(Ok(None), |bytes| {
				let (stored_origin, payload) = decode_server_key(&bytes)?;
				Ok((stored_origin == origin).then(|| payload.to_vec()))
			})
	}

	/// All live key records, with their original origins recovered from the
	/// collision-checking envelope. Used by the export path after cutover.
	pub fn scan(&self) -> Result<Vec<KeyRecord>, StorageError> {
		let collection = server_keys_collection_id();
		let records = self.store.scan(ShardType::ServerInfo, &collection)?;
		let mut out = Vec::with_capacity(records.len());
		for (node, bytes) in records {
			let (origin, payload) = decode_server_key(&bytes)?;
			let kind = KeyRecordKind::ALL
				.into_iter()
				.find(|kind| server_key_record_id(*kind, origin) == node);
			let Some(kind) = kind else {
				return Err(StorageError::Corrupt(
					"signing-key record id does not match origin".to_owned(),
				));
			};
			out.push((kind, origin.to_vec(), payload.to_vec()));
		}
		Ok(out)
	}

	/// Stage the record in `tx`, so key updates commit atomically with each
	/// other. The payload must not be empty: mtxdb treats an empty value as a
	/// removal.
	pub fn put(
		&self,
		tx: &crate::store::MtxdbTransaction<'_>,
		kind: KeyRecordKind,
		origin: &[u8],
		payload: &[u8],
	) -> Result<(), StorageError> {
		if payload.is_empty() {
			return Err(StorageError::Corrupt("empty signing-key payload".to_owned()));
		}
		let node = server_key_record_id(kind, origin);
		let collection = server_keys_collection_id();
		if let Some(existing) = self.store.get(ShardType::ServerInfo, &collection, node)? {
			if decode_server_key(&existing)?.0 != origin {
				return Err(StorageError::Corrupt("signing-key record id collision".to_owned()));
			}
		}
		tx.put(ShardType::ServerInfo, collection, node, &encode_server_key(origin, payload)?)
	}

	/// Stage deletion of one optional record.
	pub fn remove(
		&self,
		tx: &crate::store::MtxdbTransaction<'_>,
		kind: KeyRecordKind,
		origin: &[u8],
	) -> Result<(), StorageError> {
		let node = server_key_record_id(kind, origin);
		let collection = server_keys_collection_id();
		if let Some(existing) = self.store.get(ShardType::ServerInfo, &collection, node)? {
			if decode_server_key(&existing)?.0 != origin {
				return Err(StorageError::Corrupt("signing-key record id collision".to_owned()));
			}
		}
		tx.remove(ShardType::ServerInfo, collection, node)
	}
}

#[cfg(test)]
mod tests {
	use std::{fs, sync::Arc};

	use super::*;

	#[test]
	fn shared_layout_preserves_existing_key_record_bytes() {
		let bytes = encode_server_key(b"a.example", b"payload").unwrap();
		let old_bytes = [
			1, 9, 0, 0, 0, b'a', b'.', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'p', b'a',
			b'y', b'l', b'o', b'a', b'd',
		];
		assert_eq!(bytes.as_slice(), old_bytes.as_slice());
		assert_eq!(
			decode_server_key(&old_bytes).unwrap(),
			(b"a.example".as_slice(), b"payload".as_slice())
		);
	}

	#[test]
	fn has_any_records_observes_only_committed_keys() {
		let path = std::env::temp_dir()
			.join(format!("conduwuit-mtxdb-server-keys-has-any-{}", std::process::id()));
		let _ = fs::remove_dir_all(&path);
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = ServerKeysAdapter::new(store.clone());

		assert!(!adapter.has_any_records().unwrap());
		let tx = store.begin_transaction();
		adapter
			.put(&tx, KeyRecordKind::Historical, b"example.org", b"payload")
			.unwrap();
		assert!(!adapter.has_any_records().unwrap());
		tx.commit().unwrap();
		assert!(adapter.has_any_records().unwrap());
		fs::remove_dir_all(&path).unwrap();
	}

	#[test]
	fn records_are_independent_per_kind_and_origin_and_survive_restart() {
		let path = std::env::temp_dir()
			.join(format!("conduwuit-mtxdb-server-keys-{}", std::process::id()));
		let _ = fs::remove_dir_all(&path);

		let () = {
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = ServerKeysAdapter::new(store.clone());
			assert!(
				adapter
					.get(KeyRecordKind::Historical, b"a.example")
					.unwrap()
					.is_none()
			);

			let tx = store.begin_transaction();
			adapter
				.put(&tx, KeyRecordKind::Historical, b"a.example", b"{\"h\":1}")
				.unwrap();
			adapter
				.put(&tx, KeyRecordKind::Provisional, b"a.example", b"[\"ed25519:x\"]")
				.unwrap();
			adapter
				.put(&tx, KeyRecordKind::Historical, b"b.example", b"{\"h\":2}")
				.unwrap();
			assert!(
				adapter
					.put(&tx, KeyRecordKind::Notary, b"a.example", b"")
					.is_err()
			);
			tx.commit().unwrap();

			store.sync(ShardType::ServerInfo).unwrap();
		};

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = ServerKeysAdapter::new(store.clone());
		let get = |kind, origin: &[u8]| adapter.get(kind, origin).unwrap();
		assert_eq!(get(KeyRecordKind::Historical, b"a.example").unwrap(), b"{\"h\":1}");
		assert_eq!(get(KeyRecordKind::Provisional, b"a.example").unwrap(), b"[\"ed25519:x\"]");
		assert_eq!(get(KeyRecordKind::Historical, b"b.example").unwrap(), b"{\"h\":2}");
		assert!(get(KeyRecordKind::Notary, b"a.example").is_none());
		assert!(get(KeyRecordKind::Provisional, b"b.example").is_none());

		// Overwrite replaces the payload.
		let tx = store.begin_transaction();
		adapter
			.put(&tx, KeyRecordKind::Historical, b"a.example", b"{\"h\":3}")
			.unwrap();
		tx.commit().unwrap();
		assert_eq!(get(KeyRecordKind::Historical, b"a.example").unwrap(), b"{\"h\":3}");
		fs::remove_dir_all(&path).unwrap();
	}
}
