//! Domain adapter for federation signing-key records.
//!
//! The authoritative layout is mtxdb's native one: one collection per origin,
//! one record per key id. Each record carries its tier, validity window,
//! digest binding and observation timestamps, and [`ServerKeysAdapter::update`]
//! applies mtxdb's MSC4499 binding rules. The origin's verbatim Notary payload
//! is stored beside the per-origin collections. The shared-collection methods
//! remain for reading databases written before the native layout.

use std::{collections::BTreeSet, sync::Arc};

use mtxdb::server_keys as native;
pub use mtxdb::server_keys::{
	KeyIssue, KeyIssueKind, KeyLimits, KeyMerge, KeySighting, KeyTier, KeyUpdate, ObservedKey,
	ServerKey,
};

use crate::keys::{derive_id, server_key_record_id};

const ORIGIN_INDEX_DOMAIN: &[u8] = b"server-key-origins-v1";
const ORIGIN_INDEX_NODE_DOMAIN: &[u8] = b"server-key-origin-index-v1";

fn origin_index_collection() -> [u8; 16] { derive_id(ORIGIN_INDEX_DOMAIN, b"index") }

fn origin_index_node(origin: &[u8]) -> [u8; 16] { derive_id(ORIGIN_INDEX_NODE_DOMAIN, origin) }

/// Logical class of a server's signing-key payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyRecordKind {
	Historical,
	Provisional,
	Corroborated,
	Notary,
}

impl KeyRecordKind {
	const ALL: [Self; 4] =
		[Self::Historical, Self::Provisional, Self::Corroborated, Self::Notary];

	/// The kind tag written into export records.
	#[must_use]
	pub const fn as_str(self) -> &'static str {
		match self {
			| Self::Historical => "historical",
			| Self::Provisional => "provisional",
			| Self::Corroborated => "corroborated",
			| Self::Notary => "notary",
		}
	}
}
use mtxdb::{
	ShardType,
	interop::{decode_server_key, encode_server_key, server_keys_collection_id},
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

	/// One key of `origin` from mtxdb's native per-key layout.
	pub fn read_key(
		&self,
		origin: &[u8],
		key_id: &[u8],
	) -> Result<Option<ServerKey>, StorageError> {
		native::read_server_key(self.store.database().pool(ShardType::ServerInfo), origin, key_id)
	}

	/// Every key of `origin`, ordered by key id.
	pub fn read_keys(&self, origin: &[u8]) -> Result<Vec<ServerKey>, StorageError> {
		native::read_server_keys(self.store.database().pool(ShardType::ServerInfo), origin)
	}

	/// The verbatim Notary payload for `origin`.
	pub fn read_notary(&self, origin: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		native::read_notary_payload(self.store.database().pool(ShardType::ServerInfo), origin)
	}

	/// Merge one observation batch through mtxdb's MSC4499 binding rules.
	///
	/// The write is native: one record per key id, plus the origin's Notary
	/// payload, committed in one transaction by mtxdb.
	pub fn update(&self, update: &KeyUpdate<'_>) -> Result<KeyMerge, StorageError> {
		let merged = native::update_server_keys(self.store.database(), update)?;
		self.record_origin(update.origin)?;
		Ok(merged)
	}

	/// Persist an origin in the index used by the export path, which cannot
	/// enumerate per-origin collections otherwise.
	fn record_origin(&self, origin: &[u8]) -> Result<(), StorageError> {
		let tx = self.store.begin_transaction();
		tx.put(
			ShardType::ServerInfo,
			origin_index_collection(),
			origin_index_node(origin),
			origin,
		)?;
		tx.commit()
	}

	/// Every origin with native key records, in no particular order.
	///
	/// The index covers writes made through this adapter. Origins written by
	/// the mtxdb CLI importer, which only knows the native layout, are found by
	/// scanning the server-info pool for collections whose first record decodes
	/// as a key belonging to that same collection.
	pub fn origins(&self) -> Result<Vec<Vec<u8>>, StorageError> {
		let mut origins: BTreeSet<Vec<u8>> = self
			.store
			.scan(ShardType::ServerInfo, &origin_index_collection())?
			.into_iter()
			.map(|(_, origin)| origin)
			.collect();

		let pool = self.store.database().pool(ShardType::ServerInfo);
		for collection in pool.collection_ids() {
			let Some((_, bytes)) = self
				.store
				.scan(ShardType::ServerInfo, &collection)?
				.into_iter()
				.next()
			else {
				continue;
			};
			let Ok((origin, _)) = native::decode_server_key_record(&bytes) else {
				continue;
			};
			if native::origin_keys_collection_id(&origin) == collection {
				origins.insert(origin);
			}
		}

		Ok(origins.into_iter().collect())
	}

	/// Whether any native key record exists, for startup's incomplete-import
	/// check.
	pub fn has_any_native_records(&self) -> Result<bool, StorageError> {
		for origin in self.origins()? {
			if !self.read_keys(&origin)?.is_empty() {
				return Ok(true);
			}
		}
		Ok(false)
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
