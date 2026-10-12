//! Domain adapter for federation signing-key records.
//!
//! Keys are global, not per-room, so every origin's records share one
//! collection in the server-info shard. Each origin has several independent
//! records, told apart by [`KeyRecordKind`]; the payloads are opaque bytes
//! (JSON, written by the caller). Validity windows (`valid_until_ts`,
//! `expired_ts`) live inside the payloads.

use std::sync::Arc;

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::{server_key_record_id, server_keys_collection_id},
	store::MtxdbStore,
};

/// Version byte of the key record value encoding.
const KEY_RECORD_VERSION: u8 = 1;

/// Which of an origin's records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyRecordKind {
	/// Cumulative `verify_keys` and `old_verify_keys` ever seen.
	Historical,
	/// Key ids bound only through a notary so far.
	Provisional,
	/// Key ids observed as currently active by a direct fetch.
	Corroborated,
	/// The last verbatim self-signed payload, re-served by the notary.
	Notary,
}

impl KeyRecordKind {
	const ALL: [Self; 4] =
		[Self::Historical, Self::Provisional, Self::Corroborated, Self::Notary];

	const fn tag(self) -> &'static [u8] {
		match self {
			| Self::Historical => b"historical",
			| Self::Provisional => b"provisional",
			| Self::Corroborated => b"corroborated",
			| Self::Notary => b"notary",
		}
	}
}

#[derive(Clone)]
pub struct ServerKeysAdapter {
	store: Arc<MtxdbStore>,
}

impl ServerKeysAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	/// The record's payload. The origin is stored beside it, because the record
	/// id is only 128 bits; a record for a different origin is a miss.
	pub fn get(
		&self,
		kind: KeyRecordKind,
		origin: &[u8],
	) -> Result<Option<Vec<u8>>, StorageError> {
		let node = server_key_record_id(kind.tag(), origin);
		self.store
			.get(ShardType::ServerInfo, &server_keys_collection_id(), node)?
			.map_or(Ok(None), |bytes| {
				let (stored_origin, payload) = Self::decode(&bytes)?;
				Ok((stored_origin == origin).then(|| payload.to_vec()))
			})
	}

	/// All live key records, with their original origins recovered from the
	/// collision-checking envelope. Used by the export path after cutover.
	pub fn scan(&self) -> Result<Vec<(KeyRecordKind, Vec<u8>, Vec<u8>)>, StorageError> {
		let collection = server_keys_collection_id();
		let records = self.store.scan(ShardType::ServerInfo, &collection)?;
		let mut out = Vec::with_capacity(records.len());
		for (node, bytes) in records {
			let (origin, payload) = Self::decode(&bytes)?;
			let kind = KeyRecordKind::ALL
				.into_iter()
				.find(|kind| server_key_record_id(kind.tag(), origin) == node);
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
		let node = server_key_record_id(kind.tag(), origin);
		let collection = server_keys_collection_id();
		if let Some(existing) = self.store.get(ShardType::ServerInfo, &collection, node)? {
			if Self::decode(&existing)?.0 != origin {
				return Err(StorageError::Corrupt("signing-key record id collision".to_owned()));
			}
		}
		tx.put(ShardType::ServerInfo, collection, node, &Self::encode(origin, payload)?)
	}

	/// Stage deletion of one optional record.
	pub fn remove(
		&self,
		tx: &crate::store::MtxdbTransaction<'_>,
		kind: KeyRecordKind,
		origin: &[u8],
	) -> Result<(), StorageError> {
		let node = server_key_record_id(kind.tag(), origin);
		let collection = server_keys_collection_id();
		if let Some(existing) = self.store.get(ShardType::ServerInfo, &collection, node)? {
			if Self::decode(&existing)?.0 != origin {
				return Err(StorageError::Corrupt("signing-key record id collision".to_owned()));
			}
		}
		tx.remove(ShardType::ServerInfo, collection, node)
	}

	fn encode(origin: &[u8], payload: &[u8]) -> Result<Vec<u8>, StorageError> {
		let length = u32::try_from(origin.len())
			.map_err(|_| StorageError::Corrupt("origin is too large".to_owned()))?;
		let mut out = Vec::with_capacity(
			5_usize
				.saturating_add(origin.len())
				.saturating_add(payload.len()),
		);
		out.push(KEY_RECORD_VERSION);
		out.extend_from_slice(&length.to_le_bytes());
		out.extend_from_slice(origin);
		out.extend_from_slice(payload);
		Ok(out)
	}

	fn decode(bytes: &[u8]) -> Result<(&[u8], &[u8]), StorageError> {
		let malformed = || StorageError::Corrupt("malformed signing-key record".to_owned());
		let (&version, rest) = bytes.split_first().ok_or_else(malformed)?;
		if version != KEY_RECORD_VERSION {
			return Err(StorageError::Corrupt("unknown signing-key record version".to_owned()));
		}
		let (length, rest) = rest.split_first_chunk::<4>().ok_or_else(malformed)?;
		let length = usize::try_from(u32::from_le_bytes(*length)).map_err(|_| malformed())?;
		if rest.len() < length {
			return Err(malformed());
		}
		Ok(rest.split_at(length))
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use super::*;

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
