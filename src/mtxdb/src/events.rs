//! Domain adapter for Matrix event bodies and DAG edge records.

use std::sync::Arc;

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::derive_id,
	store::{CollectionId, MtxdbStore, MtxdbTransaction},
};

pub struct EventAdapter {
	store: Arc<MtxdbStore>,
	pdu_collection: CollectionId,
	prev_edges_collection: CollectionId,
	auth_edges_collection: CollectionId,
}

impl EventAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self {
		Self {
			store,
			pdu_collection: derive_id(b"sys", b"events-pdu-json"),
			prev_edges_collection: derive_id(b"sys", b"events-prev-edges"),
			auth_edges_collection: derive_id(b"sys", b"events-auth-edges"),
		}
	}

	pub fn get_event(&self, event_id: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(ShardType::EventDag, &self.pdu_collection, derive_id(b"event", event_id))
	}

	pub fn get_events(&self, event_ids: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
		let nodes = event_ids
			.iter()
			.map(|id| derive_id(b"event", id))
			.collect::<Vec<_>>();
		self.store
			.get_many(ShardType::EventDag, &self.pdu_collection, &nodes)
	}

	pub fn get_prev_events(&self, event_id: &[u8]) -> Result<Vec<Vec<u8>>, StorageError> {
		self.get_edges(&self.prev_edges_collection, event_id)
	}

	pub fn get_auth_chain(&self, event_id: &[u8]) -> Result<Vec<Vec<u8>>, StorageError> {
		self.get_edges(&self.auth_edges_collection, event_id)
	}

	fn get_edges(
		&self,
		collection: &CollectionId,
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		self.store
			.get(ShardType::EventDag, collection, derive_id(b"event", event_id))?
			.map_or(Ok(Vec::new()), |bytes| Self::deserialize_edges(&bytes))
	}

	pub fn put_event_with_edges(
		&self,
		tx: &MtxdbTransaction<'_>,
		event_id: &[u8],
		event_body: &[u8],
		prev_events: &[&[u8]],
		auth_events: &[&[u8]],
	) -> Result<(), StorageError> {
		let node = derive_id(b"event", event_id);
		tx.put(ShardType::EventDag, self.pdu_collection, node, event_body)?;
		let prev = Self::serialize_edges(prev_events)?;
		let auth = Self::serialize_edges(auth_events)?;
		tx.put(ShardType::EventDag, self.prev_edges_collection, node, &prev)?;
		tx.put(ShardType::EventDag, self.auth_edges_collection, node, &auth)?;
		Ok(())
	}

	fn serialize_edges(edges: &[&[u8]]) -> Result<Vec<u8>, StorageError> {
		let mut total = 0_usize;
		for edge in edges {
			let length = u32::try_from(edge.len())
				.map_err(|_| StorageError::Corrupt("event edge is too large".to_owned()))?;
			let length = usize::try_from(length).expect("u32 fits usize on supported targets");
			total = total
				.checked_add(4)
				.and_then(|size| size.checked_add(length))
				.ok_or_else(|| {
					StorageError::Corrupt("event edge list is too large".to_owned())
				})?;
		}

		let mut output = Vec::with_capacity(total);
		for edge in edges {
			let length = u32::try_from(edge.len())
				.map_err(|_| StorageError::Corrupt("event edge is too large".to_owned()))?;
			output.extend_from_slice(&length.to_le_bytes());
			output.extend_from_slice(edge);
		}
		Ok(output)
	}

	fn deserialize_edges(bytes: &[u8]) -> Result<Vec<Vec<u8>>, StorageError> {
		let mut edges = Vec::new();
		let mut offset = 0;
		while offset < bytes.len() {
			let header_end = offset
				.checked_add(4)
				.ok_or_else(|| StorageError::Corrupt("malformed event edge record".to_owned()))?;
			let length_bytes = bytes
				.get(offset..header_end)
				.ok_or_else(|| StorageError::Corrupt("malformed event edge record".to_owned()))?;
			let length = usize::try_from(u32::from_le_bytes(
				length_bytes.try_into().expect("four-byte slice"),
			))
			.expect("u32 fits usize on supported targets");
			offset = header_end;
			let end = offset
				.checked_add(length)
				.ok_or_else(|| StorageError::Corrupt("malformed event edge record".to_owned()))?;
			edges.push(
				bytes
					.get(offset..end)
					.ok_or_else(|| {
						StorageError::Corrupt("malformed event edge record".to_owned())
					})?
					.to_vec(),
			);
			offset = end;
		}
		Ok(edges)
	}
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	use super::*;

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("conduwuit-mtxdb-events-{name}-{}", std::process::id()))
	}

	#[test]
	fn atomic_write_restart_and_empty_edge_overwrite() {
		let path = test_path("restart");
		let _ = fs::remove_dir_all(&path);
		let event_id = b"$target:example.com";
		let prev = b"$prev:example.com";
		let auth = b"$auth:example.com";

		{
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = EventAdapter::new(store.clone());
			let tx = store.begin_transaction();
			adapter
				.put_event_with_edges(&tx, event_id, b"body", &[prev], &[auth])
				.unwrap();
			assert!(adapter.get_event(event_id).unwrap().is_none());
			tx.commit().unwrap();
			assert_eq!(adapter.get_event(event_id).unwrap().as_deref(), Some(&b"body"[..]));
			assert_eq!(adapter.get_prev_events(event_id).unwrap(), vec![prev.to_vec()]);
			assert_eq!(adapter.get_auth_chain(event_id).unwrap(), vec![auth.to_vec()]);
			store.sync(ShardType::EventDag).unwrap();
		}

		{
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = EventAdapter::new(store.clone());
			assert_eq!(adapter.get_event(event_id).unwrap().as_deref(), Some(&b"body"[..]));

			let tx = store.begin_transaction();
			adapter
				.put_event_with_edges(&tx, event_id, b"updated", &[], &[])
				.unwrap();
			tx.commit().unwrap();
			assert_eq!(adapter.get_event(event_id).unwrap().as_deref(), Some(&b"updated"[..]));
			assert!(adapter.get_prev_events(event_id).unwrap().is_empty());
			assert!(adapter.get_auth_chain(event_id).unwrap().is_empty());
		}

		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn bulk_event_lookup_preserves_order_and_misses() {
		let path = test_path("bulk");
		let _ = fs::remove_dir_all(&path);
		let first = b"$first:example.com";
		let second = b"$second:example.com";
		let missing = b"$missing:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store.clone());
		let tx = store.begin_transaction();
		adapter
			.put_event_with_edges(&tx, first, b"one", &[], &[])
			.unwrap();
		adapter
			.put_event_with_edges(&tx, second, b"two", &[], &[])
			.unwrap();
		tx.commit().unwrap();

		let result = adapter.get_events(&[first, missing, second]).unwrap();
		assert_eq!(result[0].as_deref(), Some(&b"one"[..]));
		assert_eq!(result[1], None);
		assert_eq!(result[2].as_deref(), Some(&b"two"[..]));

		drop(store);
		let _ = fs::remove_dir_all(path);
	}
}
