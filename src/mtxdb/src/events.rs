//! Domain adapter for Matrix event bodies and DAG edge records.

use std::sync::Arc;

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::{
		event_locator_collection_id, event_record_id, room_auth_collection_id,
		room_event_collection_id, room_prev_collection_id,
	},
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

#[derive(Clone)]
pub struct EventAdapter {
	store: Arc<MtxdbStore>,
}

impl EventAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	fn evnt_collection(room_id: &[u8]) -> CollectionId { room_event_collection_id(room_id) }

	fn prev_collection(room_id: &[u8]) -> CollectionId { room_prev_collection_id(room_id) }

	fn auth_collection(room_id: &[u8]) -> CollectionId { room_auth_collection_id(room_id) }

	/// Look up an event by event_id alone using the global event locator.
	///
	/// Returns the room_id bytes if found, so the caller can then fetch the
	/// event from the room's EVNT collection.
	pub fn locate_event(&self, event_id: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		self.store.get(
			ShardType::EventDag,
			&event_locator_collection_id(),
			event_record_id(event_id),
		)
	}

	/// Fetch an event from its room's EVNT collection (requires room_id).
	pub fn get_event(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Option<Vec<u8>>, StorageError> {
		self.store.get(
			ShardType::EventDag,
			&Self::evnt_collection(room_id),
			event_record_id(event_id),
		)
	}

	/// Fetch multiple events from a room's EVNT collection.
	pub fn get_events(
		&self,
		room_id: &[u8],
		event_ids: &[&[u8]],
	) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
		let node_ids: Vec<NodeId> = event_ids.iter().map(|id| event_record_id(id)).collect();
		self.store
			.get_many(ShardType::EventDag, &Self::evnt_collection(room_id), &node_ids)
	}

	pub fn get_prev_events(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		self.get_edges(Self::prev_collection(room_id), event_id)
	}

	pub fn get_auth_chain(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		self.get_edges(Self::auth_collection(room_id), event_id)
	}

	fn get_edges(
		&self,
		collection: CollectionId,
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		self.store
			.get(ShardType::Edges, &collection, event_record_id(event_id))?
			.map_or(Ok(Vec::new()), |bytes| Self::deserialize_edges(&bytes))
	}

	pub fn put_event_with_edges(
		&self,
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		event_id: &[u8],
		event_body: &[u8],
		prev_events: &[&[u8]],
		auth_events: &[&[u8]],
	) -> Result<(), StorageError> {
		let node = event_record_id(event_id);
		tx.put(ShardType::EventDag, Self::evnt_collection(room_id), node, event_body)?;
		// Also index the event -> room mapping for lookups without room_id
		tx.put(ShardType::EventDag, event_locator_collection_id(), node, room_id)?;
		let prev = Self::serialize_edges(prev_events)?;
		let auth = Self::serialize_edges(auth_events)?;
		tx.put(ShardType::Edges, Self::prev_collection(room_id), node, &prev)?;
		tx.put(ShardType::Edges, Self::auth_collection(room_id), node, &auth)?;
		Ok(())
	}

	/// Atomically writes an event body and its direct DAG/auth edges.
	pub fn write_event_with_edges(
		&self,
		room_id: &[u8],
		event_id: &[u8],
		event_body: &[u8],
		prev_events: &[&[u8]],
		auth_events: &[&[u8]],
	) -> Result<(), StorageError> {
		let tx = self.store.begin_transaction();
		self.put_event_with_edges(&tx, room_id, event_id, event_body, prev_events, auth_events)?;
		tx.commit()
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
		let room_id = b"!room:example.com";
		let event_id = b"$target:example.com";
		let prev = b"$prev:example.com";
		let auth = b"$auth:example.com";

		let () = {
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = EventAdapter::new(store.clone());
			let tx = store.begin_transaction();
			adapter
				.put_event_with_edges(&tx, room_id, event_id, b"body", &[prev], &[auth])
				.unwrap();
			assert!(adapter.get_event(room_id, event_id).unwrap().is_none());
			tx.commit().unwrap();
			assert_eq!(
				adapter.get_event(room_id, event_id).unwrap().as_deref(),
				Some(&b"body"[..])
			);
			assert_eq!(adapter.get_prev_events(room_id, event_id).unwrap(), vec![prev.to_vec()]);
			assert_eq!(adapter.get_auth_chain(room_id, event_id).unwrap(), vec![auth.to_vec()]);
			store.sync(ShardType::EventDag).unwrap();
			store.sync(ShardType::Edges).unwrap();
		};

		let () = {
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = EventAdapter::new(store.clone());
			assert_eq!(
				adapter.get_event(room_id, event_id).unwrap().as_deref(),
				Some(&b"body"[..])
			);

			let tx = store.begin_transaction();
			adapter
				.put_event_with_edges(&tx, room_id, event_id, b"updated", &[], &[])
				.unwrap();
			tx.commit().unwrap();
			assert_eq!(
				adapter.get_event(room_id, event_id).unwrap().as_deref(),
				Some(&b"updated"[..])
			);
			assert!(
				adapter
					.get_prev_events(room_id, event_id)
					.unwrap()
					.is_empty()
			);
			assert!(
				adapter
					.get_auth_chain(room_id, event_id)
					.unwrap()
					.is_empty()
			);
		};

		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn bulk_event_lookup_preserves_order_and_misses() {
		let path = test_path("bulk");
		let _ = fs::remove_dir_all(&path);
		let room_id = b"!room:example.com";
		let first = b"$first:example.com";
		let second = b"$second:example.com";
		let missing = b"$missing:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store.clone());
		let tx = store.begin_transaction();
		adapter
			.put_event_with_edges(&tx, room_id, first, b"one", &[], &[])
			.unwrap();
		adapter
			.put_event_with_edges(&tx, room_id, second, b"two", &[], &[])
			.unwrap();
		tx.commit().unwrap();

		let result = adapter
			.get_events(room_id, &[first, missing, second])
			.unwrap();
		assert_eq!(result[0].as_deref(), Some(&b"one"[..]));
		assert_eq!(result[1], None);
		assert_eq!(result[2].as_deref(), Some(&b"two"[..]));

		drop(store);
		let _ = fs::remove_dir_all(path);
	}
}
