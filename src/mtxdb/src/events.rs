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

/// Version byte of the event-locator value encoding.
const LOCATOR_VERSION: u8 = 1;

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
	///
	/// The locator stores the full event id alongside the room, because the
	/// record id is only 128 bits. A locator written for a different event with
	/// the same record id is a miss, not a match.
	pub fn locate_event(&self, event_id: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		let node = event_record_id(event_id);
		self.store
			.get(ShardType::EventDag, &event_locator_collection_id(&node), node)?
			.map_or(Ok(None), |bytes| {
				let (stored_event, room) = Self::decode_locator(&bytes)?;
				Ok((stored_event == event_id).then(|| room.to_vec()))
			})
	}

	fn encode_locator(event_id: &[u8], room_id: &[u8]) -> Result<Vec<u8>, StorageError> {
		let length = u32::try_from(event_id.len())
			.map_err(|_| StorageError::Corrupt("event id is too large".to_owned()))?;
		let mut out = Vec::with_capacity(
			1_usize
				.saturating_add(4)
				.saturating_add(event_id.len())
				.saturating_add(room_id.len()),
		);
		out.push(LOCATOR_VERSION);
		out.extend_from_slice(&length.to_le_bytes());
		out.extend_from_slice(event_id);
		out.extend_from_slice(room_id);
		Ok(out)
	}

	fn decode_locator(bytes: &[u8]) -> Result<(&[u8], &[u8]), StorageError> {
		let malformed = || StorageError::Corrupt("malformed event locator".to_owned());
		let (&version, rest) = bytes.split_first().ok_or_else(malformed)?;
		if version != LOCATOR_VERSION {
			return Err(StorageError::Corrupt("unknown event locator version".to_owned()));
		}
		let (length, rest) = rest.split_first_chunk::<4>().ok_or_else(malformed)?;
		let length = usize::try_from(u32::from_le_bytes(*length)).map_err(|_| malformed())?;
		if rest.len() < length {
			return Err(malformed());
		}
		Ok(rest.split_at(length))
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
		// Also index the event -> room mapping for lookups without room_id. A
		// committed locator for a different event under the same 128-bit record
		// id is a collision; fail the write rather than shadow that event.
		let locator_collection = event_locator_collection_id(&node);
		if let Some(existing) = self
			.store
			.get(ShardType::EventDag, &locator_collection, node)?
		{
			let (stored_event, _) = Self::decode_locator(&existing)?;
			if stored_event != event_id {
				return Err(StorageError::Corrupt(
					"event record id collision in locator".to_owned(),
				));
			}
		}
		let locator = Self::encode_locator(event_id, room_id)?;
		tx.put(ShardType::EventDag, locator_collection, node, &locator)?;
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

#[cfg(test)]
mod room_tests {
	use std::{fs, path::PathBuf};

	use super::*;

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir()
			.join(format!("conduwuit-mtxdb-events-rooms-{name}-{}", std::process::id()))
	}

	#[test]
	fn locator_maps_event_to_room() {
		let path = test_path("locator");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let event = b"$event:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);

		assert!(adapter.locate_event(event).unwrap().is_none());
		adapter
			.write_event_with_edges(room, event, b"body", &[], &[])
			.unwrap();
		assert_eq!(adapter.locate_event(event).unwrap().as_deref(), Some(&room[..]));
		assert!(
			adapter
				.locate_event(b"$unknown:example.com")
				.unwrap()
				.is_none()
		);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn same_event_id_in_two_rooms_does_not_clobber() {
		let path = test_path("two-rooms");
		let _ = fs::remove_dir_all(&path);
		let room_a = b"!a:example.com";
		let room_b = b"!b:example.com";
		let event = b"$shared:example.com";
		let prev_a = b"$prev-a:example.com";
		let prev_b = b"$prev-b:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);
		adapter
			.write_event_with_edges(room_a, event, b"in-a", &[prev_a], &[])
			.unwrap();
		adapter
			.write_event_with_edges(room_b, event, b"in-b", &[prev_b], &[])
			.unwrap();

		assert_eq!(adapter.get_event(room_a, event).unwrap().as_deref(), Some(&b"in-a"[..]));
		assert_eq!(adapter.get_event(room_b, event).unwrap().as_deref(), Some(&b"in-b"[..]));
		assert_eq!(adapter.get_prev_events(room_a, event).unwrap(), vec![prev_a.to_vec()]);
		assert_eq!(adapter.get_prev_events(room_b, event).unwrap(), vec![prev_b.to_vec()]);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn event_in_one_room_is_invisible_to_another() {
		let path = test_path("invisible");
		let _ = fs::remove_dir_all(&path);
		let event = b"$event:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);
		adapter
			.write_event_with_edges(b"!a:example.com", event, b"body", &[b"$p:example.com"], &[])
			.unwrap();

		assert!(
			adapter
				.get_event(b"!b:example.com", event)
				.unwrap()
				.is_none()
		);
		assert!(
			adapter
				.get_prev_events(b"!b:example.com", event)
				.unwrap()
				.is_empty()
		);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn edges_and_locator_survive_restart() {
		let path = test_path("restart");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let event = b"$event:example.com";
		let prev = b"$prev:example.com";
		let auth = b"$auth:example.com";

		let () = {
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = EventAdapter::new(store.clone());
			adapter
				.write_event_with_edges(room, event, b"body", &[prev], &[auth])
				.unwrap();
			store.sync(ShardType::EventDag).unwrap();
			store.sync(ShardType::Edges).unwrap();
		};

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);
		assert_eq!(adapter.locate_event(event).unwrap().as_deref(), Some(&room[..]));
		assert_eq!(adapter.get_event(room, event).unwrap().as_deref(), Some(&b"body"[..]));
		assert_eq!(adapter.get_prev_events(room, event).unwrap(), vec![prev.to_vec()]);
		assert_eq!(adapter.get_auth_chain(room, event).unwrap(), vec![auth.to_vec()]);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn locator_collision_is_a_miss_on_read_and_an_error_on_write() {
		let path = test_path("collision");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let event = b"$event:example.com";
		let other = b"$other:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store.clone());

		// Forge a locator for a different event under this event's record id,
		// as a 128-bit collision would produce.
		let node = event_record_id(event);
		let forged = EventAdapter::encode_locator(other, b"!elsewhere:example.com").unwrap();
		let tx = store.begin_transaction();
		tx.put(ShardType::EventDag, event_locator_collection_id(&node), node, &forged)
			.unwrap();
		tx.commit().unwrap();

		assert!(adapter.locate_event(event).unwrap().is_none());
		assert!(
			adapter
				.write_event_with_edges(room, event, b"body", &[], &[])
				.is_err()
		);
		assert!(adapter.get_event(room, event).unwrap().is_none());

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn rewriting_the_same_event_is_not_a_collision() {
		let path = test_path("rewrite");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let event = b"$event:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);
		adapter
			.write_event_with_edges(room, event, b"one", &[], &[])
			.unwrap();
		adapter
			.write_event_with_edges(room, event, b"two", &[], &[])
			.unwrap();
		assert_eq!(adapter.locate_event(event).unwrap().as_deref(), Some(&room[..]));
		assert_eq!(adapter.get_event(room, event).unwrap().as_deref(), Some(&b"two"[..]));

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn locator_value_round_trips_and_rejects_garbage() {
		let value = EventAdapter::encode_locator(b"$e:x", b"!r:x").unwrap();
		let (event, room) = EventAdapter::decode_locator(&value).unwrap();
		assert_eq!(event, b"$e:x");
		assert_eq!(room, b"!r:x");

		EventAdapter::decode_locator(&[]).unwrap_err();
		EventAdapter::decode_locator(&[9, 0, 0, 0, 0]).unwrap_err();
		EventAdapter::decode_locator(&[LOCATOR_VERSION, 255, 0, 0, 0, 1]).unwrap_err();
		// Legacy unversioned value: a bare room id starting with '!'.
		EventAdapter::decode_locator(b"!room:example.com").unwrap_err();
	}
}
