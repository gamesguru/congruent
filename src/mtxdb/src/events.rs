//! Domain adapter for Matrix event bodies and DAG edge records.

use std::{collections::HashMap, sync::Arc};

use mtxdb::{
	ShardType,
	interop::{
		decode_edges, decode_event_locator, encode_edges, encode_event_locator, event_record_id,
	},
};
use rezzy::bitmap::Bitmap;

use crate::{
	StorageError,
	keys::{
		EVENT_LOCATOR_BUCKETS, derive_id, event_locator_collection_id, room_auth_collection_id,
		room_event_collection_id, room_prev_collection_id,
	},
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

const MAX_EVENT_WRITE_RETRIES: u16 = 256;

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

	/// Whether any event locator exists. Used by startup to distinguish a fresh
	/// mtxdb from an interrupted import whose completion marker was not written.
	pub fn has_any_events(&self) -> Result<bool, StorageError> {
		for bucket in 0..EVENT_LOCATOR_BUCKETS {
			let mut node = [0_u8; 16];
			node[..4].copy_from_slice(&bucket.to_le_bytes());
			if self
				.store
				.has_records(ShardType::EventDag, &event_locator_collection_id(&node))?
			{
				return Ok(true);
			}
		}

		Ok(false)
	}

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
		encode_event_locator(event_id, room_id)
	}

	fn decode_locator(bytes: &[u8]) -> Result<(&[u8], &[u8]), StorageError> {
		decode_event_locator(bytes)
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

	pub fn get_auth_events(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		self.get_edges(Self::auth_collection(room_id), event_id)
	}

	/// Return auth edges when a row exists, distinguishing an empty edge list
	/// from a missing row so callers can use their compatibility fallback.
	pub fn get_auth_events_if_present(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
		self.store
			.get(ShardType::Edges, &Self::auth_collection(room_id), event_record_id(event_id))?
			.map(|bytes| Self::deserialize_edges(&bytes))
			.transpose()
	}

	/// The full auth chain of `event_id`: every event reachable through auth
	/// edges, excluding `event_id` itself, each exactly once. Order is
	/// unspecified.
	///
	/// Served from the persisted closure bitmap when one exists, which is one
	/// read plus one batched id lookup. Falls back to
	/// [`Self::walk_auth_chain`] when the event, or any ancestor it was written
	/// before, has no closure yet.
	pub fn get_auth_chain(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		if let Some(chain) = self.persisted_auth_chain(room_id, event_id)? {
			return Ok(chain);
		}
		self.walk_auth_chain(room_id, event_id)
	}

	/// The auth chain recomputed from the direct auth edges, one batched read
	/// per level of the walk. This is the reference the persisted closure must
	/// agree with.
	pub fn walk_auth_chain(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		let collection = Self::auth_collection(room_id);
		// Keyed by the full event id: a 128-bit record id could in principle
		// collide and silently drop an ancestor.
		let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
		seen.insert(event_id.to_vec());
		let mut chain = Vec::new();
		let mut frontier = vec![event_id.to_vec()];
		while !frontier.is_empty() {
			let nodes: Vec<NodeId> = frontier.iter().map(|id| event_record_id(id)).collect();
			let values = self.store.get_many(ShardType::Edges, &collection, &nodes)?;
			let mut next = Vec::new();
			for bytes in values.into_iter().flatten() {
				for auth in Self::deserialize_edges(&bytes)? {
					if seen.insert(auth.clone()) {
						chain.push(auth.clone());
						next.push(auth);
					}
				}
			}
			frontier = next;
		}
		Ok(chain)
	}

	/// The persisted closure for `event_id`, or `None` if it was never written.
	pub fn persisted_auth_chain(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
		let collection = Self::auth_collection(room_id);
		let Some(Some(short)) = self
			.committed_short_ids(&collection, &[event_id])?
			.first()
			.copied()
		else {
			return Ok(None);
		};
		let Some(bytes) = self
			.store
			.get(ShardType::Edges, &collection, closure_node(short))?
		else {
			return Ok(None);
		};
		let closure = Bitmap::decode(&bytes)
			.map_err(|e| StorageError::Corrupt(format!("auth closure bitmap: {e}")))?;
		let shorts: Vec<u32> = closure.iter().collect();
		let nodes: Vec<NodeId> = shorts.iter().map(|short| reverse_node(*short)).collect();
		let ids = self.store.get_many(ShardType::Edges, &collection, &nodes)?;
		ids.into_iter()
			.map(|id| {
				id.ok_or_else(|| {
					StorageError::Corrupt("auth closure short id has no event".to_owned())
				})
			})
			.collect::<Result<Vec<_>, _>>()
			.map(Some)
	}

	/// Committed short ids for events in a room's auth collection. A forward
	/// record stores its event id, so a 128-bit key collision reads as a miss.
	fn committed_short_ids(
		&self,
		collection: &CollectionId,
		event_ids: &[&[u8]],
	) -> Result<Vec<Option<u32>>, StorageError> {
		let nodes: Vec<NodeId> = event_ids.iter().map(|id| forward_node(id)).collect();
		let values = self.store.get_many(ShardType::Edges, collection, &nodes)?;
		event_ids
			.iter()
			.zip(values)
			.map(|(event_id, value)| {
				value.map_or(Ok(None), |bytes| {
					let (short, stored) = decode_forward(&bytes)?;
					Ok((stored == *event_id).then_some(short))
				})
			})
			.collect()
	}

	/// Stage the event's closure: short ids for it and its auth events, plus the
	/// union of the auth events' closures.
	///
	/// The closure is written only when every auth event already has one, since
	/// a missing ancestor would make it silently incomplete; readers fall back
	/// to the walk. Allocating short ids advances a per-room counter, so the
	/// commit is made conditional on the collection being unchanged and
	/// concurrent writers to one room fail with [`StorageError::StaleRead`].
	fn stage_closure(
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		event_id: &[u8],
		auth_events: &[&[u8]],
	) -> Result<(), StorageError> {
		let collection = Self::auth_collection(room_id);
		let mut ids: Vec<&[u8]> = vec![event_id];
		ids.extend_from_slice(auth_events);
		let mut read_nodes = Vec::with_capacity(ids.len().saturating_add(1));
		read_nodes.push(counter_node());
		read_nodes.extend(ids.iter().map(|id| forward_node(id)));
		let (values, version) =
			tx.get_with_collection_version(ShardType::Edges, collection, &read_nodes)?;
		let start = values
			.first()
			.and_then(Option::as_ref)
			.map_or(Ok(0), |bytes| decode_u32(bytes))?;
		let mut next = start;
		let committed = values
			.into_iter()
			.skip(1)
			.zip(ids.iter())
			.map(|(value, event_id)| {
				value.map_or(Ok(None), |bytes| {
					let (short, stored) = decode_forward(&bytes)?;
					Ok((stored == *event_id).then_some(short))
				})
			})
			.collect::<Result<Vec<_>, StorageError>>()?;
		let mut staged: HashMap<Vec<u8>, u32> = HashMap::new();
		let mut shorts = Vec::with_capacity(ids.len());
		for (id, found) in ids.iter().zip(committed) {
			if let Some(short) = found.or_else(|| staged.get(*id).copied()) {
				shorts.push(short);
				continue;
			}
			next = next
				.checked_add(1)
				.ok_or_else(|| StorageError::Corrupt("room short ids exhausted".to_owned()))?;
			staged.insert(id.to_vec(), next);
			tx.put(ShardType::Edges, collection, forward_node(id), &encode_forward(next, id)?)?;
			tx.put(ShardType::Edges, collection, reverse_node(next), id)?;
			shorts.push(next);
		}

		let Some((&own_short, auth_shorts)) = shorts.split_first() else {
			return Err(StorageError::Corrupt("no short id for event".to_owned()));
		};
		let nodes: Vec<NodeId> = auth_shorts
			.iter()
			.map(|short| closure_node(*short))
			.collect();
		let closures = tx.get_many(ShardType::Edges, collection, &nodes)?;
		let mut closure = Bitmap::new();
		let mut complete = true;
		for (short, stored) in auth_shorts.iter().zip(closures) {
			closure.insert(*short);
			match stored {
				| Some(bytes) => {
					closure |= &Bitmap::decode(&bytes).map_err(|e| {
						StorageError::Corrupt(format!("auth closure bitmap: {e}"))
					})?;
				},
				| None => complete = false,
			}
		}
		if complete {
			// An encoded bitmap is never empty (it carries a header), so a
			// leaf event's empty closure is not mistaken for a removed record.
			tx.put(ShardType::Edges, collection, closure_node(own_short), &closure.encode())?;
		} else {
			// Replacing the event's auth edges can invalidate an existing
			// closure. Remove it rather than leaving readers a stale answer;
			// they fall back to walking the new edges.
			tx.remove(ShardType::Edges, collection, closure_node(own_short))?;
		}
		if next != start {
			tx.put(ShardType::Edges, collection, counter_node(), &next.to_be_bytes())?;
		}
		tx.expect_collection_version(ShardType::Edges, collection, version)?;
		Ok(())
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
		Self::stage_closure(tx, room_id, event_id, auth_events)?;
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
		// Short-id allocation makes the commit conditional on the room's auth
		// collection being unchanged; a concurrent writer fails it and the
		// whole write is redone against the new version. Reads made while
		// staging can also be blocked when the collection keeps advancing, so
		// retry the complete transaction for those transient errors too.
		let mut attempts = 0_u16;
		loop {
			let tx = self.store.begin_transaction();
			let staged = self.put_event_with_edges(
				&tx,
				room_id,
				event_id,
				event_body,
				prev_events,
				auth_events,
			);
			let result = match staged {
				| Ok(()) => tx.commit(),
				| Err(error) => Err(error),
			};
			match result {
				| Err(error)
					if (error.is_stale_read() || error.is_would_block())
						&& attempts < MAX_EVENT_WRITE_RETRIES =>
				{
					attempts = attempts
						.checked_add(1)
						.expect("attempts is less than the retry limit");
					std::thread::sleep(std::time::Duration::from_millis(2));
				},
				| result => return result,
			}
		}
	}

	fn serialize_edges(edges: &[&[u8]]) -> Result<Vec<u8>, StorageError> { encode_edges(edges) }

	fn deserialize_edges(bytes: &[u8]) -> Result<Vec<Vec<u8>>, StorageError> {
		decode_edges(bytes)
	}
}

fn counter_node() -> NodeId { derive_id(b"authchain:counter", b"") }

fn forward_node(event_id: &[u8]) -> NodeId { derive_id(b"authchain:fwd", event_id) }

fn reverse_node(short: u32) -> NodeId { derive_id(b"authchain:rev", &short.to_be_bytes()) }

fn closure_node(short: u32) -> NodeId { derive_id(b"authchain:closure", &short.to_be_bytes()) }

fn decode_u32(bytes: &[u8]) -> Result<u32, StorageError> {
	bytes
		.try_into()
		.map(u32::from_be_bytes)
		.map_err(|_| StorageError::Corrupt("malformed auth counter".to_owned()))
}

/// `[u32 BE short id][event id]`: the event id lets a reader reject a record
/// that merely shares the 128-bit key.
fn encode_forward(short: u32, event_id: &[u8]) -> Result<Vec<u8>, StorageError> {
	let mut out = Vec::with_capacity(event_id.len().saturating_add(4));
	out.extend_from_slice(&short.to_be_bytes());
	out.extend_from_slice(event_id);
	Ok(out)
}

fn decode_forward(bytes: &[u8]) -> Result<(u32, &[u8]), StorageError> {
	let (short, event) = bytes
		.split_at_checked(4)
		.ok_or_else(|| StorageError::Corrupt("malformed auth short-id record".to_owned()))?;
	Ok((decode_u32(short)?, event))
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
			assert!(!adapter.has_any_events().unwrap());
			let tx = store.begin_transaction();
			adapter
				.put_event_with_edges(&tx, room_id, event_id, b"body", &[prev], &[auth])
				.unwrap();
			assert!(adapter.get_event(room_id, event_id).unwrap().is_none());
			assert!(!adapter.has_any_events().unwrap());
			tx.commit().unwrap();
			assert!(adapter.has_any_events().unwrap());
			assert_eq!(
				adapter.get_event(room_id, event_id).unwrap().as_deref(),
				Some(&b"body"[..])
			);
			assert_eq!(adapter.get_prev_events(room_id, event_id).unwrap(), vec![prev.to_vec()]);
			assert_eq!(adapter.get_auth_events(room_id, event_id).unwrap(), vec![auth.to_vec()]);
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
					.get_auth_events(room_id, event_id)
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

	use mtxdb::interop::LOCATOR_VERSION;

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
		assert_eq!(adapter.get_auth_events(room, event).unwrap(), vec![auth.to_vec()]);

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
	/// The importer writes these ids through `mtxdb::interop`; the server must
	/// read exactly them. The values are pinned in mtxdb's own tests too.
	#[test]
	fn record_ids_match_the_importer_layout() {
		let hex = |bytes: &[u8]| {
			use std::fmt::Write as _;
			bytes.iter().fold(String::new(), |mut out, byte| {
				let _ = write!(out, "{byte:02x}");
				out
			})
		};
		let room = b"!room:example.com";
		let node = event_record_id(b"$event:example.com");
		assert_eq!(hex(&node), "d3930850aae2d72b92a1d07108dcaa7f");
		assert_eq!(hex(&room_event_collection_id(room)), "9507be45c492c4382edb18fc074477cc");
		assert_eq!(hex(&room_prev_collection_id(room)), "fea9a58f0ab869317d696093fd7c6173");
		assert_eq!(hex(&room_auth_collection_id(room)), "4842a2eff3cf511f9e48406350253c77");
		assert_eq!(hex(&event_locator_collection_id(&node)), "8630e25f0c7e1a94558485e7176a1bda");
	}

	#[test]
	fn auth_chain_is_the_deduplicated_transitive_closure() {
		let path = test_path("auth-chain");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);

		// create <- power <- join <- message, and message also cites create
		// directly, so create is reachable by two paths.
		let (create, power, join, message) =
			(&b"$create"[..], &b"$power"[..], &b"$join"[..], &b"$message"[..]);
		adapter
			.write_event_with_edges(room, create, b"c", &[], &[])
			.unwrap();
		adapter
			.write_event_with_edges(room, power, b"p", &[], &[create])
			.unwrap();
		adapter
			.write_event_with_edges(room, join, b"j", &[], &[create, power])
			.unwrap();
		adapter
			.write_event_with_edges(room, message, b"m", &[], &[join, create])
			.unwrap();

		let mut chain = adapter.get_auth_chain(room, message).unwrap();
		chain.sort();
		assert_eq!(chain, vec![create.to_vec(), join.to_vec(), power.to_vec()]);
		assert!(adapter.get_auth_chain(room, create).unwrap().is_empty());
		assert!(
			adapter
				.get_auth_chain(room, b"$unknown")
				.unwrap()
				.is_empty()
		);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}
	#[test]
	fn persisted_closure_matches_the_walk() {
		let path = test_path("closure-persisted");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);

		let (create, power, join, message) =
			(&b"$create"[..], &b"$power"[..], &b"$join"[..], &b"$message"[..]);
		adapter
			.write_event_with_edges(room, create, b"c", &[], &[])
			.unwrap();
		adapter
			.write_event_with_edges(room, power, b"p", &[], &[create])
			.unwrap();
		adapter
			.write_event_with_edges(room, join, b"j", &[], &[create, power])
			.unwrap();
		adapter
			.write_event_with_edges(room, message, b"m", &[], &[join, create])
			.unwrap();

		for event in [create, power, join, message] {
			let mut persisted = adapter
				.persisted_auth_chain(room, event)
				.unwrap()
				.expect("closure persisted for every event written in order");
			let mut walked = adapter.walk_auth_chain(room, event).unwrap();
			persisted.sort();
			walked.sort();
			assert_eq!(persisted, walked);
		}
		// A leaf's closure is empty but still present.
		assert_eq!(adapter.persisted_auth_chain(room, create).unwrap(), Some(Vec::new()));

		// Rewriting an event reuses its short id and leaves the closure intact.
		adapter
			.write_event_with_edges(room, message, b"m", &[], &[join, create])
			.unwrap();
		let mut again = adapter.get_auth_chain(room, message).unwrap();
		again.sort();
		assert_eq!(again, vec![create.to_vec(), join.to_vec(), power.to_vec()]);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn out_of_order_write_has_no_closure_and_falls_back_to_the_walk() {
		let path = test_path("closure-out-of-order");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);

		// `child` arrives before its auth event `parent` is written.
		adapter
			.write_event_with_edges(room, b"$child", b"c", &[], &[b"$parent"])
			.unwrap();
		assert_eq!(adapter.persisted_auth_chain(room, b"$child").unwrap(), None);
		assert_eq!(adapter.get_auth_chain(room, b"$child").unwrap(), vec![b"$parent".to_vec()]);

		adapter
			.write_event_with_edges(room, b"$parent", b"p", &[], &[])
			.unwrap();
		assert_eq!(adapter.get_auth_chain(room, b"$child").unwrap(), vec![b"$parent".to_vec()]);
		assert_eq!(adapter.persisted_auth_chain(room, b"$parent").unwrap(), Some(Vec::new()));

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn closures_are_scoped_per_room() {
		let path = test_path("closure-rooms");
		let _ = fs::remove_dir_all(&path);
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);

		adapter
			.write_event_with_edges(b"!a:example.com", b"$a1", b"1", &[], &[])
			.unwrap();
		adapter
			.write_event_with_edges(b"!a:example.com", b"$a2", b"2", &[], &[b"$a1"])
			.unwrap();
		adapter
			.write_event_with_edges(b"!b:example.com", b"$b1", b"1", &[], &[])
			.unwrap();
		assert_eq!(adapter.get_auth_chain(b"!a:example.com", b"$a2").unwrap(), vec![
			b"$a1".to_vec()
		]);
		assert!(
			adapter
				.get_auth_chain(b"!b:example.com", b"$a2")
				.unwrap()
				.is_empty()
		);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}
}
