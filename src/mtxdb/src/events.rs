//! Domain adapter for Matrix event bodies and DAG edge records.
//!
//! Event bodies and the global event locator live in the `EventDag` pool.
//! `prev`/`auth` adjacency and the auth closure live in mtxdb's CSR graph store
//! (`<db_root>/graph/state/`, outside the shard packs), addressed through
//! [`CsrAdjacency`] and [`RoomAuth`]. CSR writes use their own transaction, so
//! an event's edges are recorded before
//! its body: a body is the commit marker, and re-recording identical edges is
//! a no-op, which makes an interrupted write safe to repeat.

use std::{collections::HashSet, sync::Arc};

use mtxdb::{
	ShardType,
	csr_adjacency::{CsrAdjacency, EventRecord},
	interop::{decode_event_locator, encode_event_locator, event_record_id},
	room_auth::{MAX_BATCH_EVENTS, RoomAuth, RoomAuthError},
};

use crate::{
	StorageError,
	keys::{EVENT_LOCATOR_BUCKETS, event_locator_collection_id, room_event_collection_id},
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

/// An event ID paired with the ID of the room that owns it.
pub type EventLocator = (Vec<u8>, Vec<u8>);

/// How many times a CSR write is retried after losing a version race.
const MAX_STALE_RETRIES: u8 = 8;

#[derive(Clone)]
pub struct EventAdapter {
	store: Arc<MtxdbStore>,
}

/// One event in a batch written by
/// [`EventAdapter::write_events_with_edges`].
pub struct EventWrite<'a> {
	pub event_id: &'a [u8],
	pub event_body: &'a [u8],
	pub prev_events: &'a [&'a [u8]],
	pub auth_events: &'a [&'a [u8]],
}

impl EventAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	fn evnt_collection(room_id: &[u8]) -> CollectionId { room_event_collection_id(room_id) }

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

	/// Look up an event ID and room ID by the event's 128-bit node ID using the
	/// global event locator.
	pub fn locate_event_by_node(
		&self,
		node_id: &[u8; 16],
	) -> Result<Option<EventLocator>, StorageError> {
		self.store
			.get(ShardType::EventDag, &event_locator_collection_id(node_id), *node_id)?
			.map_or(Ok(None), |bytes| {
				let (stored_event, room) = Self::decode_locator(&bytes)?;
				Ok(Some((stored_event.to_vec(), room.to_vec())))
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
		Ok(CsrAdjacency::new(utf8(room_id, "room id")?)
			.prev_of(self.store.database(), utf8(event_id, "event id")?)?
			.map_or_else(Vec::new, edge_bytes))
	}

	pub fn get_auth_events(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		Ok(self
			.get_auth_events_if_present(room_id, event_id)?
			.unwrap_or_default())
	}

	/// Return auth edges when the event was recorded, distinguishing an empty
	/// edge list from a missing record. A missing record is an incomplete
	/// import, not an event without auth events.
	pub fn get_auth_events_if_present(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
		Ok(CsrAdjacency::new(utf8(room_id, "room id")?)
			.auth_of(self.store.database(), utf8(event_id, "event id")?)?
			.map(edge_bytes))
	}

	/// The full auth chain of `event_id`: every event reachable through auth
	/// edges, excluding `event_id` itself, each exactly once. Order is
	/// unspecified.
	///
	/// Served by [`RoomAuth`] (published closure generation plus the newer
	/// tail). Falls back to [`Self::walk_auth_chain`] when the chain reaches an
	/// event that was never recorded, so a partially imported room still
	/// reports the ancestors it knows about.
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

	/// The auth chain recomputed from the direct auth edges, one lookup per
	/// event. This is the reference [`RoomAuth`] must agree with, and tolerates
	/// ancestors that were never recorded (they appear in the chain but are not
	/// expanded).
	pub fn walk_auth_chain(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Vec<Vec<u8>>, StorageError> {
		let adjacency = CsrAdjacency::new(utf8(room_id, "room id")?);
		let db = self.store.database();
		let start = utf8(event_id, "event id")?;
		let mut seen: HashSet<String> = HashSet::new();
		seen.insert(start.to_owned());
		let mut chain = Vec::new();
		let mut frontier = vec![start.to_owned()];
		while !frontier.is_empty() {
			let mut next = Vec::new();
			for id in &frontier {
				for auth in adjacency.auth_of(db, id)?.unwrap_or_default() {
					if seen.insert(auth.clone()) {
						chain.push(auth.clone());
						next.push(auth);
					}
				}
			}
			frontier = next;
		}
		Ok(edge_bytes(chain))
	}

	/// Whether the stored event body and auth-edge record already match this
	/// write.
	///
	/// Outliers are written to mtxdb by more than one path (the outlier store
	/// and a verified batch in `fetch_state`); callers use this to avoid
	/// re-committing an identical event, while still repairing an interrupted
	/// write or replacing a redacted body with a pristine copy.
	///
	/// Edges are recorded before the body, so an existing body proves the
	/// edge record was written too; the edge check guards a body without one.
	pub fn event_matches_with_edges(
		&self,
		room_id: &[u8],
		event_id: &[u8],
		event_body: &[u8],
	) -> Result<bool, StorageError> {
		let Some(stored) = self.get_event(room_id, event_id)? else {
			return Ok(false);
		};
		Ok(stored.as_slice() == event_body
			&& self
				.get_auth_events_if_present(room_id, event_id)?
				.is_some())
	}

	/// The auth chain of `event_id` if it can be computed from recorded
	/// events, or `None` if the event, or an ancestor on its chain, was never
	/// recorded.
	pub fn persisted_auth_chain(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
		let room = RoomAuth::new(utf8(room_id, "room id")?);
		match room.auth_chain(self.store.database(), utf8(event_id, "event id")?) {
			| Ok(chain) => Ok(Some(edge_bytes(chain))),
			| Err(
				RoomAuthError::UnknownEvent { .. }
				| RoomAuthError::EventNotRecorded { .. }
				| RoomAuthError::MissingParent { .. }
				| RoomAuthError::IncompleteClosure { .. },
			) => Ok(None),
			| Err(error) => Err(auth_error(error)),
		}
	}

	/// Record the events' `prev`/`auth` edges in the room's CSR adjacency.
	///
	/// Each chunk of up to [`MAX_BATCH_EVENTS`] events is one transaction. A
	/// lost version race is retried; recording identical edges again is a
	/// no-op, while different edges for a recorded event are a collision error
	/// because an event's core fields never change.
	fn record_edges(
		&self,
		room_id: &[u8],
		events: &[EventWrite<'_>],
	) -> Result<(), StorageError> {
		let adjacency = CsrAdjacency::new(utf8(room_id, "room id")?);
		for chunk in events.chunks(MAX_BATCH_EVENTS) {
			let mut prevs: Vec<Vec<&str>> = Vec::with_capacity(chunk.len());
			let mut auths: Vec<Vec<&str>> = Vec::with_capacity(chunk.len());
			for event in chunk {
				prevs.push(utf8_all(event.prev_events, "prev event id")?);
				auths.push(utf8_all(event.auth_events, "auth event id")?);
			}
			let mut records = Vec::with_capacity(chunk.len());
			for ((event, prev), auth) in chunk.iter().zip(&prevs).zip(&auths) {
				records.push(EventRecord {
					event_id: utf8(event.event_id, "event id")?,
					prev,
					auth,
					relation: None,
				});
			}
			retry_stale(|| {
				adjacency
					.record_events(self.store.database(), &records)
					.map(drop)
			})?;
		}
		Ok(())
	}

	/// Fail if a committed locator for a different event shares this event's
	/// 128-bit record id; shadowing it would lose that event.
	fn check_locator_collision(&self, event_id: &[u8]) -> Result<(), StorageError> {
		let node = event_record_id(event_id);
		if let Some(existing) =
			self.store
				.get(ShardType::EventDag, &event_locator_collection_id(&node), node)?
		{
			let (stored_event, _) = Self::decode_locator(&existing)?;
			if stored_event != event_id {
				return Err(StorageError::Corrupt(
					"event record id collision in locator".to_owned(),
				));
			}
		}
		Ok(())
	}

	/// Stage the event body and its event -> room locator in `tx`.
	fn stage_body(
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		event_id: &[u8],
		event_body: &[u8],
	) -> Result<(), StorageError> {
		let node = event_record_id(event_id);
		tx.put(ShardType::EventDag, Self::evnt_collection(room_id), node, event_body)?;
		let locator = Self::encode_locator(event_id, room_id)?;
		tx.put(ShardType::EventDag, event_locator_collection_id(&node), node, &locator)?;
		Ok(())
	}

	/// Record the event's edges and stage its body in `tx`.
	///
	/// Only the body and locator are part of `tx`: the edges are recorded
	/// immediately in their own transaction (see the module docs).
	pub fn put_event_with_edges(
		&self,
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		event_id: &[u8],
		event_body: &[u8],
		prev_events: &[&[u8]],
		auth_events: &[&[u8]],
	) -> Result<(), StorageError> {
		self.check_locator_collision(event_id)?;
		self.record_edges(room_id, &[EventWrite {
			event_id,
			event_body,
			prev_events,
			auth_events,
		}])?;
		Self::stage_body(tx, room_id, event_id, event_body)
	}

	/// Writes an event body and its direct DAG/auth edges. The edges are
	/// recorded first and the body commits last, so a failure between the two
	/// leaves a state that repeating the write repairs.
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

	/// Write a batch of one room's events and edges.
	///
	/// All edges are recorded first (one transaction per [`MAX_BATCH_EVENTS`]),
	/// then every body and locator commits in a single transaction.
	pub fn write_events_with_edges(
		&self,
		room_id: &[u8],
		events: &[EventWrite<'_>],
	) -> Result<(), StorageError> {
		for event in events {
			self.check_locator_collision(event.event_id)?;
		}
		self.record_edges(room_id, events)?;
		let tx = self.store.begin_transaction();
		for event in events {
			Self::stage_body(&tx, room_id, event.event_id, event.event_body)?;
		}
		tx.commit()
	}
}

/// View `bytes` as UTF-8: CSR addresses rooms and events by string id.
fn utf8<'a>(bytes: &'a [u8], what: &str) -> Result<&'a str, StorageError> {
	std::str::from_utf8(bytes).map_err(|_| StorageError::Corrupt(format!("{what} is not UTF-8")))
}

fn utf8_all<'a>(ids: &[&'a [u8]], what: &str) -> Result<Vec<&'a str>, StorageError> {
	ids.iter().map(|id| utf8(id, what)).collect()
}

fn edge_bytes(ids: Vec<String>) -> Vec<Vec<u8>> {
	ids.into_iter().map(String::into_bytes).collect()
}

fn auth_error(error: RoomAuthError) -> StorageError {
	match error {
		| RoomAuthError::Storage(error) => error,
		| other => StorageError::Corrupt(other.to_string()),
	}
}

/// Run `op`, retrying when it loses a collection-version race.
fn retry_stale<T>(mut op: impl FnMut() -> Result<T, StorageError>) -> Result<T, StorageError> {
	let mut retries = 0_u8;
	loop {
		match op() {
			| Err(StorageError::StaleRead { .. }) if retries < MAX_STALE_RETRIES => {
				retries = retries.saturating_add(1);
			},
			| result => return result,
		}
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
	fn body_commits_after_edges_and_survives_restart() {
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
			store.sync(ShardType::State).unwrap();
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
				.put_event_with_edges(&tx, room_id, event_id, b"updated", &[prev], &[auth])
				.unwrap();
			tx.commit().unwrap();
			assert_eq!(
				adapter.get_event(room_id, event_id).unwrap().as_deref(),
				Some(&b"updated"[..])
			);
			// An event's edges never change: the same edges are a no-op ...
			assert_eq!(adapter.get_prev_events(room_id, event_id).unwrap(), vec![prev.to_vec()]);
			assert_eq!(adapter.get_auth_events(room_id, event_id).unwrap(), vec![auth.to_vec()]);
			// ... and different ones are rejected rather than overwritten.
			let tx = store.begin_transaction();
			assert!(
				adapter
					.put_event_with_edges(&tx, room_id, event_id, b"x", &[], &[])
					.is_err()
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
			store.sync(ShardType::State).unwrap();
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

/// Concurrent same-room writers serialize inside mtxdb's CSR store (with a
/// bounded retry here for a lost version race): no caller-side lock is needed
/// and none of them sees a conflict.
#[cfg(test)]
mod contention_tests {
	use std::{
		fs,
		sync::{Arc, Barrier},
		thread,
	};

	use super::*;

	const THREADS: usize = 8;
	const PER_THREAD: usize = 25;

	#[test]
	fn same_room_writers_do_not_conflict() {
		let room: &[u8] = b"!contended:example.com";
		let path = std::env::temp_dir()
			.join(format!("conduwuit-mtxdb-contention-{}", std::process::id()));
		let _ = fs::remove_dir_all(&path);
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = EventAdapter::new(store);

		let barrier = Arc::new(Barrier::new(THREADS));
		// Spawn every writer into the barrier before joining any of them.
		let mut handles = Vec::with_capacity(THREADS);
		for t in 0..THREADS {
			let adapter = adapter.clone();
			let barrier = barrier.clone();
			handles.push(thread::spawn(move || {
				barrier.wait();
				for i in 0..PER_THREAD {
					let id = format!("${t}-{i}:example.com");
					// An auth event makes the CSR record allocate a short id.
					let auth = format!("$auth-{t}:example.com");
					adapter
						.write_event_with_edges(room, id.as_bytes(), b"body", &[], &[
							auth.as_bytes()
						])
						.expect("same-room writers must serialize, not conflict");
				}
			}));
		}
		for handle in handles {
			handle.join().unwrap();
		}

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}
}
