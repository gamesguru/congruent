//! Domain adapter for short ids: the `u64` handles HAMT leaves use for event
//! ids and `(type, state_key)` pairs.
//!
//! Four maps (event id <-> short event id, state key <-> short state key) and
//! one allocation counter live in the `ServerInfo` shard. Every allocation
//! commits its mappings and the advanced counter in one transaction, so a
//! crash cannot leave the counter behind a mapped id. The adapter is the only
//! allocator: the server and the offline importer both allocate through it, so
//! the ids in imported HAMT leaves are the ids the server resolves.
//!
//! Ids are handed out as `counter + 1`, so `0` is never a valid short id.

use std::{
	collections::HashMap,
	sync::{Arc, Mutex, PoisonError},
};

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::{EVENT_LOCATOR_BUCKETS, derive_id, encode_state_key_v1, event_record_id},
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

const SHORT: usize = size_of::<u64>();

#[derive(Clone, Copy)]
enum Map {
	EventForward,
	EventReverse,
	StateKeyForward,
	StateKeyReverse,
}

impl Map {
	const fn name(self) -> &'static str {
		match self {
			| Self::EventForward => "short-eid",
			| Self::EventReverse => "short-eid-rev",
			| Self::StateKeyForward => "short-sk",
			| Self::StateKeyReverse => "short-sk-rev",
		}
	}

	/// Bucketed like the verdict collections, so no single collection holds
	/// every record of a map.
	fn collection(self, node: &NodeId) -> CollectionId {
		let bucket =
			u32::from_le_bytes([node[0], node[1], node[2], node[3]]) % EVENT_LOCATOR_BUCKETS;
		derive_id(b"sys", format!("{}:{bucket}", self.name()).as_bytes())
	}
}

fn counter_collection() -> CollectionId { derive_id(b"sys", b"short-counter") }

fn counter_node() -> NodeId { derive_id(b"short", b"counter") }

fn forward_event_node(event_id: &[u8]) -> NodeId { event_record_id(event_id) }

fn reverse_node(domain: &[u8], short: u64) -> NodeId { derive_id(domain, &short.to_be_bytes()) }

fn forward_state_key_node(encoded: &[u8]) -> NodeId { derive_id(b"short-statekey", encoded) }

fn with_prefix(short: u64, tail: &[u8]) -> Vec<u8> {
	let mut value = Vec::with_capacity(SHORT.saturating_add(tail.len()));
	value.extend_from_slice(&short.to_be_bytes());
	value.extend_from_slice(tail);
	value
}

fn split_prefix(bytes: &[u8]) -> Result<(u64, &[u8]), StorageError> {
	let (head, tail) = bytes
		.split_first_chunk::<SHORT>()
		.ok_or_else(|| StorageError::Corrupt("short id record is truncated".to_owned()))?;
	Ok((u64::from_be_bytes(*head), tail))
}

fn decode_state_key(encoded: &[u8]) -> Result<(String, String), StorageError> {
	let corrupt = || StorageError::Corrupt("state key record is malformed".to_owned());
	let Some((1, rest)) = encoded.split_first() else {
		return Err(corrupt());
	};
	let (type_len, rest) = rest.split_first_chunk::<4>().ok_or_else(corrupt)?;
	let type_len = usize::try_from(u32::from_le_bytes(*type_len)).map_err(|_| corrupt())?;
	let (event_type, rest) = rest.split_at_checked(type_len).ok_or_else(corrupt)?;
	let (key_len, rest) = rest.split_first_chunk::<4>().ok_or_else(corrupt)?;
	let key_len = usize::try_from(u32::from_le_bytes(*key_len)).map_err(|_| corrupt())?;
	if rest.len() != key_len {
		return Err(corrupt());
	}
	Ok((
		String::from_utf8(event_type.to_vec()).map_err(|_| corrupt())?,
		String::from_utf8(rest.to_vec()).map_err(|_| corrupt())?,
	))
}

#[derive(Clone)]
pub struct ShortIdAdapter {
	store: Arc<MtxdbStore>,
	/// The highest allocated short id. Held across an allocation so concurrent
	/// allocators cannot hand out the same id; advanced only after the
	/// allocation committed.
	counter: Arc<Mutex<u64>>,
}

impl ShortIdAdapter {
	/// Open the adapter, resuming the persisted counter.
	pub fn new(store: Arc<MtxdbStore>) -> Result<Self, StorageError> {
		let counter =
			match store.get(ShardType::ServerInfo, &counter_collection(), counter_node())? {
				| Some(bytes) =>
					u64::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
						StorageError::Corrupt("short id counter record is malformed".to_owned())
					})?),
				| None => 0,
			};
		Ok(Self {
			store,
			counter: Arc::new(Mutex::new(counter)),
		})
	}

	/// The highest short id allocated so far.
	#[must_use]
	pub fn last_allocated(&self) -> u64 {
		*self.counter.lock().unwrap_or_else(PoisonError::into_inner)
	}

	fn read(&self, map: Map, node: NodeId) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(ShardType::ServerInfo, &map.collection(&node), node)
	}

	/// The short id of an event, or `None` when none was allocated. A record
	/// stored for a different event under the same 128-bit record id is a miss.
	pub fn get_event(&self, event_id: &[u8]) -> Result<Option<u64>, StorageError> {
		self.read(Map::EventForward, forward_event_node(event_id))?
			.map_or(Ok(None), |bytes| {
				let (short, stored) = split_prefix(&bytes)?;
				Ok((stored == event_id).then_some(short))
			})
	}

	/// The event id a short id was allocated for.
	pub fn get_event_id(&self, short: u64) -> Result<Option<Vec<u8>>, StorageError> {
		self.read(Map::EventReverse, reverse_node(b"short-eid-rev", short))
	}

	/// The short id of `(event_type, state_key)`, or `None`.
	pub fn get_state_key(
		&self,
		event_type: &str,
		state_key: &str,
	) -> Result<Option<u64>, StorageError> {
		let encoded = encode_state_key_v1(event_type, state_key)
			.map_err(|e| StorageError::Corrupt(e.to_owned()))?;
		self.read(Map::StateKeyForward, forward_state_key_node(&encoded))?
			.map_or(Ok(None), |bytes| {
				let (short, stored) = split_prefix(&bytes)?;
				Ok((stored == encoded).then_some(short))
			})
	}

	/// The `(event_type, state_key)` a short id was allocated for.
	pub fn get_state_key_from_short(
		&self,
		short: u64,
	) -> Result<Option<(String, String)>, StorageError> {
		self.read(Map::StateKeyReverse, reverse_node(b"short-sk-rev", short))?
			.map_or(Ok(None), |bytes| decode_state_key(&bytes).map(Some))
	}

	/// Get or allocate a short id for each event, in one transaction. Ids for
	/// events not yet mapped are assigned in input order; repeating an id in
	/// `event_ids` yields the same short id. The result matches `event_ids`
	/// positionally.
	pub fn allocate_events(&self, event_ids: &[&[u8]]) -> Result<Vec<u64>, StorageError> {
		let mut counter = self.counter.lock().unwrap_or_else(PoisonError::into_inner);
		let mut resolved: HashMap<&[u8], u64> = HashMap::new();
		let mut next = *counter;
		let tx = self.store.begin_transaction();
		let mut allocated = false;

		for &event_id in event_ids {
			if resolved.contains_key(event_id) {
				continue;
			}
			let node = forward_event_node(event_id);
			let short = if let Some(bytes) = self.read(Map::EventForward, node)? {
				let (short, stored) = split_prefix(&bytes)?;
				if stored != event_id {
					return Err(StorageError::Corrupt(
						"event record id collision in short ids".to_owned(),
					));
				}
				short
			} else {
				next = next.checked_add(1).ok_or_else(|| {
					StorageError::Corrupt("short id counter overflow".to_owned())
				})?;
				Self::put(&tx, Map::EventForward, node, &with_prefix(next, event_id))?;
				Self::put(&tx, Map::EventReverse, reverse_node(b"short-eid-rev", next), event_id)?;
				allocated = true;
				next
			};
			resolved.insert(event_id, short);
		}

		if allocated {
			Self::put_counter(&tx, next)?;
			tx.commit()?;
			*counter = next;
		}
		Ok(event_ids.iter().map(|id| resolved[id]).collect())
	}

	/// Get or allocate the short id of `(event_type, state_key)`.
	pub fn allocate_state_key(
		&self,
		event_type: &str,
		state_key: &str,
	) -> Result<u64, StorageError> {
		let encoded = encode_state_key_v1(event_type, state_key)
			.map_err(|e| StorageError::Corrupt(e.to_owned()))?;
		let mut counter = self.counter.lock().unwrap_or_else(PoisonError::into_inner);
		let node = forward_state_key_node(&encoded);
		if let Some(bytes) = self.read(Map::StateKeyForward, node)? {
			let (short, stored) = split_prefix(&bytes)?;
			if stored != encoded {
				return Err(StorageError::Corrupt(
					"state key record id collision in short ids".to_owned(),
				));
			}
			return Ok(short);
		}

		let next = counter
			.checked_add(1)
			.ok_or_else(|| StorageError::Corrupt("short id counter overflow".to_owned()))?;
		let tx = self.store.begin_transaction();
		Self::put(&tx, Map::StateKeyForward, node, &with_prefix(next, &encoded))?;
		Self::put(&tx, Map::StateKeyReverse, reverse_node(b"short-sk-rev", next), &encoded)?;
		Self::put_counter(&tx, next)?;
		tx.commit()?;
		*counter = next;
		Ok(next)
	}

	fn put(
		tx: &MtxdbTransaction<'_>,
		map: Map,
		node: NodeId,
		data: &[u8],
	) -> Result<(), StorageError> {
		tx.put(ShardType::ServerInfo, map.collection(&node), node, data)
	}

	fn put_counter(tx: &MtxdbTransaction<'_>, value: u64) -> Result<(), StorageError> {
		tx.put(
			ShardType::ServerInfo,
			counter_collection(),
			counter_node(),
			&value.to_be_bytes(),
		)
	}
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	use super::*;

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir()
			.join(format!("conduwuit-mtxdb-shortids-{name}-{}", std::process::id()))
	}

	#[test]
	fn event_ids_allocate_once_resolve_both_ways_and_survive_restart() {
		let path = test_path("events");
		let _ = fs::remove_dir_all(&path);
		let (a, b): (&[u8], &[u8]) = (b"$a:example.com", b"$b:example.com");

		let shorts = {
			let adapter =
				ShortIdAdapter::new(Arc::new(MtxdbStore::open(&path).unwrap())).unwrap();
			assert_eq!(adapter.get_event(a).unwrap(), None);
			let shorts = adapter.allocate_events(&[a, b, a]).unwrap();
			assert_eq!(shorts[0], shorts[2]);
			assert_ne!(shorts[0], shorts[1]);
			assert_eq!(adapter.last_allocated(), 2);
			assert_eq!(adapter.allocate_events(&[b]).unwrap(), vec![shorts[1]]);
			assert_eq!(adapter.last_allocated(), 2);
			assert_eq!(adapter.get_event(a).unwrap(), Some(shorts[0]));
			assert_eq!(adapter.get_event_id(shorts[1]).unwrap().as_deref(), Some(b));
			assert_eq!(adapter.get_event_id(99).unwrap(), None);
			shorts
		};

		let adapter = ShortIdAdapter::new(Arc::new(MtxdbStore::open(&path).unwrap())).unwrap();
		assert_eq!(adapter.last_allocated(), 2);
		assert_eq!(adapter.get_event(b).unwrap(), Some(shorts[1]));
		let c = adapter.allocate_events(&[b"$c:example.com"]).unwrap();
		assert_eq!(c, vec![3]);
		fs::remove_dir_all(&path).unwrap();
	}

	#[test]
	fn state_keys_allocate_once_and_resolve_both_ways() {
		let path = test_path("state-keys");
		let _ = fs::remove_dir_all(&path);
		let adapter = ShortIdAdapter::new(Arc::new(MtxdbStore::open(&path).unwrap())).unwrap();

		assert_eq!(adapter.get_state_key("m.room.name", "").unwrap(), None);
		let name = adapter.allocate_state_key("m.room.name", "").unwrap();
		let member = adapter
			.allocate_state_key("m.room.member", "@u:example.com")
			.unwrap();
		assert_ne!(name, member);
		assert_eq!(adapter.allocate_state_key("m.room.name", "").unwrap(), name);
		assert_eq!(adapter.get_state_key("m.room.name", "").unwrap(), Some(name));
		assert_eq!(
			adapter.get_state_key_from_short(member).unwrap(),
			Some(("m.room.member".to_owned(), "@u:example.com".to_owned()))
		);
		assert_eq!(adapter.get_state_key_from_short(999).unwrap(), None);

		// Events and state keys draw from one counter, so no id is reused.
		let event = adapter.allocate_events(&[b"$a:example.com"]).unwrap()[0];
		assert!(event > member);
		fs::remove_dir_all(&path).unwrap();
	}
}
