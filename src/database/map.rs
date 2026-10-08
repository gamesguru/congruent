//! Logical metadata maps backed by the redb engine.

use std::{
	convert::AsRef,
	fmt::{self, Debug, Display},
	future::Future,
	pin::Pin,
	sync::Arc,
};

use conduwuit::{Result, err};
use futures::{Stream, StreamExt, future, stream};

use crate::{
	Engine, Handle,
	dbkey::DbKey,
	keyval::{self, Key, KeyBuf, KeyVal, ValBuf},
	ser,
	watchers::Watchers,
};

pub mod batch {
	use super::Map;

	pub(crate) enum DbOp {
		Insert {
			map_name: &'static str,
			key: Vec<u8>,
			value: Vec<u8>,
		},
		Remove {
			map_name: &'static str,
			key: Vec<u8>,
		},
	}

	pub struct Batch<'a> {
		pub(crate) ops: Vec<DbOp>,
		pub(crate) wakes: Vec<(&'a Map, Vec<u8>)>,
	}

	impl Batch<'_> {
		#[must_use]
		pub fn new() -> Self { Self { ops: Vec::new(), wakes: Vec::new() } }

		#[must_use]
		pub fn len(&self) -> usize { self.ops.len() }

		#[must_use]
		pub fn is_empty(&self) -> bool { self.ops.is_empty() }
	}
	impl Default for Batch<'_> {
		fn default() -> Self { Self::new() }
	}
}

pub use batch::Batch;

pub trait Get<'a, K>: Stream<Item = K> + Sized + Send + 'a
where
	K: AsRef<[u8]> + Send + 'a,
{
	fn get(self, map: &'a Arc<Map>) -> impl Stream<Item = Result<Handle<'static>>> + Send + 'a {
		map.get_batch(self)
	}
}
impl<'a, S, K> Get<'a, K> for S
where
	S: Stream<Item = K> + Sized + Send + 'a,
	K: AsRef<[u8]> + Send + 'a,
{
}

pub trait Qry<'a, K>: Stream<Item = K> + Sized + Send + 'a
where
	K: DbKey + Debug + Send + 'a,
{
	fn qry(self, map: &'a Arc<Map>) -> impl Stream<Item = Result<Handle<'static>>> + Send + 'a {
		self.then(move |key| {
			let mut buffer = KeyBuf::new();
			let encoded = ser::serialize(&mut buffer, key)
				.expect("failed to serialize query key")
				.to_vec();
			future::ready(map.get_blocking(&encoded))
		})
	}
}
impl<'a, S, K> Qry<'a, K> for S
where
	S: Stream<Item = K> + Sized + Send + 'a,
	K: DbKey + Debug + Send + 'a,
{
}
#[derive(Debug)]
pub struct RecursiveGetOutput<V, K> {
	pub values: Vec<V>,
	pub missing: Vec<K>,
	pub truncated: bool,
}

pub mod compact {
	#[derive(Clone, Debug, Default)]
	pub struct Options {
		pub range: (Option<Vec<u8>>, Option<Vec<u8>>),
		pub level: (Option<usize>, Option<usize>),
		pub exhaustive: bool,
		pub exclusive: bool,
	}
}

pub struct Map {
	pub(crate) name: &'static str,
	pub(crate) db: Arc<Engine>,
	pub(crate) watchers: Watchers,
}

impl Map {
	pub fn open(db: &Arc<Engine>, name: &'static str) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			name,
			db: db.clone(),
			watchers: Watchers::default(),
		}))
	}

	pub fn name(&self) -> &str { self.name }

	pub(crate) fn db(&self) -> &Arc<Engine> { &self.db }

	pub fn watch_prefix<'a, K>(
		&'a self,
		prefix: &K,
	) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
	where
		K: AsRef<[u8]> + ?Sized + Debug,
	{
		self.watchers.watch(prefix.as_ref())
	}

	pub fn get<K>(
		self: &Arc<Self>,
		key: &K,
	) -> impl Future<Output = Result<Handle<'static>>> + Send + use<K>
	where
		K: AsRef<[u8]> + Debug + ?Sized,
	{
		let result = self.db.get(self.name, key.as_ref()).and_then(|value| {
			value
				.map(Handle::new)
				.ok_or_else(|| err!(Request(NotFound("not found in database"))))
		});
		future::ready(result)
	}

	pub fn qry<K>(
		self: &Arc<Self>,
		key: &K,
	) -> impl Future<Output = Result<Handle<'static>>> + Send
	where
		K: DbKey + ?Sized + Debug,
	{
		let mut buffer = KeyBuf::new();
		let key = ser::serialize(&mut buffer, key)
			.expect("failed to serialize query key")
			.to_vec();
		future::ready(self.get_blocking(&key))
	}

	pub fn get_blocking<K>(&self, key: &K) -> Result<Handle<'static>>
	where
		K: AsRef<[u8]> + ?Sized,
	{
		self.db
			.get(self.name, key.as_ref())?
			.map(Handle::new)
			.ok_or_else(|| err!(Request(NotFound("not found in database"))))
	}

	pub fn get_nocache<K>(
		self: &Arc<Self>,
		key: &K,
	) -> impl Future<Output = Result<Handle<'static>>> + Send
	where
		K: AsRef<[u8]> + Debug + ?Sized,
	{
		self.get(key)
	}

	pub fn contains<K>(self: &Arc<Self>, key: &K) -> impl Future<Output = bool> + Send
	where
		K: DbKey + ?Sized + Debug,
	{
		let mut buffer = KeyBuf::new();
		let key = ser::serialize(&mut buffer, key)
			.expect("failed to serialize key")
			.to_vec();
		future::ready(self.db.contains(self.name, &key).unwrap_or(false))
	}

	/// Compatibility view for the old RocksDB admin statistics command.
	pub fn property(&self, _name: &str) -> Option<String> {
		Some(String::from("redb metadata backend; detailed RocksDB properties unavailable"))
	}

	pub fn exists<K>(self: &Arc<Self>, key: &K) -> impl Future<Output = Result> + Send
	where
		K: AsRef<[u8]> + ?Sized + Debug,
	{
		let result = self.db.contains(self.name, key.as_ref()).and_then(|found| {
			if found {
				Ok(())
			} else {
				Err(err!(Request(NotFound("not found in database"))))
			}
		});
		future::ready(result)
	}

	pub fn exists_blocking<K>(&self, key: &K) -> Result
	where
		K: AsRef<[u8]> + ?Sized + Debug,
	{
		if self.db.contains(self.name, key.as_ref())? {
			Ok(())
		} else {
			Err(err!(Request(NotFound("not found in database"))))
		}
	}

	pub fn put<K, V>(&self, key: K, value: V)
	where
		K: DbKey + Debug,
		V: DbKey,
	{
		self.write(key, value);
	}

	pub fn insert<K, V>(&self, key: &K, value: V)
	where
		K: AsRef<[u8]> + ?Sized,
		V: AsRef<[u8]>,
	{
		self.write_bytes(key.as_ref().to_vec(), value.as_ref().to_vec());
	}

	pub fn put_raw<K, V>(&self, key: K, value: V)
	where
		K: DbKey + Debug,
		V: AsRef<[u8]>,
	{
		let mut kb = KeyBuf::new();
		let key = ser::serialize(&mut kb, key)
			.expect("failed to serialize key")
			.to_vec();
		self.write_bytes(key, value.as_ref().to_vec());
	}

	pub fn raw_put<K, V>(&self, key: K, value: V)
	where
		K: AsRef<[u8]>,
		V: DbKey,
	{
		let mut vb = ValBuf::new();
		let value = ser::serialize(&mut vb, value)
			.expect("failed to serialize value")
			.to_vec();
		self.write_bytes(key.as_ref().to_vec(), value);
	}

	fn write<K, V>(&self, key: K, value: V)
	where
		K: DbKey + Debug,
		V: DbKey,
	{
		let mut kb = KeyBuf::new();
		let mut vb = ValBuf::new();
		let key = ser::serialize(&mut kb, key)
			.expect("failed to serialize key")
			.to_vec();
		let value = ser::serialize(&mut vb, value)
			.expect("failed to serialize value")
			.to_vec();
		self.write_bytes(key, value);
	}

	fn write_raw<K, V>(&self, key: K, value: V, raw_key: bool)
	where
		K: AsRef<[u8]>,
		V: AsRef<[u8]>,
	{
		let key = if raw_key {
			key.as_ref().to_vec()
		} else {
			let mut kb = KeyBuf::new();
			ser::serialize(&mut kb, key.as_ref())
				.expect("failed to serialize key")
				.to_vec()
		};
		self.write_bytes(key, value.as_ref().to_vec());
	}

	fn write_bytes(&self, key: Vec<u8>, value: Vec<u8>) {
		self.db
			.commit_batch(vec![batch::DbOp::Insert {
				map_name: self.name,
				key: key.clone(),
				value,
			}])
			.expect("database insert error");
		self.watchers.wake(&key);
	}

	pub fn del<K>(&self, key: K)
	where
		K: DbKey + Debug,
	{
		let mut kb = KeyBuf::new();
		let key = ser::serialize(&mut kb, key)
			.expect("failed to serialize key")
			.to_vec();
		self.remove_raw(&key);
	}

	pub fn remove<K>(&self, key: &K)
	where
		K: AsRef<[u8]> + ?Sized + Debug,
	{
		self.remove_raw(key.as_ref());
	}

	pub fn remove_raw(&self, key: &[u8]) {
		self.db
			.commit_batch(vec![batch::DbOp::Remove { map_name: self.name, key: key.to_vec() }])
			.expect("database remove error");
		self.watchers.wake(key);
	}

	pub fn batch_put<'a, K, V>(&'a self, batch: &mut Batch<'a>, key: &K, value: V)
	where
		K: AsRef<[u8]> + ?Sized,
		V: AsRef<[u8]>,
	{
		batch.ops.push(batch::DbOp::Insert {
			map_name: self.name,
			key: key.as_ref().to_vec(),
			value: value.as_ref().to_vec(),
		});
		batch.wakes.push((self, key.as_ref().to_vec()));
	}

	pub fn batch_raw_put<'a, K, V>(&'a self, batch: &mut Batch<'a>, key: K, value: V)
	where
		K: AsRef<[u8]>,
		V: DbKey,
	{
		let mut vb = ValBuf::new();
		let value = ser::serialize(&mut vb, value).expect("failed to serialize batch value");
		self.batch_put(batch, &key, value);
	}

	pub fn batch_delete<'a, K>(&'a self, batch: &mut Batch<'a>, key: &K)
	where
		K: AsRef<[u8]> + ?Sized,
	{
		batch.ops.push(batch::DbOp::Remove {
			map_name: self.name,
			key: key.as_ref().to_vec(),
		});
		batch.wakes.push((self, key.as_ref().to_vec()));
	}

	pub fn apply_batch(&self, batch: Batch<'_>) {
		let Batch { ops, wakes } = batch;
		self.db.commit_batch(ops).expect("database batch error");
		for (map, key) in wakes {
			map.watchers.wake(&key);
		}
	}

	fn raw_items(&self, mode: crate::util::IteratorMode<'_>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		self.db.scan(self.name, mode)
	}

	fn raw_items_from(
		&self,
		from: Vec<u8>,
		direction: crate::util::Direction,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		self.db
			.scan(self.name, crate::util::IteratorMode::From(&from, direction))
	}

	pub fn raw_stream(&self) -> impl Stream<Item = Result<KeyVal<'static>>> + Send {
		materialized(self.raw_items(crate::util::IteratorMode::Start))
	}

	pub fn raw_stream_from<P>(
		&self,
		from: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send + 'static + use<P>
	where
		P: AsRef<[u8]> + ?Sized,
	{
		materialized(self.raw_items_from(from.as_ref().to_vec(), crate::util::Direction::Forward))
	}

	pub fn rev_raw_stream(&self) -> impl Stream<Item = Result<KeyVal<'static>>> + Send {
		materialized(self.raw_items(crate::util::IteratorMode::End))
	}

	pub fn rev_raw_stream_from<P>(
		&self,
		from: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send + 'static + use<P>
	where
		P: AsRef<[u8]> + ?Sized,
	{
		materialized(self.raw_items_from(from.as_ref().to_vec(), crate::util::Direction::Reverse))
	}

	pub fn raw_keys(&self) -> impl Stream<Item = Result<&'static [u8]>> + Send {
		self.raw_stream().map(|item| item.map(|(key, _)| key))
	}

	pub fn raw_keys_prefix<P>(
		&self,
		prefix: &P,
	) -> impl Stream<Item = Result<&'static [u8]>> + Send + 'static + use<P>
	where
		P: AsRef<[u8]> + ?Sized,
	{
		let prefix = prefix.as_ref().to_vec();
		let items = self
			.raw_items(crate::util::IteratorMode::Start)
			.map(|items| {
				items
					.into_iter()
					.filter(|(key, _)| key.starts_with(&prefix))
					.collect()
			});
		materialized(items).map(|item| item.map(|(key, _)| key))
	}

	fn raw_keys_prefix_owned(
		&self,
		prefix: Vec<u8>,
	) -> impl Stream<Item = Result<&'static [u8]>> + Send + 'static {
		let items = self
			.raw_items(crate::util::IteratorMode::Start)
			.map(|items| {
				items
					.into_iter()
					.filter(|(key, _)| key.starts_with(&prefix))
					.collect()
			});
		materialized(items).map(|item| item.map(|(key, _)| key))
	}

	pub fn keys_prefix_raw<P>(
		&self,
		prefix: &P,
	) -> impl Stream<Item = Result<&'static [u8]>> + Send + '_
	where
		P: DbKey + ?Sized + Debug,
	{
		let prefix = ser::serialize_to_vec(prefix).expect("failed to serialize prefix");
		self.raw_keys_prefix_owned(prefix)
	}

	pub fn keys_prefix<'a, K, P>(
		&'a self,
		prefix: &P,
	) -> impl Stream<Item = Result<Key<'static, K>>> + Send + 'a + use<'a, K, P>
	where
		P: DbKey + ?Sized + Debug,
		K: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		let prefix = ser::serialize_to_vec(prefix).expect("failed to serialize prefix");
		self.raw_keys_prefix_owned(prefix)
			.map(keyval::result_deserialize_key::<K>)
	}

	pub fn raw_stream_prefix<'a, P>(
		&'a self,
		prefix: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send + 'a
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.raw_stream_prefix_owned(prefix.as_ref().to_vec())
	}

	fn raw_stream_prefix_owned(
		&self,
		prefix: Vec<u8>,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send + 'static {
		let items = self
			.raw_items(crate::util::IteratorMode::Start)
			.map(|items| {
				items
					.into_iter()
					.filter(|(key, _)| key.starts_with(&prefix))
					.collect()
			});
		materialized(items)
	}

	pub fn stream<'a, K, V>(
		&'a self,
	) -> impl Stream<Item = Result<KeyVal<'static, K, V>>> + Send + 'a + use<'a, K, V>
	where
		K: crate::dbkey::DbDe<'static> + Send + 'a,
		V: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		self.raw_stream().map(keyval::result_deserialize::<K, V>)
	}

	pub fn stream_prefix<'a, K, V, P>(
		&'a self,
		prefix: &P,
	) -> impl Stream<Item = Result<KeyVal<'static, K, V>>> + Send + 'a + use<'a, K, V, P>
	where
		P: DbKey + ?Sized + Debug,
		K: crate::dbkey::DbDe<'static> + Send + 'a,
		V: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		let prefix = ser::serialize_to_vec(prefix).expect("failed to serialize prefix");
		self.raw_stream_prefix_owned(prefix)
			.map(keyval::result_deserialize::<K, V>)
	}

	pub fn stream_from<'a, K, V, P>(
		&'a self,
		from: &P,
	) -> impl Stream<Item = Result<KeyVal<'static, K, V>>> + Send + 'a + use<'a, K, V, P>
	where
		P: DbKey + ?Sized + Debug,
		K: crate::dbkey::DbDe<'static> + Send + 'a,
		V: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		let from = ser::serialize_to_vec(from).expect("failed to serialize key");
		materialized(self.raw_items_from(from, crate::util::Direction::Forward))
			.map(keyval::result_deserialize::<K, V>)
	}

	pub fn rev_stream<'a, K, V>(
		&'a self,
	) -> impl Stream<Item = Result<KeyVal<'static, K, V>>> + Send + 'a + use<'a, K, V>
	where
		K: crate::dbkey::DbDe<'static> + Send + 'a,
		V: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		self.rev_raw_stream()
			.map(keyval::result_deserialize::<K, V>)
	}

	pub fn rev_stream_from<'a, K, V, P>(
		&'a self,
		from: &P,
	) -> impl Stream<Item = Result<KeyVal<'static, K, V>>> + Send + 'a + use<'a, K, V, P>
	where
		P: DbKey + ?Sized + Debug,
		K: crate::dbkey::DbDe<'static> + Send + 'a,
		V: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		let from = ser::serialize_to_vec(from).expect("failed to serialize key");
		materialized(self.raw_items_from(from, crate::util::Direction::Reverse))
			.map(keyval::result_deserialize::<K, V>)
	}

	pub fn raw_stream_raw_prefix<P>(
		&self,
		prefix: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.raw_stream_prefix(prefix)
	}

	pub fn stream_prefix_raw<P>(
		&self,
		prefix: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send
	where
		P: DbKey + ?Sized + Debug,
	{
		let prefix = ser::serialize_to_vec(prefix).expect("failed to serialize prefix");
		self.raw_stream_prefix_owned(prefix)
	}

	pub fn rev_stream_from_raw<P>(
		&self,
		from: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send + use<'_, P>
	where
		P: DbKey + ?Sized + Debug,
	{
		let from = ser::serialize_to_vec(from).expect("failed to serialize key");
		materialized(self.raw_items_from(from, crate::util::Direction::Reverse))
	}

	pub fn rev_keys_raw_from<P>(
		&self,
		from: &P,
	) -> impl Stream<Item = Result<Key<'static>>> + Send + use<'_, P>
	where
		P: AsRef<[u8]> + ?Sized,
	{
		materialized(self.raw_items_from(from.as_ref().to_vec(), crate::util::Direction::Reverse))
			.map(|item| item.map(|(key, _)| key))
	}

	pub fn stream_raw_prefix<P>(
		&self,
		prefix: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.raw_stream_prefix(prefix)
	}

	pub fn get_batch_blocking<'a, I, K>(
		&self,
		keys: I,
	) -> impl Iterator<Item = Result<Handle<'static>>> + Send + 'a
	where
		I: Iterator<Item = &'a K> + Send + 'a,
		K: AsRef<[u8]> + ?Sized + 'a,
	{
		keys.map(|key| self.get_blocking(key))
			.collect::<Vec<_>>()
			.into_iter()
	}

	pub fn raw_count_prefix<'a, P>(
		&'a self,
		prefix: &'a P,
	) -> impl Future<Output = usize> + Send + 'a
	where
		P: AsRef<[u8]> + ?Sized + Sync + 'a,
	{
		let prefix = prefix.as_ref().to_vec();
		async move {
			self.raw_items(crate::util::IteratorMode::Start)
				.map(|items| {
					items
						.into_iter()
						.filter(|(key, _)| key.starts_with(&prefix))
						.count()
				})
				.unwrap_or_default()
		}
	}

	pub fn stream_raw_from<'a, K, V, P>(
		&'a self,
		from: &P,
	) -> impl Stream<Item = Result<KeyVal<'static, K, V>>> + Send + 'a + use<'a, K, V, P>
	where
		P: AsRef<[u8]> + ?Sized,
		K: crate::dbkey::DbDe<'static> + Send + 'a,
		V: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		self.raw_stream_from(from)
			.map(keyval::result_deserialize::<K, V>)
	}

	pub fn aqry<const MAX: usize, K>(
		self: &Arc<Self>,
		key: &K,
	) -> impl Future<Output = Result<Handle<'static>>> + Send
	where
		K: DbKey + ?Sized + Debug,
	{
		self.qry(key)
	}

	pub fn bqry<K, B>(
		self: &Arc<Self>,
		key: &K,
		buf: &mut B,
	) -> impl Future<Output = Result<Handle<'static>>> + Send
	where
		K: DbKey + ?Sized + Debug,
		B: std::io::Write + AsRef<[u8]>,
	{
		let encoded = ser::serialize(buf, key)
			.expect("failed to serialize query key")
			.to_vec();
		future::ready(self.get_blocking(&encoded))
	}

	pub fn put_aput<const VMAX: usize, K, V>(&self, key: K, value: V)
	where
		K: DbKey + Debug,
		V: DbKey,
	{
		self.put(key, value);
	}

	pub fn aput_put<const KMAX: usize, K, V>(&self, key: K, value: V)
	where
		K: DbKey + Debug,
		V: DbKey,
	{
		self.put(key, value);
	}

	pub fn raw_aput<const VMAX: usize, K, V>(&self, key: K, value: V)
	where
		K: AsRef<[u8]>,
		V: DbKey,
	{
		self.raw_put(key, value);
	}

	pub fn aput_raw<const KMAX: usize, K, V>(&self, key: K, value: V)
	where
		K: DbKey + Debug,
		V: AsRef<[u8]>,
	{
		self.put_raw(key, value);
	}

	pub fn clear(self: &Arc<Self>) -> impl Future<Output = ()> + Send {
		let map = self.clone();
		async move {
			let keys = map.raw_keys().collect::<Vec<_>>().await;
			for key in keys.into_iter().flatten() {
				map.remove_raw(key);
			}
		}
	}

	pub fn get_batch<'a, S, K>(
		self: &'a Arc<Self>,
		keys: S,
	) -> impl Stream<Item = Result<Handle<'static>>> + Send + 'a
	where
		S: Stream<Item = K> + Send + 'a,
		K: AsRef<[u8]> + Send + 'a,
	{
		keys.then(move |key| future::ready(self.get_blocking(&key)))
	}

	pub fn recursive_multi_get<K, V, P, F, I>(
		self: &Arc<Self>,
		roots: I,
		max_nodes: Option<usize>,
		max_depth: Option<usize>,
		parse_value: P,
		extract_children: F,
	) -> impl Future<Output = Result<RecursiveGetOutput<V, K>>> + Send
	where
		K: AsRef<[u8]> + Ord + std::hash::Hash + Clone + Send + 'static,
		V: Send + 'static,
		P: Fn(&[u8]) -> Result<V> + Send + Sync + 'static,
		F: Fn(&V, &mut Vec<K>) + Send + Sync + 'static,
		I: IntoIterator<Item = K> + Send + 'static,
	{
		let map = self.clone();
		async move {
			let mut visited = std::collections::HashSet::new();
			let mut current = Vec::new();
			for root in roots {
				if visited.insert(root.clone()) {
					current.push(root);
				}
			}
			let mut values = Vec::new();
			let mut missing = Vec::new();
			let mut depth = 0_usize;
			let mut truncated = false;
			while !current.is_empty() {
				if max_depth.is_some_and(|limit| depth >= limit)
					|| max_nodes.is_some_and(|limit| values.len() >= limit)
				{
					truncated = true;
					break;
				}
				let mut next = Vec::new();
				for key in current.drain(..) {
					match map.db.get(map.name, key.as_ref())? {
						| Some(bytes) if max_nodes.is_none_or(|limit| values.len() < limit) => {
							let value = parse_value(&bytes)?;
							extract_children(&value, &mut next);
							values.push(value);
						},
						| Some(_) => {
							truncated = true;
						},
						| None => missing.push(key),
					}
				}
				if truncated {
					break;
				}
				next.retain(|key| visited.insert(key.clone()));
				current = next;
				depth = depth.saturating_add(1);
			}
			Ok(RecursiveGetOutput { values, missing, truncated })
		}
	}

	pub fn keys<'a, K>(&'a self) -> impl Stream<Item = Result<Key<'static, K>>> + Send + 'a
	where
		K: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		self.raw_keys().map(keyval::result_deserialize_key::<K>)
	}

	pub fn keys_raw_from<P>(&self, from: &P) -> impl Stream<Item = Result<Key<'static>>> + Send
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.raw_keys_from(from)
	}

	pub fn raw_keys_from<P>(
		&self,
		from: &P,
	) -> impl Stream<Item = Result<Key<'static>>> + Send + 'static + use<P>
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.raw_stream_from(from)
			.map(|item| item.map(|(key, _)| key))
	}

	pub fn rev_raw_keys_from<P>(
		&self,
		from: &P,
	) -> impl Stream<Item = Result<Key<'static>>> + Send + 'static + use<P>
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.rev_raw_stream_from(from)
			.map(|item| item.map(|(key, _)| key))
	}

	pub fn rev_keys_from<'a, K, P>(
		&'a self,
		from: &'a P,
	) -> impl Stream<Item = Result<Key<'static, K>>> + Send + 'a
	where
		P: DbKey + ?Sized + Debug,
		K: crate::dbkey::DbDe<'static> + Send + 'a,
	{
		let from = ser::serialize_to_vec(from).expect("failed to serialize key");
		materialized(self.raw_items_from(from, crate::util::Direction::Reverse))
			.map(|item| item.map(|(key, _)| key))
			.map(keyval::result_deserialize_key::<K>)
	}

	pub fn keys_raw_prefix<P>(
		&self,
		prefix: &P,
	) -> impl Stream<Item = Result<Key<'static>>> + Send
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.raw_keys_prefix(prefix)
	}

	pub fn rev_raw_stream_prefix<P>(
		&self,
		prefix: &P,
	) -> impl Stream<Item = Result<KeyVal<'static>>> + Send
	where
		P: AsRef<[u8]> + ?Sized,
	{
		self.rev_raw_stream().filter({
			let prefix = prefix.as_ref().to_vec();
			move |item| {
				future::ready(item.as_ref().is_ok_and(|(key, _)| key.starts_with(&prefix)))
			}
		})
	}

	pub fn rev_raw_keys(&self) -> impl Stream<Item = Result<Key<'static>>> + Send {
		self.rev_raw_stream().map(|item| item.map(|(key, _)| key))
	}

	pub fn count(self: &Arc<Self>) -> impl Future<Output = usize> + Send {
		async move {
			self.raw_items(crate::util::IteratorMode::Start)
				.map_or(0, |items| items.len())
		}
	}

	pub fn count_prefix<P>(&self, prefix: &P) -> impl Future<Output = usize> + Send
	where
		P: AsRef<[u8]> + ?Sized + Sync,
	{
		let prefix = prefix.as_ref().to_vec();
		async move {
			self.raw_items(crate::util::IteratorMode::Start)
				.map(|items| {
					items
						.into_iter()
						.filter(|(key, _)| key.starts_with(&prefix))
						.count()
				})
				.unwrap_or(0)
		}
	}

	pub fn compact_blocking(&self, _options: compact::Options) -> Result { Ok(()) }
}

impl Debug for Map {
	fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
		out.debug_struct("Map").field("name", &self.name).finish()
	}
}
impl Display for Map {
	fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result { out.write_str(self.name) }
}

fn materialized(
	items: Result<Vec<(Vec<u8>, Vec<u8>)>>,
) -> impl Stream<Item = Result<KeyVal<'static>>> + Send {
	let items = match items {
		| Ok(items) => items
			.into_iter()
			.map(|(key, value)| Ok((leak(key), leak(value))))
			.collect(),
		| Err(error) => vec![Err(error)],
	};
	stream::iter(items)
}
fn leak(value: Vec<u8>) -> &'static [u8] { Box::leak(value.into_boxed_slice()) }
