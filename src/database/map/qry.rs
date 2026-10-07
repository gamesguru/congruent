use std::{convert::AsRef, fmt::Debug, io::Write, sync::Arc};

use conduwuit::{Result, arrayvec::ArrayVec, implement};
use futures::Future;

use crate::{Handle, dbkey::DbKey, keyval::KeyBuf, ser};

/// Fetch a value from the database into cache, returning a reference-handle
/// asynchronously. The key is serialized into an allocated buffer to perform
/// the query.
#[implement(super::Map)]
#[inline]
pub fn qry<K>(
	self: &Arc<Self>,
	key: &K,
) -> impl Future<Output = Result<Handle<'_>>> + Send + use<'_, K>
where
	K: DbKey + ?Sized + Debug,
{
	let mut buf = KeyBuf::new();
	self.bqry(key, &mut buf)
}

/// Fetch a value from the database into cache, returning a reference-handle
/// asynchronously. The key is serialized into a fixed-sized buffer to perform
/// the query. The maximum size is supplied as const generic parameter.
#[implement(super::Map)]
#[inline]
pub fn aqry<const MAX: usize, K>(
	self: &Arc<Self>,
	key: &K,
) -> impl Future<Output = Result<Handle<'_>>> + Send + use<'_, MAX, K>
where
	K: DbKey + ?Sized + Debug,
{
	let mut buf = ArrayVec::<u8, MAX>::new();
	self.bqry(key, &mut buf)
}

/// Fetch a value from the database into cache, returning a reference-handle
/// asynchronously. The key is serialized into a user-supplied Writer.
#[implement(super::Map)]
pub fn bqry<K, B>(
	self: &Arc<Self>,
	key: &K,
	buf: &mut B,
) -> impl Future<Output = Result<Handle<'_>>> + Send + use<'_, K, B>
where
	K: DbKey + ?Sized + Debug,
	B: Write + AsRef<[u8]>,
{
	let key = ser::serialize(buf, key).expect("failed to serialize query key");
	self.get(key)
}
