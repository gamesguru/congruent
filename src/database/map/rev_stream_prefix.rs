use std::{convert::AsRef, fmt::Debug, sync::Arc};

use conduwuit::{Result, implement};
use futures::{Stream, StreamExt, TryStreamExt, future};

use crate::{
	dbkey::{DbDe, DbKey},
	keyval::{KeyVal, result_deserialize, serialize_key},
};

/// Iterate key-value entries in the map where the key matches a prefix.
///
/// - Query is serialized
/// - Result is deserialized
#[implement(super::Map)]
pub fn rev_stream_prefix<'a, K, V, P>(
	self: &'a Arc<Self>,
	prefix: &P,
) -> impl Stream<Item = Result<KeyVal<'a, K, V>>> + Send + use<'a, K, V, P>
where
	P: DbKey + ?Sized + Debug,
	K: DbDe<'a> + Send,
	V: DbDe<'a> + Send,
{
	self.rev_stream_prefix_raw(prefix)
		.map(result_deserialize::<K, V>)
}

/// Iterate key-value entries in the map where the key matches a prefix.
///
/// - Query is serialized
/// - Result is raw
#[implement(super::Map)]
#[tracing::instrument(skip(self), level = "trace")]
pub fn rev_stream_prefix_raw<P>(
	self: &Arc<Self>,
	prefix: &P,
) -> impl Stream<Item = Result<KeyVal<'_>>> + Send + use<'_, P>
where
	P: DbKey + ?Sized + Debug,
{
	let key = serialize_key(prefix).expect("failed to serialize query key");
	self.rev_raw_stream_from(&key)
		.try_take_while(move |(k, _): &KeyVal<'_>| future::ok(k.starts_with(&key)))
}

/// Iterate key-value entries in the map where the key matches a prefix.
///
/// - Query is raw
/// - Result is deserialized
#[implement(super::Map)]
pub fn rev_stream_raw_prefix<'a, K, V, P>(
	self: &'a Arc<Self>,
	prefix: &'a P,
) -> impl Stream<Item = Result<KeyVal<'a, K, V>>> + Send + 'a
where
	P: AsRef<[u8]> + ?Sized + Debug + Sync + 'a,
	K: DbDe<'a> + Send + 'a,
	V: DbDe<'a> + Send + 'a,
{
	self.rev_raw_stream_prefix(prefix)
		.map(result_deserialize::<K, V>)
}

/// Iterate key-value entries in the map where the key matches a prefix.
///
/// - Query is raw
/// - Result is raw
#[implement(super::Map)]
pub fn rev_raw_stream_prefix<'a, P>(
	self: &'a Arc<Self>,
	prefix: &'a P,
) -> impl Stream<Item = Result<KeyVal<'a>>> + Send + 'a
where
	P: AsRef<[u8]> + ?Sized + Debug + Sync + 'a,
{
	self.rev_raw_stream_from(prefix)
		.try_take_while(|(k, _): &KeyVal<'_>| future::ok(k.starts_with(prefix.as_ref())))
}
