use std::{convert::AsRef, fmt::Debug, sync::Arc};

use conduwuit::{Result, implement};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
	keyval::{KeyVal, result_deserialize, serialize_key},
	stream,
};

/// Iterate key-value entries in the map starting from lower-bound.
///
/// - Query is serialized
/// - Result is deserialized
#[implement(super::Map)]
pub fn stream_from<'a, K, V, P>(
	self: &'a Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'a, K, V>>> + Send + use<'a, K, V, P>
where
	P: Serialize + ?Sized + Debug,
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	self.stream_from_raw(from).map(result_deserialize::<K, V>)
}

/// Iterate key-value entries in the map starting from lower-bound.
///
/// - Query is serialized
/// - Result is raw
#[implement(super::Map)]
#[tracing::instrument(skip(self), level = "trace")]
pub fn stream_from_raw<P>(
	self: &Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'_>>> + Send + use<'_, P>
where
	P: Serialize + ?Sized + Debug,
{
	let key = serialize_key(from).expect("failed to serialize query key");
	self.raw_stream_from(&key)
}

/// Iterate key-value entries in the map starting from lower-bound.
///
/// - Query is raw
/// - Result is deserialized
#[implement(super::Map)]
pub fn stream_raw_from<'a, K, V, P>(
	self: &'a Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'a, K, V>>> + Send + use<'a, K, V, P>
where
	P: AsRef<[u8]> + ?Sized + Debug + Sync,
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	self.raw_stream_from(from).map(result_deserialize::<K, V>)
}

/// Iterate key-value entries in the map starting from lower-bound.
///
/// - Query is raw
/// - Result is raw
#[implement(super::Map)]
#[tracing::instrument(skip(self, from), fields(%self), level = "trace")]
pub fn raw_stream_from<P>(
	self: &Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'_>>> + Send + use<'_, P>
where
	P: AsRef<[u8]> + ?Sized + Debug,
{
	super::macros::stream_boilerplate!(
		map = self,
		is_cached = is_cached(self, from),
		init = init_fwd,
		key = Some(from.as_ref()),
		dir = rocksdb::Direction::Forward,
		stream_type = Items
	)
}

#[tracing::instrument(
    name = "cached",
    level = "trace",
    skip(map, from),
    fields(%map),
)]
pub(super) fn is_cached<P>(map: &Arc<super::Map>, from: &P) -> bool
where
	P: AsRef<[u8]> + ?Sized,
{
	let opts = super::cache_iter_options_default(&map.db);
	let state = stream::State::new(map, opts).init_fwd(from.as_ref().into());

	!state.is_incomplete()
}
