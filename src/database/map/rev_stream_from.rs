use std::{convert::AsRef, fmt::Debug, sync::Arc};

use conduwuit::{Result, implement};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
	keyval::{KeyVal, result_deserialize, serialize_key},
	stream,
	util::is_incomplete,
};

/// Iterate key-value entries in the map starting from upper-bound.
///
/// - Query is serialized
/// - Result is deserialized
#[implement(super::Map)]
pub fn rev_stream_from<'a, K, V, P>(
	self: &'a Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'a, K, V>>> + Send + use<'a, K, V, P>
where
	P: Serialize + ?Sized + Debug,
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	self.rev_stream_from_raw(from)
		.map(result_deserialize::<K, V>)
}

/// Iterate key-value entries in the map starting from upper-bound.
///
/// - Query is serialized
/// - Result is raw
#[implement(super::Map)]
#[tracing::instrument(skip(self), level = "trace")]
pub fn rev_stream_from_raw<P>(
	self: &Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'_>>> + Send + use<'_, P>
where
	P: Serialize + ?Sized + Debug,
{
	let key = serialize_key(from).expect("failed to serialize query key");
	self.rev_raw_stream_from(&key)
}

/// Iterate key-value entries in the map starting from upper-bound.
///
/// - Query is raw
/// - Result is deserialized
#[implement(super::Map)]
pub fn rev_stream_raw_from<'a, K, V, P>(
	self: &'a Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'a, K, V>>> + Send + use<'a, K, V, P>
where
	P: AsRef<[u8]> + ?Sized + Debug + Sync,
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	self.rev_raw_stream_from(from)
		.map(result_deserialize::<K, V>)
}

/// Iterate key-value entries in the map starting from upper-bound.
///
/// - Query is raw
/// - Result is raw
#[implement(super::Map)]
#[tracing::instrument(skip(self, from), fields(%self), level = "trace")]
pub fn rev_raw_stream_from<P>(
	self: &Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<KeyVal<'_>>> + Send + use<'_, P>
where
	P: AsRef<[u8]> + ?Sized + Debug,
{
	super::macros::stream_boilerplate!(
		map = self,
		is_cached = is_cached(self, from),
		init = init_rev,
		key = Some(from.as_ref()),
		dir = rocksdb::Direction::Reverse,
		stream_type = ItemsRev
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
	let cache_opts = super::cache_iter_options_default(&map.db);
	let cache_status = stream::State::new(map, cache_opts)
		.init_rev(from.as_ref().into())
		.status();

	!matches!(cache_status, Some(e) if is_incomplete(&e))
}
