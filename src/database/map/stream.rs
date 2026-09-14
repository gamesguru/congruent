use std::sync::Arc;

use conduwuit::{Result, implement};
use futures::{Stream, StreamExt};
use serde::Deserialize;

use crate::{keyval, keyval::KeyVal, stream};

/// Iterate key-value entries in the map from the beginning.
///
/// - Result is deserialized
#[implement(super::Map)]
pub fn stream<'a, K, V>(
	self: &'a Arc<Self>,
) -> impl Stream<Item = Result<KeyVal<'a, K, V>>> + Send
where
	K: Deserialize<'a> + Send,
	V: Deserialize<'a> + Send,
{
	self.raw_stream().map(keyval::result_deserialize::<K, V>)
}

/// Iterate key-value entries in the map from the beginning.
///
/// - Result is raw
#[implement(super::Map)]
#[tracing::instrument(skip(self), fields(%self), level = "trace")]
pub fn raw_stream(self: &Arc<Self>) -> impl Stream<Item = Result<KeyVal<'_>>> + Send {
	super::macros::stream_boilerplate!(
		map = self,
		is_cached = is_cached(self),
		init = init_fwd,
		key = None,
		dir = rocksdb::Direction::Forward,
		stream_type = Items
	)
}

#[tracing::instrument(
    name = "cached",
    level = "trace",
    skip_all,
    fields(%map),
)]
pub(super) fn is_cached(map: &Arc<super::Map>) -> bool {
	let opts = super::cache_iter_options_default(&map.db);
	let state = stream::State::new(map, opts).init_fwd(None);

	!state.is_incomplete()
}
