use std::sync::Arc;

use conduwuit::{Result, implement};
use futures::{Stream, StreamExt};
use serde::Deserialize;

use super::rev_stream::is_cached;
use crate::{keyval, keyval::Key};

#[implement(super::Map)]
pub fn rev_keys<'a, K>(self: &'a Arc<Self>) -> impl Stream<Item = Result<Key<'a, K>>> + Send
where
	K: Deserialize<'a> + Send,
{
	self.rev_raw_keys().map(keyval::result_deserialize_key::<K>)
}

#[implement(super::Map)]
#[tracing::instrument(skip(self), fields(%self), level = "trace")]
pub fn rev_raw_keys(self: &Arc<Self>) -> impl Stream<Item = Result<Key<'_>>> + Send {
	super::macros::stream_boilerplate!(
		map = self,
		is_cached = is_cached(self),
		init = init_rev,
		key = None,
		dir = rocksdb::Direction::Reverse,
		stream_type = KeysRev
	)
}
