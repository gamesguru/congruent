use std::sync::Arc;

use conduwuit::{Result, implement};
use futures::{Stream, StreamExt};

use super::stream::is_cached;
use crate::{dbkey::DbDe, keyval, keyval::Key};

#[implement(super::Map)]
pub fn keys<'a, K>(self: &'a Arc<Self>) -> impl Stream<Item = Result<Key<'a, K>>> + Send
where
	K: DbDe<'a> + Send,
{
	self.raw_keys().map(keyval::result_deserialize_key::<K>)
}

#[implement(super::Map)]
#[tracing::instrument(skip(self), fields(%self), level = "trace")]
pub fn raw_keys(self: &Arc<Self>) -> impl Stream<Item = Result<Key<'_>>> + Send {
	super::macros::stream_boilerplate!(
		map = self,
		is_cached = is_cached(self),
		init = init_fwd,
		key = None,
		dir = rocksdb::Direction::Forward,
		stream_type = Keys
	)
}
