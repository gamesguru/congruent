use std::{convert::AsRef, fmt::Debug, sync::Arc};

use conduwuit::{Result, implement};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use super::stream_from::is_cached;
use crate::keyval::{Key, result_deserialize_key, serialize_key};

#[implement(super::Map)]
pub fn keys_from<'a, K, P>(
	self: &'a Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<Key<'a, K>>> + Send + use<'a, K, P>
where
	P: Serialize + ?Sized + Debug,
	K: Deserialize<'a> + Send,
{
	self.keys_from_raw(from).map(result_deserialize_key::<K>)
}

#[implement(super::Map)]
#[tracing::instrument(skip(self), level = "trace")]
pub fn keys_from_raw<P>(
	self: &Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<Key<'_>>> + Send + use<'_, P>
where
	P: Serialize + ?Sized + Debug,
{
	let key = serialize_key(from).expect("failed to serialize query key");
	self.raw_keys_from(&key)
}

#[implement(super::Map)]
pub fn keys_raw_from<'a, K, P>(
	self: &'a Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<Key<'a, K>>> + Send + use<'a, K, P>
where
	P: AsRef<[u8]> + ?Sized + Debug + Sync,
	K: Deserialize<'a> + Send,
{
	self.raw_keys_from(from).map(result_deserialize_key::<K>)
}

#[implement(super::Map)]
#[tracing::instrument(skip(self, from), fields(%self), level = "trace")]
pub fn raw_keys_from<P>(
	self: &Arc<Self>,
	from: &P,
) -> impl Stream<Item = Result<Key<'_>>> + Send + use<'_, P>
where
	P: AsRef<[u8]> + ?Sized + Debug,
{
	super::macros::stream_boilerplate!(
		map = self,
		is_cached = is_cached(self, from),
		init = init_fwd,
		key = Some(from.as_ref()),
		dir = rocksdb::Direction::Forward,
		stream_type = Keys
	)
}
