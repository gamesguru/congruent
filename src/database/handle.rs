use std::{fmt, fmt::Debug, marker::PhantomData, ops::Deref};

use conduwuit::Result;
use serde::{Serialize, Serializer};

use crate::{Deserialized, Slice, dbkey::DbDeOwned};

pub struct Handle<'a> {
	val: Vec<u8>,
	_lifetime: PhantomData<&'a [u8]>,
}

impl Handle<'_> {
	pub(crate) fn new(value: Vec<u8>) -> Self { Self { val: value, _lifetime: PhantomData } }
}

impl Debug for Handle<'_> {
	fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
		out.debug_tuple("Handle").field(&self.val).finish()
	}
}

impl Serialize for Handle<'_> {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_bytes(self)
	}
}

impl Deserialized for Result<Handle<'_>> {
	fn map_de<T, U, F>(self, f: F) -> Result<U>
	where
		F: FnOnce(T) -> U,
		T: DbDeOwned,
	{
		self?.map_de(f)
	}
}

impl From<Handle<'_>> for Vec<u8> {
	fn from(handle: Handle<'_>) -> Self { handle.val }
}
impl Deref for Handle<'_> {
	type Target = Slice;

	fn deref(&self) -> &Self::Target { &self.val }
}
impl AsRef<Slice> for Handle<'_> {
	fn as_ref(&self) -> &Slice { self }
}

impl Deserialized for &Handle<'_> {
	fn map_de<T, U, F>(self, f: F) -> Result<U>
	where
		F: FnOnce(T) -> U,
		T: DbDeOwned,
	{
		crate::keyval::deserialize_val(self.as_ref()).map(f)
	}
}
