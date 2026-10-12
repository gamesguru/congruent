use std::convert::identity;

use conduwuit::Result;

use crate::dbkey::DbDeOwned;

pub trait Deserialized {
	fn map_de<T, U, F>(self, f: F) -> Result<U>
	where
		F: FnOnce(T) -> U,
		T: DbDeOwned;

	#[inline]
	fn deserialized<T>(self) -> Result<T>
	where
		T: DbDeOwned,
		Self: Sized,
	{
		self.map_de(identity::<T>)
	}
}
