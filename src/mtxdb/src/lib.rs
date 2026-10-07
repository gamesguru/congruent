//! Conduwuit's integration boundary for [`mtxdb`].
//!
//! This crate deliberately owns the mtxdb dependency and its lifecycle. Higher
//! layers should depend on this crate rather than importing mtxdb directly, so
//! storage policy and the eventual HAMT adapter can evolve independently of
//! room and service code.

use std::{
	path::{Path, PathBuf},
	sync::Arc,
};

/// Errors returned while opening or operating the mtxdb database.
pub use mtxdb::storage::StorageError;

/// An opened conduwuit mtxdb instance.
pub struct Database {
	inner: Arc<mtxdb::Database>,
}

impl Database {
	/// Open or create a writable mtxdb database rooted at `path`.
	pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
		let inner = mtxdb::Database::open(PathBuf::from(path.as_ref()))?;
		Ok(Self { inner: Arc::new(inner) })
	}

	/// Access the underlying shared database for adapter implementation code.
	#[must_use]
	pub fn shared(&self) -> &Arc<mtxdb::Database> { &self.inner }
}

impl Clone for Database {
	fn clone(&self) -> Self { Self { inner: Arc::clone(&self.inner) } }
}
