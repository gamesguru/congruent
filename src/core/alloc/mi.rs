//! mimalloc allocator.

#[global_allocator]
static MIMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub fn trim<I: Into<Option<usize>>>(_: I) -> crate::Result { Ok(()) }

#[must_use]
pub fn memory_usage() -> Option<String> { None }

#[must_use]
pub fn memory_stats(_opts: &str) -> Option<String> { None }

pub fn background_thread_enable(_: bool) -> crate::Result<bool> { Ok(false) }

#[must_use]
pub fn is_affine_arena() -> bool { false }

pub mod this_thread {
	pub fn set_arena(_: usize) -> crate::Result<usize> { Ok(0) }

	pub fn set_muzzy_decay(_: isize) -> crate::Result<isize> { Ok(0) }

	pub fn decay() -> crate::Result { Ok(()) }
}
