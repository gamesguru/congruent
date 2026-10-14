//! Integration with allocators

#[cfg(all(not(target_env = "msvc"), feature = "mimalloc"))]
pub mod mi;
#[cfg(all(not(target_env = "msvc"), feature = "mimalloc"))]
pub use mi::{memory_stats, memory_usage, trim};

#[cfg(all(not(target_env = "msvc"), feature = "hardened_malloc", not(feature = "mimalloc")))]
pub mod hardened;
#[cfg(all(
	not(target_env = "msvc"),
	feature = "hardened_malloc",
	not(feature = "mimalloc")
))]
pub use hardened::{memory_stats, memory_usage, trim};

#[cfg(any(
	target_env = "msvc",
	all(not(feature = "hardened_malloc"), not(feature = "mimalloc"))
))]
pub mod default;
#[cfg(any(
	target_env = "msvc",
	all(not(feature = "hardened_malloc"), not(feature = "mimalloc"))
))]
pub use default::{memory_stats, memory_usage, trim};
