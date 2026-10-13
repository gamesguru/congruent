//! System utilities related to compute/processing

use std::{cell::Cell, fmt::Debug, path::PathBuf, sync::LazyLock};

use crate::{Result, expected, is_equal_to};

type Id = usize;

type Mask = u128;
type Masks = [Mask; MASK_BITS];

const MASK_BITS: usize = 128;

/// The mask of logical cores available to the process (at startup).
static CORES_AVAILABLE: LazyLock<Mask> = LazyLock::new(|| into_mask(query_cores_available()));

/// Stores the mask of logical-cores with thread/HT/SMT association. Each group
/// here makes up a physical-core.
static SMT_TOPOLOGY: LazyLock<Masks> = LazyLock::new(init_smt_topology);

/// Stores the mask of logical-core associations on a node/socket. Bits are set
/// for all logical cores within all physical cores of the node.
static NODE_TOPOLOGY: LazyLock<Masks> = LazyLock::new(init_node_topology);

thread_local! {
	/// Tracks the affinity for this thread. This is updated when affinities
	/// are set via our set_affinity() interface.
	static CORE_AFFINITY: Cell<Mask> = Cell::default();
}

/// Set the core affinity for this thread. The ID should be listed in
/// CORES_AVAILABLE. Empty input is a no-op; prior affinity unchanged.
pub fn set_affinity<I>(ids: I)
where
	I: Iterator<Item = Id> + Debug,
{
	let mask: Mask = ids.fold(0, |mask, id| {
		debug_assert!(is_core_available(id), "setting affinity to unavailable core");
		mask | (1 << id)
	});

	if mask != 0 && set_thread_affinity(mask) {
		CORE_AFFINITY.replace(mask);
	}
}

/// Get the core affinity for this thread.
pub fn get_affinity() -> impl Iterator<Item = Id> { from_mask(CORE_AFFINITY.get()) }

/// List the cores sharing SMT-tier resources
pub fn smt_siblings() -> impl Iterator<Item = Id> {
	from_mask(get_affinity().fold(0_u128, |mask, id| {
		mask | SMT_TOPOLOGY.get(id).expect("ID must not exceed max cpus")
	}))
}

/// List the cores sharing Node-tier resources relative to this threads current
/// affinity.
pub fn node_siblings() -> impl Iterator<Item = Id> {
	from_mask(get_affinity().fold(0_u128, |mask, id| {
		mask | NODE_TOPOLOGY.get(id).expect("Id must not exceed max cpus")
	}))
}

/// Get the cores sharing SMT resources relative to id.
#[inline]
pub fn smt_affinity(id: Id) -> impl Iterator<Item = Id> {
	from_mask(*SMT_TOPOLOGY.get(id).expect("ID must not exceed max cpus"))
}

/// Get the cores sharing Node resources relative to id.
#[inline]
pub fn node_affinity(id: Id) -> impl Iterator<Item = Id> {
	from_mask(*NODE_TOPOLOGY.get(id).expect("ID must not exceed max cpus"))
}

/// Get the number of threads which could execute in parallel based on hardware
/// constraints of this system.
#[inline]
#[must_use]
pub fn available_parallelism() -> usize { cores_available().count() }

/// Gets the ID of the nth core available. This bijects our sequence of cores to
/// actual ID's which may have gaps for cores which are not available.
#[inline]
#[must_use]
pub fn nth_core_available(i: usize) -> Option<Id> { cores_available().nth(i) }

/// Determine if core (by id) is available to the process.
#[inline]
#[must_use]
pub fn is_core_available(id: Id) -> bool { cores_available().any(is_equal_to!(id)) }

/// Get the list of cores available. The values were recorded at program start.
#[inline]
pub fn cores_available() -> impl Iterator<Item = Id> { from_mask(*CORES_AVAILABLE) }

#[cfg(target_os = "linux")]
#[inline]
pub fn getcpu() -> Result<usize> {
	use crate::{Error, utils::math};

	// SAFETY: This is part of an interface with many low-level calls taking many
	// raw params, but it's unclear why this specific call is unsafe. Nevertheless
	// the value obtained here is semantically unsafe because it can change on the
	// instruction boundary trailing its own acquisition and also any other time.
	let ret: i32 = unsafe { libc::sched_getcpu() };

	#[cfg(target_os = "linux")]
	// SAFETY: On modern linux systems with a vdso if we can optimize away the branch checking
	// for error (see getcpu(2)) then this system call becomes a memory access.
	unsafe {
		std::hint::assert_unchecked(ret >= 0);
	};

	if ret == -1 {
		return Err(Error::from_errno());
	}

	math::try_into(ret)
}

#[cfg(not(target_os = "linux"))]
#[inline]
pub fn getcpu() -> Result<usize> { Err(crate::Error::Io(std::io::ErrorKind::Unsupported.into())) }

/// Query the cores this process may run on. Only the first MASK_BITS ids are
/// reported; higher ids cannot be represented in our masks.
#[cfg(target_os = "linux")]
fn query_cores_available() -> impl Iterator<Item = Id> {
	// SAFETY: An all-zero cpu_set_t is a valid (empty) set.
	let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };

	// SAFETY: pid 0 is the calling thread; the pointer and size describe `set`.
	let ret = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &raw mut set) };

	let bytes = size_of::<libc::cpu_set_t>();
	let bits = if ret == 0 {
		expected!(bytes * 8).min(MASK_BITS)
	} else {
		0
	};

	// SAFETY: id is below the bit capacity of the set.
	(0..bits).filter(move |&id| unsafe { libc::CPU_ISSET(id, &set) })
}

/// Pinning is only implemented on linux. Elsewhere the cores are reported so
/// parallelism queries remain correct, but set_affinity() is a no-op.
#[cfg(not(target_os = "linux"))]
fn query_cores_available() -> impl Iterator<Item = Id> {
	let n = std::thread::available_parallelism().map_or(1, usize::from);
	0..n.min(MASK_BITS)
}

/// Apply the mask to the calling thread. Returns false on failure, in which
/// case the prior affinity is unchanged.
#[cfg(target_os = "linux")]
fn set_thread_affinity(mask: Mask) -> bool {
	// SAFETY: An all-zero cpu_set_t is a valid (empty) set.
	let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
	for id in from_mask(mask) {
		// SAFETY: id < MASK_BITS (128) which is within the capacity of the set.
		unsafe { libc::CPU_SET(id, &mut set) };
	}

	// SAFETY: pid 0 is the calling thread; the pointer and size describe `set`.
	let ret = unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &raw const set) };

	ret == 0
}

#[cfg(not(target_os = "linux"))]
fn set_thread_affinity(_: Mask) -> bool { false }

fn init_smt_topology() -> [Mask; MASK_BITS] { [Mask::default(); MASK_BITS] }

fn init_node_topology() -> [Mask; MASK_BITS] { [Mask::default(); MASK_BITS] }

fn into_mask<I>(ids: I) -> Mask
where
	I: Iterator<Item = Id>,
{
	ids.inspect(|&id| {
		debug_assert!(id < MASK_BITS, "Core ID must be < Mask::BITS at least for now");
	})
	.fold(Mask::default(), |mask, id| mask | (1 << id))
}

fn from_mask(v: Mask) -> impl Iterator<Item = Id> {
	(0..MASK_BITS).filter(move |&i| (v & (1 << i)) != 0)
}

fn _sys_path(id: usize, suffix: &str) -> PathBuf {
	format!("/sys/devices/system/cpu/cpu{id}/{suffix}").into()
}
