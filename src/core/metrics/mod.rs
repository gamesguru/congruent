use std::sync::atomic::{AtomicU32, AtomicU64};

pub struct Metrics {
	// TODO: move stats
	pub requests_handle_active: AtomicU32,
	pub requests_handle_finished: AtomicU32,
	pub requests_panic: AtomicU32,

	// Router aggregated metrics
	pub requests_success: AtomicU64,
	pub requests_fail: AtomicU64,
	pub requests_time: AtomicU64,

	// DNS aggregated metrics
	pub dns_requests_success: AtomicU64,
	pub dns_requests_fail: AtomicU64,
	pub dns_requests_time: AtomicU64,

	// Federation metrics
	pub transactions_processed: AtomicU64,
	pub transactions_time: AtomicU64,
	pub transactions_max_time_1m: AtomicU64,
	pub transactions_slow_1s: AtomicU64,
	pub transactions_slow_10s: AtomicU64,
	pub transactions_slow_100s: AtomicU64,

	// 1-minute rates
	pub requests_rate_1m: AtomicU64,
	pub dns_rate_1m: AtomicU64,
	pub transactions_rate_1m: AtomicU64,

	// Background metrics
	pub presence_pending_updates: AtomicU64,
	pub federation_active_rooms: AtomicU32,
	pub sending_queue_total: AtomicU64,
	pub state_res_active: AtomicU32,
	pub auth_chain_fetches_active: AtomicU32,
	pub space_hierarchy_workers_active: AtomicU32,
}

impl Metrics {
	#[must_use]
	pub fn new<T>(_runtime: Option<&T>) -> Self {
		Self {
			requests_handle_active: AtomicU32::new(0),
			requests_handle_finished: AtomicU32::new(0),
			requests_panic: AtomicU32::new(0),

			requests_success: AtomicU64::new(0),
			requests_fail: AtomicU64::new(0),
			requests_time: AtomicU64::new(0),

			dns_requests_success: AtomicU64::new(0),
			dns_requests_fail: AtomicU64::new(0),
			dns_requests_time: AtomicU64::new(0),

			transactions_processed: AtomicU64::new(0),
			transactions_time: AtomicU64::new(0),
			transactions_max_time_1m: AtomicU64::new(0),
			transactions_slow_1s: AtomicU64::new(0),
			transactions_slow_10s: AtomicU64::new(0),
			transactions_slow_100s: AtomicU64::new(0),
			requests_rate_1m: AtomicU64::new(0),
			dns_rate_1m: AtomicU64::new(0),
			transactions_rate_1m: AtomicU64::new(0),
			presence_pending_updates: AtomicU64::new(0),
			federation_active_rooms: AtomicU32::new(0),
			sending_queue_total: AtomicU64::new(0),
			state_res_active: AtomicU32::new(0),
			auth_chain_fetches_active: AtomicU32::new(0),
			space_hierarchy_workers_active: AtomicU32::new(0),
		}
	}

	#[inline]
	pub fn num_workers(&self) -> usize {
		std::thread::available_parallelism().map_or(1, usize::from)
	}
}
