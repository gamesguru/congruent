use std::sync::atomic::{AtomicU32, AtomicU64};

use tokio::runtime;
#[cfg(feature = "tokio_metrics")]
use tokio_metrics::TaskMonitor;
#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
use tokio_metrics::{RuntimeIntervals, RuntimeMonitor};

pub struct Metrics {
	_runtime: Option<runtime::Handle>,

	runtime_metrics: Option<runtime::RuntimeMetrics>,

	#[cfg(feature = "tokio_metrics")]
	task_monitor: Option<TaskMonitor>,

	#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
	_runtime_monitor: Option<RuntimeMonitor>,

	#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
	runtime_intervals: std::sync::Mutex<Option<RuntimeIntervals>>,

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
	pub fn new(runtime: Option<&runtime::Handle>) -> Self {
		#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
		let runtime_monitor = runtime.as_ref().map(|rt| RuntimeMonitor::new(*rt));

		#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
		let runtime_intervals = runtime_monitor.as_ref().map(RuntimeMonitor::intervals);

		Self {
			_runtime: runtime.cloned(),

			runtime_metrics: runtime.map(runtime::Handle::metrics),

			#[cfg(feature = "tokio_metrics")]
			task_monitor: runtime.map(|_| TaskMonitor::new()),

			#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
			_runtime_monitor: runtime_monitor,

			#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
			runtime_intervals: std::sync::Mutex::new(runtime_intervals),

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

	#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
	pub fn runtime_interval(&self) -> Option<tokio_metrics::RuntimeMetrics> {
		self.runtime_intervals
			.lock()
			.expect("locked")
			.as_mut()
			.map(Iterator::next)
			.expect("next interval")
	}

	#[inline]
	#[cfg(feature = "tokio_metrics")]
	pub fn task_root(&self) -> Option<&TaskMonitor> { self.task_monitor.as_ref() }

	#[inline]
	pub fn num_workers(&self) -> usize {
		self.runtime_metrics()
			.map_or(0, runtime::RuntimeMetrics::num_workers)
	}

	#[inline]
	pub fn runtime_metrics(&self) -> Option<&runtime::RuntimeMetrics> {
		self.runtime_metrics.as_ref()
	}
}
