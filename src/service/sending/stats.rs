use std::sync::atomic::{AtomicU64, Ordering};

use conduwuit;

/// Lightweight atomic counters for federation activity.
/// Logged periodically and reset after each report.
#[derive(Default)]
pub struct FederationStats {
	pub outgoing_txns: AtomicU64,
	pub outgoing_pdus: AtomicU64,
	pub outgoing_edus: AtomicU64, // Catch-all for unknown
	pub outgoing_receipts: AtomicU64,
	pub outgoing_device_lists: AtomicU64,
	pub outgoing_to_device: AtomicU64,
	pub outgoing_typing: AtomicU64,
	pub outgoing_presence: AtomicU64,
	pub outgoing_errors: AtomicU64,
}

impl FederationStats {
	/// Log a summary and reset all counters. Returns true if any activity
	/// occurred.
	pub fn report_and_reset(&self) -> bool {
		let txns = self.outgoing_txns.swap(0, Ordering::Relaxed);
		let pdus = self.outgoing_pdus.swap(0, Ordering::Relaxed);
		let edus = self.outgoing_edus.swap(0, Ordering::Relaxed);
		let receipts = self.outgoing_receipts.swap(0, Ordering::Relaxed);
		let device_lists = self.outgoing_device_lists.swap(0, Ordering::Relaxed);
		let to_device = self.outgoing_to_device.swap(0, Ordering::Relaxed);
		let typing = self.outgoing_typing.swap(0, Ordering::Relaxed);
		let presence = self.outgoing_presence.swap(0, Ordering::Relaxed);
		let errors = self.outgoing_errors.swap(0, Ordering::Relaxed);

		if txns == 0 && pdus == 0 && edus == 0 {
			return false;
		}

		conduwuit::info!(
			target: "stats",
			"Federation stats: {txns} txns ({pdus} PDUs, {edus} general EDUs, {presence} \
			 presence, {receipts} receipts, {device_lists} device lists, {to_device} to-device, \
			 {typing} typing), {errors} errors"
		);

		true
	}
}
