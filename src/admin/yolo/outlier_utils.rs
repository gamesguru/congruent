//! Pure helper functions for outlier diagnostics — extracted for testability.

/// Status flags for an outlier event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutlierStatus {
	pub is_stuck: bool,
	pub is_rejected: bool,
	pub is_soft_failed: bool,
}

/// Action to take for an outlier based on filter flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutlierAction {
	/// Skip this outlier (doesn't match filter).
	Skip,
	/// Show this outlier (optionally clear markers).
	Show {
		should_clear: bool,
	},
}

/// Determine what action to take for an outlier given the filter flags.
#[must_use]
pub(crate) fn classify_outlier(
	status: &OutlierStatus,
	rejected_filter: bool,
	clear_flag: bool,
) -> OutlierAction {
	let is_poisoned = status.is_rejected || status.is_soft_failed;

	// If --rejected filter is active, skip non-rejected events
	if rejected_filter && !is_poisoned {
		return OutlierAction::Skip;
	}

	OutlierAction::Show { should_clear: clear_flag && is_poisoned }
}

/// Render status flags for display.
#[must_use]
pub(crate) fn render_flags(status: &OutlierStatus) -> String {
	let mut flags = String::new();
	if status.is_stuck {
		flags.push_str(" [STUCK]");
	}
	if status.is_rejected {
		flags.push_str(" [REJECTED]");
	}
	if status.is_soft_failed {
		flags.push_str(" [SOFT-FAIL]");
	}
	flags
}

/// Determine the human-readable header/summary for a list-outliers result.
#[must_use]
pub(crate) fn summary_header(rejected_filter: bool) -> &'static str {
	if rejected_filter {
		"Rejected outliers"
	} else {
		"Outliers"
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// -- classify_outlier tests --

	#[test]
	fn clean_event_shown_without_filter() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: false,
			is_soft_failed: false,
		};
		assert_eq!(classify_outlier(&status, false, false), OutlierAction::Show {
			should_clear: false
		});
	}

	#[test]
	fn clean_event_skipped_with_rejected_filter() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: false,
			is_soft_failed: false,
		};
		assert_eq!(classify_outlier(&status, true, false), OutlierAction::Skip);
	}

	#[test]
	fn rejected_event_shown_with_filter() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: true,
			is_soft_failed: false,
		};
		assert_eq!(classify_outlier(&status, true, false), OutlierAction::Show {
			should_clear: false
		});
	}

	#[test]
	fn rejected_event_shown_and_cleared() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: true,
			is_soft_failed: false,
		};
		assert_eq!(classify_outlier(&status, true, true), OutlierAction::Show {
			should_clear: true
		});
	}

	#[test]
	fn soft_failed_event_shown_with_filter() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: false,
			is_soft_failed: true,
		};
		assert_eq!(classify_outlier(&status, true, false), OutlierAction::Show {
			should_clear: false
		});
	}

	#[test]
	fn soft_failed_event_cleared() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: false,
			is_soft_failed: true,
		};
		assert_eq!(classify_outlier(&status, true, true), OutlierAction::Show {
			should_clear: true
		});
	}

	#[test]
	fn both_rejected_and_soft_failed_cleared() {
		let status = OutlierStatus {
			is_stuck: true,
			is_rejected: true,
			is_soft_failed: true,
		};
		assert_eq!(classify_outlier(&status, true, true), OutlierAction::Show {
			should_clear: true
		});
	}

	#[test]
	fn rejected_event_no_clear_without_flag() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: true,
			is_soft_failed: false,
		};
		assert_eq!(classify_outlier(&status, true, false), OutlierAction::Show {
			should_clear: false
		});
	}

	#[test]
	fn clean_event_not_cleared_even_with_clear_flag() {
		// clear_flag only has effect on poisoned events
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: false,
			is_soft_failed: false,
		};
		assert_eq!(classify_outlier(&status, false, true), OutlierAction::Show {
			should_clear: false
		});
	}

	// -- render_flags tests --

	#[test]
	fn no_flags_for_clean_event() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: false,
			is_soft_failed: false,
		};
		assert_eq!(render_flags(&status), "");
	}

	#[test]
	fn stuck_flag_only() {
		let status = OutlierStatus {
			is_stuck: true,
			is_rejected: false,
			is_soft_failed: false,
		};
		assert_eq!(render_flags(&status), " [STUCK]");
	}

	#[test]
	fn rejected_flag_only() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: true,
			is_soft_failed: false,
		};
		assert_eq!(render_flags(&status), " [REJECTED]");
	}

	#[test]
	fn soft_fail_flag_only() {
		let status = OutlierStatus {
			is_stuck: false,
			is_rejected: false,
			is_soft_failed: true,
		};
		assert_eq!(render_flags(&status), " [SOFT-FAIL]");
	}

	#[test]
	fn all_flags_combined() {
		let status = OutlierStatus {
			is_stuck: true,
			is_rejected: true,
			is_soft_failed: true,
		};
		assert_eq!(render_flags(&status), " [STUCK] [REJECTED] [SOFT-FAIL]");
	}

	#[test]
	fn flag_order_is_stuck_rejected_softfail() {
		let status = OutlierStatus {
			is_stuck: true,
			is_rejected: true,
			is_soft_failed: true,
		};
		let flags = render_flags(&status);
		let stuck_pos = flags.find("[STUCK]").unwrap();
		let rejected_pos = flags.find("[REJECTED]").unwrap();
		let soft_pos = flags.find("[SOFT-FAIL]").unwrap();
		assert!(stuck_pos < rejected_pos);
		assert!(rejected_pos < soft_pos);
	}

	// -- summary_header tests --

	#[test]
	fn header_without_filter() {
		assert_eq!(summary_header(false), "Outliers");
	}

	#[test]
	fn header_with_rejected_filter() {
		assert_eq!(summary_header(true), "Rejected outliers");
	}
}
