//! Read-only audit of the global derived short-ID indexes.
//!
//! Phase 1 of `check-rooms --deep`. Verifies the short-ID bijections and the
//! derived edge/state index families against the canonical `eventid_pdu` and
//! short-ID maps. It never decodes a PDU and never writes to the database.

use std::{fmt::Write as _, sync::Arc};

use conduwuit_core::utils::stream::TryIgnore;
use conduwuit_database::{Get, Map};
use conduwuit_service::Services;
use futures::{StreamExt, pin_mut, stream};

/// Bitmap over the short-ID space; one bit per id up to the global counter.
type Bits = Vec<u64>;

/// Short-ID space above which the scan refuses to allocate bitmaps.
const MAX_SHORT: u64 = 1 << 30;

#[derive(Debug, Default)]
pub(super) struct DerivedIndexAudit {
	pub counter: u64,
	pub unverifiable: bool,

	pub event_rows_fwd: u64,
	pub event_rows_rev: u64,
	pub event_dangling: u64,
	pub event_reverse_only: u64,
	pub event_malformed: u64,

	pub statekey_rows_fwd: u64,
	pub statekey_rows_rev: u64,
	pub statekey_dangling: u64,
	pub statekey_reverse_only: u64,
	pub statekey_malformed: u64,

	pub canonical_events: u64,
	pub canonical_bits: u64,
	pub internal_error: Option<String>,

	pub prev_rows: u64,
	pub prev_stale: u64,
	pub prev_missing: u64,
	pub prev_malformed: u64,
	pub prev_parent_unresolved: u64,
	pub prev_parent_absent: u64,
	pub prev_stale_samples: Vec<u64>,
	pub prev_parent_unresolved_samples: Vec<u64>,
	pub prev_parent_absent_samples: Vec<u64>,

	pub auth_rows: u64,
	pub auth_stale: u64,
	pub auth_missing: u64,
	pub auth_malformed: u64,
	pub auth_parent_unresolved: u64,
	pub auth_parent_absent: u64,
	pub auth_stale_samples: Vec<u64>,
	pub auth_parent_unresolved_samples: Vec<u64>,
	pub auth_parent_absent_samples: Vec<u64>,

	pub event_reverse_only_samples: Vec<u64>,

	pub authchain_rows: u64,

	pub statediff_rows: u64,
	pub statediff_malformed: u64,
	pub statediff_missing_parents: u64,

	pub digest_rows: u64,
	pub digest_duplicate_shorts: u64,
	pub digest_malformed: u64,

	pub event_statehash_rows: u64,
	pub event_statehash_dangling: u64,
	pub event_statehash_malformed: u64,
}

impl DerivedIndexAudit {
	pub(super) fn report(&self) -> String {
		let mut out = String::new();
		if self.unverifiable {
			writeln!(
				out,
				"  short-id counter {} exceeds the verifiable bound; audit skipped.",
				self.counter
			)
			.ok();
			return out;
		}

		writeln!(out, "  short-id counter: {}", self.counter).ok();
		match &self.internal_error {
			| Some(error) => {
				writeln!(out, "  audit validity: INVALID -- {error}").ok();
				writeln!(
					out,
					"  (prev/auth figures below are internally inconsistent; do not act on them)"
				)
				.ok();
			},
			| None => {
				writeln!(out, "  audit validity: ok").ok();
			},
		}
		writeln!(
			out,
			"  eventid_shorteventid <-> shorteventid_eventid: fwd_rows={}, rev_rows={}, \
			 dangling={}, reverse_only={}, malformed={}",
			self.event_rows_fwd,
			self.event_rows_rev,
			self.event_dangling,
			self.event_reverse_only,
			self.event_malformed
		)
		.ok();
		write_samples(&mut out, "eventid reverse_only samples", &self.event_reverse_only_samples);
		writeln!(
			out,
			"  statekey_shortstatekey <-> shortstatekey_statekey: fwd_rows={}, rev_rows={}, \
			 dangling={}, reverse_only={}, malformed={}",
			self.statekey_rows_fwd,
			self.statekey_rows_rev,
			self.statekey_dangling,
			self.statekey_reverse_only,
			self.statekey_malformed
		)
		.ok();
		writeln!(
			out,
			"  canonical events (eventid_pdu): {}, distinct shorts: {}",
			self.canonical_events, self.canonical_bits
		)
		.ok();
		writeln!(
			out,
			"  shorteventid_shortprevevents: rows={}, stale={}, missing={}, malformed={}, \
			 parent_unresolved={}, parent_absent={}",
			self.prev_rows,
			self.prev_stale,
			self.prev_missing,
			self.prev_malformed,
			self.prev_parent_unresolved,
			self.prev_parent_absent
		)
		.ok();
		write_samples(&mut out, "prev stale samples", &self.prev_stale_samples);
		write_samples(
			out,
			"prev parent_unresolved samples",
			&self.prev_parent_unresolved_samples,
		);
		write_samples(
			out,
			"prev parent_absent samples",
			&self.prev_parent_absent_samples,
		);
		writeln!(
			out,
			"  shorteventid_shortauthevents: rows={}, stale={}, missing={}, malformed={}, \
			 parent_unresolved={}, parent_absent={}",
			self.auth_rows,
			self.auth_stale,
			self.auth_missing,
			self.auth_malformed,
			self.auth_parent_unresolved,
			self.auth_parent_absent
		)
		.ok();
		write_samples(&mut out, "auth stale samples", &self.auth_stale_samples);
		write_samples(
			out,
			"auth parent_unresolved samples",
			&self.auth_parent_unresolved_samples,
		);
		write_samples(
			out,
			"auth parent_absent samples",
			&self.auth_parent_absent_samples,
		);
		writeln!(out, "  shorteventid_authchain: rows={}", self.authchain_rows).ok();
		writeln!(
			out,
			"  shortstatehash_statediff: rows={}, malformed={}, missing_parents={}",
			self.statediff_rows, self.statediff_malformed, self.statediff_missing_parents
		)
		.ok();
		writeln!(
			out,
			"  statehash_shortstatehash: rows={}, duplicate_shorts={}, malformed={}",
			self.digest_rows, self.digest_duplicate_shorts, self.digest_malformed
		)
		.ok();
		writeln!(
			out,
			"  shorteventid_shortstatehash: rows={}, dangling={}, malformed={}",
			self.event_statehash_rows,
			self.event_statehash_dangling,
			self.event_statehash_malformed
		)
		.ok();

		out
	}
}

/// Runs every phase-1 check. Infallible: unreadable rows are counted, not
/// propagated, so the audit always completes.
pub(super) async fn audit(services: &Services) -> DerivedIndexAudit {
	let counter = services.globals.current_count().unwrap_or(0);
	if counter >= MAX_SHORT {
		return DerivedIndexAudit {
			counter,
			unverifiable: true,
			..Default::default()
		};
	}
	let words = usize::try_from(counter / 64).unwrap_or(0).saturating_add(1);

	let mut audit = DerivedIndexAudit { counter, ..Default::default() };

	// Event short-ID bijection.
	let (event_fwd, event_fwd_malformed) =
		forward_vals(&services.db["eventid_shorteventid"], words).await;
	let (event_rev, event_rev_rows, event_rev_malformed) =
		reverse_keys(&services.db["shorteventid_eventid"], words).await;
	audit.event_rows_fwd = count_bits(&event_fwd);
	audit.event_rows_rev = event_rev_rows;
	audit.event_malformed = event_fwd_malformed.saturating_add(event_rev_malformed);
	audit.event_dangling = masked_diff_count(&event_fwd, &event_rev, counter);
	audit.event_reverse_only = masked_diff_count(&event_rev, &event_fwd, counter);
	audit.event_reverse_only_samples = sample_diff(&event_rev, &event_fwd, counter, 8);

	// State-key short-ID bijection.
	let (statekey_fwd, statekey_fwd_malformed) =
		forward_vals(&services.db["statekey_shortstatekey"], words).await;
	let (statekey_rev, statekey_rev_rows, statekey_rev_malformed) =
		reverse_keys(&services.db["shortstatekey_statekey"], words).await;
	audit.statekey_rows_fwd = count_bits(&statekey_fwd);
	audit.statekey_rows_rev = statekey_rev_rows;
	audit.statekey_malformed = statekey_fwd_malformed.saturating_add(statekey_rev_malformed);
	audit.statekey_dangling = masked_diff_count(&statekey_fwd, &statekey_rev, counter);
	audit.statekey_reverse_only = masked_diff_count(&statekey_rev, &statekey_fwd, counter);

	// Canonical short-ID set (one short id per eventid_pdu row).
	let (canonical, canonical_events) = canonical_bits(services, words).await;
	audit.canonical_events = canonical_events;
	audit.canonical_bits = count_bits(&canonical);
	let canonical_not_fwd = masked_diff_count(&canonical, &event_fwd, counter);

	// Short prev/auth edge families.
	let prev =
		scan_edges(&services.db["shorteventid_shortprevevents"], &event_rev, &canonical, words)
			.await;
	audit.prev_rows = prev.rows;
	audit.prev_malformed = prev.malformed;
	audit.prev_parent_unresolved = prev.parent_unresolved;
	audit.prev_parent_absent = prev.parent_absent;
	audit.prev_parent_unresolved_samples = prev.parent_unresolved_samples;
	audit.prev_parent_absent_samples = prev.parent_absent_samples;
	audit.prev_stale = masked_diff_count(&prev.indexed, &canonical, counter);
	audit.prev_missing = masked_diff_count(&canonical, &prev.indexed, counter);
	audit.prev_stale_samples = sample_diff(&prev.indexed, &canonical, counter, 8);

	let auth =
		scan_edges(&services.db["shorteventid_shortauthevents"], &event_rev, &canonical, words)
			.await;
	audit.auth_rows = auth.rows;
	audit.auth_malformed = auth.malformed;
	audit.auth_parent_unresolved = auth.parent_unresolved;
	audit.auth_parent_absent = auth.parent_absent;
	audit.auth_parent_unresolved_samples = auth.parent_unresolved_samples;
	audit.auth_parent_absent_samples = auth.parent_absent_samples;
	audit.auth_stale = masked_diff_count(&auth.indexed, &canonical, counter);
	audit.auth_missing = masked_diff_count(&canonical, &auth.indexed, counter);
	audit.auth_stale_samples = sample_diff(&auth.indexed, &canonical, counter, 8);

	// Auth-chain cache: room-prefixed keys, counted but not deeply verified.
	audit.authchain_rows = u64::try_from(
		services.db["shorteventid_authchain"]
			.raw_keys()
			.ignore_err()
			.count()
			.await,
	)
	.unwrap_or(u64::MAX);

	// State-hash family.
	let (statediff, statediff_rows, statediff_malformed) =
		reverse_keys(&services.db["shortstatehash_statediff"], words).await;
	audit.statediff_rows = statediff_rows;
	audit.statediff_malformed = statediff_malformed;
	audit.statediff_missing_parents =
		statediff_missing_parents(&services.db["shortstatehash_statediff"], &statediff).await;

	let (digest_rows, digest_dups, digest_malformed) =
		digest_claims(&services.db["statehash_shortstatehash"], words).await;
	audit.digest_rows = digest_rows;
	audit.digest_duplicate_shorts = digest_dups;
	audit.digest_malformed = digest_malformed;

	let (esh_rows, esh_dangling, esh_malformed) =
		event_statehash(&services.db["shorteventid_shortstatehash"], &statediff, counter).await;
	audit.event_statehash_rows = esh_rows;
	audit.event_statehash_dangling = esh_dangling;
	audit.event_statehash_malformed = esh_malformed;

	audit.internal_error = consistency_error(&audit, canonical_not_fwd);

	audit
}

struct EdgeFamily {
	rows: u64,
	malformed: u64,
	parent_unresolved: u64,
	parent_absent: u64,
	parent_unresolved_samples: Vec<u64>,
	parent_absent_samples: Vec<u64>,
	indexed: Bits,
}

async fn scan_edges(
	map: &Arc<Map>,
	reverse_events: &Bits,
	canonical: &Bits,
	words: usize,
) -> EdgeFamily {
	const SAMPLES: usize = 8;
	let folded = map
		.raw_stream()
		.ignore_err()
		.fold(
			(
				vec![0_u64; words],
				0_u64,
				0_u64,
				0_u64,
				0_u64,
				Vec::new(),
				Vec::new(),
			),
			|(
				mut indexed,
				mut rows,
				mut malformed,
				mut parent_unresolved,
				mut parent_absent,
				mut unresolved_samples,
				mut absent_samples,
			),
			 (key, val)| async move {
				rows = rows.saturating_add(1);
				match short_of(key) {
					| Some(short) => set_bit(&mut indexed, short),
					| None => malformed = malformed.saturating_add(1),
				}

				if val.len().is_multiple_of(8) {
					for chunk in val.as_chunks::<8>().0 {
						let parent = u64::from_be_bytes(*chunk);

						if !get_bit(reverse_events, parent) {
							parent_unresolved = parent_unresolved.saturating_add(1);
							if unresolved_samples.len() < SAMPLES {
								unresolved_samples.push(parent);
							}
						} else if !get_bit(canonical, parent) {
							parent_absent = parent_absent.saturating_add(1);
							if absent_samples.len() < SAMPLES {
								absent_samples.push(parent);
							}
						}
					}
				} else {
					malformed = malformed.saturating_add(1);
				}

				(
					indexed,
					rows,
					malformed,
					parent_unresolved,
					parent_absent,
					unresolved_samples,
					absent_samples,
				)
			},
		)
		.await;

	EdgeFamily {
		rows: folded.1,
		malformed: folded.2,
		parent_unresolved: folded.3,
		parent_absent: folded.4,
		parent_unresolved_samples: folded.5,
		parent_absent_samples: folded.6,
		indexed: folded.0,
	}
}

/// Builds the canonical short-ID bitmap from `eventid_pdu` via batched lookups
/// in `eventid_shorteventid`. Returns the bitmap and the number resolved.
async fn canonical_bits(services: &Services, words: usize) -> (Bits, u64) {
	let pdu = services.db["eventid_pdu"].clone();
	let lookup = services.db["eventid_shorteventid"].clone();
	let mut bits = vec![0_u64; words];
	let mut count = 0_u64;

	let keys = pdu.raw_keys().chunks(1024);
	pin_mut!(keys);

	while let Some(chunk) = keys.next().await {
		let ids: Vec<Vec<u8>> = chunk
			.into_iter()
			.filter_map(Result::ok)
			.map(<[u8]>::to_vec)
			.collect();
		if ids.is_empty() {
			continue;
		}

		let results = stream::iter(ids).get(&lookup);
		pin_mut!(results);

		while let Some(res) = results.next().await {
			if let Ok(handle) = res {
				if let Some(short) = short_of(handle.as_ref()) {
					set_bit(&mut bits, short);
					count = count.saturating_add(1);
				}
			}
		}
	}

	(bits, count)
}

/// Bitmap of a reverse map's 8-byte keys, plus row count and malformed count.
async fn reverse_keys(map: &Arc<Map>, words: usize) -> (Bits, u64, u64) {
	map.raw_keys()
		.ignore_err()
		.fold(
			(vec![0_u64; words], 0_u64, 0_u64),
			|(mut bits, mut rows, mut malformed), key| async move {
				rows = rows.saturating_add(1);
				match short_of(key) {
					| Some(short) => set_bit(&mut bits, short),
					| None => malformed = malformed.saturating_add(1),
				}
				(bits, rows, malformed)
			},
		)
		.await
}

/// Bitmap of a forward map's 8-byte values, plus malformed value count.
async fn forward_vals(map: &Arc<Map>, words: usize) -> (Bits, u64) {
	map.raw_stream()
		.ignore_err()
		.fold(
			(vec![0_u64; words], 0_u64),
			|(mut bits, mut malformed), (_key, val)| async move {
				match short_of(val) {
					| Some(short) => set_bit(&mut bits, short),
					| None => malformed = malformed.saturating_add(1),
				}
				(bits, malformed)
			},
		)
		.await
}

/// Counts statediff rows whose 8-byte parent has no statediff key.
async fn statediff_missing_parents(map: &Arc<Map>, keys: &Bits) -> u64 {
	map.raw_stream()
		.ignore_err()
		.fold(0_u64, |missing, (key, val)| {
			let parent = short_of(key).and_then(|_| val.get(0..8)).and_then(short_of);
			let is_missing = parent.is_some_and(|parent| parent != 0 && !get_bit(keys, parent));
			async move { missing.saturating_add(u64::from(is_missing)) }
		})
		.await
}

/// Claims short state-hash ids from digest rows, counting duplicate claims.
async fn digest_claims(map: &Arc<Map>, words: usize) -> (u64, u64, u64) {
	let (_, rows, dups, malformed) = map
		.raw_stream()
		.ignore_err()
		.fold(
			(vec![0_u64; words], 0_u64, 0_u64, 0_u64),
			|(mut bits, mut rows, mut dups, mut malformed), (_key, val)| async move {
				rows = rows.saturating_add(1);
				if val.len() == 8 {
					if let Some(short) = short_of(val) {
						if get_bit(&bits, short) {
							dups = dups.saturating_add(1);
						} else {
							set_bit(&mut bits, short);
						}
					} else {
						malformed = malformed.saturating_add(1);
					}
				} else {
					malformed = malformed.saturating_add(1);
				}
				(bits, rows, dups, malformed)
			},
		)
		.await;
	(rows, dups, malformed)
}

/// Checks `shorteventid_shortstatehash` values exist as statediff keys.
async fn event_statehash(map: &Arc<Map>, statediff: &Bits, counter: u64) -> (u64, u64, u64) {
	map.raw_stream()
		.ignore_err()
		.fold(
			(0_u64, 0_u64, 0_u64),
			|(mut rows, mut dangling, mut malformed), (_key, val)| async move {
				rows = rows.saturating_add(1);
				if val.len() == 8 {
					if let Some(state) = short_of(val) {
						if state != 0 && state <= counter && !get_bit(statediff, state) {
							dangling = dangling.saturating_add(1);
						}
					} else {
						malformed = malformed.saturating_add(1);
					}
				} else {
					malformed = malformed.saturating_add(1);
				}
				(rows, dangling, malformed)
			},
		)
		.await
}

fn short_of(bytes: &[u8]) -> Option<u64> { bytes.try_into().ok().map(u64::from_be_bytes) }

fn count_bits(bits: &[u64]) -> u64 { bits.iter().map(|word| u64::from(word.count_ones())).sum() }

/// `popcount(a & !b)` over the bits `0..=counter`.
fn masked_diff_count(a: &[u64], b: &[u64], counter: u64) -> u64 {
	let last = usize::try_from(counter / 64).unwrap_or(usize::MAX);
	let tail = u64::MAX >> 63_u64.saturating_sub(counter % 64);

	a.iter()
		.zip(b.iter())
		.enumerate()
		.map(|(word, (&x, &y))| {
			let mask = match word.cmp(&last) {
				| std::cmp::Ordering::Less => u64::MAX,
				| std::cmp::Ordering::Equal => tail,
				| std::cmp::Ordering::Greater => 0,
			};
			u64::from((x & !y & mask).count_ones())
		})
		.sum()
}

/// First `limit` short ids set in `a` but not `b`, within `0..=counter`.
fn sample_diff(a: &[u64], b: &[u64], counter: u64, limit: usize) -> Vec<u64> {
	let last = usize::try_from(counter / 64).unwrap_or(usize::MAX);
	let tail = u64::MAX >> 63_u64.saturating_sub(counter % 64);
	let mut out = Vec::new();

	for (word, (&x, &y)) in a.iter().zip(b.iter()).enumerate() {
		let mask = match word.cmp(&last) {
			| std::cmp::Ordering::Less => u64::MAX,
			| std::cmp::Ordering::Equal => tail,
			| std::cmp::Ordering::Greater => 0,
		};
		let mut bits = x & !y & mask;

		while bits != 0 {
			let bit = bits.trailing_zeros();
			let short = u64::try_from(word)
				.unwrap_or(u64::MAX)
				.saturating_mul(64)
				.saturating_add(u64::from(bit));
			out.push(short);

			if out.len() >= limit {
				return out;
			}

			bits &= bits.wrapping_sub(1);
		}
	}

	out
}

/// Detects internal contradictions that mean the reported prev/auth figures
/// cannot be trusted. Returning `Some` marks the whole audit `INVALID`.
fn consistency_error(audit: &DerivedIndexAudit, canonical_not_fwd: u64) -> Option<String> {
	let mut errors = Vec::new();

	if audit.canonical_events > 0 && audit.canonical_bits == 0 {
		errors.push(format!(
			"{} canonical lookups resolved but zero short-id bits were set",
			audit.canonical_events
		));
	}

	if audit.canonical_bits > audit.canonical_events {
		errors.push(format!(
			"canonical distinct shorts ({}) exceed successful lookups ({})",
			audit.canonical_bits, audit.canonical_events
		));
	}

	if canonical_not_fwd > 0 {
		errors.push(format!(
			"{canonical_not_fwd} canonical shorts are absent from eventid_shorteventid"
		));
	}

	if audit.prev_missing == 0 {
		let spread = audit.prev_stale.saturating_add(audit.canonical_bits);
		if spread > audit.prev_rows {
			errors.push(format!(
				"prev_missing=0 requires canonical ⊆ indexed, but prev_stale ({}) + canonical_bits \
				 ({}) = {spread} exceeds prev_rows ({})",
				audit.prev_stale, audit.prev_rows
			));
		}
	}

	if errors.is_empty() {
		None
	} else {
		Some(errors.join("; "))
	}
}

fn write_samples(out: &mut String, label: &str, samples: &[u64]) {
	if samples.is_empty() {
		return;
	}

	let list: Vec<String> = samples.iter().map(|short| format!("{short:#x}")).collect();
	writeln!(out, "    {label}: {}", list.join(" ")).ok();
}

fn set_bit(bits: &mut [u64], index: u64) {
	if let Some(word) = usize::try_from(index / 64)
		.ok()
		.and_then(|word| bits.get_mut(word))
	{
		*word |= 1_u64 << (index % 64);
	}
}

fn get_bit(bits: &[u64], index: u64) -> bool {
	usize::try_from(index / 64)
		.ok()
		.and_then(|word| bits.get(word))
		.is_some_and(|word| word & (1_u64 << (index % 64)) != 0)
}
