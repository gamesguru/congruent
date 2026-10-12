//! Dry run of an orphan sweep over the mtxdb shadow store.
//!
//! The shadow store keeps HAMT nodes per room under the 16-byte prefix of their
//! structural hash, and a room's roots collection holds every root of that
//! room. A node is an orphan when no root of its room reaches it. Reachability
//! is rezzy's `walk_reachable_node_hashes`; this module holds only the types
//! the scan in [`super::store::Store::shadow_sweep_dry_run`] reports with.
//!
//! Nothing here deletes anything.

use std::fmt::Write as _;

/// First 16 bytes of a structural hash: the key a node is stored under in mtxdb.
pub type NodePrefix = [u8; 16];

/// A candidate is scoped by its fixed-size mtxdb room-collection id.
pub type Candidate = ([u8; 16], NodePrefix);

/// How many example prefixes a log line shows.
pub const SAMPLE_LIMIT: usize = 20;

/// Why a room was left out. A skipped room contributes no orphans, so an
/// untrustworthy answer for it cannot look like garbage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
	/// The room id could not be parsed from its key.
	BadRoomId,
	/// The shadow roots collection has no roots for the room.
	NoShadowRoots,
	/// A shadow root did not decode as a root handle.
	UndecodableRoot,
	/// The walk met a node the shadow store lacks or fails to verify.
	IncompleteWalk,
	/// A root was published to the room while it was being scanned, so the
	/// orphan set may be missing a node the new root reaches. A live sweep's
	/// version pin would abort on exactly this.
	RootsChanged,
	/// Reading the shadow store failed.
	Io,
}

/// Counts of skipped rooms, by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SkipCounts {
	pub bad_room_id: usize,
	pub no_shadow_roots: usize,
	pub undecodable_root: usize,
	pub incomplete_walk: usize,
	pub roots_changed: usize,
	pub io: usize,
}

impl SkipCounts {
	pub const fn count(&mut self, reason: SkipReason) {
		let slot = match reason {
			| SkipReason::BadRoomId => &mut self.bad_room_id,
			| SkipReason::NoShadowRoots => &mut self.no_shadow_roots,
			| SkipReason::UndecodableRoot => &mut self.undecodable_root,
			| SkipReason::IncompleteWalk => &mut self.incomplete_walk,
			| SkipReason::RootsChanged => &mut self.roots_changed,
			| SkipReason::Io => &mut self.io,
		};
		*slot = slot.saturating_add(1);
	}

	#[must_use]
	pub const fn total(&self) -> usize {
		self.bad_room_id
			.saturating_add(self.no_shadow_roots)
			.saturating_add(self.undecodable_root)
			.saturating_add(self.incomplete_walk)
			.saturating_add(self.roots_changed)
			.saturating_add(self.io)
	}
}

/// One room to scan, gathered by the caller from `roomid_roothandle`.
#[derive(Debug, Clone)]
pub struct ShadowRoomInput {
	/// The room id as stored in `roomid_roothandle` and used as the shadow key.
	pub room_id: Vec<u8>,
	/// The room's structural key, needed to verify nodes read from the shadow.
	pub structural_key: [u8; 32],
}

/// What a completely scanned room holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomOrphans {
	pub room_id: Vec<u8>,
	/// Nodes stored in the room's shadow collection.
	pub nodes: usize,
	/// Of those, the ones no shadow root of the room reaches.
	pub orphans: Vec<NodePrefix>,
}

/// What a dry-run pass found. Nothing in it has been acted on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShadowSweepReport {
	/// Rooms whose shadow store was walked completely, with their orphans.
	pub rooms: Vec<RoomOrphans>,
	/// Rooms left out, by reason.
	pub skipped: SkipCounts,
	/// Candidates that were also orphans on the previous scheduled invocation.
	/// This is diagnostic only; a root can still be published later.
	pub persisted_two_passes: usize,
}

impl ShadowSweepReport {
	#[must_use]
	pub fn shadow_nodes(&self) -> usize {
		self.rooms
			.iter()
			.fold(0_usize, |total, room| total.saturating_add(room.nodes))
	}

	#[must_use]
	pub fn shadow_orphans(&self) -> usize {
		self.rooms
			.iter()
			.fold(0_usize, |total, room| total.saturating_add(room.orphans.len()))
	}
}

#[must_use]
pub fn persisted_candidates<S1, S2>(
	current: &std::collections::HashSet<Candidate, S1>,
	previous: &std::collections::HashSet<Candidate, S2>,
) -> usize
where
	S1: std::hash::BuildHasher,
	S2: std::hash::BuildHasher,
{
	current
		.iter()
		.filter(|candidate| previous.contains(*candidate))
		.count()
}

/// Up to [`SAMPLE_LIMIT`] prefixes as lowercase hex, for logs.
#[must_use]
pub fn sample<'a, I>(prefixes: I) -> Vec<String>
where
	I: IntoIterator<Item = &'a NodePrefix>,
{
	prefixes
		.into_iter()
		.take(SAMPLE_LIMIT)
		.map(|prefix| {
			prefix
				.iter()
				.fold(String::with_capacity(32), |mut hex, byte| {
					let _ = write!(hex, "{byte:02x}");
					hex
				})
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn prefix(byte: u8) -> NodePrefix { [byte; 16] }

	#[test]
	fn samples_are_capped_and_hex_encoded() {
		let many: Vec<NodePrefix> = (0..=255).map(prefix).collect();
		let shown = sample(&many);
		assert_eq!(shown.len(), SAMPLE_LIMIT);
		assert_eq!(shown[0], "00".repeat(16));
		assert_eq!(shown[1], "01".repeat(16));
	}

	#[test]
	fn skip_counts_tally_each_reason_separately() {
		let mut counts = SkipCounts::default();
		for reason in [
			SkipReason::NoShadowRoots,
			SkipReason::NoShadowRoots,
			SkipReason::IncompleteWalk,
			SkipReason::UndecodableRoot,
			SkipReason::BadRoomId,
			SkipReason::RootsChanged,
			SkipReason::Io,
		] {
			counts.count(reason);
		}
		assert_eq!(counts.no_shadow_roots, 2);
		assert_eq!(counts.incomplete_walk, 1);
		assert_eq!(counts.undecodable_root, 1);
		assert_eq!(counts.bad_room_id, 1);
		assert_eq!(counts.roots_changed, 1);
		assert_eq!(counts.io, 1);
		assert_eq!(counts.total(), 7);
	}

	#[test]
	fn the_report_totals_sum_over_scanned_rooms() {
		let report = ShadowSweepReport {
			rooms: vec![
				RoomOrphans {
					room_id: b"!a:x".to_vec(),
					nodes: 10,
					orphans: vec![prefix(1)],
				},
				RoomOrphans {
					room_id: b"!b:x".to_vec(),
					nodes: 5,
					orphans: vec![prefix(2), prefix(3)],
				},
			],
			skipped: SkipCounts::default(),
			persisted_two_passes: 0,
		};
		assert_eq!(report.shadow_nodes(), 15);
		assert_eq!(report.shadow_orphans(), 3);
	}

	#[test]
	fn candidates_persist_only_for_the_same_room_and_node() {
		let current =
			std::collections::HashSet::from([([1; 16], prefix(1)), ([2; 16], prefix(1))]);
		let previous = std::collections::HashSet::from([([1; 16], prefix(1))]);
		assert_eq!(persisted_candidates(&current, &previous), 1);
	}
}
