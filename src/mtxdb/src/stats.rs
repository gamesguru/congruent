//! Process-wide mtxdb operation counters, cheap enough to leave always on.
//!
//! Totals only: callers that want a rate or per-phase cost take a
//! [`Snapshot`] before and after and subtract.

use std::{
	fmt,
	sync::atomic::{AtomicU64, Ordering::Relaxed},
	time::Duration,
};

static READS: AtomicU64 = AtomicU64::new(0);
static READ_NODES: AtomicU64 = AtomicU64::new(0);
static READ_NANOS: AtomicU64 = AtomicU64::new(0);
static READ_ERRORS: AtomicU64 = AtomicU64::new(0);
static PUTS: AtomicU64 = AtomicU64::new(0);
static PUT_BYTES: AtomicU64 = AtomicU64::new(0);
static REMOVES: AtomicU64 = AtomicU64::new(0);
static COMMITS: AtomicU64 = AtomicU64::new(0);
static COMMIT_NANOS: AtomicU64 = AtomicU64::new(0);
static COMMIT_ERRORS: AtomicU64 = AtomicU64::new(0);

fn nanos(elapsed: Duration) -> u64 { u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX) }

pub(crate) fn record_read(nodes: usize, elapsed: Duration, ok: bool) {
	READS.fetch_add(1, Relaxed);
	READ_NODES.fetch_add(u64::try_from(nodes).unwrap_or(u64::MAX), Relaxed);
	READ_NANOS.fetch_add(nanos(elapsed), Relaxed);
	if !ok {
		READ_ERRORS.fetch_add(1, Relaxed);
	}
}

pub(crate) fn record_put(bytes: usize) {
	PUTS.fetch_add(1, Relaxed);
	PUT_BYTES.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Relaxed);
}

pub(crate) fn record_removes(count: usize) {
	REMOVES.fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Relaxed);
}

pub(crate) fn record_commit(elapsed: Duration, ok: bool) {
	COMMITS.fetch_add(1, Relaxed);
	COMMIT_NANOS.fetch_add(nanos(elapsed), Relaxed);
	if !ok {
		COMMIT_ERRORS.fetch_add(1, Relaxed);
	}
}

/// A point-in-time copy of the counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
	pub reads: u64,
	pub read_nodes: u64,
	pub read_micros: u64,
	pub read_errors: u64,
	pub puts: u64,
	pub put_bytes: u64,
	pub removes: u64,
	pub commits: u64,
	pub commit_micros: u64,
	pub commit_errors: u64,
}

/// Per-pool engine counters suitable for periodic diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolSnapshot {
	pub name: &'static str,
	pub gets: u64,
	pub get_many: u64,
	pub get_many_records: u64,
	pub read_micros: u64,
	pub read_max_micros: u64,
	pub misses: u64,
	pub miss_refreshes: u64,
	pub candidate_reads: u64,
	pub puts: u64,
	pub put_many: u64,
	pub put_records: u64,
	pub put_bytes: u64,
	pub put_micros: u64,
	pub put_max_micros: u64,
	pub syncs: u64,
	pub sync_micros: u64,
	pub pack_flush_micros: u64,
	pub pack_fsync_micros: u64,
	pub wal_micros: u64,
	pub checkpoint_micros: u64,
	pub journal_lock_wait_micros: u64,
	pub journal_syncs: u64,
	pub journal_fsync_micros: u64,
	pub journal_coalesced: u64,
	pub checkpoints: u64,
	pub delta_appends: u64,
	pub index_grows: u64,
	pub index_rebuilds: u64,
	pub repacks: u64,
	pub cache_hits: u64,
	pub cache_misses: u64,
	pub collections: u64,
	pub index_bytes: u64,
}

impl PoolSnapshot {
	fn since(self, earlier: Self) -> Self {
		Self {
			name: self.name,
			gets: self.gets.saturating_sub(earlier.gets),
			get_many: self.get_many.saturating_sub(earlier.get_many),
			get_many_records: self
				.get_many_records
				.saturating_sub(earlier.get_many_records),
			read_micros: self.read_micros.saturating_sub(earlier.read_micros),
			read_max_micros: self.read_max_micros,
			misses: self.misses.saturating_sub(earlier.misses),
			miss_refreshes: self.miss_refreshes.saturating_sub(earlier.miss_refreshes),
			candidate_reads: self.candidate_reads.saturating_sub(earlier.candidate_reads),
			puts: self.puts.saturating_sub(earlier.puts),
			put_many: self.put_many.saturating_sub(earlier.put_many),
			put_records: self.put_records.saturating_sub(earlier.put_records),
			put_bytes: self.put_bytes.saturating_sub(earlier.put_bytes),
			put_micros: self.put_micros.saturating_sub(earlier.put_micros),
			put_max_micros: self.put_max_micros,
			syncs: self.syncs.saturating_sub(earlier.syncs),
			sync_micros: self.sync_micros.saturating_sub(earlier.sync_micros),
			pack_flush_micros: self
				.pack_flush_micros
				.saturating_sub(earlier.pack_flush_micros),
			pack_fsync_micros: self
				.pack_fsync_micros
				.saturating_sub(earlier.pack_fsync_micros),
			wal_micros: self.wal_micros.saturating_sub(earlier.wal_micros),
			checkpoint_micros: self
				.checkpoint_micros
				.saturating_sub(earlier.checkpoint_micros),
			journal_lock_wait_micros: self
				.journal_lock_wait_micros
				.saturating_sub(earlier.journal_lock_wait_micros),
			journal_syncs: self.journal_syncs.saturating_sub(earlier.journal_syncs),
			journal_fsync_micros: self
				.journal_fsync_micros
				.saturating_sub(earlier.journal_fsync_micros),
			journal_coalesced: self
				.journal_coalesced
				.saturating_sub(earlier.journal_coalesced),
			checkpoints: self.checkpoints.saturating_sub(earlier.checkpoints),
			delta_appends: self.delta_appends.saturating_sub(earlier.delta_appends),
			index_grows: self.index_grows.saturating_sub(earlier.index_grows),
			index_rebuilds: self.index_rebuilds.saturating_sub(earlier.index_rebuilds),
			repacks: self.repacks.saturating_sub(earlier.repacks),
			cache_hits: self.cache_hits.saturating_sub(earlier.cache_hits),
			cache_misses: self.cache_misses.saturating_sub(earlier.cache_misses),
			collections: self.collections,
			index_bytes: self.index_bytes,
		}
	}
}

impl fmt::Display for PoolSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"{}: gets={} get_many={} get_many_records={} read_us={} read_max_us={} misses={} \
			 miss_refreshes={} candidate_reads={} puts={} put_many={} put_records={} \
			 put_bytes={} put_us={} put_max_us={} syncs={} sync_us={} flush_us={} \
			 pack_fsync_us={} wal_us={} checkpoint_us={} journal_lock_wait_us={} \
			 journal_syncs={} journal_fsync_us={} journal_coalesced={} checkpoints={} \
			 delta_appends={} index_grows={} index_rebuilds={} repacks={} cache_hits={} \
			 cache_misses={} collections={} index_bytes={}",
			self.name,
			self.gets,
			self.get_many,
			self.get_many_records,
			self.read_micros,
			self.read_max_micros,
			self.misses,
			self.miss_refreshes,
			self.candidate_reads,
			self.puts,
			self.put_many,
			self.put_records,
			self.put_bytes,
			self.put_micros,
			self.put_max_micros,
			self.syncs,
			self.sync_micros,
			self.pack_flush_micros,
			self.pack_fsync_micros,
			self.wal_micros,
			self.checkpoint_micros,
			self.journal_lock_wait_micros,
			self.journal_syncs,
			self.journal_fsync_micros,
			self.journal_coalesced,
			self.checkpoints,
			self.delta_appends,
			self.index_grows,
			self.index_rebuilds,
			self.repacks,
			self.cache_hits,
			self.cache_misses,
			self.collections,
			self.index_bytes,
		)
	}
}

/// Engine-level mtxdb totals, broken down by storage pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineSnapshot {
	pub pools: [PoolSnapshot; 4],
	pub commit_calls: u64,
	pub commit_publish_micros: u64,
	pub commit_materialize_micros: u64,
	pub journal_durable_requests: u64,
	pub journal_sync_requests: u64,
	pub journal_sync_waiters: u64,
	pub journal_sync_coalesced: u64,
	pub journal_fsyncs: u64,
	pub journal_commit_records: u64,
}

impl EngineSnapshot {
	/// Enable per-operation counters on every storage pool.
	pub fn enable(db: &mtxdb::Database) {
		for shard in mtxdb::ShardType::ALL {
			db.pool(shard).set_stats_enabled(true);
		}
	}

	/// Capture current engine counters. Read-path metrics are meaningful after
	/// [`Self::enable`] has been called.
	#[must_use]
	pub fn capture(db: &mtxdb::Database) -> Self {
		let pools = std::array::from_fn(|index| {
			let shard = mtxdb::ShardType::ALL[index];
			let stats = db.pool(shard).stats();
			let latency = |value: Duration| u64::try_from(value.as_micros()).unwrap_or(u64::MAX);
			let name = match shard {
				| mtxdb::ShardType::State => "state",
				| mtxdb::ShardType::EventDag => "event_dag",
				| mtxdb::ShardType::Edges => "edges",
				| mtxdb::ShardType::ServerInfo => "server_info",
			};
			PoolSnapshot {
				name,
				gets: stats.get_calls,
				get_many: stats.get_many_calls,
				get_many_records: stats.get_many_records,
				read_micros: latency(
					stats
						.get_latency
						.total
						.saturating_add(stats.get_many_latency.total),
				),
				read_max_micros: latency(stats.get_latency.max.max(stats.get_many_latency.max)),
				misses: stats.get_misses.saturating_add(stats.get_many_misses),
				miss_refreshes: stats.miss_refreshes,
				candidate_reads: stats.candidate_reads,
				puts: stats.put_calls,
				put_many: stats.put_many_calls,
				put_records: stats.put_many_records,
				put_bytes: stats.put_bytes.saturating_add(stats.put_many_bytes),
				put_micros: latency(
					stats
						.put_latency
						.total
						.saturating_add(stats.put_many_latency.total),
				),
				put_max_micros: latency(stats.put_latency.max.max(stats.put_many_latency.max)),
				syncs: stats.sync_calls,
				sync_micros: latency(stats.sync_totals.total),
				pack_flush_micros: latency(stats.sync_totals.pack_flush),
				pack_fsync_micros: latency(stats.sync_totals.pack_fsync),
				wal_micros: latency(stats.sync_totals.wal),
				checkpoint_micros: latency(stats.sync_totals.checkpoint),
				journal_lock_wait_micros: latency(stats.sync_totals.journal_lock_wait),
				journal_syncs: stats.sync_totals.journal_sync_calls,
				journal_fsync_micros: latency(stats.sync_totals.journal_fsync),
				journal_coalesced: stats.sync_totals.journal_coalesced,
				checkpoints: stats.checkpoint_writes,
				delta_appends: stats.delta_appends,
				index_grows: stats.index_grow_count,
				index_rebuilds: stats.index_rebuild_count,
				repacks: stats.repack.repack_count,
				cache_hits: stats.cache.hits,
				cache_misses: stats.cache.misses,
				collections: u64::try_from(stats.collection_count).unwrap_or(u64::MAX),
				index_bytes: stats.index_bytes,
			}
		});

		let commits = db.commit_phase_stats();
		let durability = db.coordinator().durability_stats();
		let micros = |value: Duration| u64::try_from(value.as_micros()).unwrap_or(u64::MAX);
		Self {
			pools,
			commit_calls: commits.publish.calls,
			commit_publish_micros: micros(commits.publish.total),
			commit_materialize_micros: micros(commits.materialize.total),
			journal_durable_requests: durability.durable_requests,
			journal_sync_requests: durability.sync_requests,
			journal_sync_waiters: durability.sync_waiters,
			journal_sync_coalesced: durability.sync_coalesced,
			journal_fsyncs: durability.commits,
			journal_commit_records: durability.commit_records,
		}
	}

	/// Counter deltas since an earlier snapshot, retaining current gauges.
	#[must_use]
	pub fn since(self, earlier: Self) -> Self {
		Self {
			pools: std::array::from_fn(|index| self.pools[index].since(earlier.pools[index])),
			commit_calls: self.commit_calls.saturating_sub(earlier.commit_calls),
			commit_publish_micros: self
				.commit_publish_micros
				.saturating_sub(earlier.commit_publish_micros),
			commit_materialize_micros: self
				.commit_materialize_micros
				.saturating_sub(earlier.commit_materialize_micros),
			journal_durable_requests: self
				.journal_durable_requests
				.saturating_sub(earlier.journal_durable_requests),
			journal_sync_requests: self
				.journal_sync_requests
				.saturating_sub(earlier.journal_sync_requests),
			journal_sync_waiters: self
				.journal_sync_waiters
				.saturating_sub(earlier.journal_sync_waiters),
			journal_sync_coalesced: self
				.journal_sync_coalesced
				.saturating_sub(earlier.journal_sync_coalesced),
			journal_fsyncs: self.journal_fsyncs.saturating_sub(earlier.journal_fsyncs),
			journal_commit_records: self
				.journal_commit_records
				.saturating_sub(earlier.journal_commit_records),
		}
	}
}

impl fmt::Display for EngineSnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"commit_calls={} commit_publish_us={} commit_materialize_us={} \
			 journal_durable_requests={} journal_sync_requests={} journal_sync_waiters={} \
			 journal_sync_coalesced={} journal_fsyncs={} journal_commit_records={} ",
			self.commit_calls,
			self.commit_publish_micros,
			self.commit_materialize_micros,
			self.journal_durable_requests,
			self.journal_sync_requests,
			self.journal_sync_waiters,
			self.journal_sync_coalesced,
			self.journal_fsyncs,
			self.journal_commit_records,
		)?;
		for (index, pool) in self.pools.iter().enumerate() {
			if index > 0 {
				f.write_str(" | ")?;
			}
			fmt::Display::fmt(pool, f)?;
		}
		Ok(())
	}
}

#[must_use]
pub fn snapshot() -> Snapshot {
	Snapshot {
		reads: READS.load(Relaxed),
		read_nodes: READ_NODES.load(Relaxed),
		read_micros: READ_NANOS.load(Relaxed) / 1_000,
		read_errors: READ_ERRORS.load(Relaxed),
		puts: PUTS.load(Relaxed),
		put_bytes: PUT_BYTES.load(Relaxed),
		removes: REMOVES.load(Relaxed),
		commits: COMMITS.load(Relaxed),
		commit_micros: COMMIT_NANOS.load(Relaxed) / 1_000,
		commit_errors: COMMIT_ERRORS.load(Relaxed),
	}
}

/// Print the process totals to stderr when anything was recorded. Test
/// binaries call this at exit so a whole run gets one summary line.
#[allow(clippy::print_stderr)]
pub fn eprint_totals(label: &str) {
	let totals = snapshot();
	if totals != Snapshot::default() {
		eprintln!("mtxdb stats ({label}): {totals}");
	}
}

impl Snapshot {
	/// Counters accumulated since `earlier`.
	#[must_use]
	pub fn since(self, earlier: Self) -> Self {
		Self {
			reads: self.reads.saturating_sub(earlier.reads),
			read_nodes: self.read_nodes.saturating_sub(earlier.read_nodes),
			read_micros: self.read_micros.saturating_sub(earlier.read_micros),
			read_errors: self.read_errors.saturating_sub(earlier.read_errors),
			puts: self.puts.saturating_sub(earlier.puts),
			put_bytes: self.put_bytes.saturating_sub(earlier.put_bytes),
			removes: self.removes.saturating_sub(earlier.removes),
			commits: self.commits.saturating_sub(earlier.commits),
			commit_micros: self.commit_micros.saturating_sub(earlier.commit_micros),
			commit_errors: self.commit_errors.saturating_sub(earlier.commit_errors),
		}
	}
}

impl fmt::Display for Snapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"reads={} read_nodes={} read_us={} read_errors={} puts={} put_bytes={} removes={} \
			 commits={} commit_us={} commit_errors={}",
			self.reads,
			self.read_nodes,
			self.read_micros,
			self.read_errors,
			self.puts,
			self.put_bytes,
			self.removes,
			self.commits,
			self.commit_micros,
			self.commit_errors,
		)
	}
}
