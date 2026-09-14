//! Abstract multi-armed bandit server pool for federation operations.
//!
//! Each server is an "arm" with:
//! - **Hard constraints**: cooldown timers (429), error budgets — binary
//!   pass/fail
//! - **Soft signals**: named `f64` dimensions (latency, knowledge, abuse, …)
//!   scored via caller-supplied `(weight, name)` tuples
//!
//! Selection picks the available arm with the lowest composite score:
//!   `score = Σ(weight_i × signal_i)`
//!
//! Callers drive the signals:
//! ```ignore
//! let mut pool = ServerPool::from_servers(servers);
//! while let Some(server) = pool.next_scored(&WEIGHTS) {
//!     match send_request(&server, ...).await {
//!         Ok(r) => {
//!             pool.record_success(&server);
//!             pool.add_signal(&server, "knowledge", r.pdus.len() as f64);
//!         },
//!         Err(e) if pool.is_rate_limit_err(&e) => pool.record_rate_limit(&server),
//!         Err(_) => pool.record_error(&server),
//!     }
//! }
//! ```

use std::{
	cmp::Ordering,
	collections::HashMap,
	fmt,
	time::{Duration, Instant},
};

use ruma::OwnedServerName;

/// Endpoint cost constants — how expensive each federation request is for
/// the remote server. Used as values for `add_signal(server,
/// "request_cost", COST_*)` so the bandit penalises servers proportionally
/// to how hard we're hitting them.
///
/// Sky reported that `/state_ids` was crushing his federation worker, while
/// `GET /event` is basically a key-value lookup. These weights reflect
/// that.
pub mod endpoint_cost {
	/// `GET /event/{id}` — trivial key lookup
	pub const EVENT: f64 = 0.1;
	/// `/get_missing_events` — bounded graph walk (max ~20 events)
	pub const MISSING_EVENTS: f64 = 0.5;
	/// `/backfill` — bulk crawl, expected and batched
	pub const BACKFILL: f64 = 1.0;
	/// `/state_ids` — expensive state computation at a point in the DAG
	pub const STATE_IDS: f64 = 5.0;
	/// `/state` — full state dump, very expensive
	pub const STATE: f64 = 10.0;
	/// `/make_join`, `/send_join` — heavyweight join operations
	pub const JOIN: f64 = 8.0;
}

/// Per-server hard constraint state.
#[derive(Debug)]
struct HardState {
	cooldown_until: Option<Instant>,
	backoff_secs: u64,
	errors: usize,
	error_budget: usize,
}

impl Default for HardState {
	fn default() -> Self {
		Self {
			cooldown_until: None,
			backoff_secs: 2,
			errors: 0,
			error_budget: 5,
		}
	}
}

impl HardState {
	fn is_available(&self) -> bool {
		if self.errors >= self.error_budget {
			return false;
		}
		self.cooldown_until
			.is_none_or(|until| Instant::now() >= until)
	}
}

/// A session-scoped multi-armed bandit pool of federation servers.
///
/// TODO: Generalize this concept and use it to build server lists in more areas
/// (like backfilling, missing events, and auth chain fetching).
///
/// Combines hard constraints (cooldown, error budget) with soft signal-based
/// scoring for server selection.
pub struct ServerPool {
	/// Ordered server list (index 0 = initial best-ranked)
	servers: Vec<OwnedServerName>,
	/// Per-server hard constraint state
	hard: HashMap<OwnedServerName, HardState>,
	/// Per-server soft signal dimensions (name → value)
	signals: HashMap<OwnedServerName, HashMap<String, f64>>,
	/// Round-robin cursor fallback (used when all scores are equal)
	cursor: usize,
}

impl ServerPool {
	/// Build a pool from a pre-ranked list of servers.
	pub fn from_servers(servers: Vec<OwnedServerName>) -> Self {
		let mut hard = HashMap::with_capacity(servers.len());
		let mut signals = HashMap::with_capacity(servers.len());
		for (i, s) in servers.iter().enumerate() {
			hard.entry(s.clone()).or_insert_with(HardState::default);
			// Seed initial rank signal so discovery order is a tiebreaker
			let sigs = signals.entry(s.clone()).or_insert_with(HashMap::new);
			let rank = u32::try_from(i).unwrap_or(u32::MAX);
			sigs.insert("rank".into(), f64::from(rank));
		}
		Self { servers, hard, signals, cursor: 0 }
	}

	// ── Accessors ────────────────────────────────────────────────────────

	/// Number of servers in the pool.
	#[must_use]
	pub fn len(&self) -> usize { self.servers.len() }

	/// Is the pool empty?
	#[must_use]
	pub fn is_empty(&self) -> bool { self.servers.is_empty() }

	/// The ordered list of server names.
	#[must_use]
	pub fn server_names(&self) -> &[OwnedServerName] { &self.servers }

	/// Whether more than one server is available.
	#[must_use]
	pub fn is_multi(&self) -> bool { self.servers.len() > 1 }

	// ── Selection ────────────────────────────────────────────────────────

	/// Pick the best available server using weighted signal scoring.
	///
	/// `weights` is a slice of `(signal_name, weight)`. For each available
	/// server, the composite score is `Σ(weight × signal_value)`. The
	/// server with the **lowest** score wins.
	///
	/// Convention:
	/// - **Lower-is-better** dimensions (latency): use positive weight
	/// - **Higher-is-better** dimensions (knowledge): use negative weight
	/// - **Abuse prevention** (consecutive_picks): use positive weight
	///
	/// Automatically increments the `"consecutive_picks"` signal on the
	/// chosen server (self-abuse monitoring).
	///
	/// Returns `None` if all servers are exhausted or in cooldown.
	pub fn next_scored(&mut self, weights: &[(&str, f64)]) -> Option<OwnedServerName> {
		let available: Vec<_> = self
			.servers
			.iter()
			.filter(|s| self.hard.get(*s).is_none_or(HardState::is_available))
			.cloned()
			.collect();

		if available.is_empty() {
			return None;
		}

		let best = available
			.into_iter()
			.min_by(|a, b| {
				let sa = self.compute_score(a, weights);
				let sb = self.compute_score(b, weights);
				sa.partial_cmp(&sb).unwrap_or(Ordering::Equal)
			})
			.expect("available is non-empty");

		// Self-abuse tracking: increment consecutive pick counter
		self.add_signal(&best, "consecutive_picks", 1.0);

		Some(best)
	}

	/// Simple round-robin selection (ignores signals, respects hard
	/// constraints only). Use when no scoring context is needed.
	pub fn next_available(&mut self) -> Option<OwnedServerName> {
		let n = self.servers.len();
		for offset in 0..n {
			let idx = self
				.cursor
				.saturating_add(offset)
				.checked_rem(n)
				.unwrap_or(0);
			let server = &self.servers[idx];
			if self.hard.get(server).is_none_or(HardState::is_available) {
				self.cursor = idx.saturating_add(1).checked_rem(n).unwrap_or(0);
				return Some(server.clone());
			}
		}
		None
	}

	/// Compute the composite score for a server.
	fn compute_score(&self, server: &OwnedServerName, weights: &[(&str, f64)]) -> f64 {
		let sigs = self.signals.get(server);
		weights
			.iter()
			.map(|(name, weight)| {
				let value = sigs.and_then(|s| s.get(*name)).copied().unwrap_or(0.0);
				weight * value
			})
			.sum()
	}

	// ── Signal management ────────────────────────────────────────────────

	/// Add a delta to a named signal for a server.
	pub fn add_signal(&mut self, server: &ruma::ServerName, name: &str, delta: f64) {
		self.signals
			.entry(server.to_owned())
			.or_default()
			.entry(name.to_owned())
			.and_modify(|v| *v += delta)
			.or_insert(delta);
	}

	/// Set a named signal to an absolute value for a server.
	pub fn set_signal(&mut self, server: &ruma::ServerName, name: &str, value: f64) {
		self.signals
			.entry(server.to_owned())
			.or_default()
			.insert(name.to_owned(), value);
	}

	/// Get the current value of a signal for a server.
	#[must_use]
	pub fn get_signal(&self, server: &ruma::ServerName, name: &str) -> f64 {
		self.signals
			.get(server)
			.and_then(|s| s.get(name))
			.copied()
			.unwrap_or(0.0)
	}

	// ── Hard constraint recording ────────────────────────────────────────

	/// Record a successful response. Resets backoff and cooldown.
	pub fn record_success(&mut self, server: &ruma::ServerName) {
		if let Some(state) = self.hard.get_mut(server) {
			state.backoff_secs = 2;
			state.cooldown_until = None;
		}
		// Reset consecutive picks on success (server is cooperating)
		self.set_signal(server, "consecutive_picks", 0.0);
	}

	/// Record a 429 rate limit. Enters exponential backoff cooldown
	/// (2s → 4s → 8s → 16s → 32s max). Does NOT count against error
	/// budget.
	pub fn record_rate_limit(&mut self, server: &ruma::ServerName) {
		if let Some(state) = self.hard.get_mut(server) {
			state.cooldown_until =
				Instant::now().checked_add(Duration::from_secs(state.backoff_secs));
			state.backoff_secs = state.backoff_secs.saturating_mul(2).min(32);
		}
		self.add_signal(server, "rate_limits", 1.0);
	}

	/// Record a non-429 error. Counts against error budget (default 5).
	pub fn record_error(&mut self, server: &ruma::ServerName) {
		if let Some(state) = self.hard.get_mut(server) {
			state.errors = state.errors.saturating_add(1);
		}
		self.add_signal(server, "errors", 1.0);
	}

	/// Record an empty response (dead-end). Short 10s cooldown.
	pub fn record_dead_end(&mut self, server: &ruma::ServerName) {
		if let Some(state) = self.hard.get_mut(server) {
			state.cooldown_until = Instant::now().checked_add(Duration::from_secs(10));
		}
		self.add_signal(server, "dead_ends", 1.0);
	}

	/// Record a federation request with its endpoint cost.
	///
	/// Use [`endpoint_cost`] constants to weight how expensive each
	/// request type is for the remote server:
	///
	/// ```ignore
	/// use server_pool::endpoint_cost;
	/// pool.record_request(&server, endpoint_cost::BACKFILL);   // moderate
	/// pool.record_request(&server, endpoint_cost::EVENT);      // cheap
	/// pool.record_request(&server, endpoint_cost::STATE_IDS);  // expensive
	/// ```
	///
	/// Accumulates into the `"request_cost"` signal, which scoring weights
	/// can use to avoid hammering servers with expensive endpoints.
	pub fn record_request(&mut self, server: &ruma::ServerName, cost: f64) {
		self.add_signal(server, "request_cost", cost);
	}

	// ── Queries ──────────────────────────────────────────────────────────

	/// Are ALL servers currently unavailable?
	#[must_use]
	pub fn all_exhausted(&self) -> bool { self.hard.values().all(|s| !s.is_available()) }

	/// Returns `true` if the error string looks like a 429 rate limit.
	#[must_use]
	pub fn is_rate_limit(err: &str) -> bool { err.contains("429") }

	/// Per-server summary for end-of-operation logging.
	#[must_use]
	pub fn summary(&self) -> String {
		let mut out = String::new();
		for server in &self.servers {
			let sigs = self.signals.get(server);
			let knowledge = sigs
				.and_then(|s| s.get("knowledge"))
				.copied()
				.unwrap_or(0.0);
			let errors = sigs.and_then(|s| s.get("errors")).copied().unwrap_or(0.0);
			let rate_limits = sigs
				.and_then(|s| s.get("rate_limits"))
				.copied()
				.unwrap_or(0.0);

			if knowledge > 0.0 || errors > 0.0 || rate_limits > 0.0 {
				use fmt::Write;
				let _ = writeln!(
					out,
					"  {server}: {knowledge:.0} events, {errors:.0} errors, {rate_limits:.0} \
					 rate-limits",
				);
			}
		}
		out
	}

	/// Display string for the server list.
	#[must_use]
	pub fn display(&self) -> String {
		if self.servers.len() == 1 {
			self.servers[0].to_string()
		} else {
			format!(
				"{} servers: {}",
				self.servers.len(),
				self.servers
					.iter()
					.map(|s| s.as_str())
					.collect::<Vec<_>>()
					.join(", ")
			)
		}
	}
}

#[cfg(test)]
mod tests {
	use ruma::OwnedServerName;

	use super::ServerPool;

	fn s(name: &str) -> OwnedServerName { name.try_into().unwrap() }

	fn pool3() -> ServerPool {
		ServerPool::from_servers(vec![s("alpha.com"), s("beta.com"), s("gamma.com")])
	}

	// ── Construction ─────────────────────────────────────────────────────

	#[test]
	fn from_servers_seeds_rank() {
		let pool = pool3();
		assert_eq!(pool.len(), 3);
		assert!(pool.is_multi());
		assert_eq!(pool.get_signal(&s("alpha.com"), "rank"), 0.0);
		assert_eq!(pool.get_signal(&s("beta.com"), "rank"), 1.0);
		assert_eq!(pool.get_signal(&s("gamma.com"), "rank"), 2.0);
	}

	#[test]
	fn single_server_not_multi() {
		let pool = ServerPool::from_servers(vec![s("only.com")]);
		assert!(!pool.is_multi());
		assert!(!pool.is_empty());
	}

	#[test]
	fn empty_pool() {
		let mut pool = ServerPool::from_servers(vec![]);
		assert!(pool.is_empty());
		assert_eq!(pool.next_available(), None);
		assert_eq!(pool.next_scored(&[("rank", 1.0)]), None);
		assert!(pool.all_exhausted());
	}

	// ── Round-robin next() ───────────────────────────────────────────────

	#[test]
	fn next_round_robins() {
		let mut pool = pool3();
		assert_eq!(pool.next_available().unwrap(), s("alpha.com"));
		assert_eq!(pool.next_available().unwrap(), s("beta.com"));
		assert_eq!(pool.next_available().unwrap(), s("gamma.com"));
		assert_eq!(pool.next_available().unwrap(), s("alpha.com")); // wraps
	}

	#[test]
	fn next_skips_errored_out_server() {
		let mut pool = pool3();
		// Exhaust beta's error budget
		for _ in 0..5 {
			pool.record_error(&s("beta.com"));
		}
		assert_eq!(pool.next_available().unwrap(), s("alpha.com"));
		assert_eq!(pool.next_available().unwrap(), s("gamma.com")); // skipped beta
		assert_eq!(pool.next_available().unwrap(), s("alpha.com"));
	}

	// ── Scored selection ─────────────────────────────────────────────────

	#[test]
	fn next_scored_prefers_lower_latency() {
		let mut pool = pool3();
		pool.set_signal(&s("alpha.com"), "latency", 500.0);
		pool.set_signal(&s("beta.com"), "latency", 50.0);
		pool.set_signal(&s("gamma.com"), "latency", 200.0);

		let weights = &[("latency", 1.0), ("rank", 0.01)];
		// beta has lowest latency → should win
		assert_eq!(pool.next_scored(weights).unwrap(), s("beta.com"));
	}

	#[test]
	fn next_scored_negative_weight_prefers_higher() {
		let mut pool = pool3();
		pool.set_signal(&s("alpha.com"), "knowledge", 100.0);
		pool.set_signal(&s("beta.com"), "knowledge", 500.0);
		pool.set_signal(&s("gamma.com"), "knowledge", 200.0);

		// Negative weight = higher value = lower score = preferred
		let weights = &[("knowledge", -1.0)];
		assert_eq!(pool.next_scored(weights).unwrap(), s("beta.com"));
	}

	#[test]
	fn next_scored_rank_breaks_ties() {
		let mut pool = pool3();
		// All equal on latency — rank tiebreaks
		pool.set_signal(&s("alpha.com"), "latency", 100.0);
		pool.set_signal(&s("beta.com"), "latency", 100.0);
		pool.set_signal(&s("gamma.com"), "latency", 100.0);

		let weights = &[("latency", 1.0), ("rank", 0.01)];
		// alpha has rank 0 → lowest total score
		assert_eq!(pool.next_scored(weights).unwrap(), s("alpha.com"));
	}

	#[test]
	fn next_scored_abuse_prevention() {
		let mut pool = pool3();
		pool.set_signal(&s("alpha.com"), "latency", 10.0);
		pool.set_signal(&s("beta.com"), "latency", 200.0);
		pool.set_signal(&s("gamma.com"), "latency", 200.0);

		let weights = &[
			("latency", 1.0),
			("consecutive_picks", 100.0), // Heavy abuse penalty
		];

		// First pick: alpha (lowest latency, 0 abuse)
		assert_eq!(pool.next_scored(weights).unwrap(), s("alpha.com"));
		// consecutive_picks is now 1 for alpha, score = 10 + 100 = 110
		// beta score = 200 + 0 = 200, gamma = 200 + 0 = 200

		// Second pick: alpha still wins (110 < 200)
		assert_eq!(pool.next_scored(weights).unwrap(), s("alpha.com"));
		// Now alpha has consecutive_picks = 2, score = 10 + 200 = 210

		// Third pick: beta or gamma (200 < 210)
		let third = pool.next_scored(weights).unwrap();
		assert!(third == s("beta.com") || third == s("gamma.com"));
	}

	// ── Hard constraints ─────────────────────────────────────────────────

	#[test]
	fn error_budget_exhaustion() {
		let mut pool = pool3();
		for _ in 0..4 {
			pool.record_error(&s("alpha.com"));
		}
		// 4 errors, budget is 5 — still available
		assert!(!pool.all_exhausted());
		assert_eq!(pool.next_available().unwrap(), s("alpha.com"));

		// 5th error — exhausted
		pool.record_error(&s("alpha.com"));
		assert_eq!(pool.next_available().unwrap(), s("beta.com")); // skips alpha

		// But not all exhausted
		assert!(!pool.all_exhausted());
	}

	#[test]
	fn all_exhausted_when_all_errored() {
		let mut pool = pool3();
		for server in ["alpha.com", "beta.com", "gamma.com"] {
			for _ in 0..5 {
				pool.record_error(&s(server));
			}
		}
		assert!(pool.all_exhausted());
		assert_eq!(pool.next_available(), None);
	}

	#[test]
	fn record_success_resets_cooldown() {
		let mut pool = pool3();
		pool.record_rate_limit(&s("alpha.com"));
		// alpha is in cooldown, but record_success clears it
		pool.record_success(&s("alpha.com"));
		// Should be available again immediately
		assert_eq!(pool.next_available().unwrap(), s("alpha.com"));
	}

	#[test]
	fn record_success_resets_consecutive_picks() {
		let mut pool = pool3();
		pool.add_signal(&s("alpha.com"), "consecutive_picks", 10.0);
		pool.record_success(&s("alpha.com"));
		assert_eq!(pool.get_signal(&s("alpha.com"), "consecutive_picks"), 0.0);
	}

	// ── Signal management ────────────────────────────────────────────────

	#[test]
	fn add_signal_accumulates() {
		let mut pool = pool3();
		pool.add_signal(&s("alpha.com"), "knowledge", 100.0);
		pool.add_signal(&s("alpha.com"), "knowledge", 50.0);
		assert_eq!(pool.get_signal(&s("alpha.com"), "knowledge"), 150.0);
	}

	#[test]
	fn set_signal_overwrites() {
		let mut pool = pool3();
		pool.set_signal(&s("alpha.com"), "latency", 100.0);
		pool.set_signal(&s("alpha.com"), "latency", 42.0);
		assert_eq!(pool.get_signal(&s("alpha.com"), "latency"), 42.0);
	}

	#[test]
	fn get_signal_default_zero() {
		let pool = pool3();
		assert_eq!(pool.get_signal(&s("alpha.com"), "nonexistent"), 0.0);
	}

	// ── Rate limit detection ─────────────────────────────────────────────

	#[test]
	fn is_rate_limit_detects_429() {
		assert!(ServerPool::is_rate_limit("HTTP 429 Too Many Requests"));
		assert!(ServerPool::is_rate_limit("error code 429"));
		assert!(!ServerPool::is_rate_limit("HTTP 500 Internal Server Error"));
		assert!(!ServerPool::is_rate_limit("connection refused"));
	}

	// ── Display ──────────────────────────────────────────────────────────

	#[test]
	fn display_single() {
		let pool = ServerPool::from_servers(vec![s("only.com")]);
		assert_eq!(pool.display(), "only.com");
	}

	#[test]
	fn display_multi() {
		let pool = pool3();
		assert!(pool.display().starts_with("3 servers:"));
		assert!(pool.display().contains("alpha.com"));
	}

	// ── Scoring with multiple dimensions ─────────────────────────────────

	#[test]
	fn multi_dimension_scoring() {
		let mut pool = pool3();
		// alpha: fast but low knowledge
		pool.set_signal(&s("alpha.com"), "latency", 50.0);
		pool.set_signal(&s("alpha.com"), "knowledge", 10.0);
		// beta: slow but high knowledge
		pool.set_signal(&s("beta.com"), "latency", 500.0);
		pool.set_signal(&s("beta.com"), "knowledge", 1000.0);
		// gamma: medium everything
		pool.set_signal(&s("gamma.com"), "latency", 200.0);
		pool.set_signal(&s("gamma.com"), "knowledge", 200.0);

		// Heavily prefer knowledge (exploitation)
		let exploit_weights = &[("latency", 0.1), ("knowledge", -1.0)];
		// alpha: 50*0.1 + 10*(-1) = -5
		// beta:  500*0.1 + 1000*(-1) = -950
		// gamma: 200*0.1 + 200*(-1) = -180
		assert_eq!(pool.next_scored(exploit_weights).unwrap(), s("beta.com"));

		// Heavily prefer speed (exploration)
		let explore_weights = &[("latency", 1.0), ("knowledge", -0.001)];
		// alpha: 50 - 0.01 = 49.99
		// beta: 500 - 1.0 = 499
		// gamma: 200 - 0.2 = 199.8
		// (reset consecutive_picks from previous call)
		pool.set_signal(&s("beta.com"), "consecutive_picks", 0.0);
		assert_eq!(pool.next_scored(explore_weights).unwrap(), s("alpha.com"));
	}

	// ── Dead-end recording ───────────────────────────────────────────────

	#[test]
	fn dead_end_tracks_signal() {
		let mut pool = pool3();
		pool.record_dead_end(&s("alpha.com"));
		pool.record_dead_end(&s("alpha.com"));
		assert_eq!(pool.get_signal(&s("alpha.com"), "dead_ends"), 2.0);
	}

	// ── Summary ──────────────────────────────────────────────────────────

	#[test]
	fn summary_shows_active_servers() {
		let mut pool = pool3();
		pool.add_signal(&s("alpha.com"), "knowledge", 500.0);
		pool.add_signal(&s("beta.com"), "errors", 3.0);
		let summary = pool.summary();
		assert!(summary.contains("alpha.com"));
		assert!(summary.contains("500"));
		assert!(summary.contains("beta.com"));
		// gamma has no activity — shouldn't appear
		assert!(!summary.contains("gamma.com"));
	}
}
