use std::{collections::HashSet, convert::AsRef, hash::Hash, sync::Arc};

use conduwuit::{Result, implement};
use tokio::task;

use crate::util::map_err;

/// Result container for recursive multi-get DAG traversals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecursiveGetOutput<V, K> {
	/// Values successfully fetched and parsed during traversal.
	pub values: Vec<V>,

	/// Keys requested during traversal that were missing from the database.
	pub missing: Vec<K>,

	/// Indicates whether the traversal stopped early due to node or depth
	/// caps.
	pub truncated: bool,
}

/// Performs a recursive breadth-first traversal over database keys.
///
/// Starting from `roots`, each batch of keys is fetched in RocksDB using
/// `batched_multi_get_cf_opt` against a point-in-time snapshot. Returned
/// values are parsed by `parse_value`, and any child keys appended to the
/// sink buffer by `extract_children` are queued for the next level of
/// traversal.
///
/// # Traversal Ordering
/// Results are ordered level-by-level (BFS order). Within a single level,
/// results reflect key sorting order. Note that this is **not** a
/// topological sort.
///
/// # Bounds & Limits
/// Traversal halts early if `max_nodes` (total parsed values) or `max_depth`
/// (BFS depth iterations) is reached, marking `truncated = true` on the
/// returned output.
///
/// # Errors
/// Fails fast on server shutdown, key parsing failure, RocksDB I/O errors, or
/// block corruption.
#[implement(super::Map)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn recursive_multi_get<K, V, P, F, I>(
	self: &Arc<Self>,
	roots: I,
	max_nodes: Option<usize>,
	max_depth: Option<usize>,
	parse_value: P,
	extract_children: F,
) -> Result<RecursiveGetOutput<V, K>>
where
	K: AsRef<[u8]> + Ord + Hash + Clone + Send + Sync + 'static,
	V: Send + 'static,
	P: Fn(&[u8]) -> Result<V> + Send + Sync + 'static,
	F: Fn(&V, &mut Vec<K>) + Send + Sync + 'static,
	I: IntoIterator<Item = K> + Send + 'static,
{
	let map = self.clone();

	task::spawn_blocking(move || {
		const SORTED: bool = true;

		map.db.ctx.server.check_running()?;

		let snapshot = map.db.db.snapshot();
		let mut read_options = super::read_options_default(&map.db);
		read_options.set_snapshot(&snapshot);

		let mut visited = HashSet::new();
		let mut current_batch = Vec::new();
		for root in roots {
			if visited.insert(root.clone()) {
				current_batch.push(root);
			}
		}

		let mut values = Vec::new();
		let mut missing = Vec::new();
		let mut depth: usize = 0;
		let mut truncated = false;

		while !current_batch.is_empty() {
			if let Some(max_d) = max_depth
				&& depth >= max_d
			{
				truncated = true;
				break;
			}

			// Sort keys for optimal sequential RocksDB multi-get access
			current_batch.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));

			if max_nodes.is_some_and(|max_n| values.len() >= max_n) {
				truncated = true;
				break;
			}

			let db_results = map.db.db.batched_multi_get_cf_opt(
				&map.cf(),
				current_batch.iter(),
				SORTED,
				&read_options,
			);

			let mut next_batch = Vec::with_capacity(current_batch.len().saturating_mul(2));

			for (key, result) in current_batch.into_iter().zip(db_results) {
				match result {
					| Ok(Some(slice)) =>
						if max_nodes.is_none_or(|max_n| values.len() < max_n) {
							let parsed_value = parse_value(slice.as_ref())?;
							extract_children(&parsed_value, &mut next_batch);
							values.push(parsed_value);

							if max_nodes.is_some_and(|max_n| values.len() >= max_n) {
								truncated = true;
							}
						} else {
							truncated = true;
						},
					| Ok(None) => {
						missing.push(key);
					},
					| Err(e) => {
						tracing::error!(
							key = ?key.as_ref(),
							%e,
							"RocksDB multi-get failure during recursive DAG traversal"
						);
						return Err(map_err(e));
					},
				}
			}

			// Filter out already visited keys from next_batch while preserving order
			next_batch.retain(|child| visited.insert(child.clone()));

			depth = depth.saturating_add(1);

			if truncated {
				break;
			}

			current_batch = next_batch;

			map.db.ctx.server.check_running()?;
		}

		Ok(RecursiveGetOutput { values, missing, truncated })
	})
	.await
	.map_err(|e| {
		if e.is_panic() {
			let panic = e.into_panic();
			let reason = panic
				.downcast_ref::<&str>()
				.map(|s| (*s).to_owned())
				.or_else(|| panic.downcast_ref::<String>().cloned())
				.unwrap_or_else(|| "non-string panic payload".to_owned());

			tracing::error!(%reason, "blocking task panicked during recursive_multi_get");
			std::io::Error::other(format!("recursive_multi_get task panicked: {reason}"))
		} else {
			std::io::Error::other("recursive_multi_get task cancelled")
		}
	})?
}
