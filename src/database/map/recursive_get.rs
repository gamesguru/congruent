use std::{collections::HashSet, convert::AsRef, hash::Hash, sync::Arc};

use conduwuit::{Result, implement};
use tokio::task;

use crate::util::map_err;

#[implement(super::Map)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn recursive_multi_get<K, V, P, F, I: IntoIterator<Item = K>>(
	self: &Arc<Self>,
	roots: I,
	parse_value: P,
	extract_children: F,
) -> Result<Vec<V>>
where
	K: AsRef<[u8]> + Eq + Hash + Clone + Send + Sync + 'static,
	V: Send + 'static,
	P: Fn(&[u8]) -> V + Send + Sync + 'static,
	F: Fn(&V) -> Vec<K> + Send + Sync + 'static,
{
	let map = self.clone();
	let mut current_batch: Vec<K> = roots.into_iter().collect();

	task::spawn_blocking(move || {
		let mut results = Vec::new();
		let mut visited = HashSet::new();

		// Mark roots as visited initially to avoid re-fetching if they appear again
		for root in &current_batch {
			visited.insert(root.clone());
		}

		while !current_batch.is_empty() {
			const SORTED: bool = false;
			let db_results = map.db.db.batched_multi_get_cf_opt(
				&map.cf(),
				current_batch.iter(),
				SORTED,
				&map.read_options,
			);

			let mut next_batch = Vec::new();

			for result in db_results {
				match result {
					| Ok(Some(slice)) => {
						// Parse the raw bytes into our generic value V
						let parsed_value = parse_value(slice.as_ref());

						// Extract the next generation of keys to fetch
						let children = extract_children(&parsed_value);

						// Keep track of the parsed value
						results.push(parsed_value);

						// Deduplicate and queue the children
						for child in children {
							if visited.insert(child.clone()) {
								next_batch.push(child);
							}
						}
					},
					| Ok(None) => {},
					| Err(e) => return Err(map_err(e)),
				}
			}

			current_batch = next_batch;
		}

		Ok(results)
	})
	.await
	.expect("blocking task panicked")
}
