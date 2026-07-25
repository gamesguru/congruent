use std::{
	cmp::{Ord, Reverse},
	collections::BinaryHeap,
	hash::Hash,
};

use rustc_hash::{FxBuildHasher, FxHashMap};

/// Topologically sorts a directed acyclic graph using Kahn's algorithm.
///
/// `nodes` is an iterator of `(id, parents, key)`.
/// The algorithm guarantees that parents come before children in the output.
/// If there are multiple nodes with no pending parents, they are ordered by
/// `key` according to `Ord`. Nodes with a *smaller* key are popped first
/// (min-heap behavior).
///
/// Note: Any parent IDs yielded by the `parents` iterator that are NOT present
/// in the `nodes` set are ignored (treated as already resolved external nodes).
#[must_use]
pub fn kahn_sort<Id, I, P, K>(nodes: I) -> Vec<Id>
where
	Id: Clone + Eq + Hash,
	K: Ord,
	I: IntoIterator<Item = (Id, P, K)>,
	P: IntoIterator<Item = Id>,
{
	let nodes_iter = nodes.into_iter();
	let (lower_bound, _) = nodes_iter.size_hint();

	let mut id_to_index: FxHashMap<Id, usize> =
		FxHashMap::with_capacity_and_hasher(lower_bound, FxBuildHasher);
	let mut index_to_id: Vec<Id> = Vec::with_capacity(lower_bound);
	let mut keys: Vec<K> = Vec::with_capacity(lower_bound);
	let mut edges_list: Vec<P> = Vec::with_capacity(lower_bound);

	// 1. Assign consecutive indices to all nodes
	for (id, parents, key) in nodes_iter {
		id_to_index.insert(id.clone(), index_to_id.len());
		index_to_id.push(id);
		keys.push(key);
		edges_list.push(parents);
	}

	let num_nodes = index_to_id.len();
	if num_nodes == 0 {
		return Vec::new();
	}

	// 2. Build in-degree counts and forward adjacency list using O(1) indices
	let mut in_degree = vec![0_usize; num_nodes];
	let mut children = vec![Vec::new(); num_nodes];

	for (node_idx, parents) in edges_list.into_iter().enumerate() {
		for parent_id in parents {
			if let Some(&parent_idx) = id_to_index.get(&parent_id) {
				in_degree[node_idx] = in_degree[node_idx].saturating_add(1);
				children[parent_idx].push(node_idx);
			}
		}
	}

	// 3. Populate min-heap with all nodes having no unresolved parents
	// We use `Reverse((&keys[idx], idx))` so that:
	// a) Minimum keys are popped first.
	// b) The tie-breaker tuple resolves exactly as `Ord` demands.
	let mut heap = BinaryHeap::with_capacity(num_nodes);
	for (idx, &deg) in in_degree.iter().enumerate() {
		if deg == 0 {
			heap.push(Reverse((&keys[idx], idx)));
		}
	}

	let mut sorted = Vec::with_capacity(num_nodes);
	let mut visited = vec![false; num_nodes];

	// 4. Drain the heap, appending to result and decrementing children
	while let Some(Reverse((_, node_idx))) = heap.pop() {
		if visited[node_idx] {
			continue;
		}
		visited[node_idx] = true;
		sorted.push(index_to_id[node_idx].clone());

		for &child_idx in &children[node_idx] {
			in_degree[child_idx] = in_degree[child_idx].saturating_sub(1);
			if in_degree[child_idx] == 0 {
				heap.push(Reverse((&keys[child_idx], child_idx)));
			}
		}
	}

	// 5. If cycles exist, gracefully append remaining nodes sorted by key
	if sorted.len() < num_nodes {
		let mut remaining = Vec::with_capacity(num_nodes.saturating_sub(sorted.len()));
		for (idx, &is_visited) in visited.iter().enumerate() {
			if !is_visited {
				remaining.push(idx);
			}
		}
		// Sort remaining by key, tie-breaking on the original index so cyclic
		// input still produces a deterministic order (sort_unstable_by does not
		// preserve input order for equal keys on its own).
		remaining.sort_unstable_by(|&a, &b| keys[a].cmp(&keys[b]).then_with(|| a.cmp(&b)));
		for idx in remaining {
			sorted.push(index_to_id[idx].clone());
		}
	}

	sorted
}
