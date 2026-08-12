use std::{hash::Hash, sync::Arc};

use rezzy::LtHash;

use super::HamtNode;

/// Isolates the delta (added/removed items) between two HAMT tries in O(|Delta|
/// * log32 N) time. Uses the LtHash lattice to quickly short-circuit if the
///   tries are convergently identical.
pub fn isolate_delta<K, V>(
	hash_key: &[u8],
	root_a: &Arc<HamtNode<K, V>>,
	lattice_a: &LtHash,
	root_b: &Arc<HamtNode<K, V>>,
	lattice_b: &LtHash,
) -> (Vec<(K, V)>, Vec<(K, V)>)
where
	K: Hash + Clone + Eq,
	V: Hash + Clone + Eq,
{
	// Short-circuit: if the LtHash lattices match, the sets are identical (with
	// very high probability).
	if lattice_a == lattice_b {
		return (Vec::new(), Vec::new());
	}

	let mut added = Vec::new();
	let mut removed = Vec::new();

	// Begin recursive diffing
	diff_nodes(hash_key, root_a, root_b, &mut added, &mut removed);

	(added, removed)
}

fn diff_nodes<K, V>(
	hash_key: &[u8],
	node_a: &Arc<HamtNode<K, V>>,
	node_b: &Arc<HamtNode<K, V>>,
	added: &mut Vec<(K, V)>,
	removed: &mut Vec<(K, V)>,
) where
	K: Hash + Clone + Eq,
	V: Hash + Clone + Eq,
{
	// Pointer equality check (fastest path for structurally shared nodes)
	if Arc::ptr_eq(node_a, node_b) {
		return;
	}

	// Structural hash check (fast path across process/storage boundaries)
	if node_a.structural_hash(hash_key) == node_b.structural_hash(hash_key) {
		return;
	}

	// Fall back to deep diff based on node variants
	match (&**node_a, &**node_b) {
		| (
			HamtNode::Internal {
				datamap: _d_a,
				nodemap: _n_a,
				children: _c_a,
				..
			},
			HamtNode::Internal {
				datamap: _d_b,
				nodemap: _n_b,
				children: _c_b,
				..
			},
		) => {
			// Find nodes that only exist in A (removed from B's perspective)
			// Find nodes that only exist in B (added from A's perspective)
			// For matching bits, recurse down.
			// A full implementation would iterate over set bits of (d_a | n_a)
			// and (d_b | n_b) and recurse into matching indexes, or gather
			// the entire subtree if the bit is disjoint. TODO: Implement
			// CHAMP bit-level traversal
		},
		| (HamtNode::Leaf { key: k_a, value: v_a }, HamtNode::Leaf { key: k_b, value: v_b }) =>
			if k_a != k_b || v_a != v_b {
				removed.push((k_a.clone(), v_a.clone()));
				added.push((k_b.clone(), v_b.clone()));
			},
		| (HamtNode::Internal { .. }, HamtNode::Leaf { key, value }) => {
			added.push((key.clone(), value.clone()));
			// TODO: collect all leaves from Internal and add to `removed`
		},
		| (HamtNode::Leaf { key, value }, HamtNode::Internal { .. }) => {
			removed.push((key.clone(), value.clone()));
			// TODO: collect all leaves from Internal and add to `added`
		},
	}
}
