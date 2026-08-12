use super::*;
use std::sync::Arc;
use rezzy::LtHash;
use crate::rooms::state_hamt::delta::isolate_delta;

#[test]
fn test_structural_hash_equivalence() {
    let key = b"dummy_server_key";
    let leaf1 = Arc::new(HamtNode::Leaf { key: 1, value: 100 });
    let leaf2 = Arc::new(HamtNode::Leaf { key: 1, value: 100 });
    let leaf3 = Arc::new(HamtNode::Leaf { key: 2, value: 200 });

    // Identical leaves should have the same structural hash
    assert_eq!(leaf1.structural_hash(key), leaf2.structural_hash(key));
    assert_ne!(leaf1.structural_hash(key), leaf3.structural_hash(key));

    let internal1 = Arc::new(HamtNode::Internal {
        datamap: 1,
        nodemap: 0,
        children: vec![leaf1.clone()],
        structural_hash: compute_structural_hash(key, 1, 0, &[leaf1.structural_hash(key)]),
    });

    let internal2 = Arc::new(HamtNode::Internal {
        datamap: 1,
        nodemap: 0,
        children: vec![leaf2.clone()],
        structural_hash: compute_structural_hash(key, 1, 0, &[leaf2.structural_hash(key)]),
    });

    // Even though they are different Arc instances, their structural hashes must match
    assert_eq!(internal1.structural_hash(key), internal2.structural_hash(key));
}

#[test]
fn test_lthash_short_circuit() {
    let key = b"dummy_server_key";
    let leaf1 = Arc::new(HamtNode::Leaf { key: 1, value: 100 });
    let leaf2 = Arc::new(HamtNode::Leaf { key: 2, value: 200 });

    let mut lattice_a = LtHash::default();
    let mut lattice_b = LtHash::default();

    // Simulate identical sets
    let (added, removed) = isolate_delta(key, &leaf1, &lattice_a, &leaf2, &lattice_b);
    assert!(added.is_empty());
    assert!(removed.is_empty());
}
