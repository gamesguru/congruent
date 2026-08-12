use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub mod delta;

/// A non-cryptographic structural hash for HAMT nodes.
/// Used to quickly skip identical subtrees across different HAMT instances
/// (e.g. across process restarts or memory boundaries) without deep equality checks.
pub type StructuralHash = u64;

/// Computes the 64-bit structural hash for an internal node based on its bitmap and children.
fn compute_structural_hash(datamap: u32, nodemap: u32, children_hashes: &[StructuralHash]) -> StructuralHash {
    let mut hasher = DefaultHasher::new();
    datamap.hash(&mut hasher);
    nodemap.hash(&mut hasher);
    for h in children_hashes {
        h.hash(&mut hasher);
    }
    hasher.finish()
}

/// A node in the 32-way CHAMP (Compressed Hash Array Mapped Prefix) Trie.
#[derive(Clone, Debug)]
pub enum HamtNode<K, V> {
    /// An internal node carrying child pointers and a structural hash.
    Internal {
        /// Bitmap marking which of the 32 slots contain leaf data.
        datamap: u32,
        /// Bitmap marking which of the 32 slots contain child internal nodes.
        nodemap: u32,
        /// The compressed array of child nodes (leaves and internal nodes combined).
        /// Length matches `datamap.count_ones() + nodemap.count_ones()`.
        children: Vec<Arc<HamtNode<K, V>>>,
        /// Structural hash for O(1) subtree equivalence checks.
        structural_hash: StructuralHash,
    },
    /// A leaf node containing key-value pairs (or collision nodes if hashes collide).
    Leaf {
        key: K,
        value: V,
    },
}

impl<K, V> HamtNode<K, V> {
    /// Returns the structural hash of this node.
    pub fn structural_hash(&self) -> StructuralHash
    where
        K: Hash,
        V: Hash,
    {
        match self {
            HamtNode::Internal { structural_hash, .. } => *structural_hash,
            HamtNode::Leaf { key, value } => {
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                value.hash(&mut hasher);
                hasher.finish()
            }
        }
    }
}
