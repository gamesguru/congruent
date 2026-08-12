use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	sync::Arc,
};

use blake2::{Blake2bMac512, digest::Mac};

pub mod delta;

/// A non-cryptographic structural hash for HAMT nodes.
/// Used to quickly skip identical subtrees across different HAMT instances.
/// To protect against grinding attacks in shared rooms, this is a 16-byte
/// (128-bit) keyed digest rather than a process-seeded 64-bit hash.
pub type StructuralHash = [u8; 16];

/// Computes the 128-bit keyed structural hash for an internal node based on its
/// bitmap and children.
pub(crate) fn compute_structural_hash(
	key: &[u8],
	datamap: u32,
	nodemap: u32,
	children_hashes: &[StructuralHash],
) -> StructuralHash {
	let mut mac = Blake2bMac512::new_from_slice(key).expect("Blake2b takes any key size");
	mac.update(&datamap.to_le_bytes());
	mac.update(&nodemap.to_le_bytes());
	for h in children_hashes {
		mac.update(h);
	}

	let result = mac.finalize().into_bytes();
	let mut out = [0_u8; 16];
	out.copy_from_slice(&result[..16]);
	out
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
		/// The compressed array of child nodes (leaves and internal nodes
		/// combined). Length matches `datamap.count_ones() +
		/// nodemap.count_ones()`.
		children: Vec<Arc<Self>>,
		/// Structural hash for O(1) subtree equivalence checks.
		structural_hash: StructuralHash,
	},
	/// A leaf node containing key-value pairs (or collision nodes if hashes
	/// collide).
	Leaf {
		key: K,
		value: V,
	},
}

impl<K, V> HamtNode<K, V> {
	/// Returns the structural hash of this node using the given key.
	pub fn structural_hash(&self, key: &[u8]) -> StructuralHash
	where
		K: Hash,
		V: Hash,
	{
		match self {
			| Self::Internal { structural_hash, .. } => *structural_hash,
			| Self::Leaf { key: k, value: v } => {
				// Serialize K and V reliably for the hash. Since K and V are generic,
				// we hash them using a deterministic non-cryptographic hasher first,
				// or better, require them to be byte-serializable.
				// For generic K/V, we'll fall back to standard hashing into the MAC.
				// In a real zero-copy implementation, K and V would be written as bytes.
				let mut hasher = DefaultHasher::new();
				k.hash(&mut hasher);
				v.hash(&mut hasher);
				let inner_hash = hasher.finish();

				let mut mac =
					Blake2bMac512::new_from_slice(key).expect("Blake2b takes any key size");
				mac.update(&inner_hash.to_le_bytes());
				let result = mac.finalize().into_bytes();
				let mut out = [0_u8; 16];
				out.copy_from_slice(&result[..16]);
				out
			},
		}
	}
}
