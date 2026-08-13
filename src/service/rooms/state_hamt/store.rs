use std::sync::Arc;

use conduwuit::{Result, err};
use database::Map;
use rezzy::{
	LtHash,
	hamt::{HamtNode, RootHandle, codec::PersistedInternalNode, hash::StructuralHash},
};

/// Adapter mapping the rezzy HAMT nodes into RocksDB and memory caches.
pub struct Store {
	/// RocksDB column family containing the densely persisted nodes.
	db: Arc<Map>,

	/// Content-addressed cache of parsed HamtNodes in memory.
	/// Deduplicates subtrees across different room states automatically since
	/// the key is purely the StructuralHash.
	///
	/// Note: `u64, u64` is currently a placeholder for `ShortStateKey` and
	/// `ShortEventId` which are the actual domain types.
	node_cache: moka::sync::Cache<StructuralHash, Arc<HamtNode<u64, u64>>>,
}

impl Store {
	pub fn new(db: Arc<Map>) -> Self {
		// Use a generic capacity for now. In a full production setup, this
		// could be wired to a config value like other caches.
		let node_cache = moka::sync::Cache::builder().max_capacity(100_000).build();

		Self { db, node_cache }
	}

	/// Fetches a node by its structural hash synchronously (for the resolver).
	///
	/// Checks the memory cache first. On miss, reads from RocksDB and
	/// parses the dense format into an in-memory `HamtNode`.
	pub fn get_node_blocking(&self, hash: &StructuralHash) -> Result<Arc<HamtNode<u64, u64>>> {
		if let Some(node) = self.node_cache.get(hash) {
			return Ok(node);
		}

		let bytes = self.db.get_blocking(hash)?;
		if bytes.is_empty() {
			return Err(err!(Database(error!("State HAMT node not found in database."))));
		}

		let persisted = PersistedInternalNode::<u64, u64>::decode_v1(&bytes)
			.map_err(|e| err!(Database(error!("{e}"))))?;

		let node =
			Arc::new(HamtNode::try_from(persisted).map_err(|e| err!(Database(error!("{e}"))))?);

		self.node_cache.insert(*hash, node.clone());

		Ok(node)
	}

	/// Persists a node to RocksDB and populates the cache.
	pub fn put_node(&self, node: Arc<HamtNode<u64, u64>>) {
		let persisted: PersistedInternalNode<u64, u64> = node.as_ref().into();
		let bytes = persisted.encode_v1();

		// Cache it immediately so concurrent reads can hit memory
		self.node_cache.insert(node.structural_hash, node);

		self.db.insert(&persisted.structural_hash, &bytes);
	}

	/// Persists a node and all of its resolved children recursively.
	pub fn persist_node_recursive(&self, node: Arc<HamtNode<u64, u64>>) {
		for child in &node.children {
			if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
				self.persist_node_recursive(child_node.clone());
			}
		}
		self.put_node(node);
	}

	/// Provides a synchronous resolver closure for `isolate_delta`.
	///
	/// # Important Architecture Note
	/// `isolate_delta` is synchronous and `#![no_std]` in `rezzy`. However,
	/// it triggers lazy node resolutions which require hitting the database.
	/// Because this closure runs in a sync context but must perform blocking
	/// I/O, we use `tokio::task::block_in_place` if running under a
	/// multithreaded runtime, or block the thread directly (via the blocking
	/// RocksDB API) if we're not executing within Tokio's worker pool (e.g.
	/// tests or synchronous spawns).
	pub fn get_blocking_resolver(
		&self,
	) -> impl FnMut(&StructuralHash) -> Result<Arc<HamtNode<u64, u64>>, conduwuit::Error> + '_ {
		move |hash: &StructuralHash| {
			if let Ok(handle) = tokio::runtime::Handle::try_current() {
				if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
					return tokio::task::block_in_place(|| self.get_node_blocking(hash));
				}
				// `block_in_place` panics on a `CurrentThread` runtime, so fall back to
				// an explicit error to prevent stalling the only executor thread.
				return Err(conduwuit::err!(error!(
					"HAMT operations require a multithreaded Tokio runtime to avoid stalling \
					 the executor."
				)));
			}
			self.get_node_blocking(hash)
		}
	}

	/// Derives the deterministic `RootHandle` (structural hash + the
	/// cross-server-comparable `StateGroupId`) for a resolved root, from
	/// rezzy's `LtHash` state accumulator.
	///
	/// This is the MSC00DC root: `state_group_id` is `BLAKE2b-256(lattice)`
	/// (rezzy's `LtHash::checksum`), reproducible by any server that
	/// resolves to the same state — unlike `structural_hash`, which is a
	/// local-only cache key and must never be compared across servers.
	#[must_use]
	pub fn root_handle(&self, structural_hash: StructuralHash, lattice: &LtHash) -> RootHandle {
		RootHandle::from_lthash(structural_hash, lattice)
	}
}
