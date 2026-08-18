use std::sync::Arc;

use conduwuit::Result;

pub mod store;

pub struct Service {
	pub store: store::Store,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let state_hamt_nodes = args.db["state_hamt_nodes"].clone();
		let state_hamt_node_mtimes = args.db["state_hamt_node_mtimes"].clone();

		Ok(Arc::new(Self {
			store: store::Store::new(state_hamt_nodes, state_hamt_node_mtimes),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Computes the structural key for a room's HAMT using a per-server secret
/// and the room ID. This prevents grinding attacks on the HAMT topology.
#[must_use]
pub fn room_structural_key(server_secret: &[u8; 32], room_id: &ruma::RoomId) -> [u8; 32] {
	use hmac::{Hmac, Mac, digest::KeyInit};
	use sha2::Sha256;

	let mut mac =
		Hmac::<Sha256>::new_from_slice(server_secret).expect("HMAC can take key of any size");
	mac.update(room_id.as_bytes());
	mac.finalize().into_bytes().into()
}
