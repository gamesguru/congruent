use std::sync::Arc;

use conduwuit::Result;

pub mod store;

pub struct Service {
	pub store: store::Store,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let state_hamt_nodes = args.db["state_hamt_nodes"].clone();

		Ok(Arc::new(Self {
			store: store::Store::new(state_hamt_nodes),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}
