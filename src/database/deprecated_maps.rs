use crate::engine::descriptor::{self, Descriptor};

pub(super) static DEPRECATED_MAPS: &[Descriptor] = &[
	// Legacy private-read receipt maps. Superseded by
	// `roomuserid_privatereadreceipt`, but migrations still conditionally open
	// these CFs by name, so their descriptors must remain until that code is
	// removed.
	Descriptor {
		name: "roomuserid_privateread",
		val_size_hint: Some(16),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuserid_privatereadevent",
		val_size_hint: Some(1024),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuserid_lastprivatereadupdate",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	// Legacy rejection marker table. Superseded by `eventid_metadata`, but keep
	// the descriptor until the last startup/migration/admin paths that still
	// expect the CF to be describable are removed.
	Descriptor {
		name: "rejectedeventids",
		key_size_hint: Some(48),
		..descriptor::RANDOM_SMALL
	},
];
