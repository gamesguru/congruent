use crate::engine::descriptor::{self, Descriptor};

/// Legacy maps kept registered as column families (and opened into the maps
/// table by `maps::open`) so migrations can still read and then clear them.
///
/// * The state-shortening maps are superseded by the HAMT root handles
///   (`roomid_roothandle`, `shorteventid_roothandle`). They are no longer
///   consulted by the state layer at runtime, but the v19-v24 migrations read
///   them to build HAMT roots and then clear their data for databases arriving
///   from older schema versions.
/// * The private-read receipt maps are superseded by
///   `roomuserid_privatereadreceipt`, and the rejection marker table by
///   `eventid_metadata`; migrations still conditionally open these CFs by name,
///   so their descriptors must remain until that code is removed.
pub(super) static DEPRECATED_MAPS: &[Descriptor] = &[
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
	Descriptor {
		name: "rejectedeventids",
		key_size_hint: Some(48),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_shortstatehash",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "shorteventid_shortstatehash",
		key_size_hint: Some(8),
		val_size_hint: Some(8),
		block_size: 512,
		index_size: 512,
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "shortstatehash_lthash",
		key_size_hint: Some(8),
		val_size_hint: Some(2048),
		..descriptor::SEQUENTIAL_SMALL
	},
	Descriptor {
		name: "shortstatehash_statediff",
		key_size_hint: Some(8),
		..descriptor::SEQUENTIAL_SMALL
	},
];
