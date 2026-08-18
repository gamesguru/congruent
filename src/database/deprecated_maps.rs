use crate::engine::descriptor::{self, Descriptor};

/// Legacy state-shortening maps, superseded by the HAMT root handles
/// (`roomid_roothandle`, `shorteventid_roothandle`). These CFs are no longer
/// consulted at runtime, but their descriptors must remain registered so that
/// the v20->v21 migrations can still read and then clear their data for
/// databases arriving from schema < 20.
pub(super) static DEPRECATED_MAPS: &[Descriptor] = &[
	Descriptor {
		name: "roomid_shortstatehash",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomsynctoken_shortstatehash",
		..descriptor::DROPPED
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
