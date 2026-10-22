//! Database maps retained only as compatibility mirrors and migration inputs.
//!
//! Keep deprecated names out of the active map registry. Access them through
//! `Database::deprecated`; the room short-ID map remains a compatibility
//! mirror because exports still need to preserve its assigned IDs.

use std::{collections::BTreeMap, sync::Arc};

use crate::{Engine, Map};

pub(super) type Maps = BTreeMap<&'static str, Arc<Map>>;

const MAP_NAMES: &[&str] = &[
	"eventid_shorteventid",
	"roomid_shortroomid",
	"shorteventid_authchain",
	"shorteventid_eventid",
	"server_signingkeys",
	"shorteventid_shortauthevents",
	"shortstatekey_statekey",
	"statekey_shortstatekey",
];

/// Build handles for all registered legacy compatibility and migration maps.
pub(super) fn open(db: &Arc<Engine>) -> Maps {
	MAP_NAMES
		.iter()
		.copied()
		.map(|name| (name, Map::open(db, name).expect("failed to open deprecated map")))
		.collect()
}
