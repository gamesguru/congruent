use conduwuit::{Result, implement};
use ruma::{CanonicalJsonObject, RoomVersionId};

#[implement(super::Service)]
pub fn sign_json(&self, object: &mut CanonicalJsonObject) -> Result {
	use ruma::signatures::sign_json;

	let server_name = self.services.globals.server_name().as_str();
	sign_json(server_name, self.keypair(), object).map_err(Into::into)
}

#[implement(super::Service)]
pub fn hash_and_sign_event(
	&self,
	object: &mut CanonicalJsonObject,
	room_version: &RoomVersionId,
) -> Result {
	use conduwuit::matrix::state_res::RoomVersion;
	use ruma::signatures::hash_and_sign_event;

	let is_create_event = matches!(
		object.get("type"),
		Some(ruma::CanonicalJsonValue::String(event_type)) if event_type == "m.room.create"
	);

	// MSC4291: Omit room_id for m.room.create in v12+
	if let Ok(rv) = RoomVersion::new(room_version) {
		if rv.room_ids_as_hashes && is_create_event {
			object.remove("room_id");
		}
		if rv.state_dags {
			object.remove("auth_events");
		}
	}

	let server_name = self.services.globals.server_name().as_str();
	let ruma_version = super::ruma_signatures_version(room_version);
	hash_and_sign_event(server_name, self.keypair(), object, &ruma_version).map_err(Into::into)
}
