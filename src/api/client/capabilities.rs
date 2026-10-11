use std::collections::BTreeMap;

use conduwuit::{Result, Server};
use slipstream::{
	RoomVersionId,
	api::client::discovery::get_capabilities::{
		self, Capabilities, GetLoginTokenCapability, RoomVersionStability,
		RoomVersionsCapability, ThirdPartyIdChangesCapability,
	},
};

use crate::{Ruma, json_util::single_field, router::extract::State};

/// # `GET /_matrix/client/v3/capabilities`
///
/// Get information on the supported feature set and other relevant capabilities
/// of this server.
pub(crate) async fn get_capabilities_route(
	State(services): State<crate::State>,
	body: Ruma<get_capabilities::Request>,
) -> Result<get_capabilities::Response> {
	let available: BTreeMap<RoomVersionId, RoomVersionStability> =
		Server::available_room_versions()
			.filter(|(version, _)| services.server.supported_room_version(version))
			.collect();
	let default = if available.contains_key(&services.server.config.default_room_version) {
		services.server.config.default_room_version.clone()
	} else {
		available
			.keys()
			.next_back()
			.cloned()
			.expect("server must advertise at least one room version")
	};

	let mut capabilities = Capabilities {
		room_versions: RoomVersionsCapability { available, default },
		..Default::default()
	};

	// Only allow 3pid changes if SMTP is configured
	capabilities.thirdparty_id_changes = ThirdPartyIdChangesCapability {
		enabled: services.threepid.email_requirement().may_change(),
	};

	capabilities.get_login_token = GetLoginTokenCapability {
		enabled: services.server.config.login_via_existing_session,
	};

	// MSC4133 capability
	capabilities.set("uk.tcpip.msc4133.profile_fields", single_field("enabled", &true))?;

	capabilities.set(
		"org.matrix.msc4267.forget_forced_upon_leave",
		single_field("enabled", &services.config.forget_forced_upon_leave),
	)?;

	if services
		.users
		.is_admin(body.sender_user.as_ref().unwrap())
		.await
	{
		// Advertise suspension API
		let mut object = slipstream::ObjectBuilder::new();
		object.field("suspend", &true);
		object.field("lock", &false);
		capabilities.set("uk.timedout.msc4323", object.finish())?;
	}

	Ok(get_capabilities::Response { capabilities })
}
