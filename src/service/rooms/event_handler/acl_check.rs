use conduwuit::{Err, Result, debug, implement, info, warn};
use ruma::{
	RoomId, ServerName,
	events::{StateEventType, room::server_acl::RoomServerAclEventContent},
};

/// Returns Ok if the acl allows the server
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "debug")]
pub async fn acl_check(&self, server_name: &ServerName, room_id: &RoomId) -> Result {
	let Ok(acl_event_content) = self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomServerAcl, "")
		.await
		.map(|c: RoomServerAclEventContent| c)
	else {
		info!(
			target: "acl_debug",
			%room_id, %server_name, "acl_check: No ACL content found in state, allowing."
		);
		return Ok(());
	};

	debug!(
		target: "acl_debug",
		%room_id, %server_name,
		allow = ?acl_event_content.allow,
		deny = ?acl_event_content.deny,
		"acl_check: Evaluated ACL content from state"
	);

	if acl_event_content.allow.is_empty() {
		warn!(%room_id, "Ignoring broken ACL event (allow key is empty)");
		return Ok(());
	}

	if acl_event_content.deny.contains(&String::from("*"))
		&& acl_event_content.allow.contains(&String::from("*"))
	{
		warn!(%room_id, "Ignoring broken ACL event (allow key and deny key both contain wildcard \"*\"");
		return Ok(());
	}

	if acl_event_content.is_allowed(server_name) {
		debug!(
			"ACL ALLOWED: server_name={}, room_id={}, acl_content={:?}",
			server_name, room_id, acl_event_content
		);
		Ok(())
	} else {
		debug!("Server {server_name} was denied by room ACL in {room_id}");
		Err!(Request(Forbidden("Server was denied by room ACL")))
	}
}
