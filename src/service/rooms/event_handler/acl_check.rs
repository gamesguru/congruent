use conduwuit::{Err, Result, debug, implement, info, warn};
use futures::StreamExt;
use slipstream::{
	RoomId, ServerName,
	events::{StateEventType, TimelineEventType, room::server_acl::RoomServerAclEventContent},
};

/// Returns Ok if the acl allows the server
#[implement(super::Service)]
pub async fn acl_check(&self, server_name: &ServerName, room_id: &RoomId) -> Result {
	let acl_event_content = match self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomServerAcl, "")
		.await
		.map(|c: RoomServerAclEventContent| c)
	{
		| Ok(content) => content,
		| Err(e) if e.is_not_found() => {
			// Temporary: tell "the room has no ACL event" from "the ACL event
			// exists but the state lookup does not see it".
			let pdus = self.services.timeline.all_pdus(room_id);
			futures::pin_mut!(pdus);
			let mut in_timeline = Vec::new();
			while let Some((_, pdu)) = pdus.next().await {
				if pdu.kind == TimelineEventType::RoomServerAcl {
					in_timeline.push(pdu.event_id.to_string());
				}
			}
			info!(
				target: "acl_debug",
				%room_id, %server_name, ?in_timeline,
				"acl_check: No ACL content found in state, allowing."
			);
			return Ok(());
		},
		| Err(e) => {
			// Anything other than a plain absence means the ACL could not be read,
			// not that none exists; keep allowing, but make the cause visible.
			warn!(
				%room_id, %server_name,
				"acl_check: could not read the room's ACL from state, allowing: {e}"
			);
			return Ok(());
		},
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
