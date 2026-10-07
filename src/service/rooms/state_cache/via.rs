use conduwuit::{
	Result, implement,
	utils::{StreamTools, stream::TryIgnore},
};
use database::Ignore;
use futures::{Stream, StreamExt, future, stream::iter};
use itertools::Itertools;
use slipstream::{
	OwnedServerName, RoomId,
	events::{StateEventType, room::power_levels::RoomPowerLevelsEventContent},
	int,
};

#[implement(super::Service)]
pub async fn add_servers_invite_via(&self, room_id: &RoomId, servers: Vec<OwnedServerName>) {
	let mut servers: Vec<_> = self
		.servers_invite_via(room_id)
		.chain(iter(servers.into_iter()))
		.collect()
		.await;

	servers.sort_unstable();
	servers.dedup();

	let servers = servers
		.iter()
		.map(OwnedServerName::as_bytes)
		.collect_vec()
		.join(&[0xFF][..]);

	self.db
		.roomid_inviteviaservers
		.insert(room_id.as_bytes(), &servers);
}

/// Gets up to five servers that are likely to be in the room in the
/// distant future.
///
/// See <https://spec.matrix.org/latest/appendices/#routing>
#[implement(super::Service)]
pub async fn servers_route_via(&self, room_id: &RoomId) -> Result<Vec<OwnedServerName>> {
	let most_powerful_user_server = self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomPowerLevels, "")
		.await
		.map(|content: RoomPowerLevelsEventContent| {
			content
				.users
				.iter()
				.max_by_key(|(_, power)| *power)
				.and_then(|x| (x.1 >= &int!(50)).then_some(x))
				.map(|(user, _power)| user.server_name())
		});

	let mut servers: Vec<OwnedServerName> = self
		.room_members(room_id)
		.counts_by(|user| user.server_name())
		.await
		.into_iter()
		.sorted_by_key(|(_, users)| *users)
		.map(|(server, _)| server)
		.rev()
		.take(5)
		.collect();

	if let Ok(Some(server)) = most_powerful_user_server {
		servers.insert(0, server);
		servers.truncate(5);
	}

	Ok(servers)
}

#[implement(super::Service)]
pub fn servers_invite_via<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = OwnedServerName> + Send + 'a {
	// The value is a 0xFF-separated list of server names; as before, only the
	// last one is yielded per row.
	self.db
		.roomid_inviteviaservers
		.stream_raw_prefix(room_id)
		.ignore_err()
		.filter_map(|(_, servers): (Ignore, &[u8])| {
			let server = servers
				.split(|&b| b == 0xFF)
				.next_back()
				.and_then(|server| std::str::from_utf8(server).ok())
				.and_then(|server| OwnedServerName::parse(server).ok());
			future::ready(server)
		})
}
