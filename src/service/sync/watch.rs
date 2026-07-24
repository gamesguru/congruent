use std::collections::HashSet;

use conduwuit::{implement, trace};
use futures::{Future, FutureExt, StreamExt, stream::FuturesUnordered};
use ruma::{DeviceId, UserId};

#[implement(super::Service)]
pub async fn setup_watch<'a>(
	&'a self,
	user_id: &'a UserId,
	device_id: &'a DeviceId,
) -> impl Future<Output = ()> + Send + 'a {
	let userid_bytes = user_id.as_bytes().to_vec();
	let mut userid_prefix = userid_bytes.clone();
	userid_prefix.push(0xFF);

	let mut userdeviceid_prefix = userid_prefix.clone();
	userdeviceid_prefix.extend_from_slice(device_id.as_bytes());
	userdeviceid_prefix.push(0xFF);

	let mut futures = FuturesUnordered::new();

	// Return when *any* user changed their key
	// TODO: only send for user they share a room with
	futures.push(self.db.todeviceid_events.watch_prefix(&userdeviceid_prefix));

	futures.push(self.db.userroomid_joined.watch_prefix(&userid_prefix));
	futures.push(self.db.userroomid_invitestate.watch_prefix(&userid_prefix));
	futures.push(self.db.userroomid_leftstate.watch_prefix(&userid_prefix));
	futures.push(
		self.db
			.userroomid_notificationcount
			.watch_prefix(&userid_prefix),
	);
	futures.push(
		self.db
			.userroomid_highlightcount
			.watch_prefix(&userid_prefix),
	);

	// Collect joined rooms into a HashSet for O(1) lookups
	let rooms_joined_stream = self.services.state_cache.rooms_joined(user_id);
	let joined_rooms: HashSet<_> = rooms_joined_stream.map(ToOwned::to_owned).collect().await;

	// Exactly ONE typing watcher for the entire sync loop
	if !joined_rooms.is_empty() {
		let mut typing_rx = self.services.typing.typing_update_sender.subscribe();
		let user_rooms = joined_rooms.clone();

		futures.push(
			async move {
				loop {
					match typing_rx.recv().await {
						| Ok(typing_room_id) if user_rooms.contains(&typing_room_id) => return,
						// If it was for a room we aren't in, just try again
						| Ok(_) => continue,
						// If it lagged, we know typing changed but lost which room; recompute.
						// Server shutdown / channel close should also wake the waiter.
						| Err(
							tokio::sync::broadcast::error::RecvError::Lagged(_)
							| tokio::sync::broadcast::error::RecvError::Closed,
						) => return,
					}
				}
			}
			.boxed(),
		);
	}

	// Iterate over the set for database prefix watchers ONLY
	for room_id in &joined_rooms {
		let Ok(short_roomid) = self.services.short.get_shortroomid(room_id).await else {
			continue;
		};

		let roomid_bytes = room_id.as_bytes().to_vec();
		let mut roomid_prefix = roomid_bytes.clone();
		roomid_prefix.push(0xFF);

		// Key changes
		futures.push(self.db.keychangeid_userid.watch_prefix(&roomid_prefix));

		// Room account data
		let mut roomuser_prefix = roomid_prefix.clone();
		roomuser_prefix.extend_from_slice(&userid_prefix);

		futures.push(
			self.db
				.roomusertype_roomuserdataid
				.watch_prefix(&roomuser_prefix),
		);

		// PDUs
		let short_roomid_bytes = short_roomid.to_be_bytes().to_vec();
		futures.push(self.db.pduid_pdu.watch_prefix(&short_roomid_bytes));

		futures.push(
			self.db
				.readreceiptid_readreceipt
				.watch_prefix(&roomid_prefix),
		);
	}

	let mut globaluserdata_prefix = vec![0xFF];
	globaluserdata_prefix.extend_from_slice(&userid_prefix);

	futures.push(
		self.db
			.roomusertype_roomuserdataid
			.watch_prefix(&globaluserdata_prefix),
	);

	// More key changes (used when user is not joined to any rooms)
	futures.push(self.db.keychangeid_userid.watch_prefix(&userid_prefix));

	// One time keys
	futures.push(
		self.db
			.userid_lastonetimekeyupdate
			.watch_prefix(&userid_bytes),
	);

	// Server shutdown
	futures.push(self.services.server.until_shutdown().boxed());

	async move {
		if !self.services.server.running() {
			return;
		}

		// Wait until one of them finds something
		trace!(futures = futures.len(), "watch started");
		futures.next().await;
		trace!(futures = futures.len(), "watch finished");
	}
}
