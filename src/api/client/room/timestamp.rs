use axum::extract::State;
use conduwuit::{Err, Error, Result, debug, info, warn};
use futures::{StreamExt, pin_mut};
use ruma::{
	MilliSecondsSinceUnixEpoch, ServerName,
	api::{
		Direction, client::room::get_event_by_timestamp,
		federation::event::get_event_by_timestamp as federation_ts,
	},
};

use crate::Ruma;

/// # `GET /_matrix/client/unstable/org.matrix.msc3030/rooms/{roomId}/timestamp_to_event`
///
/// Get the ID of the event closest to the given timestamp.
pub(crate) async fn get_room_event_by_timestamp_route(
	State(services): State<crate::State>,
	body: Ruma<get_event_by_timestamp::v1::Request>,
) -> Result<get_event_by_timestamp::v1::Response> {
	let room_id = &body.room_id;
	let ts = body.ts;
	let dir = body.dir;

	if !services
		.rooms
		.state_cache
		.is_joined(body.sender_user(), room_id)
		.await
	{
		return Err!(Request(Forbidden(
			"You don't have permission to view events in this room."
		)));
	}

	// Walk the timestamp index starting from the target timestamp in the
	// requested direction. The first visible event we encounter is the answer.
	let stream = services
		.rooms
		.timeline
		.pdus_by_timestamp(room_id, ts.0.into(), dir);
	pin_mut!(stream);

	let mut local_result = None;
	let mut local_error = None::<Error>;
	while let Some(item) = stream.next().await {
		let pdu = match item {
			| Ok(pdu) => pdu,
			| Err(e) => {
				warn!(
					%room_id,
					?ts,
					?dir,
					"Local timestamp index lookup failed; trying federation fallback"
				);
				local_error = Some(e);
				break;
			},
		};

		if services
			.rooms
			.state_accessor
			.user_can_see_event(body.sender_user(), room_id, &pdu.event_id)
			.await
		{
			local_result = Some(get_event_by_timestamp::v1::Response::new(
				pdu.event_id.clone(),
				MilliSecondsSinceUnixEpoch(pdu.origin_server_ts),
			));
			break;
		}
	}

	// For rooms where we are NOT the origin server, also ask the origin via
	// federation. The origin has the complete timeline and will return
	// the correct event (e.g. the m.room.create event for "go to beginning").
	// We pick whichever result is closer to the requested timestamp.
	if services.server.config.allow_federation {
		let mut origin = room_id.server_name().map(ToOwned::to_owned);
		if origin.is_none() {
			if let Ok(create_event) = services
				.rooms
				.state_accessor
				.room_state_get(room_id, &ruma::events::StateEventType::RoomCreate, "")
				.await
			{
				origin = Some(create_event.sender.server_name().to_owned());
			}
		}

		if let Some(ref origin_server) = origin {
			if origin_server != services.globals.server_name() {
				let fed_result =
					federation_query(&services, origin_server, room_id, ts, dir).await;

				// Fetch the federation result locally before checking visibility.
				// `user_can_see_event` is permissive when local event state is missing.
				let fed_result = if let Some(fed) = fed_result {
					if !federation_can_win(ts, dir, local_result.as_ref(), &fed) {
						debug!(
							event_id = %fed.event_id,
							"Skipping federation timestamp result because local result is closer"
						);
						None
					} else {
						match services
							.rooms
							.timeline
							.get_remote_pdu(room_id, &fed.event_id)
							.await
						{
							| Ok(pdu) => {
								if services
									.rooms
									.state_accessor
									.user_can_see_event(
										body.sender_user(),
										room_id,
										&pdu.event_id,
									)
									.await
								{
									Some(get_event_by_timestamp::v1::Response::new(
										pdu.event_id.clone(),
										MilliSecondsSinceUnixEpoch(pdu.origin_server_ts),
									))
								} else {
									debug!(
										event_id = %pdu.event_id,
										"Federation timestamp result is not visible to requester"
									);
									None
								}
							},
							| Err(e) => {
								warn!(
									event_id = %fed.event_id,
									"Failed to fetch federation timestamp result locally: {e}"
								);
								None
							},
						}
					}
				} else {
					None
				};

				return pick_closer(ts, dir, local_result, fed_result);
			}
		}
	}

	local_result.map_or_else(
		|| {
			Err(local_error.unwrap_or_else(|| {
				conduwuit::err!(Request(NotFound(
					"No visible event found near the given timestamp"
				)))
			}))
		},
		Ok,
	)
}

/// Pick whichever result is closer to the target timestamp. In case of a tie,
/// prefer the federation result (it comes from the authoritative source).
fn pick_closer(
	target: MilliSecondsSinceUnixEpoch,
	dir: Direction,
	local: Option<get_event_by_timestamp::v1::Response>,
	federation: Option<get_event_by_timestamp::v1::Response>,
) -> Result<get_event_by_timestamp::v1::Response> {
	match (local, federation) {
		| (Some(l), Some(f)) => {
			let target_u64 = u64::from(target.0);
			let l_ts = u64::from(l.origin_server_ts.0);
			let f_ts = u64::from(f.origin_server_ts.0);

			let l_dist = l_ts.abs_diff(target_u64);
			let f_dist = f_ts.abs_diff(target_u64);

			match f_dist.cmp(&l_dist) {
				| std::cmp::Ordering::Less => {
					debug!(
						local_ts = l_ts,
						fed_ts = f_ts,
						target_ts = target_u64,
						"Preferring federation result (closer to target)"
					);
					Ok(f)
				},
				| std::cmp::Ordering::Greater => Ok(l),
				| std::cmp::Ordering::Equal => {
					// For forward search, prefer the earlier event on tie.
					// For backward search, prefer the later event on tie.
					let prefer_fed = match dir {
						| Direction::Forward => f_ts <= l_ts,
						| Direction::Backward => f_ts >= l_ts,
					};

					if prefer_fed {
						debug!(
							local_ts = l_ts,
							fed_ts = f_ts,
							target_ts = target_u64,
							"Preferring federation result (tie-break)"
						);
						Ok(f)
					} else {
						Ok(l)
					}
				},
			}
		},
		| (Some(l), None) => Ok(l),
		| (None, Some(f)) => Ok(f),
		| (None, None) => Err(conduwuit::err!(Request(NotFound(
			"No visible event found near the given timestamp"
		)))),
	}
}

fn federation_can_win(
	target: MilliSecondsSinceUnixEpoch,
	dir: Direction,
	local: Option<&get_event_by_timestamp::v1::Response>,
	federation: &get_event_by_timestamp::v1::Response,
) -> bool {
	let Some(local) = local else {
		return true;
	};

	let target_u64 = u64::from(target.0);
	let local_ts = u64::from(local.origin_server_ts.0);
	let fed_ts = u64::from(federation.origin_server_ts.0);
	let local_dist = local_ts.abs_diff(target_u64);
	let fed_dist = fed_ts.abs_diff(target_u64);

	fed_dist < local_dist
		|| fed_dist == local_dist
			&& match dir {
				| Direction::Forward => fed_ts <= local_ts,
				| Direction::Backward => fed_ts >= local_ts,
			}
}

/// Ask a remote server for the event closest to `ts` via the federation
/// `timestamp_to_event` endpoint.
async fn federation_query(
	services: &crate::State,
	origin_server: &ServerName,
	room_id: &ruma::RoomId,
	ts: MilliSecondsSinceUnixEpoch,
	dir: Direction,
) -> Option<get_event_by_timestamp::v1::Response> {
	let request = federation_ts::v1::Request::new(room_id.to_owned(), ts, dir);

	info!(
		%room_id,
		%origin_server,
		ts = ?ts,
		?dir,
		"Querying origin server for timestamp_to_event"
	);

	match services
		.sending
		.send_federation_request(origin_server, request)
		.await
	{
		| Ok(response) => {
			info!(
				%room_id,
				event_id = %response.event_id,
				ts = ?response.origin_server_ts,
				"Federation timestamp_to_event returned event"
			);
			Some(get_event_by_timestamp::v1::Response::new(
				response.event_id,
				response.origin_server_ts,
			))
		},
		| Err(e) => {
			warn!(
				%room_id,
				%origin_server,
				"Federation timestamp_to_event failed: {e}"
			);
			None
		},
	}
}
