use axum::{extract::State, response::Json};
use conduwuit::{
	Err, Event, Pdu, PduCount, Result, err, info,
	utils::{
		future::TryExtExt,
		stream::{BroadbandExt, ReadyExt},
	},
};
use futures::{StreamExt, future::join};
use ruma::{
	OwnedEventId,
	api::client::membership::{
		get_member_events::{self, v3::MembershipEventFilter},
		joined_members,
	},
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};

use crate::Ruma;

/// # `POST /_matrix/client/r0/rooms/{roomId}/members`
///
/// Lists all joined users in a room (TODO: at a specific point in time, with a
/// specific membership).
///
/// - Only works if the user is currently joined
pub(crate) async fn get_member_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_member_events::v3::Request>,
) -> Result<get_member_events::v3::Response> {
	let sender_user = body.sender_user();
	let room_id = &body.room_id;
	let membership = body.membership.as_ref();
	let not_membership = body.not_membership.as_ref();

	let is_joined = services
		.rooms
		.state_cache
		.is_joined(sender_user, room_id)
		.await;

	if !services
		.rooms
		.state_cache
		.can_access_history(sender_user, room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view this room.")));
	}

	if let Some(at) = body.at.as_deref() {
		let pdu_count: PduCount = at
			.parse()
			.map_err(|_| err!(Request(InvalidParam("Invalid 'at' token."))))?;

		let mut pdus_rev = services
			.rooms
			.timeline
			.pdus_rev_inclusive(room_id, pdu_count)
			.boxed();

		let Some(Ok((_, pdu))) = pdus_rev.next().await else {
			return Err!(Request(NotFound("Point in time not found in timeline.")));
		};

		let shortstatehash = services
			.rooms
			.state_accessor
			.pdu_shortstatehash(pdu.event_id())
			.await?;

		let chunk: Vec<_> = services
			.rooms
			.state_accessor
			.state_keys_with_ids::<OwnedEventId>(shortstatehash, &StateEventType::RoomMember)
			.broadn_filter_map(256, |(_, event_id)| async move {
				services.rooms.timeline.get_pdu(&event_id).await.ok()
			})
			.ready_filter_map(|pdu| {
				let pdu: Pdu = pdu.into_pdu();
				membership_filter(pdu, membership, not_membership)
			})
			.map(Event::into_format)
			.collect()
			.await;

		return Ok(get_member_events::v3::Response { chunk });
	}

	// For departed users, use state snapshot at the time of departure.
	// Note: pdu_shortstatehash stores state BEFORE the event, so for the
	// leave event the user still appears as "join". We collect the leave_pdu
	// separately and overlay it on the snapshot results.
	let (shortstatehash, leave_pdu) = if !is_joined {
		if let Ok(Some(leave_pdu)) = services
			.rooms
			.state_cache
			.left_state(sender_user, room_id)
			.await
		{
			let ssh = services
				.rooms
				.state_accessor
				.pdu_shortstatehash(leave_pdu.event_id())
				.await
				.ok();
			info!(
				target: "membership_debug",
				"/members: departed user {sender_user} in {room_id}, leave_ssh={ssh:?}"
			);
			(ssh, Some(leave_pdu))
		} else {
			(None, None)
		}
	} else {
		(None, None)
	};

	let shortstatehash = match shortstatehash {
		| Some(ssh) => ssh,
		| None =>
			services
				.rooms
				.state
				.get_room_shortstatehash(room_id)
				.await?,
	};

	let mut members: Vec<Pdu> = services
		.rooms
		.state_accessor
		.state_keys_with_ids::<OwnedEventId>(shortstatehash, &StateEventType::RoomMember)
		.broadn_filter_map(256, |(_, event_id)| async move {
			services.rooms.timeline.get_pdu(&event_id).await.ok()
		})
		.map(Event::into_pdu)
		.collect()
		.await;

	// Overlay the leave PDU: replace the user's "join" entry with their
	// actual leave event so the membership is correct (pdu_shortstatehash
	// stores state BEFORE the event, so the leave isn't reflected yet).
	if let Some(leave_pdu) = leave_pdu {
		let leave_pdu: Pdu = leave_pdu.into_pdu();
		if let Some(leave_sk) = leave_pdu.state_key.as_deref() {
			if let Some(pos) = members
				.iter()
				.position(|m| m.state_key.as_deref() == Some(leave_sk))
			{
				members[pos] = leave_pdu;
			} else {
				members.push(leave_pdu);
			}
		}
	}

	Ok(get_member_events::v3::Response {
		chunk: members
			.into_iter()
			.filter_map(|pdu| membership_filter(pdu, membership, not_membership))
			.map(Event::into_format)
			.collect(),
	})
}

/// # `POST /_matrix/client/r0/rooms/{roomId}/joined_members`
///
/// Lists all members of a room.
///
/// - The sender user must be in the room
/// - TODO: An appservice just needs a puppet joined
pub(crate) async fn joined_members_route(
	State(services): State<crate::State>,
	body: Ruma<joined_members::v3::Request>,
) -> Result<Json<Response>> {
	if !services
		.rooms
		.state_cache
		.is_joined(body.sender_user(), &body.room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view this room.")));
	}

	let room_members = services
		.rooms
		.state_cache
		.room_members(&body.room_id)
		.map(ToOwned::to_owned)
		.broad_then(|user_id| async move {
			let (display_name, avatar_url) = join(
				services.users.displayname(&user_id).ok(),
				services.users.avatar_url(&user_id).ok(),
			)
			.await;

			(user_id, RoomMemberResponse { display_name, avatar_url })
		})
		.collect()
		.await;

	Ok(Json(Response { joined: room_members }))
}

#[derive(serde::Serialize)]
pub(crate) struct RoomMemberResponse {
	pub(crate) display_name: Option<String>,
	pub(crate) avatar_url: Option<ruma::OwnedMxcUri>,
}

#[derive(serde::Serialize)]
pub(crate) struct Response {
	pub(crate) joined: std::collections::BTreeMap<ruma::OwnedUserId, RoomMemberResponse>,
}

fn membership_filter<Pdu: Event>(
	pdu: Pdu,
	for_membership: Option<&MembershipEventFilter>,
	not_membership: Option<&MembershipEventFilter>,
) -> Option<impl Event> {
	let membership_state_filter = match for_membership {
		| Some(MembershipEventFilter::Ban) => MembershipState::Ban,
		| Some(MembershipEventFilter::Invite) => MembershipState::Invite,
		| Some(MembershipEventFilter::Knock) => MembershipState::Knock,
		| Some(MembershipEventFilter::Leave) => MembershipState::Leave,
		| Some(_) | None => MembershipState::Join,
	};

	let not_membership_state_filter = match not_membership {
		| Some(MembershipEventFilter::Ban) => MembershipState::Ban,
		| Some(MembershipEventFilter::Invite) => MembershipState::Invite,
		| Some(MembershipEventFilter::Join) => MembershipState::Join,
		| Some(MembershipEventFilter::Knock) => MembershipState::Knock,
		| Some(_) | None => MembershipState::Leave,
	};

	let evt_membership = pdu.get_content::<RoomMemberEventContent>().ok()?.membership;

	if for_membership.is_some() && not_membership.is_some() {
		if membership_state_filter != evt_membership
			|| not_membership_state_filter == evt_membership
		{
			None
		} else {
			Some(pdu)
		}
	} else if for_membership.is_some() && not_membership.is_none() {
		if membership_state_filter != evt_membership {
			None
		} else {
			Some(pdu)
		}
	} else if not_membership.is_some() && for_membership.is_none() {
		if not_membership_state_filter == evt_membership {
			None
		} else {
			Some(pdu)
		}
	} else {
		Some(pdu)
	}
}
