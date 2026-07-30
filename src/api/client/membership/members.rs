use axum::{extract::State, response::Json};
use conduwuit::{
	Err, Event, Pdu, PduCount, Result, at, err,
	utils::{
		future::TryExtExt,
		stream::{BroadbandExt, ReadyExt},
	},
};
use futures::{FutureExt, StreamExt, future::join};
use ruma::{
	OwnedUserId,
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
	let membership = body.membership.as_ref();
	let not_membership = body.not_membership.as_ref();

	if !services
		.rooms
		.state_accessor
		.user_can_see_state_events(sender_user, &body.room_id)
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
			.pdus_rev(&body.room_id, Some(pdu_count))
			.boxed();

		let Some(Ok((_, pdu))) = pdus_rev.next().await else {
			return Err!(Request(NotFound("Point in time not found in timeline.")));
		};

		let shortstatehash = services
			.rooms
			.state_accessor
			.pdu_shortstatehash(pdu.event_id())
			.await?;

		// Collect into Vec<Pdu> to avoid HRTB/opaque-type conflicts with
		// room_state_full's impl Event stream used later in this function.
		let all_pdus: Vec<Pdu> = services
			.rooms
			.state_accessor
			.state_full_pdus(shortstatehash)
			.map(Event::into_pdu)
			.collect()
			.await;

		let mut chunk: Vec<Pdu> = all_pdus
			.into_iter()
			.filter(|pdu| *pdu.kind() == ruma::events::TimelineEventType::RoomMember)
			.filter_map(|pdu| membership_filter(pdu, membership, not_membership))
			.map(Event::into_pdu)
			.collect();

		let power_levels: ruma::events::room::power_levels::RoomPowerLevelsEventContent =
			services
				.rooms
				.state_accessor
				.state_get_content(shortstatehash, &StateEventType::RoomPowerLevels, "")
				.await
				.unwrap_or_default();

		let get_pdu_info = |pdu: &Pdu| {
			let user_id = pdu
				.state_key
				.as_ref()
				.map(|k| OwnedUserId::parse(k.as_str()))
				.transpose()
				.ok()
				.flatten()
				.unwrap_or_else(|| pdu.sender.clone());

			let power_level = power_levels
				.users
				.get(&user_id)
				.copied()
				.unwrap_or(power_levels.users_default);

			let member_content = pdu.get_content::<RoomMemberEventContent>().ok();
			let displayname = member_content
				.as_ref()
				.and_then(|c| c.displayname.as_ref().map(|s| s.to_lowercase()));

			(power_level, displayname, user_id)
		};

		chunk.sort_by(|a, b| {
			let (pl_a, name_a, id_a) = get_pdu_info(a);
			let (pl_b, name_b, id_b) = get_pdu_info(b);

			let pl_cmp = pl_b.cmp(&pl_a);
			if pl_cmp != std::cmp::Ordering::Equal {
				return pl_cmp;
			}

			let name_cmp = match (&name_a, &name_b) {
				| (Some(na), Some(nb)) => na.cmp(nb),
				| (Some(_), None) => std::cmp::Ordering::Less,
				| (None, Some(_)) => std::cmp::Ordering::Greater,
				| (None, None) => std::cmp::Ordering::Equal,
			};
			if name_cmp != std::cmp::Ordering::Equal {
				return name_cmp;
			}

			id_a.cmp(&id_b)
		});

		let chunk = chunk.into_iter().map(Event::into_format).collect();

		return Ok(get_member_events::v3::Response { chunk });
	}

	let mut chunk: Vec<Pdu> = services
		.rooms
		.state_accessor
		.room_state_full(&body.room_id)
		.ready_filter_map(Result::ok)
		.ready_filter(|((ty, _), _)| *ty == StateEventType::RoomMember)
		.map(at!(1))
		.ready_filter_map(|pdu| membership_filter(pdu, membership, not_membership))
		.map(Event::into_pdu)
		.collect()
		.boxed()
		.await;

	let power_levels: ruma::events::room::power_levels::RoomPowerLevelsEventContent = services
		.rooms
		.state_accessor
		.room_state_get_content(&body.room_id, &StateEventType::RoomPowerLevels, "")
		.await
		.unwrap_or_default();

	let get_pdu_info = |pdu: &Pdu| {
		let user_id = pdu
			.state_key
			.as_ref()
			.map(|k| OwnedUserId::parse(k.as_str()))
			.transpose()
			.ok()
			.flatten()
			.unwrap_or_else(|| pdu.sender.clone());

		let power_level = power_levels
			.users
			.get(&user_id)
			.copied()
			.unwrap_or(power_levels.users_default);

		let member_content = pdu.get_content::<RoomMemberEventContent>().ok();
		let displayname = member_content
			.as_ref()
			.and_then(|c| c.displayname.as_ref().map(|s| s.to_lowercase()));

		(power_level, displayname, user_id)
	};

	chunk.sort_by(|a, b| {
		let (pl_a, name_a, id_a) = get_pdu_info(a);
		let (pl_b, name_b, id_b) = get_pdu_info(b);

		let pl_cmp = pl_b.cmp(&pl_a);
		if pl_cmp != std::cmp::Ordering::Equal {
			return pl_cmp;
		}

		let name_cmp = match (&name_a, &name_b) {
			| (Some(na), Some(nb)) => na.cmp(nb),
			| (Some(_), None) => std::cmp::Ordering::Less,
			| (None, Some(_)) => std::cmp::Ordering::Greater,
			| (None, None) => std::cmp::Ordering::Equal,
		};
		if name_cmp != std::cmp::Ordering::Equal {
			return name_cmp;
		}

		id_a.cmp(&id_b)
	});

	let chunk = chunk.into_iter().map(Event::into_format).collect();

	Ok(get_member_events::v3::Response { chunk })
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
		.state_accessor
		.user_can_see_state_events(body.sender_user(), &body.room_id)
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
	pub(crate) joined: std::collections::BTreeMap<OwnedUserId, RoomMemberResponse>,
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

#[cfg(test)]
mod tests {
	use ruma::{
		OwnedEventId, OwnedUserId,
		events::{
			TimelineEventType,
			room::member::{MembershipState, RoomMemberEventContent},
		},
		uint,
	};

	use super::*;

	fn create_test_pdu(user_id: &str, displayname: Option<&str>) -> Pdu {
		let user = OwnedUserId::parse(user_id).unwrap();
		let content = RoomMemberEventContent::new(MembershipState::Join);
		let mut content_value = serde_json::to_value(&content).unwrap();
		if let Some(dn) = displayname {
			content_value
				.as_object_mut()
				.unwrap()
				.insert("displayname".to_owned(), serde_json::Value::String(dn.to_owned()));
		}
		let content_json = serde_json::to_string(&content_value).unwrap();
		let raw_content = serde_json::value::RawValue::from_string(content_json).unwrap();

		Pdu {
			event_id: OwnedEventId::parse("$test_event_id:example.org").unwrap(),
			room_id: None,
			sender: user.clone(),
			origin: None,
			origin_server_ts: uint!(123456),
			kind: TimelineEventType::RoomMember,
			content: raw_content,
			state_key: Some(user.as_str().into()),
			prev_events: Vec::new(),
			depth: uint!(1),
			auth_events: Vec::new(),
			redacts: None,
			unsigned: None,
			hashes: conduwuit_core::pdu::EventHash { sha256: String::new() },
			signatures: None,
		}
	}

	#[test]
	fn test_canonical_sorting() {
		// Mock Power Levels
		let mut power_levels =
			ruma::events::room::power_levels::RoomPowerLevelsEventContent::default();
		power_levels
			.users
			.insert(OwnedUserId::parse("@admin:example.org").unwrap(), 100.into());
		power_levels
			.users
			.insert(OwnedUserId::parse("@moderator:example.org").unwrap(), 50.into());

		// Create PDUs to sort
		let admin = create_test_pdu("@admin:example.org", Some("Admin User"));
		let moderator = create_test_pdu("@moderator:example.org", Some("Mod User"));
		let alice = create_test_pdu("@alice:example.org", Some("Alice"));
		let bob = create_test_pdu("@bob:example.org", Some("bob"));
		let charlie = create_test_pdu("@charlie:example.org", Some("Charlie"));
		let alice_z = create_test_pdu("@alice_z:example.org", Some("Alice"));
		let alice_a = create_test_pdu("@alice_a:example.org", Some("Alice"));

		// Shuffle them
		let mut chunk = vec![
			charlie.clone(),
			alice_z.clone(),
			moderator.clone(),
			bob.clone(),
			alice_a.clone(),
			admin.clone(),
			alice.clone(),
		];

		let get_pdu_info = |pdu: &Pdu| {
			let user_id = pdu
				.state_key
				.as_ref()
				.map(|k| OwnedUserId::parse(k.as_str()))
				.transpose()
				.ok()
				.flatten()
				.unwrap_or_else(|| pdu.sender.clone());

			let power_level = power_levels
				.users
				.get(&user_id)
				.copied()
				.unwrap_or(power_levels.users_default);

			let member_content = pdu.get_content::<RoomMemberEventContent>().ok();
			let displayname = member_content
				.as_ref()
				.and_then(|c| c.displayname.as_ref().map(|s| s.to_lowercase()));

			(power_level, displayname, user_id)
		};

		chunk.sort_by(|a, b| {
			let (pl_a, name_a, id_a) = get_pdu_info(a);
			let (pl_b, name_b, id_b) = get_pdu_info(b);

			let pl_cmp = pl_b.cmp(&pl_a);
			if pl_cmp != std::cmp::Ordering::Equal {
				return pl_cmp;
			}

			let name_cmp = match (&name_a, &name_b) {
				| (Some(na), Some(nb)) => na.cmp(nb),
				| (Some(_), None) => std::cmp::Ordering::Less,
				| (None, Some(_)) => std::cmp::Ordering::Greater,
				| (None, None) => std::cmp::Ordering::Equal,
			};
			if name_cmp != std::cmp::Ordering::Equal {
				return name_cmp;
			}

			id_a.cmp(&id_b)
		});

		// Verification of Sort Order:
		// 1. Admin (PL 100)
		// 2. Moderator (PL 50)
		// 3. Alice (PL 0, Display Name "alice", ID @alice:example.org)
		// 4. Alice (PL 0, Display Name "alice", ID @alice_a:example.org)
		// 5. Alice (PL 0, Display Name "alice", ID @alice_z:example.org)
		// 6. bob (PL 0, Display Name "bob", ID @bob:example.org)
		// 7. Charlie (PL 0, Display Name "charlie", ID @charlie:example.org)
		assert_eq!(chunk[0].sender.as_str(), "@admin:example.org");
		assert_eq!(chunk[1].sender.as_str(), "@moderator:example.org");
		assert_eq!(chunk[2].sender.as_str(), "@alice:example.org");
		assert_eq!(chunk[3].sender.as_str(), "@alice_a:example.org");
		assert_eq!(chunk[4].sender.as_str(), "@alice_z:example.org");
		assert_eq!(chunk[5].sender.as_str(), "@bob:example.org");
		assert_eq!(chunk[6].sender.as_str(), "@charlie:example.org");
	}
}
