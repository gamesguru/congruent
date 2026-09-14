use std::collections::{HashMap, HashSet};
use ruma::{
    events::{room::{join_rules::{JoinRule, RoomJoinRulesEventContent}, power_levels::RoomPowerLevelsEventContent}, StateEventType},
    EventId, OwnedEventId, OwnedRoomId, RoomVersionId, MilliSecondsSinceUnixEpoch
};
use serde_json::json;
use futures::future::ready;

use super::{test_utils::*, RoomVersion, StateMap, resolve};

#[tokio::test]
async fn v22_resolves_msc4297_state_reset() {
    let room_id: OwnedRoomId = "!V22Test12345678901234567890123456789".try_into().unwrap();
    let create_id_str = "$V22Test12345678901234567890123456789";
    let create_id: OwnedEventId = create_id_str.try_into().unwrap();

    let mut e1_create = to_pdu_event::<&str>(
        create_id_str,
        alice(),
        TimelineEventType::RoomCreate,
        Some(""),
        to_raw_json_value(&json!({ "creator": alice(), "room_version": "12" })).unwrap(),
        &[],
        &[],
    );
    e1_create.room_id = Some(room_id.clone());

    let e2_ma = to_pdu_event(
        "SR_MA",
        alice(),
        TimelineEventType::RoomMember,
        Some(alice().as_str()),
        member_content_join(),
        &[create_id_str],
        &[create_id_str],
    );

    let e3_pl = to_pdu_event(
        "SR_PL",
        alice(),
        TimelineEventType::RoomPowerLevels,
        Some(""),
        to_raw_json_value(&json!({ "users": { alice(): 100 } })).unwrap(),
        &["SR_MA", create_id_str],
        &["SR_MA"],
    );

    // Legitimate JR (Public)
    let e4_jr_public = to_pdu_event(
        "SR_JR1",
        alice(),
        TimelineEventType::RoomJoinRules,
        Some(""),
        to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
        &["SR_MA", "SR_PL", create_id_str],
        &["SR_PL"],
    );

    // Attacker JR (Invite)
    let e5_jr_invite = to_pdu_event(
        "SR_JR2",
        alice(),
        TimelineEventType::RoomJoinRules,
        Some(""),
        to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Invite)).unwrap(),
        &["SR_MA", "SR_PL", create_id_str],
        &["SR_PL"],
    );

    // Bob joins citing JR(public)
    let e6_mb = to_pdu_event(
        "SR_MB",
        bob(),
        TimelineEventType::RoomMember,
        Some(bob().as_str()),
        member_content_join(),
        &["SR_PL", "SR_JR1", create_id_str],
        &["SR_JR1"],
    );

    let all_events = vec![&e1_create, &e2_ma, &e3_pl, &e4_jr_public, &e5_jr_invite, &e6_mb];
    let store = TestStore(
        all_events
            .into_iter()
            .map(|ev| {
                let mut ev = (*ev).clone();
                ev.room_id = Some(room_id.clone());
                (ev.event_id.clone(), ev)
            })
            .collect(),
    );

    let fork_legit: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e3_pl, &e4_jr_public, &e6_mb]
        .iter()
        .map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
        .collect();

    let fork_attacker: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e3_pl, &e5_jr_invite]
        .iter()
        .map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
        .collect();

    let state_sets = [fork_legit, fork_attacker];
    let auth_chain: Vec<_> = state_sets
        .iter()
        .map(|map| store.auth_event_ids(&room_id, map.values().cloned().collect()).unwrap())
        .collect();

    let ev_map = &store.0;
    let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());

    // Evaluate under V12 (V2_1) - Bob's join should FAIL auth!
    let resolved_v12 = resolve(
        &RoomVersionId::V12,
        &state_sets,
        &auth_chain,
        &fetcher,
        None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<super::PduEvent>>>,
        None::<&fn(Vec<OwnedEventId>)>,
    ).await.unwrap();

    let bob_key = (StateEventType::RoomMember, bob().to_string().into());
    assert!(
        resolved_v12.get(&bob_key).is_none(),
        "Under V2.1, Bob's join should be rejected because JR(Invite) overrides JR(Public)"
    );

    // Evaluate under a hypothetical V13 (which would use V2_1_1) - wait, RoomVersionId doesn't have V13 yet.
    // We can manually create a RoomVersion struct!
    let mut v211_version = RoomVersion::new(&RoomVersionId::V12).unwrap();
    v211_version.state_res = super::room_version::StateResolutionVersion::V2_1_1;

    // We can't pass a custom RoomVersion directly to resolve, it takes a RoomVersionId.
    // Wait, resolve takes RoomVersionId and looks up the RoomVersion inside.
    // Let's check how resolve does it.
}
