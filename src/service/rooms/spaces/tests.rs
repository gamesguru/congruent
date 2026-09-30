use std::str::FromStr;

use ruma::{
	UInt,
	api::federation::space::{SpaceHierarchyParentSummary, SpaceHierarchyParentSummaryInit},
	owned_room_id, owned_server_name,
	space::SpaceRoomJoinRule,
};

use crate::rooms::spaces::{PaginationToken, get_parent_children_via, summary_to_chunk};

#[test]
fn get_summary_children() {
	let mut summary: SpaceHierarchyParentSummary = SpaceHierarchyParentSummaryInit {
		num_joined_members: UInt::from(1_u32),
		room_id: owned_room_id!("!root:example.org"),
		world_readable: true,
		guest_can_join: true,
		join_rule: SpaceRoomJoinRule::Public,
		children_state: vec![
			serde_json::from_str(
				r#"{
                      "content": {
                        "via": [
                          "example.org"
                        ],
                        "suggested": false
                      },
                      "origin_server_ts": 1629413349153,
                      "sender": "@alice:example.org",
                      "state_key": "!foo:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			serde_json::from_str(
				r#"{
                      "content": {
                        "via": [
                          "example.org"
                        ],
                        "suggested": true
                      },
                      "origin_server_ts": 1629413349157,
                      "sender": "@alice:example.org",
                      "state_key": "!bar:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			serde_json::from_str(
				r#"{
                      "content": {
                        "via": [
                          "example.org"
                        ]
                      },
                      "origin_server_ts": 1629413349160,
                      "sender": "@alice:example.org",
                      "state_key": "!baz:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
		],
		allowed_room_ids: vec![],
	}
	.into();
	summary.room_type = Some(ruma::room::RoomType::Space);

	let all = get_parent_children_via(&summary, false);
	assert_eq!(all, vec![
		(owned_room_id!("!foo:example.org"), vec![owned_server_name!("example.org")]),
		(owned_room_id!("!bar:example.org"), vec![owned_server_name!("example.org")]),
		(owned_room_id!("!baz:example.org"), vec![owned_server_name!("example.org")])
	]);
	let suggested = get_parent_children_via(&summary, true);
	assert_eq!(suggested, vec![(owned_room_id!("!bar:example.org"), vec![owned_server_name!(
		"example.org"
	)])]);
}

#[test]
fn summary_chunk_filters_children_state_for_suggested_only() {
	let mut summary: SpaceHierarchyParentSummary = SpaceHierarchyParentSummaryInit {
		num_joined_members: UInt::from(1_u32),
		room_id: owned_room_id!("!root:example.org"),
		world_readable: true,
		guest_can_join: true,
		join_rule: SpaceRoomJoinRule::Public,
		children_state: vec![
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "suggested": true },
                      "origin_server_ts": 1,
                      "sender": "@alice:example.org",
                      "state_key": "!suggested:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"] },
                      "origin_server_ts": 2,
                      "sender": "@alice:example.org",
                      "state_key": "!plain:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
		],
		allowed_room_ids: vec![],
	}
	.into();
	summary.room_type = Some(ruma::room::RoomType::Space);

	let chunk = summary_to_chunk(summary, true);
	let children: Vec<_> = chunk
		.children_state
		.into_iter()
		.map(|child| child.deserialize().unwrap().state_key)
		.collect();

	assert_eq!(children, vec![owned_room_id!("!suggested:example.org")]);
}

#[test]
fn get_summary_children_sorted_by_order() {
	let mut summary: SpaceHierarchyParentSummary = SpaceHierarchyParentSummaryInit {
		num_joined_members: UInt::from(1_u32),
		room_id: owned_room_id!("!root:example.org"),
		world_readable: true,
		guest_can_join: true,
		join_rule: SpaceRoomJoinRule::Public,
		children_state: vec![
			// No order field — should sort last using the spec tie-breakers.
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "suggested": false },
                      "origin_server_ts": 1,
                      "sender": "@alice:example.org",
                      "state_key": "!zoo:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			// order = "b"
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "order": "b", "suggested": false },
                      "origin_server_ts": 2,
                      "sender": "@alice:example.org",
                      "state_key": "!beta:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			// order = "a"
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "order": "a", "suggested": false },
                      "origin_server_ts": 3,
                      "sender": "@alice:example.org",
                      "state_key": "!alpha:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			// No order field — should sort last using the spec tie-breakers.
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "suggested": false },
                      "origin_server_ts": 4,
                      "sender": "@alice:example.org",
                      "state_key": "!aaa:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
		],
		allowed_room_ids: vec![],
	}
	.into();
	summary.room_type = Some(ruma::room::RoomType::Space);
	assert_eq!(
		summary
			.room_type
			.as_ref()
			.map(|room_type| room_type.as_str()),
		Some("m.space")
	);

	let result = get_parent_children_via(&summary, false);
	let room_ids: Vec<_> = result.iter().map(|(id, _)| id.as_str()).collect();
	// order="a" first, then order="b", then no-order children sorted by the
	// spec tie-breakers.
	assert_eq!(room_ids, vec![
		"!alpha:example.org",
		"!beta:example.org",
		"!zoo:example.org",
		"!aaa:example.org",
	]);
}

#[test]
fn get_summary_children_tie_breaks_by_timestamp_then_room_id() {
	let mut summary: SpaceHierarchyParentSummary = SpaceHierarchyParentSummaryInit {
		num_joined_members: UInt::from(1_u32),
		room_id: owned_room_id!("!root:example.org"),
		world_readable: true,
		guest_can_join: true,
		join_rule: SpaceRoomJoinRule::Public,
		children_state: vec![
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "order": "a", "suggested": false },
                      "origin_server_ts": 20,
                      "sender": "@alice:example.org",
                      "state_key": "!zulu:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "order": "a", "suggested": false },
                      "origin_server_ts": 10,
                      "sender": "@alice:example.org",
                      "state_key": "!alpha:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "suggested": false },
                      "origin_server_ts": 30,
                      "sender": "@alice:example.org",
                      "state_key": "!later:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
			serde_json::from_str(
				r#"{
                      "content": { "via": ["example.org"], "suggested": false },
                      "origin_server_ts": 30,
                      "sender": "@alice:example.org",
                      "state_key": "!earlier:example.org",
                      "type": "m.space.child"
                    }"#,
			)
			.unwrap(),
		],
		allowed_room_ids: vec![],
	}
	.into();
	summary.room_type = Some(ruma::room::RoomType::Space);

	assert_eq!(
		summary
			.room_type
			.as_ref()
			.map(|room_type| room_type.as_str()),
		Some("m.space")
	);
	assert!(
		summary
			.children_state
			.iter()
			.all(|child| child.deserialize().is_ok())
	);

	let result = get_parent_children_via(&summary, false);
	let room_ids: Vec<_> = result.iter().map(|(id, _)| id.as_str()).collect();

	assert_eq!(room_ids, vec![
		"!alpha:example.org",
		"!zulu:example.org",
		"!earlier:example.org",
		"!later:example.org",
	]);
}

#[test]
fn invalid_pagination_tokens() {
	fn token_is_err(token: &str) { PaginationToken::from_str(token).unwrap_err(); }

	token_is_err("231_2_noabool");
	token_is_err("");
	token_is_err("111_3_");
	token_is_err("foo_not_int");
	token_is_err("11_4_true_");
	token_is_err("___");
	token_is_err("__false");
}

#[test]
fn valid_pagination_tokens() {
	assert_eq!(
		PaginationToken {
			short_room_ids: vec![5383, 42934, 283, 423],
			limit: UInt::from(20_u32),
			max_depth: UInt::from(1_u32),
			suggested_only: true
		},
		PaginationToken::from_str("5383,42934,283,423_20_1_true").unwrap()
	);

	assert_eq!(
		PaginationToken {
			short_room_ids: vec![740],
			limit: UInt::from(97_u32),
			max_depth: UInt::from(10539_u32),
			suggested_only: false
		},
		PaginationToken::from_str("740_97_10539_false").unwrap()
	);
}

#[test]
fn pagination_token_to_string() {
	assert_eq!(
		PaginationToken {
			short_room_ids: vec![740],
			limit: UInt::from(97_u32),
			max_depth: UInt::from(10539_u32),
			suggested_only: false
		}
		.to_string(),
		"740_97_10539_false"
	);

	assert_eq!(
		PaginationToken {
			short_room_ids: vec![9, 34],
			limit: UInt::from(3_u32),
			max_depth: UInt::from(1_u32),
			suggested_only: true
		}
		.to_string(),
		"9,34_3_1_true"
	);
}

use crate::rooms::spaces::is_join_rule_accessible;

#[test]
fn test_is_join_rule_accessible() {
	// If user is joined or invited, always true
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Invite, false, true), Some(true));
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Restricted, false, true), Some(true));

	// If world_readable is true, always true
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Invite, true, false), Some(true));
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Restricted, true, false), Some(true));

	// Public join rules are always true
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Public, false, false), Some(true));
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Knock, false, false), Some(true));

	// Restricted requires checking allowed_rooms (returns None)
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Restricted, false, false), None);

	// Private/Invite returns false
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Invite, false, false), Some(false));
	assert_eq!(is_join_rule_accessible(&SpaceRoomJoinRule::Private, false, false), Some(false));
}
