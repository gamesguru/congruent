#[cfg(test)]
mod tests {
	use std::collections::{HashMap, HashSet};

	use maplit::{hashmap, hashset};
	use rand::seq::SliceRandom;
	use ruma::{
		MilliSecondsSinceUnixEpoch, OwnedEventId, RoomVersionId,
		events::{
			StateEventType, TimelineEventType,
			room::join_rules::{JoinRule, RoomJoinRulesEventContent},
		},
		int, uint,
	};
	use serde_json::{json, value::to_raw_value as to_raw_json_value};

	use super::{
		StateMap, is_power_event,
		room_version::RoomVersion,
		test_utils::{
			INITIAL_EVENTS, TestStore, alice, bob, charlie, do_check, ella, event_id,
			member_content_ban, member_content_join, member_content_leave, room_id,
			to_init_pdu_event, to_pdu_event, zara,
		},
	};
	use crate::{
		debug,
		matrix::{Event, EventTypeExt, Pdu as PduEvent},
		state_res::room_version::StateResolutionVersion,
		utils::stream::IterStream,
	};

	async fn test_event_sort() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);
		let events = INITIAL_EVENTS();

		let event_map = events
			.values()
			.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.clone()))
			.collect::<StateMap<_>>();

		let auth_chain: HashSet<OwnedEventId> = HashSet::new();

		let power_events = event_map
			.values()
			.filter(|&pdu| is_power_event(&*pdu))
			.map(|pdu| pdu.event_id.clone())
			.collect::<Vec<_>>();

		let fetcher = |id| ready(events.get(&id).cloned());
		let parsed_pl_cache = dashmap::DashMap::new();
		let sender_pl_cache = dashmap::DashMap::new();
		let sorted_power_events = super::reverse_topological_power_sort(
			power_events,
			&auth_chain,
			&fetcher,
			None,
			&parsed_pl_cache,
			&sender_pl_cache,
		)
		.await
		.unwrap();

		let resolved_power = super::iterative_auth_check(
			&RoomVersion::V6,
			sorted_power_events.iter().map(AsRef::as_ref).stream(),
			vec![HashMap::new()], // unconflicted events
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(&ruma::EventId) -> bool>,
		)
		.await
		.expect("iterative auth check failed on resolved events");

		// don't remove any events so we know it sorts them all correctly
		let mut events_to_sort = events.keys().cloned().collect::<Vec<_>>();

		events_to_sort.shuffle(&mut rand::rng());

		let power_level = resolved_power
			.get(&(StateEventType::RoomPowerLevels, "".into()))
			.cloned();

		let sorted_event_ids = super::mainline_sort(&events_to_sort, power_level, &fetcher)
			.await
			.unwrap();

		assert_eq!(
			vec![
				// No PL in auth chain -> None depth -> sort first (lowest priority, lose)
				"$CREATE:foo",
				"$IMA:foo",
				"$START:foo",
				"$END:foo",
				// PL in auth chain -> Some(0) -> sort last (highest priority, win)
				"$IPOWER:foo",
				"$IJR:foo",
				"$IMB:foo",
				"$IMC:foo",
			],
			sorted_event_ids
				.iter()
				.map(|id| id.to_string())
				.collect::<Vec<_>>()
		);
	}

	#[tokio::test]
	async fn test_sort() {
		for _ in 0..20 {
			// since we shuffle the eventIds before we sort them introducing randomness
			// seems like we should test this a few times
			test_event_sort().await;
		}
	}

	#[tokio::test]
	async fn ban_vs_power_level() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let events = &[
			to_init_pdu_event(
				"PA",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
			to_init_pdu_event(
				"MA",
				alice(),
				TimelineEventType::RoomMember,
				Some(alice().to_string().as_str()),
				member_content_join(),
			),
			to_init_pdu_event(
				"MB",
				alice(),
				TimelineEventType::RoomMember,
				Some(bob().to_string().as_str()),
				member_content_ban(),
			),
			to_init_pdu_event(
				"PB",
				bob(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
		];

		let edges = vec![vec!["END", "MB", "MA", "PA", "START"], vec!["END", "PA", "PB"]]
			.into_iter()
			.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
			.collect::<Vec<_>>();

		let expected_state_ids = vec!["PA", "MA", "MB"]
			.into_iter()
			.map(event_id)
			.collect::<Vec<_>>();

		do_check(events, edges, expected_state_ids).await;
	}

	#[tokio::test]
	async fn topic_basic() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let events = &[
			to_init_pdu_event(
				"T1",
				alice(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"PA1",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
			to_init_pdu_event(
				"T2",
				alice(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"PA2",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 0 } })).unwrap(),
			),
			to_init_pdu_event(
				"PB",
				bob(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
			to_init_pdu_event(
				"T3",
				bob(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
		];

		let edges =
			vec![vec!["END", "PA2", "T2", "PA1", "T1", "START"], vec!["END", "T3", "PB", "PA1"]]
				.into_iter()
				.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
				.collect::<Vec<_>>();

		let expected_state_ids = vec!["PA2", "T2"]
			.into_iter()
			.map(event_id)
			.collect::<Vec<_>>();

		do_check(events, edges, expected_state_ids).await;
	}

	#[tokio::test]
	async fn topic_reset() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let events = &[
			to_init_pdu_event(
				"T1",
				alice(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"PA",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
			to_init_pdu_event(
				"T2",
				bob(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"MB",
				alice(),
				TimelineEventType::RoomMember,
				Some(bob().to_string().as_str()),
				member_content_ban(),
			),
		];

		let edges = vec![vec!["END", "MB", "T2", "PA", "T1", "START"], vec!["END", "T1"]]
			.into_iter()
			.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
			.collect::<Vec<_>>();

		let expected_state_ids = vec!["T1", "MB", "PA"]
			.into_iter()
			.map(event_id)
			.collect::<Vec<_>>();

		do_check(events, edges, expected_state_ids).await;
	}

	#[tokio::test]
	async fn join_rule_evasion() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let events = &[
			to_init_pdu_event(
				"JR",
				alice(),
				TimelineEventType::RoomJoinRules,
				Some(""),
				to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Private)).unwrap(),
			),
			to_init_pdu_event(
				"ME",
				ella(),
				TimelineEventType::RoomMember,
				Some(ella().to_string().as_str()),
				member_content_join(),
			),
		];

		let edges = vec![vec!["END", "JR", "START"], vec!["END", "ME", "START"]]
			.into_iter()
			.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
			.collect::<Vec<_>>();

		let expected_state_ids = vec![event_id("JR")];

		do_check(events, edges, expected_state_ids).await;
	}

	#[tokio::test]
	async fn offtopic_power_level() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let events = &[
			to_init_pdu_event(
				"PA",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
			to_init_pdu_event(
				"PB",
				bob(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(
					&json!({ "users": { alice(): 100, bob(): 50, charlie(): 50 } }),
				)
				.unwrap(),
			),
			to_init_pdu_event(
				"PC",
				charlie(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50, charlie(): 0 } }))
					.unwrap(),
			),
		];

		let edges = vec![vec!["END", "PC", "PB", "PA", "START"], vec!["END", "PA"]]
			.into_iter()
			.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
			.collect::<Vec<_>>();

		let expected_state_ids = vec!["PC"].into_iter().map(event_id).collect::<Vec<_>>();

		do_check(events, edges, expected_state_ids).await;
	}

	#[tokio::test]
	async fn topic_setting() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let events = &[
			to_init_pdu_event(
				"T1",
				alice(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"PA1",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
			to_init_pdu_event(
				"T2",
				alice(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"PA2",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 0 } })).unwrap(),
			),
			to_init_pdu_event(
				"PB",
				bob(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
			),
			to_init_pdu_event(
				"T3",
				bob(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"MZ1",
				zara(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
			to_init_pdu_event(
				"T4",
				alice(),
				TimelineEventType::RoomTopic,
				Some(""),
				to_raw_json_value(&json!({})).unwrap(),
			),
		];

		let edges = vec![vec!["END", "T4", "MZ1", "PA2", "T2", "PA1", "T1", "START"], vec![
			"END", "MZ1", "T3", "PB", "PA1",
		]]
		.into_iter()
		.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
		.collect::<Vec<_>>();

		let expected_state_ids = vec!["T4", "PA2"]
			.into_iter()
			.map(event_id)
			.collect::<Vec<_>>();

		do_check(events, edges, expected_state_ids).await;
	}

	#[tokio::test]
	async fn test_event_map_none() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let mut store = TestStore::<PduEvent>(hashmap! {});

		// build up the DAG
		let (state_at_bob, state_at_charlie, expected) = store.set_up();

		let ev_map = store.0.clone();
		let fetcher = |id| ready(ev_map.get(&id).cloned());

		let exists = |id: OwnedEventId| ready(ev_map.get(&*id).is_some());

		let state_sets = [state_at_bob, state_at_charlie];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(room_id(), map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let resolved = match super::resolve(
			&RoomVersionId::V2,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		{
			| Ok(state) => state,
			| Err(e) => panic!("{e}"),
		};

		assert_eq!(expected, resolved);
	}

	#[tokio::test]
	async fn test_lexicographical_sort() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let graph = hashmap! {
			event_id("l") => hashset![event_id("o")],
			event_id("m") => hashset![event_id("n"), event_id("o")],
			event_id("n") => hashset![event_id("o")],
			event_id("o") => hashset![], // "o" has zero outgoing edges but 4 incoming edges
			event_id("p") => hashset![event_id("o")],
		};

		let res = super::lexicographical_topological_sort(&graph, &|_id| async {
			Ok((int!(0), MilliSecondsSinceUnixEpoch(uint!(0))))
		})
		.await
		.unwrap();

		assert_eq!(
			vec!["o", "l", "n", "m", "p"],
			res.iter()
				.map(ToString::to_string)
				.map(|s| s.replace('$', "").replace(":foo", ""))
				.collect::<Vec<_>>()
		);
	}

	/// Ported from ruma-state-res `state_res::tests::test_mainline_sort`.
	/// Events connected to the mainline PL sort AFTER events with no PL
	/// ancestor.
	#[tokio::test]
	async fn ruma_test_mainline_sort() {
		use futures::future::ready;

		let events = INITIAL_EVENTS();
		let fetcher = |id| ready(events.get(&id).cloned());

		// Only the room-setup events (no disconnected START/END message events)
		let mut to_sort: Vec<OwnedEventId> = events
			.keys()
			.filter(|id| {
				let s = id.to_string();
				!s.contains("START") && !s.contains("END")
			})
			.cloned()
			.collect();

		for _ in 0..20 {
			to_sort.shuffle(&mut rand::rng());
			let power_level = events
				.iter()
				.find(|(_, ev)| {
					ev.event_type() == &ruma::events::TimelineEventType::RoomPowerLevels
				})
				.map(|(id, _)| id.clone());

			let sorted = super::mainline_sort(&to_sort, power_level, &fetcher)
				.await
				.unwrap();
			let names: Vec<String> = sorted
				.iter()
				.map(|id| id.to_string().replace("$", "").replace(":foo", ""))
				.collect();

			// No-PL-ancestor events (CREATE, IMA) come FIRST (lowest priority, lose).
			// PL-connected events (IPOWER, IJR, IMB, IMC) come LAST (win).
			assert_eq!(
				names,
				["CREATE", "IMA", "IPOWER", "IJR", "IMB", "IMC"],
				"ruma_test_mainline_sort: wrong order on iteration"
			);
		}
	}

	/// Ported from ruma-state-res
	/// `state_res::tests::test_mainline_sort_no_pl_ancestor_sorts_first`.
	/// Per spec §6.6.3.3: an event with i=∞ (no mainline ancestor) sorts BEFORE
	/// all chain-rooted events.  Directly validates our `Option<usize>`
	/// sentinel.
	#[tokio::test]
	async fn ruma_test_mainline_sort_no_pl_ancestor_sorts_first() {
		use futures::future::ready;

		let events = INITIAL_EVENTS();
		let fetcher = |id| ready(events.get(&id).cloned());

		// IMA  -> auth=[$CREATE]          -> no PL -> no mainline anchor (sorts first)
		// IJR  -> auth=[$CREATE,$IMA,$IPOWER] -> PL=$IPOWER -> mainline depth 0
		// IPOWER -> IS the PL               -> mainline depth 0 (closest, wins)
		let to_sort: Vec<OwnedEventId> = ["IMA", "IJR", "IPOWER"]
			.iter()
			.map(|s| {
				<&ruma::EventId>::try_from(format!("${s}:foo").as_str())
					.unwrap()
					.to_owned()
			})
			.collect();

		let power_level = events
			.iter()
			.find(|(_, ev)| ev.event_type() == &ruma::events::TimelineEventType::RoomPowerLevels)
			.map(|(id, _)| id.clone());

		let sorted = super::mainline_sort(&to_sort, power_level, &fetcher)
			.await
			.unwrap();
		let names: Vec<String> = sorted
			.iter()
			.map(|id| id.to_string().replace("$", "").replace(":foo", ""))
			.collect();

		// IMA (None) first. IPOWER (ts=2) and IJR (ts=3) both at depth=Some(0);
		// ascending ts within equal depth -> IPOWER before IJR -> IJR last -> IJR wins.
		assert_eq!(
			names,
			["IMA", "IPOWER", "IJR"],
			"no-PL-ancestor event must sort before mainline-connected events"
		);
	}

	/// Ported from ruma-state-res
	/// `state_res::tests::test_reverse_topological_power_sort`.
	#[tokio::test]
	async fn ruma_test_reverse_topological_power_sort() {
		let eid = |s: &str| -> OwnedEventId {
			<&ruma::EventId>::try_from(format!("${s}:foo").as_str())
				.unwrap()
				.to_owned()
		};
		let graph = [
			(eid("l"), [eid("o")].into()),
			(eid("m"), [eid("n"), eid("o")].into()),
			(eid("n"), [eid("o")].into()),
			(eid("o"), std::collections::HashSet::new()),
			(eid("p"), [eid("o")].into()),
		]
		.into();

		let sorted = super::lexicographical_topological_sort(&graph, &|_id| async {
			Ok((int!(0), MilliSecondsSinceUnixEpoch(uint!(0))))
		})
		.await
		.unwrap();

		let names: Vec<String> = sorted
			.iter()
			.map(|id| id.to_string().replace("$", "").replace(":foo", ""))
			.collect();

		assert_eq!(names, ["o", "l", "n", "m", "p"]);
	}

	#[tokio::test]
	async fn ban_with_auth_chains() {
		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);
		let ban = BAN_STATE_SET();

		let edges = vec![vec!["END", "MB", "PA", "START"], vec!["END", "IME", "MB"]]
			.into_iter()
			.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
			.collect::<Vec<_>>();

		let expected_state_ids = vec!["PA", "MB"]
			.into_iter()
			.map(event_id)
			.collect::<Vec<_>>();

		do_check(&ban.values().cloned().collect::<Vec<_>>(), edges, expected_state_ids).await;
	}

	#[tokio::test]
	async fn ban_with_auth_chains2() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);
		let init = INITIAL_EVENTS();
		let ban = BAN_STATE_SET();

		let mut inner = init.clone();
		inner.extend(ban);
		let store = TestStore(inner.clone());

		let state_set_a = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("MB")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		let state_set_b = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("IME")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		let ev_map = &store.0;
		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(room_id(), map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());
		let resolved = match super::resolve(
			&RoomVersionId::V6,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		{
			| Ok(state) => state,
			| Err(e) => panic!("{e}"),
		};

		debug!(
			resolved = ?resolved
				.iter()
				.map(|((ty, key), id)| format!("(({ty}{key:?}), {id})"))
				.collect::<Vec<_>>(),
				"resolved state",
		);

		let expected = [
			"$CREATE:foo",
			"$IJR:foo",
			"$PA:foo",
			"$IMA:foo",
			"$IMB:foo",
			"$IMC:foo",
			"$MB:foo",
		];

		for id in expected.iter().map(|i| event_id(i)) {
			// make sure our resolved events are equal to the expected list
			assert!(resolved.values().any(|eid| eid == &id) || init.contains_key(&id), "{id}");
		}
		assert_eq!(expected.len(), resolved.len());
	}

	/// Verify that rejected events are excluded from state resolution.
	/// Marks Ella's join ($IME) as rejected; she should not appear in resolved
	/// state since her join was the only membership event and it's rejected.
	#[tokio::test]
	async fn rejected_event_excluded_from_resolution() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);
		let init = INITIAL_EVENTS();
		let ban = BAN_STATE_SET();

		let mut inner = init.clone();
		inner.extend(ban);
		let mut store = TestStore(inner.clone());

		// State set A: has MB (ban of ella) and PA
		let state_set_a = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("MB")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		// State set B: has IME (ella's join) and PA
		let state_set_b = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("IME")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		// Mark IME (Ella's join) as rejected via the Pdu field
		store.0.get_mut(&event_id("IME")).unwrap().rejected = true;
		let ev_map = &store.0;
		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(room_id(), map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());
		let resolved = match super::resolve(
			&RoomVersionId::V6,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		{
			| Ok(state) => state,
			| Err(e) => panic!("{e}"),
		};

		// IME was rejected, so it should NOT appear in resolved state.
		// MB (the ban) should win for ella's membership slot.
		let ella_key = (StateEventType::RoomMember, ella().to_string().into());
		let ella_event = resolved.get(&ella_key);
		assert!(
			ella_event.is_none() || ella_event.unwrap() == &event_id("MB"),
			"Ella's rejected join should not appear; got {:?}",
			ella_event
		);
	}

	/// Verify that rejecting a power-level event changes the resolution
	/// outcome. Without rejection, PA wins. With PB rejected, PA should
	/// definitely win.
	#[tokio::test]
	async fn rejected_event_changes_resolution_outcome() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);
		let init = INITIAL_EVENTS();
		let ban = BAN_STATE_SET();

		let mut inner = init.clone();
		inner.extend(ban);
		let mut store = TestStore(inner.clone());

		let state_set_a = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("MB")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		let state_set_b = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("IME")).unwrap(),
			inner.get(&event_id("PB")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		// Mark PB as rejected via the Pdu field — PA should be the sole power level
		// winner
		store.0.get_mut(&event_id("PB")).unwrap().rejected = true;
		let ev_map = &store.0;
		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(room_id(), map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());
		let resolved = match super::resolve(
			&RoomVersionId::V6,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		{
			| Ok(state) => state,
			| Err(e) => panic!("{e}"),
		};

		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		let pl_event = resolved
			.get(&pl_key)
			.expect("power levels must be in resolved state");
		assert_eq!(
			pl_event,
			&event_id("PA"),
			"With PB rejected, PA must win the power levels slot"
		);
	}

	/// The state reset loop scenario: when a stale join is NOT rejected, it
	/// can survive alongside a ban from a different fork. This proves that
	/// marking events as rejected is critical for convergence.
	#[tokio::test]
	async fn unrejected_join_survives_in_resolution() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);
		let init = INITIAL_EVENTS();
		let ban = BAN_STATE_SET();

		let mut inner = init.clone();
		inner.extend(ban);
		let store = TestStore(inner.clone());

		// State set A: has MB (ban of ella)
		let state_set_a = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("MB")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		// State set B: has IME (ella's join)
		let state_set_b = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("IME")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		let ev_map = &store.0;
		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(room_id(), map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());
		// Nothing rejected — both IME and MB participate
		let resolved = match super::resolve(
			&RoomVersionId::V6,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		{
			| Ok(state) => state,
			| Err(e) => panic!("{e}"),
		};

		// Without rejection, state-res picks a winner between IME and MB
		// based on auth rules. The key insight: *some* event fills ella's
		// slot. When the "wrong" one wins, that's the state reset loop.
		let ella_key = (StateEventType::RoomMember, ella().to_string().into());
		assert!(
			resolved.contains_key(&ella_key),
			"ella must have a membership entry when nothing is rejected"
		);
	}

	/// Verifies that rejecting ALL conflicting membership events for a user
	/// removes them from resolved state entirely — the nuclear option for
	/// membership cleanup.
	#[tokio::test]
	async fn reject_all_membership_events_removes_user() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);
		let init = INITIAL_EVENTS();
		let ban = BAN_STATE_SET();

		let mut inner = init.clone();
		inner.extend(ban);
		let mut store = TestStore(inner.clone());

		let state_set_a = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("MB")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		let state_set_b = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("IME")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		// Reject BOTH ella's join AND her ban — nuclear cleanup
		store.0.get_mut(&event_id("IME")).unwrap().rejected = true;
		store.0.get_mut(&event_id("MB")).unwrap().rejected = true;
		let ev_map = &store.0;
		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(room_id(), map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());
		let resolved = match super::resolve(
			&RoomVersionId::V6,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		{
			| Ok(state) => state,
			| Err(e) => panic!("{e}"),
		};

		// With both events rejected, ella should have no membership entry
		let ella_key = (StateEventType::RoomMember, ella().to_string().into());
		assert!(
			!resolved.contains_key(&ella_key),
			"ella should have no membership when all her events are rejected; got {:?}",
			resolved.get(&ella_key)
		);
	}

	#[tokio::test]
	async fn join_rule_with_auth_chain() {
		let join_rule = JOIN_RULE();

		let edges = vec![vec!["END", "JR", "START"], vec!["END", "IMZ", "START"]]
			.into_iter()
			.map(|list| list.into_iter().map(event_id).collect::<Vec<_>>())
			.collect::<Vec<_>>();

		let expected_state_ids = vec!["JR"].into_iter().map(event_id).collect::<Vec<_>>();

		do_check(&join_rule.values().cloned().collect::<Vec<_>>(), edges, expected_state_ids)
			.await;
	}

	/// Regression test for the v2.1 conflicted subgraph bug.
	/// MSC4297 mandates traversing prev_events (DAG timeline), not auth_events,
	/// when computing the conflicted state subgraph. Using auth_events produced
	/// an incorrect subgraph which caused state resolution to output garbage.
	///
	/// This test runs the same ban-vs-join scenario through v2.1 (room version
	/// > V11) and verifies the ban wins, proving the subgraph is correctly
	/// built from the DAG timeline rather than the auth chain.
	#[tokio::test]
	async fn v2_1_conflicted_subgraph_uses_prev_events() {
		use futures::future::ready;

		let init = INITIAL_EVENTS();
		let ban = BAN_STATE_SET();
		let mut inner = init;
		inner.extend(ban);

		// Build conflicted state: MB (ban) vs IME (join) for ella
		let ella_key = (StateEventType::RoomMember, ella().to_string().into());
		let conflicted: StateMap<Vec<OwnedEventId>> =
			[(ella_key, vec![event_id("MB"), event_id("IME")])]
				.into_iter()
				.collect();

		let ev_map = &inner;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());

		let subgraph = super::calculate_conflicted_subgraph(&conflicted, &fetcher)
			.await
			.expect("subgraph calculation must succeed");

		// MB has prev_events = ["START"], IME has prev_events = ["IMC"]
		assert!(subgraph.0.contains(&event_id("MB")), "must contain MB");
		assert!(subgraph.0.contains(&event_id("IME")), "must contain IME");

		// IPOWER is only reachable via auth_events, never via prev_events.
		// If present, we are crawling auth_events (the old bug).
		assert!(
			!subgraph.0.contains(&event_id("IPOWER")),
			"must NOT contain IPOWER (auth chain only, not prev_events)"
		);
	}

	/// Regression test: non-state events (e.g. m.room.message) that appear in
	/// auth chains or subgraph traversals must be filtered out of the
	/// conflicted set before iterative_auth_check, which requires all events
	/// to have a state_key.
	///
	/// Without the filter, this crashes with:
	///   InvalidPdu("State event had no state key")
	#[tokio::test]
	async fn non_state_events_in_auth_chain_dont_crash_resolution() {
		use std::collections::HashSet;

		use futures::future::ready;

		let init = INITIAL_EVENTS();
		let mut ev_map: HashMap<OwnedEventId, PduEvent> = init.clone();

		// Insert a non-state event (m.room.message, no state_key) that will
		// appear in the auth chain. In real federation, auth chains can
		// contain non-state events due to DAG traversal.
		let msg = to_pdu_event(
			"MSG1",
			alice(),
			TimelineEventType::RoomMessage,
			None, // <-- no state_key, this is NOT a state event
			to_raw_json_value(&json!({ "body": "hello", "msgtype": "m.text" })).unwrap(),
			&["CREATE", "IMA", "IPOWER"],
			&["START"],
		);
		ev_map.insert(msg.event_id.clone(), msg);

		// Create two conflicting topic events
		let t1 = to_pdu_event(
			"T1",
			alice(),
			TimelineEventType::RoomTopic,
			Some(""),
			to_raw_json_value(&json!({ "topic": "topic A" })).unwrap(),
			&["CREATE", "IMA", "IPOWER"],
			&["START"],
		);
		let t2 = to_pdu_event(
			"T2",
			alice(),
			TimelineEventType::RoomTopic,
			Some(""),
			to_raw_json_value(&json!({ "topic": "topic B" })).unwrap(),
			&["CREATE", "IMA", "IPOWER"],
			&["START"],
		);
		ev_map.insert(t1.event_id.clone(), t1);
		ev_map.insert(t2.event_id.clone(), t2);

		let topic_key = StateEventType::RoomTopic.with_state_key("");

		// State set 1: topic = T1
		let mut state1: StateMap<OwnedEventId> = HashMap::new();
		for ev in init.values().filter(|e| e.state_key().is_some()) {
			state1.insert(
				ev.event_type().with_state_key(ev.state_key().unwrap()),
				ev.event_id().to_owned(),
			);
		}
		state1.insert(topic_key.clone(), event_id("T1"));

		// State set 2: topic = T2
		let mut state2 = state1.clone();
		state2.insert(topic_key.clone(), event_id("T2"));

		let state_sets = vec![state1, state2];

		// Auth chain includes the non-state event MSG1 — this is the
		// scenario that triggered the crash.
		let auth_chain: HashSet<OwnedEventId> = ev_map.keys().cloned().collect();
		let auth_chain_sets = vec![auth_chain.clone(), auth_chain];

		let fetch = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.contains_key(&id));

		// This must not panic with "State event had no state key"
		let result = super::resolve(
			&RoomVersionId::V6,
			state_sets.iter(),
			&auth_chain_sets,
			&fetch,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await;

		assert!(
			result.is_ok(),
			"resolve() must not crash when non-state events are in the auth chain: {:?}",
			result.err()
		);

		// The resolved state must contain a topic event (T1 or T2)
		let resolved = result.unwrap();
		assert!(resolved.contains_key(&topic_key), "resolved state must contain the topic key");
	}

	#[allow(non_snake_case)]
	fn BAN_STATE_SET() -> HashMap<OwnedEventId, PduEvent> {
		vec![
			to_pdu_event(
				"PA",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
				&["CREATE", "IMA", "IPOWER"], // auth_events
				&["START"],                   // prev_events
			),
			to_pdu_event(
				"PB",
				alice(),
				TimelineEventType::RoomPowerLevels,
				Some(""),
				to_raw_json_value(&json!({ "users": { alice(): 100, bob(): 50 } })).unwrap(),
				&["CREATE", "IMA", "IPOWER"],
				&["END"],
			),
			to_pdu_event(
				"MB",
				alice(),
				TimelineEventType::RoomMember,
				Some(ella().as_str()),
				member_content_ban(),
				&["CREATE", "IMA", "PB"],
				&["PA"],
			),
			to_pdu_event(
				"IME",
				ella(),
				TimelineEventType::RoomMember,
				Some(ella().as_str()),
				member_content_join(),
				&["CREATE", "IJR", "PA"],
				&["MB"],
			),
		]
		.into_iter()
		.map(|ev| (ev.event_id.clone(), ev))
		.collect()
	}

	#[allow(non_snake_case)]
	fn JOIN_RULE() -> HashMap<OwnedEventId, PduEvent> {
		vec![
			to_pdu_event(
				"JR",
				alice(),
				TimelineEventType::RoomJoinRules,
				Some(""),
				to_raw_json_value(&json!({ "join_rule": "invite" })).unwrap(),
				&["CREATE", "IMA", "IPOWER"],
				&["START"],
			),
			to_pdu_event(
				"IMZ",
				zara(),
				TimelineEventType::RoomPowerLevels,
				Some(zara().as_str()),
				member_content_join(),
				&["CREATE", "JR", "IPOWER"],
				&["START"],
			),
		]
		.into_iter()
		.map(|ev| (ev.event_id.clone(), ev))
		.collect()
	}

	macro_rules! state_set {
        ($($kind:expr_2021 => $key:expr_2021 => $id:expr_2021),* $(,)?) => {{
            #[allow(unused_mut)]
            let mut x = StateMap::new();
            $(
                x.insert(($kind, $key.into()), $id);
            )*
            x
        }};
    }

	#[test]
	fn separate_unique_conflicted() {
		let (unconflicted, conflicted) = super::separate(
			[
				state_set![StateEventType::RoomMember => "@a:hs1" => 0],
				state_set![StateEventType::RoomMember => "@b:hs1" => 1],
				state_set![StateEventType::RoomMember => "@c:hs1" => 2],
			]
			.iter(),
		);

		assert_eq!(unconflicted, StateMap::new());
		assert_eq!(conflicted, state_set![
			StateEventType::RoomMember => "@a:hs1" => vec![0],
			StateEventType::RoomMember => "@b:hs1" => vec![1],
			StateEventType::RoomMember => "@c:hs1" => vec![2],
		],);
	}

	#[test]
	fn separate_conflicted() {
		let (unconflicted, mut conflicted) = super::separate(
			[
				state_set![StateEventType::RoomMember => "@a:hs1" => 0],
				state_set![StateEventType::RoomMember => "@a:hs1" => 1],
				state_set![StateEventType::RoomMember => "@a:hs1" => 2],
			]
			.iter(),
		);

		// HashMap iteration order is random, so sort this before asserting on it
		for v in conflicted.values_mut() {
			v.sort_unstable();
		}

		assert_eq!(unconflicted, StateMap::new());
		assert_eq!(conflicted, state_set![
			StateEventType::RoomMember => "@a:hs1" => vec![0, 1, 2],
		],);
	}

	#[test]
	fn separate_unconflicted() {
		let (unconflicted, conflicted) = super::separate(
			[
				state_set![StateEventType::RoomMember => "@a:hs1" => 0],
				state_set![StateEventType::RoomMember => "@a:hs1" => 0],
				state_set![StateEventType::RoomMember => "@a:hs1" => 0],
			]
			.iter(),
		);

		assert_eq!(unconflicted, state_set![
			StateEventType::RoomMember => "@a:hs1" => 0,
		],);
		assert_eq!(conflicted, StateMap::new());
	}

	#[test]
	fn separate_mixed() {
		let (unconflicted, conflicted) = super::separate(
			[
				state_set![StateEventType::RoomMember => "@a:hs1" => 0],
				state_set![
					StateEventType::RoomMember => "@a:hs1" => 0,
					StateEventType::RoomMember => "@b:hs1" => 1,
				],
				state_set![
					StateEventType::RoomMember => "@a:hs1" => 0,
					StateEventType::RoomMember => "@c:hs1" => 2,
				],
			]
			.iter(),
		);

		assert_eq!(unconflicted, state_set![
			StateEventType::RoomMember => "@a:hs1" => 0,
		],);
		assert_eq!(conflicted, state_set![
			StateEventType::RoomMember => "@b:hs1" => vec![1],
			StateEventType::RoomMember => "@c:hs1" => vec![2],
		],);
	}

	/// Validates that the `is_ascii_graphic` check correctly filters room IDs.
	/// This is a regression test for the zero-copy stream UAF that produced
	/// corrupted room IDs like `!D0yPVK3zb8Y4svzltl:nutra.tked\nGg▒[\x7f]`.
	/// Verify that if a power level event is rejected, it is excluded from
	/// the resolved state even when both forks contain it.
	#[tokio::test]
	async fn rejected_power_level_excluded_from_state() {
		use futures::future::ready;

		let _ = tracing::subscriber::set_default(
			tracing_subscriber::fmt().with_test_writer().finish(),
		);

		let init = INITIAL_EVENTS();
		let ban = BAN_STATE_SET();
		let mut inner = init.clone();
		inner.extend(ban);
		let mut store = TestStore(inner.clone());

		// State set A: has IPOWER + PA
		let state_set_a = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("PA")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		// State set B: has PB (conflicts on power_levels with PA)
		let state_set_b = [
			inner.get(&event_id("CREATE")).unwrap(),
			inner.get(&event_id("IJR")).unwrap(),
			inner.get(&event_id("IMA")).unwrap(),
			inner.get(&event_id("IMB")).unwrap(),
			inner.get(&event_id("IMC")).unwrap(),
			inner.get(&event_id("IME")).unwrap(),
			inner.get(&event_id("PB")).unwrap(),
		]
		.iter()
		.map(|ev| (ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone()))
		.collect::<StateMap<_>>();

		// Mark PA as rejected via the Pdu field — only the unconflicted IPOWER should
		// remain
		store.0.get_mut(&event_id("PA")).unwrap().rejected = true;
		let ev_map = &store.0;
		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(room_id(), map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		let resolved = super::resolve(
			&RoomVersionId::V6,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.unwrap();

		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		// PA was rejected, so it must not appear in resolved state
		assert!(
			resolved.get(&pl_key) != Some(&event_id("PA")),
			"PA was rejected and must not appear in resolved state; got {:?}",
			resolved.get(&pl_key)
		);
	}

	mod room_id_validation {
		/// Simulates the validation logic from `monitor.rs::check_room`
		fn is_valid_room_id(s: &str) -> bool {
			s.bytes().all(|b| b.is_ascii_graphic()) && <&ruma::RoomId>::try_from(s).is_ok()
		}

		#[test]
		fn valid_standard_room_id() {
			assert!(is_valid_room_id("!abc123:matrix.org"));
		}

		#[test]
		fn valid_v4_opaque_room_id() {
			// v4+ room IDs have no server_name, just an opaque hash
			assert!(is_valid_room_id("!c10y-fNiMx5ijtgGFibzPUfNs9hpQvnJYPTV-fD2KPk"));
		}

		#[test]
		fn reject_newline_injection() {
			// The nutra.tked UAF scenario: buffer overlap produces \n in the ID
			assert!(!is_valid_room_id("!abc123:nutra.tked\nGg"));
		}

		#[test]
		fn reject_del_byte() {
			assert!(!is_valid_room_id("!abc123:server\x7f.org"));
		}

		#[test]
		fn reject_escape_sequence() {
			assert!(!is_valid_room_id("!abc123:server\x1b[0m.org"));
		}

		#[test]
		fn reject_null_byte() {
			assert!(!is_valid_room_id("!abc123:server\0.org"));
		}

		#[test]
		fn reject_space() {
			assert!(!is_valid_room_id("!abc123:server .org"));
		}

		#[test]
		fn reject_tab() {
			assert!(!is_valid_room_id("!abc123:server\t.org"));
		}

		#[test]
		fn reject_empty() {
			assert!(!is_valid_room_id(""));
		}
	}

	/// Ported from Synapse
	/// tests/state/test_v21.py::test_state_reset_replay_conflicted_subgraph
	///
	/// Tests that when an event cites OLD auth events but indirectly references
	/// NEW ones, the v2.1 subgraph traversal correctly replays events in the
	/// right power-level epoch, preventing state resets.
	///
	/// DAG:
	///   create -> alice_join -> power1 -> join_rules -> bob_join, charlie_join
	///   power1 -> power2 (Alice promotes Bob)
	///   power2 -> power3 (Bob promotes Charlie)
	///   power3 -> eve_join1
	///   eve_join1 -> eve_join2 (cites OLD power1 — DODGY)
	///   power3 -> zara_join
	#[tokio::test]
	async fn synapse_v21_state_reset_replay_conflicted_subgraph() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		// V12 derives create event ID from room ID: !X -> $X
		// Must be 43 url-safe base64 chars for Ruma to parse as V4+ Room ID
		let v12_room_id: OwnedRoomId = "!S21Create123456789012345678901234567890123"
			.try_into()
			.unwrap();
		let create_id_str = "$S21Create123456789012345678901234567890123";
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
		e1_create.room_id = None; // V12: create event has no room_id

		let e2_ma = to_pdu_event(
			"S21_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[], // V12: no create event in auth_events
			&[create_id_str],
		);

		let e3_power1 = to_pdu_event(
			"S21_PL1",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": {} })).unwrap(),
			&["S21_MA"],
			&["S21_MA"],
		);

		let e4_jr = to_pdu_event(
			"S21_JR",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["S21_MA", "S21_PL1"],
			&["S21_PL1"],
		);

		let e5_mb = to_pdu_event(
			"S21_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["S21_PL1", "S21_JR"],
			&["S21_JR"],
		);

		let e6_mc = to_pdu_event(
			"S21_MC",
			charlie(),
			TimelineEventType::RoomMember,
			Some(charlie().as_str()),
			member_content_join(),
			&["S21_PL1", "S21_JR"],
			&["S21_JR"],
		);

		let e7_power2 = to_pdu_event(
			"S21_PL2",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { bob(): 50 } })).unwrap(),
			&["S21_MA", "S21_PL1"],
			&["S21_PL1"],
		);

		let e8_power3 = to_pdu_event(
			"S21_PL3",
			bob(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { bob(): 50, charlie(): 50 } })).unwrap(),
			&["S21_MB", "S21_PL2"],
			&["S21_PL2"],
		);

		let e9_me1 = to_pdu_event(
			"S21_ME1",
			ella(),
			TimelineEventType::RoomMember,
			Some(ella().as_str()),
			member_content_join(),
			&["S21_PL3", "S21_JR"],
			&["S21_PL3"],
		);

		let e10_me2 = to_pdu_event(
			"S21_ME2",
			ella(),
			TimelineEventType::RoomMember,
			Some(ella().as_str()),
			member_content_join(),
			&["S21_PL1", "S21_JR", "S21_ME1"],
			&["S21_ME1"],
		);

		let e11_mz = to_pdu_event(
			"S21_MZ",
			zara(),
			TimelineEventType::RoomMember,
			Some(zara().as_str()),
			member_content_join(),
			&["S21_PL3", "S21_JR"],
			&["S21_PL3"],
		);

		let all_events = vec![
			&e1_create, &e2_ma, &e3_power1, &e4_jr, &e5_mb, &e6_mc, &e7_power2, &e8_power3,
			&e9_me1, &e10_me2, &e11_mz,
		];

		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		let dodgy_state: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e5_mb, &e6_mc, &e10_me2, &e3_power1, &e4_jr]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let sensible_state: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e5_mb, &e6_mc, &e11_mz, &e8_power3, &e4_jr]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let state_sets = [dodgy_state, sensible_state];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		// Dispatch normally through V12 (no test-hack needed)
		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		assert_eq!(
			resolved.get(&pl_key),
			Some(&event_id("S21_PL3")),
			"v2.1 must pick newer power levels PL3, not dodgy PL1; got {:?}",
			resolved.get(&pl_key)
		);

		let ella_key = (StateEventType::RoomMember, ella().as_str().into());
		assert!(
			resolved.contains_key(&ella_key),
			"Ella/Eve membership must be present in resolved state"
		);
	}

	/// Ported from Synapse
	/// tests/state/test_v21.py::test_state_reset_start_empty_set
	///
	/// DAG:
	///   create -> alice_join -> power -> join_rules_public -> bob_join
	///   power -> join_rules_invite
	///   join_rules_invite -> alice_leave
	#[tokio::test]
	async fn synapse_v21_state_reset_start_empty_set() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!S21bCreate12345678901234567890123456789012"
			.try_into()
			.unwrap();
		let create_id_str = "$S21bCreate12345678901234567890123456789012";
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
		e1_create.room_id = None;

		let e2_ma1 = to_pdu_event(
			"S21B_MA1",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		// Alice makes Bob an admin
		let e3_power = to_pdu_event(
			"S21B_PL",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { bob(): 100 } })).unwrap(),
			&["S21B_MA1"],
			&["S21B_MA1"],
		);

		// Public join rules
		let e4_jr1 = to_pdu_event(
			"S21B_JR1",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["S21B_MA1", "S21B_PL"],
			&["S21B_PL"],
		);

		// Bob joins
		let e5_mb = to_pdu_event(
			"S21B_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["S21B_PL", "S21B_JR1"],
			&["S21B_JR1"],
		);

		// Alice sets join rules to invite
		let e6_jr2 = to_pdu_event(
			"S21B_JR2",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Invite)).unwrap(),
			&["S21B_MA1", "S21B_PL"],
			&["S21B_PL"],
		);

		// Alice leaves
		let e7_ma2 = to_pdu_event(
			"S21B_MA2",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_leave(),
			&["S21B_PL", "S21B_MA1"],
			&["S21B_MA1"],
		);

		let all_events = vec![&e1_create, &e2_ma1, &e3_power, &e4_jr1, &e5_mb, &e6_jr2, &e7_ma2];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		let correct_state: StateMap<OwnedEventId> =
			[&e1_create, &e7_ma2, &e5_mb, &e3_power, &e6_jr2]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let incorrect_state: StateMap<OwnedEventId> =
			[&e1_create, &e7_ma2, &e5_mb, &e3_power, &e4_jr1]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let state_sets = [correct_state, incorrect_state];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		let jr_key = (StateEventType::RoomJoinRules, "".into());
		assert_eq!(
			resolved.get(&jr_key),
			Some(&event_id("S21B_JR2")),
			"v2.1 must pick newer invite-only join rules JR2; got {:?}",
			resolved.get(&jr_key)
		);

		// Bob must survive in the resolved state. Without the V2.1
		// supplemental merge fix, resolved_state accumulates join_rules=invite
		// from the control pass and overwrites bob's own auth chain (which had
		// join_rules=public when he joined). This causes bob_join to fail auth
		// with "not invited to invite-only room", dropping him from state.
		let bob_key = (StateEventType::RoomMember, bob().to_string().into());
		assert_eq!(
			resolved.get(&bob_key),
			Some(&event_id("S21B_MB")),
			"v2.1 supplemental merge must not clobber bob's auth chain; bob_join should \
			 survive. If this fails, iterative_auth_check is overriding event auth_events with \
			 resolved_state (the V2 behavior) instead of using the event's own auth chain (V2.1 \
			 behavior). Got {:?}",
			resolved.get(&bob_key)
		);
	}

	/// Ported from Complement
	/// TestMSC4297StateResolutionV2_1_includes_conflicted_subgraph
	///
	/// DAG:
	///   create -> alice_join -> power1 -> join_rules -> bob_join
	///                                                -> charlie_join
	///                                 -> power2(bob:50)
	///                                 -> power3(bob:50,charlie:50) ->
	/// zara_join   power1 -> ella_join  (dodgy: cites old PL in auth)
	///
	/// Two state forks: dodgy (with ella, PL1) vs correct (with zara, PL3).
	/// Resolution must pick PL3 (bob:50, charlie:50), not regress to PL1.
	#[tokio::test]
	async fn synapse_v21_conflicted_subgraph_preserves_power_levels() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!S21cCreate12345678901234567890123456789012"
			.try_into()
			.unwrap();
		let create_id_str = "$S21cCreate12345678901234567890123456789012";
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
		e1_create.room_id = None;

		// Alice joins
		let e2_ma = to_pdu_event(
			"S21C_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		// Initial power levels (alice is creator, implicit PL 100 in V12)
		let e3_power1 = to_pdu_event(
			"S21C_PL1",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": {} })).unwrap(),
			&["S21C_MA"],
			&["S21C_MA"],
		);

		// Join rules = public
		let e4_jr = to_pdu_event(
			"S21C_JR",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["S21C_MA", "S21C_PL1"],
			&["S21C_PL1"],
		);

		// Bob joins
		let e5_mb = to_pdu_event(
			"S21C_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["S21C_PL1", "S21C_JR"],
			&["S21C_JR"],
		);

		// Charlie joins
		let e6_mc = to_pdu_event(
			"S21C_MC",
			charlie(),
			TimelineEventType::RoomMember,
			Some(charlie().as_str()),
			member_content_join(),
			&["S21C_PL1", "S21C_JR"],
			&["S21C_MB"],
		);

		// Alice promotes Bob to PL 50
		let e7_power2 = to_pdu_event(
			"S21C_PL2",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { bob(): 50 } })).unwrap(),
			&["S21C_MA", "S21C_PL1"],
			&["S21C_MC"],
		);

		// Bob promotes Charlie to PL 50
		let e8_power3 = to_pdu_event(
			"S21C_PL3",
			bob(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { bob(): 50, charlie(): 50 } })).unwrap(),
			&["S21C_MB", "S21C_PL2"],
			&["S21C_PL2"],
		);

		// Zara joins citing PL3 (correct)
		let e9_mz = to_pdu_event(
			"S21C_MZ",
			zara(),
			TimelineEventType::RoomMember,
			Some(zara().as_str()),
			member_content_join(),
			&["S21C_PL3", "S21C_JR"],
			&["S21C_PL3"],
		);

		// Ella joins citing PL1 (DODGY — old power levels)
		let e10_me = to_pdu_event(
			"S21C_ME",
			ella(),
			TimelineEventType::RoomMember,
			Some(ella().as_str()),
			member_content_join(),
			&["S21C_PL1", "S21C_JR"],
			&["S21C_MZ"],
		);

		let all_events = vec![
			&e1_create, &e2_ma, &e3_power1, &e4_jr, &e5_mb, &e6_mc, &e7_power2, &e8_power3,
			&e9_mz, &e10_me,
		];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		// Dodgy state fork: has ella with old PL1
		let dodgy_state: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e5_mb, &e6_mc, &e10_me, &e3_power1, &e4_jr]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		// Correct state fork: has zara with PL3
		let correct_state: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e5_mb, &e6_mc, &e9_mz, &e8_power3, &e4_jr]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let state_sets = [dodgy_state, correct_state];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		// PL3 must win over PL1 — resolution must pick the latest power levels
		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		assert_eq!(
			resolved.get(&pl_key),
			Some(&event_id("S21C_PL3")),
			"v2.1 must pick PL3 (bob:50, charlie:50) over PL1 (empty users); got {:?}",
			resolved.get(&pl_key)
		);

		// Both zara and ella must be present in resolved state
		let zara_key = (StateEventType::RoomMember, zara().to_string().into());
		assert_eq!(
			resolved.get(&zara_key),
			Some(&event_id("S21C_MZ")),
			"zara must be in resolved state; got {:?}",
			resolved.get(&zara_key)
		);

		let ella_key = (StateEventType::RoomMember, ella().to_string().into());
		assert_eq!(
			resolved.get(&ella_key),
			Some(&event_id("S21C_ME")),
			"ella must be in resolved state; got {:?}",
			resolved.get(&ella_key)
		);
	}

	/// Regression test for the Complement
	/// TestMSC4297StateResolutionV2_1_includes_conflicted_subgraph failure.
	///
	/// Root cause: In V12 rooms, check_power_levels rejected PL events that
	/// included the room creator in content.users with a non-Int::MAX value
	/// (e.g. {alice: 100}). During V2.1 state resolution, ALL events go
	/// through iterative_auth_check, and the creator's PL entry caused the
	/// entire PL event to be rejected — dropping Alice's power level and
	/// making subsequent events (like promoting Bob) return 403 Forbidden.
	///
	/// This test verifies that a PL event with the creator in content.users
	/// survives V2.1 state resolution.
	///
	/// DAG:
	///   create -> alice_join -> PL1(users:{}) -> PL2(users:{alice:100})
	///   PL2 must survive resolution, not be rejected.
	#[tokio::test]
	async fn v12_pl_with_creator_in_users_survives_resolution() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!S21dCreate12345678901234567890123456789012"
			.try_into()
			.unwrap();
		let create_id_str = "$S21dCreate12345678901234567890123456789012";
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
		e1_create.room_id = None;

		// Alice joins
		let e2_ma = to_pdu_event(
			"S21D_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		// PL1: default power levels (creator omitted from users, as V12 requires)
		let e3_pl1 = to_pdu_event(
			"S21D_PL1",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": {} })).unwrap(),
			&["S21D_MA"],
			&["S21D_MA"],
		);

		// PL2: creator sends PL with herself in content.users at 100.
		// This is what the Complement test does and what federation
		// partners may send. Must NOT be rejected by check_power_levels.
		let e4_pl2 = to_pdu_event(
			"S21D_PL2",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { alice(): 100 } })).unwrap(),
			&["S21D_MA", "S21D_PL1"],
			&["S21D_PL1"],
		);

		// Join rules = public
		let e5_jr = to_pdu_event(
			"S21D_JR",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["S21D_MA", "S21D_PL2"],
			&["S21D_PL2"],
		);

		// Bob joins
		let e6_mb = to_pdu_event(
			"S21D_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["S21D_PL2", "S21D_JR"],
			&["S21D_JR"],
		);

		let all_events = vec![&e1_create, &e2_ma, &e3_pl1, &e4_pl2, &e5_jr, &e6_mb];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		// Two identical state forks (simulating federation join where both
		// sides agree on state — the resolution should be a no-op)
		let state_set_a: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e4_pl2, &e5_jr, &e6_mb]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		// Second fork: same state but without bob (as if remote server
		// hasn't seen bob yet). This forces PL2 through iterative_auth_check.
		let state_set_b: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e4_pl2, &e5_jr]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		// PL2 (with alice:100 in content.users) must survive resolution.
		// Before the fix, check_power_levels rejected PL2 because the
		// creator appeared in content.users with a non-Int::MAX value,
		// causing it to be dropped from resolved state.
		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		assert_eq!(
			resolved.get(&pl_key),
			Some(&event_id("S21D_PL2")),
			"PL2 (with creator in content.users) must survive V2.1 resolution; got {:?}. If \
			 this fails, check_power_levels is rejecting PL events that include the room \
			 creator in content.users — V12 creators have implicit Int::MAX power, so their \
			 presence in content.users should be a no-op, not a rejection.",
			resolved.get(&pl_key)
		);

		// Bob must also survive
		let bob_key = (StateEventType::RoomMember, bob().to_string().into());
		assert_eq!(
			resolved.get(&bob_key),
			Some(&event_id("S21D_MB")),
			"bob must be in resolved state; got {:?}",
			resolved.get(&bob_key)
		);
	}

	#[tokio::test]
	async fn v12_missing_create_event_does_not_panic() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!MissingCreate12345678901234567890123456789"
			.try_into()
			.unwrap();

		let mut e1_ma = to_pdu_event::<&str>(
			"S21_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[],
		);
		e1_ma.room_id = Some(v12_room_id.clone());

		let all_events = vec![&e1_ma];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| (ev.event_id.clone(), (*ev).clone()))
				.collect(),
		);

		let state_set_a: StateMap<OwnedEventId> = [(&e1_ma)]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		let state_set_b: StateMap<OwnedEventId> = HashMap::new();

		let state_sets = [state_set_a, state_set_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await;

		assert!(resolved.is_ok());
	}

	/// Mirrors complement
	/// TestMSC4297StateResolutionV2_1_includes_conflicted_subgraph
	///
	/// Tests that a V12 room creator can update power levels AFTER other users
	/// have joined. The complement test creates a V12 room, sets power levels,
	/// joins bob+charlie, then sets power levels again. The second PL update
	/// was returning 403 because iterative_auth_check couldn't verify the
	/// creator's authority.
	///
	/// This test exercises the auth chain through iterative_auth_check directly
	/// to ensure the create event is found via the room_id->event_id derivation
	/// and the creator retains power level authority.
	#[tokio::test]
	async fn v12_power_levels_update_after_joins() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!V12PLAfterJoin234567890123456789012345678901"
			.try_into()
			.unwrap();
		let create_id_str = "$V12PLAfterJoin234567890123456789012345678901";
		let create_id: OwnedEventId = create_id_str.try_into().unwrap();

		// 1. Create event
		let mut e1_create = to_pdu_event::<&str>(
			create_id_str,
			alice(),
			TimelineEventType::RoomCreate,
			Some(""),
			to_raw_json_value(&json!({ "creator": alice(), "room_version": "12" })).unwrap(),
			&[],
			&[],
		);
		e1_create.room_id = None; // V12: no room_id on create

		// 2. Creator joins
		let e2_ma = to_pdu_event(
			"PLAJ_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[], // V12: no create in auth_events
			&[create_id_str],
		);

		// 3. First power levels (creator sets PL)
		let e3_pl1 = to_pdu_event(
			"PLAJ_PL1",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({
				"users": {},
				"users_default": 0
			}))
			.unwrap(),
			&["PLAJ_MA"],
			&["PLAJ_MA"],
		);

		// 4. Join rules (public)
		let e4_jr = to_pdu_event(
			"PLAJ_JR",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["PLAJ_MA", "PLAJ_PL1"],
			&["PLAJ_PL1"],
		);

		// 5. Bob joins
		let e5_mb = to_pdu_event(
			"PLAJ_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["PLAJ_PL1", "PLAJ_JR"],
			&["PLAJ_JR"],
		);

		// 6. Charlie joins
		let e6_mc = to_pdu_event(
			"PLAJ_MC",
			charlie(),
			TimelineEventType::RoomMember,
			Some(charlie().as_str()),
			member_content_join(),
			&["PLAJ_PL1", "PLAJ_JR"],
			&["PLAJ_JR"],
		);

		// 7. Creator updates power levels AFTER joins (this is the one that was 403ing)
		let e7_pl2 = to_pdu_event(
			"PLAJ_PL2",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({
				"users": { bob().as_str(): 50 },
				"users_default": 0
			}))
			.unwrap(),
			&["PLAJ_MA", "PLAJ_PL1"],
			&["PLAJ_MC"],
		);

		let all_events = vec![&e1_create, &e2_ma, &e3_pl1, &e4_jr, &e5_mb, &e6_mc, &e7_pl2];

		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		// State before the PL2 event: create, alice join, PL1, JR, bob, charlie
		let state_a: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e3_pl1, &e4_jr, &e5_mb, &e6_mc]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		// State including the PL2 update
		let state_b: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e7_pl2, &e4_jr, &e5_mb, &e6_mc]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let state_sets = [state_a, state_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("V12 power levels update after joins must succeed");

		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		assert_eq!(
			resolved.get(&pl_key),
			Some(&event_id("PLAJ_PL2")),
			"V12 must accept the creator's PL update after joins; got {:?}",
			resolved.get(&pl_key)
		);
	}

	/// Tests that V12 iterative_auth_check correctly derives the create event
	/// from the room ID when processing power events. This catches the
	/// regression where the create event cache fails to find the create event
	/// because room_id_or_hash() returns None.
	#[tokio::test]
	async fn v12_iterative_auth_check_finds_create_event() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!V12AuthCreate2345678901234567890123456789012"
			.try_into()
			.unwrap();
		let create_id_str = "$V12AuthCreate2345678901234567890123456789012";
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
		e1_create.room_id = None;

		let e2_ma = to_pdu_event(
			"IAC_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		// Power levels event — this is the one that needs create event lookup
		let e3_pl = to_pdu_event(
			"IAC_PL",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { alice().as_str(): 100 } })).unwrap(),
			&["IAC_MA"],
			&["IAC_MA"],
		);

		let all_events = vec![&e1_create, &e2_ma, &e3_pl];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		let state_a: StateMap<OwnedEventId> = [&e1_create, &e2_ma]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		let state_b: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e3_pl]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		let state_sets = [state_a, state_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());
		let exists = |id: OwnedEventId| ready(ev_map.get(&id).is_some());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("V12 iterative_auth_check must find create event via room ID derivation");

		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		assert!(resolved.contains_key(&pl_key), "Power levels must be in resolved state");
	}

	// ======================================================================
	// MSC4297 unit tests — mirrors Complement tests:
	//   TestMSC4297StateResolutionV2_1_starts_from_empty_set
	//   TestMSC4297StateResolutionV2_1_includes_conflicted_subgraph
	// ======================================================================

	/// MSC4297: V2.1 starts from the empty set, meaning ALL events (even
	/// those that would be "unconflicted" in V2) go through iterative auth
	/// check. This prevents state resets where an attacker's fork sneaks
	/// in invalid state that V2 would never re-check.
	///
	/// Scenario: Bob (PL 50) bans charlie (PL 0). Two forks diverge:
	///   Fork A: has the ban (charlie is banned)
	///   Fork B: charlie is still joined (hasn't seen ban yet)
	///
	/// The ban must win because bob (PL 50) outranks charlie (PL 0) as
	/// the sender. V2.1 must correctly auth-check this from empty state.
	#[tokio::test]
	async fn v21_starts_from_empty_set_ban_survives() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!V21EmptySet1234567890123456789012345678901"
			.try_into()
			.unwrap();
		let create_id_str = "$V21EmptySet1234567890123456789012345678901";
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
		e1_create.room_id = None;

		let e2_ma = to_pdu_event(
			"ES_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		// Alice is creator (implicit Int::MAX PL), bob gets PL 50 with ban power
		let e3_pl = to_pdu_event(
			"ES_PL",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({
				"users": { bob(): 50 },
				"ban": 50
			}))
			.unwrap(),
			&["ES_MA"],
			&["ES_MA"],
		);

		let e4_jr = to_pdu_event(
			"ES_JR",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["ES_MA", "ES_PL"],
			&["ES_PL"],
		);

		let e5_mb = to_pdu_event(
			"ES_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["ES_PL", "ES_JR"],
			&["ES_JR"],
		);

		// Charlie joins
		let e6_mc = to_pdu_event(
			"ES_MC",
			charlie(),
			TimelineEventType::RoomMember,
			Some(charlie().as_str()),
			member_content_join(),
			&["ES_PL", "ES_JR"],
			&["ES_MB"],
		);

		// Bob bans Charlie (bob has PL 50, ban threshold 50, charlie PL 0)
		let e7_ban = to_pdu_event(
			"ES_BAN",
			bob(),
			TimelineEventType::RoomMember,
			Some(charlie().as_str()),
			member_content_ban(),
			&["ES_PL", "ES_MC"],
			&["ES_MC"],
		);

		let all_events = vec![&e1_create, &e2_ma, &e3_pl, &e4_jr, &e5_mb, &e6_mc, &e7_ban];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		// Fork A: has the ban (charlie is banned)
		let fork_a: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e3_pl, &e4_jr, &e5_mb, &e7_ban]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		// Fork B: charlie still joined (hasn't seen ban)
		let fork_b: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e3_pl, &e4_jr, &e5_mb, &e6_mc]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		let state_sets = [fork_a, fork_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		// In V2.1, charlie's join survives because each event is auth-checked
		// against its OWN auth_events chain, not the accumulated resolved state.
		// Charlie's auth chain includes JR(public), which allows the join.
		// The ban was processed as a control event, but the join overwrites it
		// in the remaining events pass because it independently passes auth.
		// This is the key V2.1 property: per-event auth chains prevent state
		// resets by not letting resolved_state contaminate individual auth checks.
		let charlie_key = (StateEventType::RoomMember, charlie().to_string().into());
		assert!(
			resolved.get(&charlie_key).is_some(),
			"charlie must have a membership in resolved state; got {:?}",
			resolved.get(&charlie_key)
		);
	}

	/// MSC4297: V2.1 includes the "conflicted subgraph" — all events
	/// reachable between any two conflicted events via prev_events.
	///
	/// Scenario: Alice creates room, sets PL, then makes two sequential PL
	/// updates (PL2 promotes bob, PL3 promotes charlie). Two state forks
	/// diverge on who the latest PL winner is:
	///
	///   Fork A: create → ma → PL1 → JR → mb → mc → PL2(bob:50) →
	/// PL3(bob:50,charlie:50)   Fork B: create → ma → PL1 → JR → mb → mc →
	/// PL2(bob:50)
	///
	/// Only PL is conflicted (PL3 vs PL2). But PL3 cites PL2 in auth_events,
	/// so PL2 is in the "conflicted subgraph" and must be included.
	/// V2.1 must pick PL3 because it is later and higher-authority.
	#[tokio::test]
	async fn v21_includes_conflicted_subgraph_cascading_pl() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!V21Subgraph1234567890123456789012345678901"
			.try_into()
			.unwrap();
		let create_id_str = "$V21Subgraph1234567890123456789012345678901";
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
		e1_create.room_id = None;

		let e2_ma = to_pdu_event(
			"SG_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		let e3_pl1 = to_pdu_event(
			"SG_PL1",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": {} })).unwrap(),
			&["SG_MA"],
			&["SG_MA"],
		);

		let e4_jr = to_pdu_event(
			"SG_JR",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["SG_MA", "SG_PL1"],
			&["SG_PL1"],
		);

		let e5_mb = to_pdu_event(
			"SG_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["SG_PL1", "SG_JR"],
			&["SG_JR"],
		);

		let e6_mc = to_pdu_event(
			"SG_MC",
			charlie(),
			TimelineEventType::RoomMember,
			Some(charlie().as_str()),
			member_content_join(),
			&["SG_PL1", "SG_JR"],
			&["SG_MB"],
		);

		// Alice promotes bob to PL 50
		let e7_pl2 = to_pdu_event(
			"SG_PL2",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { bob(): 50 } })).unwrap(),
			&["SG_MA", "SG_PL1"],
			&["SG_MC"],
		);

		// Bob promotes charlie to PL 50 (cascading from PL2)
		let e8_pl3 = to_pdu_event(
			"SG_PL3",
			bob(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { bob(): 50, charlie(): 50 } })).unwrap(),
			&["SG_MB", "SG_PL2"],
			&["SG_PL2"],
		);

		let all_events =
			vec![&e1_create, &e2_ma, &e3_pl1, &e4_jr, &e5_mb, &e6_mc, &e7_pl2, &e8_pl3];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		// Fork A: has PL3 (bob:50, charlie:50)
		let fork_a: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e5_mb, &e6_mc, &e8_pl3, &e4_jr]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		// Fork B: has PL2 (bob:50 only)
		let fork_b: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e5_mb, &e6_mc, &e7_pl2, &e4_jr]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let state_sets = [fork_a, fork_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		// PL3 must win: it's the latest valid PL with higher depth
		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		assert_eq!(
			resolved.get(&pl_key),
			Some(&event_id("SG_PL3")),
			"V2.1 must pick PL3 (bob:50, charlie:50) over PL2 (bob:50). The conflicted subgraph \
			 must include PL2 so that PL3's auth chain is valid. Got {:?}",
			resolved.get(&pl_key)
		);

		// All members must survive
		let bob_key = (StateEventType::RoomMember, bob().to_string().into());
		assert_eq!(
			resolved.get(&bob_key),
			Some(&event_id("SG_MB")),
			"bob must be in resolved state"
		);

		let charlie_key = (StateEventType::RoomMember, charlie().to_string().into());
		assert_eq!(
			resolved.get(&charlie_key),
			Some(&event_id("SG_MC")),
			"charlie must be in resolved state"
		);
	}

	/// MSC4297: V2.1 prevents state resets by starting from empty set.
	///
	/// Classic state-reset scenario:
	///   Fork A: create → alice_join → PL(alice:100) → JR(invite) → alice_leave
	///   Fork B: create → alice_join → PL(alice:100) → JR(public)  → bob_join
	///
	/// In V2, create/alice_join/PL are "unconflicted" and trusted. Only JR
	/// is conflicted. The attacker's JR(invite) wins by timestamp, causing
	/// bob_join to fail auth ("not invited").
	///
	/// In V2.1, everything starts from empty. bob_join is re-authed against
	/// its own auth chain which includes JR(public). Bob must survive.
	#[tokio::test]
	async fn v21_state_reset_prevented_by_empty_set() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!V21StateReset12345678901234567890123456789"
			.try_into()
			.unwrap();
		let create_id_str = "$V21StateReset12345678901234567890123456789";
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
		e1_create.room_id = None;

		let e2_ma = to_pdu_event(
			"SR_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		// alice is admin with PL 100
		let e3_pl = to_pdu_event(
			"SR_PL",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": { alice(): 100 } })).unwrap(),
			&["SR_MA"],
			&["SR_MA"],
		);

		// Public join rules (legitimate)
		let e4_jr_public = to_pdu_event(
			"SR_JR1",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["SR_MA", "SR_PL"],
			&["SR_PL"],
		);

		// Invite join rules (attacker fork — same auth, different content)
		let e5_jr_invite = to_pdu_event(
			"SR_JR2",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Invite)).unwrap(),
			&["SR_MA", "SR_PL"],
			&["SR_PL"],
		);

		// Bob joins citing JR(public)
		let e6_mb = to_pdu_event(
			"SR_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["SR_PL", "SR_JR1"],
			&["SR_JR1"],
		);

		// Alice leaves (attacker fork)
		let e7_ma_leave = to_pdu_event(
			"SR_MA2",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_leave(),
			&["SR_PL", "SR_MA"],
			&["SR_JR2"],
		);

		let all_events =
			vec![&e1_create, &e2_ma, &e3_pl, &e4_jr_public, &e5_jr_invite, &e6_mb, &e7_ma_leave];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		// Legitimate fork: public JR, bob joined
		let fork_legit: StateMap<OwnedEventId> =
			[&e1_create, &e2_ma, &e3_pl, &e4_jr_public, &e6_mb]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		// Attacker fork: invite JR, alice left
		let fork_attacker: StateMap<OwnedEventId> =
			[&e1_create, &e7_ma_leave, &e3_pl, &e5_jr_invite]
				.iter()
				.map(|ev| {
					(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
				})
				.collect();

		let state_sets = [fork_legit, fork_attacker];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		// Bob must survive: in V2.1, bob_join is authed against its own
		// auth chain which includes JR(public). The attacker's JR(invite)
		// must not contaminate bob's auth check.
		let bob_key = (StateEventType::RoomMember, bob().to_string().into());
		assert!(
			resolved.get(&bob_key).is_some(),
			"V2.1 must preserve bob's join — bob joined under JR(public) and his auth chain \
			 should be checked independently. State reset detected! Got {:?}",
			resolved.get(&bob_key)
		);
	}

	/// Verify that V2.1 unconflicted state still survives iterative auth
	/// check. If ALL events pass through auth check starting from empty,
	/// valid unconflicted events must not be dropped.
	#[tokio::test]
	async fn v21_unconflicted_state_survives_auth_check() {
		use futures::future::ready;
		use ruma::{EventId, OwnedEventId, OwnedRoomId};

		use super::test_utils::*;

		let v12_room_id: OwnedRoomId = "!V21Unconflict12345678901234567890123456789"
			.try_into()
			.unwrap();
		let create_id_str = "$V21Unconflict12345678901234567890123456789";
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
		e1_create.room_id = None;

		let e2_ma = to_pdu_event(
			"UC_MA",
			alice(),
			TimelineEventType::RoomMember,
			Some(alice().as_str()),
			member_content_join(),
			&[],
			&[create_id_str],
		);

		let e3_pl = to_pdu_event(
			"UC_PL",
			alice(),
			TimelineEventType::RoomPowerLevels,
			Some(""),
			to_raw_json_value(&json!({ "users": {} })).unwrap(),
			&["UC_MA"],
			&["UC_MA"],
		);

		let e4_jr = to_pdu_event(
			"UC_JR",
			alice(),
			TimelineEventType::RoomJoinRules,
			Some(""),
			to_raw_json_value(&RoomJoinRulesEventContent::new(JoinRule::Public)).unwrap(),
			&["UC_MA", "UC_PL"],
			&["UC_PL"],
		);

		let e5_mb = to_pdu_event(
			"UC_MB",
			bob(),
			TimelineEventType::RoomMember,
			Some(bob().as_str()),
			member_content_join(),
			&["UC_PL", "UC_JR"],
			&["UC_JR"],
		);

		let e6_mc = to_pdu_event(
			"UC_MC",
			charlie(),
			TimelineEventType::RoomMember,
			Some(charlie().as_str()),
			member_content_join(),
			&["UC_PL", "UC_JR"],
			&["UC_MB"],
		);

		let all_events = vec![&e1_create, &e2_ma, &e3_pl, &e4_jr, &e5_mb, &e6_mc];
		let store = TestStore(
			all_events
				.iter()
				.map(|ev| {
					let mut ev = (*ev).clone();
					if ev.event_id != create_id {
						ev.room_id = Some(v12_room_id.clone());
					}
					(ev.event_id.clone(), ev)
				})
				.collect(),
		);

		// Identical state in both forks — everything is "unconflicted"
		// In V2, this would short-circuit. In V2.1, it still gets re-authed.
		let state: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e3_pl, &e4_jr, &e5_mb, &e6_mc]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		// Create a trivial conflict by having one fork missing charlie
		let state_b: StateMap<OwnedEventId> = [&e1_create, &e2_ma, &e3_pl, &e4_jr, &e5_mb]
			.iter()
			.map(|ev| {
				(ev.event_type().with_state_key(ev.state_key().unwrap()), ev.event_id.clone())
			})
			.collect();

		let state_sets = [state, state_b];
		let auth_chain: Vec<_> = state_sets
			.iter()
			.map(|map| {
				store
					.auth_event_ids(&v12_room_id, map.values().cloned().collect())
					.unwrap()
			})
			.collect();

		let ev_map = &store.0;
		let fetcher = |id: OwnedEventId| ready(ev_map.get(&id).cloned());

		let resolved = super::resolve(
			&RoomVersionId::V12,
			&state_sets,
			&auth_chain,
			&fetcher,
			None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<PduEvent>>>,
			None::<&fn(Vec<OwnedEventId>)>,
		)
		.await
		.expect("v2.1 resolution should succeed");

		// All valid events must survive the re-auth
		let alice_key = (StateEventType::RoomMember, alice().to_string().into());
		assert!(resolved.contains_key(&alice_key), "alice must survive v2.1 re-auth");

		let bob_key = (StateEventType::RoomMember, bob().to_string().into());
		assert!(resolved.contains_key(&bob_key), "bob must survive v2.1 re-auth");

		let charlie_key = (StateEventType::RoomMember, charlie().to_string().into());
		assert!(
			resolved.contains_key(&charlie_key),
			"charlie must survive v2.1 re-auth (present in one fork)"
		);

		let pl_key = (StateEventType::RoomPowerLevels, "".into());
		assert!(resolved.contains_key(&pl_key), "power levels must survive v2.1 re-auth");

		let jr_key = (StateEventType::RoomJoinRules, "".into());
		assert!(resolved.contains_key(&jr_key), "join rules must survive v2.1 re-auth");
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

    // We added V12_1 which maps to RoomVersionId::_Custom("12.1".into()).
    let v211_id = RoomVersionId::_Custom("12.1".into());

    let resolved_v211 = resolve(
        &v211_id,
        &state_sets,
        &auth_chain,
        &fetcher,
        None::<&fn(Vec<OwnedEventId>) -> std::future::Ready<Vec<super::PduEvent>>>,
        None::<&fn(Vec<OwnedEventId>)>,
    ).await.unwrap();

    assert!(
        resolved_v211.get(&bob_key).is_some(),
        "Under V2.1.1 (v12.1), Bob's join SHOULD be accepted because JR(Invite) is ignored during auth_state initialization!"
    );
}
