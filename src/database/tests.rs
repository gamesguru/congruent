#![allow(clippy::needless_borrows_for_generic_args)]

use std::fmt::Debug;

use conduwuit::{
	arrayvec::ArrayVec,
	slipstream::{OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, sswire::Raw},
};
use serde::Serialize;

use crate::{
	Ignore, Interfix,
	dbkey::compact_filter,
	de, ser,
	ser::{Json, serialize_to_vec},
};

// RocksDB Env::new() returns the global default env. Context::Drop calls
// env.join_all_threads() which kills background threads shared by ALL
// databases in the process, so these tests must run serially to prevent one
// test's teardown from deadlocking the others.
static DB_TEST_MUTEX: std::sync::LazyLock<tokio::sync::Mutex<()>> =
	std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

struct TempDbGuard {
	path: std::path::PathBuf,
}

impl Drop for TempDbGuard {
	fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.path); }
}

async fn open_test_database(prefix: &str) -> (TempDbGuard, std::sync::Arc<crate::Database>) {
	use conduwuit::{
		Server,
		config::{Config, RawConfig},
		log::{Log, LogLevelReloadHandles, capture},
	};

	static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let count = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let db_path = std::env::temp_dir().join(format!("conduwuit_test_db_{prefix}_{count}"));
	let _ = std::fs::remove_dir_all(&db_path);

	let guard = TempDbGuard { path: db_path.clone() };

	let config_raw = RawConfig::from_toml(&format!(
		r#"
			server_name = "test.conduwuit.local"
			database_path = "{}"
			"#,
		db_path.to_string_lossy().replace('\\', "/")
	))
	.expect("failed to parse test config");

	let config = Config::new(&config_raw).expect("failed to parse config");
	let runtime_handle = tokio::runtime::Handle::current();
	let server = std::sync::Arc::new(Server::new(config, Some(&runtime_handle), Log {
		reload: LogLevelReloadHandles,
		capture: std::sync::Arc::new(capture::State),
	}));

	let db = crate::Database::open(&server).expect("failed to open database");

	(guard, db)
}

#[tokio::test]
async fn recursive_multi_get_traversal() {
	let _serial = DB_TEST_MUTEX.lock().await;
	let (_guard, db) = open_test_database("recursive_get").await;
	let map = &db["global"];

	// Insert DAG nodes:
	// A -> B, C
	// B -> A (cycle) & D (diamond convergence)
	// C -> D (diamond convergence) & M (missing)
	// D -> E
	map.insert(b"node_A", b"node_B,node_C");
	map.insert(b"node_B", b"node_A,node_D");
	map.insert(b"node_C", b"node_D,node_M"); // node_M is never inserted
	map.insert(b"node_D", b"node_E");
	map.insert(b"node_E", b"");

	let parse_val = |slice: &[u8]| -> conduwuit::Result<String> {
		String::from_utf8(slice.to_vec()).map_err(|e| std::io::Error::other(e).into())
	};

	let extract_children = |val: &String, sink: &mut Vec<Vec<u8>>| {
		if !val.is_empty() {
			for part in val.split(',') {
				sink.push(part.as_bytes().to_vec());
			}
		}
	};

	// Test 1: full traversal with cycle, diamond, and missing key detection
	let output = map
		.recursive_multi_get(
			vec![b"node_A".to_vec(), b"node_A".to_vec()],
			None,
			None,
			parse_val,
			extract_children,
		)
		.await
		.expect("traversal failed");

	assert!(!output.truncated);
	assert_eq!(output.missing, vec![b"node_M".to_vec()]);
	assert_eq!(output.values, vec![
		"node_B,node_C".to_owned(),
		"node_A,node_D".to_owned(),
		"node_D,node_M".to_owned(),
		"node_E".to_owned(),
		String::new(),
	]);

	// Test 2: truncation via max_depth
	let depth_output = map
		.recursive_multi_get(vec![b"node_A".to_vec()], None, Some(1), parse_val, extract_children)
		.await
		.expect("traversal failed");

	assert!(depth_output.truncated);
	assert_eq!(depth_output.values, vec!["node_B,node_C".to_owned()]);

	// Test 3: truncation via max_nodes
	let node_output = map
		.recursive_multi_get(vec![b"node_A".to_vec()], Some(2), None, parse_val, extract_children)
		.await
		.expect("traversal failed");

	assert!(node_output.truncated);
	assert_eq!(
		node_output.values,
		vec!["node_B,node_C".to_owned(), "node_A,node_D".to_owned(),]
	);

	// Test 4: truncation via max_nodes = Some(0)
	let zero_node_output = map
		.recursive_multi_get(vec![b"node_A".to_vec()], Some(0), None, parse_val, extract_children)
		.await
		.expect("traversal failed");

	assert!(zero_node_output.truncated);
	assert!(zero_node_output.values.is_empty());

	// Test 5: mid-batch truncation still records missing keys
	let mid_batch_output = map
		.recursive_multi_get(
			vec![b"node_C".to_vec(), b"node_M".to_vec()],
			Some(1),
			None,
			parse_val,
			extract_children,
		)
		.await
		.expect("traversal failed");

	assert!(mid_batch_output.truncated);
	assert_eq!(mid_batch_output.values, vec!["node_D,node_M".to_owned()]);
	assert_eq!(mid_batch_output.missing, vec![b"node_M".to_vec()]);
}

#[test]
fn ser_str() {
	let user_id = "@user:example.com".parse::<OwnedUserId>().unwrap();
	let s = serialize_to_vec(&user_id).expect("failed to serialize user_id");
	assert_eq!(&s, user_id.as_bytes());
}

#[test]
fn ser_tuple() {
	let user_id = "@user:example.com".parse::<OwnedUserId>().unwrap();
	let room_id = "!room:example.com".parse::<OwnedRoomId>().unwrap();

	let mut a = user_id.as_bytes().to_vec();
	a.push(0xFF);
	a.extend_from_slice(room_id.as_bytes());

	let b = (user_id, room_id);
	let b = serialize_to_vec(&b).expect("failed to serialize tuple");

	assert_eq!(a, b);
}

#[test]
fn ser_tuple_option() {
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let mut a = Vec::<u8>::new();
	a.push(0xFF);
	a.extend_from_slice(user_id.as_bytes());

	let mut aa = Vec::<u8>::new();
	aa.extend_from_slice(room_id.as_bytes());
	aa.push(0xFF);
	aa.extend_from_slice(user_id.as_bytes());

	let b: (Option<OwnedRoomId>, OwnedUserId) = (None, user_id.clone());
	let b = serialize_to_vec(&b).expect("failed to serialize tuple");
	assert_eq!(a, b);

	let bb: (Option<OwnedRoomId>, OwnedUserId) = (Some(room_id), user_id);
	let bb = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bb);
}

#[test]
#[should_panic(expected = "I/O error: failed to write whole buffer")]
fn ser_overflow() {
	const BUFSIZE: usize = 10;

	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();

	assert!(BUFSIZE < user_id.as_str().len() + room_id.as_str().len());
	let mut buf = ArrayVec::<u8, BUFSIZE>::new();

	let val = (user_id, room_id);
	_ = ser::serialize(&mut buf, val).unwrap();
}

#[test]
fn ser_complex() {
	use conduwuit::slipstream::Mxc;

	#[derive(Debug, Serialize)]
	struct Dim {
		width: u32,
		height: u32,
	}

	let mxc = Mxc {
		server_name: &OwnedServerName::parse("example.com").unwrap(),
		media_id: "AbCdEfGhIjK",
	};

	let dim = Dim { width: 123, height: 456 };

	let mut a = Vec::new();
	a.extend_from_slice(b"mxc://");
	a.extend_from_slice(mxc.server_name.as_bytes());
	a.extend_from_slice(b"/");
	a.extend_from_slice(mxc.media_id.as_bytes());
	a.push(0xFF);
	a.extend_from_slice(&dim.width.to_be_bytes());
	a.extend_from_slice(&dim.height.to_be_bytes());
	a.push(0xFF);

	let d: &[u32] = &[dim.width, dim.height];
	let b = (mxc, d, Interfix);
	let b = serialize_to_vec(b).expect("failed to serialize complex");

	assert_eq!(a, b);
}

/// Defaults are omitted from stored filters, as in rows written before the
/// codec migration.
#[test]
fn ser_json() {
	use conduwuit::slipstream::filter::FilterDefinition;

	let filter = FilterDefinition {
		event_fields: Some(vec!["content.body".to_owned()]),
		..Default::default()
	};

	let serialized =
		serialize_to_vec(Json(compact_filter(&filter))).expect("failed to serialize value");
	assert_eq!(String::from_utf8_lossy(&serialized), r#"{"event_fields":["content.body"]}"#);

	let decoded: Json<FilterDefinition> = de::from_slice(&serialized).expect("failed to decode");
	assert_eq!(decoded.0.event_fields, filter.event_fields);

	let again =
		serialize_to_vec(Json(compact_filter(&decoded.0))).expect("failed to serialize again");
	assert_eq!(serialized, again, "re-encoding is not stable");
}

#[test]
fn ser_json_filter_default_is_empty_object() {
	use conduwuit::slipstream::filter::FilterDefinition;

	let serialized =
		serialize_to_vec(Json(compact_filter(&FilterDefinition::default()))).expect("serialize");
	assert_eq!(String::from_utf8_lossy(&serialized), "{}");
}

#[test]
fn ser_json_object() {
	use conduwuit::slipstream::json::Value;

	let value = Value::parse(r#"{"sender":"@foo:example.com","content":{"foo":"bar"}}"#)
		.expect("failed to parse value");
	let serialized = serialize_to_vec(Json(value)).expect("failed to serialize value");

	// Objects are key-sorted, matching the historical serde_json encoding.
	let s = String::from_utf8_lossy(&serialized);
	assert_eq!(&s, r#"{"content":{"foo":"bar"},"sender":"@foo:example.com"}"#);
}

#[test]
fn ser_raw_json_text() {
	let text = r#"{"event_fields":["content.body"]}"#;
	let a = serialize_to_vec(text).expect("failed to serialize raw text");
	assert_eq!(String::from_utf8_lossy(&a), text);
}

/// Stored `Json` values must decode and re-encode to identical bytes, since
/// that is the on-disk format of existing databases.
#[test]
fn json_value_roundtrip() {
	use conduwuit::slipstream::json::Value;

	let cases: &[&str] = &[
		r#"null"#,
		r#"true"#,
		r#"-12"#,
		r#"18446744073709551615"#,
		r#"1.5"#,
		r#""plain""#,
		r#""quote\" slash/ \\ tab\t nl\n""#,
		r#""héllo ☃ \ud83d\ude00""#,
		r#"[]"#,
		r#"{}"#,
		r#"{"a":[1,2,{"b":null}],"c":{"d":"e"}}"#,
		r#"{"users":{"@a:x":100,"@b:x":0},"events":{}}"#,
	];

	for case in cases {
		let value: Json<Value> = de::from_slice(case.as_bytes()).expect("failed to deserialize");
		let first = serialize_to_vec(&value).expect("failed to serialize");
		let again: Json<Value> = de::from_slice(&first).expect("failed to deserialize again");
		let second = serialize_to_vec(&again).expect("failed to serialize again");
		assert_eq!(first, second, "re-encoding is not stable for {case}");
		assert_eq!(value.0, again.0, "value changed across round-trip for {case}");
	}
}

#[test]
fn json_filter_legacy_row_decodes() {
	use conduwuit::slipstream::filter::FilterDefinition;

	let stored = br#"{"event_fields":["content.body"]}"#;
	let filter: Json<FilterDefinition> = de::from_slice(stored).expect("failed to deserialize");
	assert_eq!(filter.0.event_fields, Some(vec!["content.body".to_owned()]));

	let again = serialize_to_vec(Json(compact_filter(&filter.0))).expect("failed to serialize");
	assert_eq!(&*again, &stored[..], "legacy row did not re-encode byte-identically");
}

#[test]
fn json_malformed_is_error() {
	use conduwuit::slipstream::json::Value;

	de::from_slice::<Json<Value>>(b"{not json").unwrap_err();
	de::from_slice::<Json<Value>>(&[0xFF, 0xFE]).unwrap_err();
}

#[test]
fn de_tuple() {
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();

	let raw: &[u8] = b"@user:example.com\xFF!room:example.com";
	let (a, b): (OwnedUserId, OwnedRoomId) = de::from_slice(raw).expect("failed to deserialize");

	assert_eq!(a, user_id, "deserialized user_id does not match");
	assert_eq!(b, room_id, "deserialized room_id does not match");
}

#[test]
#[should_panic(expected = "failed to deserialize")]
fn de_tuple_invalid() {
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();

	let raw: &[u8] = b"@user:example.com\xFF@user:example.com";
	let (a, b): (OwnedUserId, OwnedRoomId) = de::from_slice(raw).expect("failed to deserialize");

	assert_eq!(a, user_id, "deserialized user_id does not match");
	assert_eq!(b, room_id, "deserialized room_id does not match");
}

#[test]
#[should_panic(expected = "failed to deserialize")]
fn de_tuple_incomplete() {
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let raw: &[u8] = b"@user:example.com";
	let (a, _): (OwnedUserId, OwnedRoomId) = de::from_slice(raw).expect("failed to deserialize");

	assert_eq!(a, user_id, "deserialized user_id does not match");
}

#[test]
#[should_panic(expected = "failed to deserialize")]
fn de_tuple_incomplete_with_sep() {
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let raw: &[u8] = b"@user:example.com\xFF";
	let (a, _): (OwnedUserId, OwnedRoomId) = de::from_slice(raw).expect("failed to deserialize");

	assert_eq!(a, user_id, "deserialized user_id does not match");
}

#[test]
#[cfg_attr(
	debug_assertions,
	should_panic(expected = "deserialization failed to consume trailing bytes")
)]
fn de_tuple_unfinished() {
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();

	let raw: &[u8] = b"@user:example.com\xFF!room:example.com\xFF@user:example.com";
	let (a, b): (OwnedUserId, OwnedRoomId) = de::from_slice(raw).expect("failed to deserialize");

	assert_eq!(a, user_id, "deserialized user_id does not match");
	assert_eq!(b, room_id, "deserialized room_id does not match");
}

#[test]
fn de_tuple_ignore() {
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();

	let raw: &[u8] = b"@user:example.com\xFF@user2:example.net\xFF!room:example.com";
	let (a, _, c): (OwnedUserId, Ignore, OwnedRoomId) =
		de::from_slice(raw).expect("failed to deserialize");

	assert_eq!(a, user_id, "deserialized user_id does not match");
	assert_eq!(c, room_id, "deserialized room_id does not match");
}

#[test]
fn de_json_array() {
	let s = br#"["foo","bar","baz"]"#;
	let b: Raw<Vec<Raw<String>>> = de::from_slice(s).expect("failed to deserialize");
	assert_eq!(b.0.as_bytes(), s);
}

#[test]
fn ser_array() {
	let a: u64 = 123_456;
	let b: u64 = 987_654;

	let arr: &[u64] = &[a, b];
	let vec: Vec<u64> = vec![a, b];
	let arv: ArrayVec<u64, 2> = [a, b].into();

	let mut v = Vec::new();
	v.extend_from_slice(&a.to_be_bytes());
	v.extend_from_slice(&b.to_be_bytes());

	let s = serialize_to_vec(arr).expect("failed to serialize");
	assert_eq!(&s, &v, "serialization does not match");

	let s = serialize_to_vec(arv.as_slice()).expect("failed to serialize arrayvec");
	assert_eq!(&s, &v, "arrayvec serialization does not match");

	let s = serialize_to_vec(&vec).expect("failed to serialize vec");
	assert_eq!(&s, &v, "vec serialization does not match");
}

#[test]
#[ignore = "arrayvec deserialization is not implemented (separators)"]
fn de_array() {
	let a: u64 = 123_456;
	let b: u64 = 987_654;

	let mut v: Vec<u8> = Vec::new();
	v.extend_from_slice(&a.to_be_bytes());
	v.extend_from_slice(&b.to_be_bytes());

	let arv: ArrayVec<u64, 2> = de::from_slice::<ArrayVec<u64, 2>>(v.as_slice())
		.map(TryInto::try_into)
		.expect("failed to deserialize to arrayvec")
		.expect("failed to deserialize into");

	assert_eq!(arv[0], a, "deserialized arv [0] does not match");
	assert_eq!(arv[1], b, "deserialized arv [1] does not match");

	let vec: Vec<u64> = de::from_slice(v.as_slice()).expect("failed to deserialize to vec");

	assert_eq!(vec[0], a, "deserialized vec [0] does not match");
	assert_eq!(vec[1], b, "deserialized vec [1] does not match");
}

#[test]
#[ignore = "Nested sequences are not supported"]
fn de_complex() {
	type Key = (OwnedUserId, ArrayVec<u64, 2>, OwnedRoomId);

	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();
	let a: u64 = 123_456;
	let b: u64 = 987_654;

	let mut v = Vec::new();
	v.extend_from_slice(user_id.as_bytes());
	v.extend_from_slice(b"\xFF");
	v.extend_from_slice(&a.to_be_bytes());
	v.extend_from_slice(&b.to_be_bytes());
	v.extend_from_slice(b"\xFF");
	v.extend_from_slice(room_id.as_bytes());

	let arr: &[u64] = &[a, b];
	let key = (user_id.clone(), arr, room_id.clone());
	let s = serialize_to_vec(&key).expect("failed to serialize");

	assert_eq!(&s, &v, "serialization does not match");

	let key = (user_id, [a, b].into(), room_id);
	let arr: Key = de::from_slice(&v).expect("failed to deserialize");

	assert_eq!(arr, key, "deserialization does not match");

	let arr: Key = de::from_slice(&s).expect("failed to deserialize");

	assert_eq!(arr, key, "deserialization of serialization does not match");
}

#[test]
fn serde_tuple_option_value_some() {
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let mut aa = Vec::<u8>::new();
	aa.extend_from_slice(room_id.as_bytes());
	aa.push(0xFF);
	aa.extend_from_slice(user_id.as_bytes());

	let bb: (OwnedRoomId, Option<OwnedUserId>) = (room_id, Some(user_id));
	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (OwnedRoomId, Option<OwnedUserId>) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(bb.1, cc.1);
	assert_eq!(cc.0, bb.0);
}

#[test]
fn serde_tuple_option_value_none() {
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();

	let mut aa = Vec::<u8>::new();
	aa.extend_from_slice(room_id.as_bytes());
	aa.push(0xFF);

	let bb: (OwnedRoomId, Option<OwnedUserId>) = (room_id, None);
	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (OwnedRoomId, Option<OwnedUserId>) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(None, cc.1);
	assert_eq!(cc.0, bb.0);
}

#[test]
fn serde_tuple_option_none_value() {
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let mut aa = Vec::<u8>::new();
	aa.push(0xFF);
	aa.extend_from_slice(user_id.as_bytes());

	let bb: (Option<OwnedRoomId>, OwnedUserId) = (None, user_id);
	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (Option<OwnedRoomId>, OwnedUserId) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(None, cc.0);
	assert_eq!(cc.1, bb.1);
}

#[test]
fn serde_tuple_option_some_value() {
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let mut aa = Vec::<u8>::new();
	aa.extend_from_slice(room_id.as_bytes());
	aa.push(0xFF);
	aa.extend_from_slice(user_id.as_bytes());

	let bb: (Option<OwnedRoomId>, OwnedUserId) = (Some(room_id), user_id);
	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (Option<OwnedRoomId>, OwnedUserId) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(bb.0, cc.0);
	assert_eq!(cc.1, bb.1);
}

#[test]
fn serde_tuple_option_some_some() {
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let mut aa = Vec::<u8>::new();
	aa.extend_from_slice(room_id.as_bytes());
	aa.push(0xFF);
	aa.extend_from_slice(user_id.as_bytes());

	let bb: (Option<OwnedRoomId>, Option<OwnedUserId>) = (Some(room_id), Some(user_id));
	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (Option<OwnedRoomId>, Option<OwnedUserId>) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(cc.0, bb.0);
	assert_eq!(bb.1, cc.1);
}

#[test]
fn serde_tuple_option_none_none() {
	let aa = vec![0xFF];

	let bb: (Option<OwnedRoomId>, Option<OwnedUserId>) = (None, None);
	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (Option<OwnedRoomId>, Option<OwnedUserId>) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(cc.0, bb.0);
	assert_eq!(None, cc.1);
}

#[test]
fn serde_tuple_option_some_none_some() {
	let room_id: OwnedRoomId = "!room:example.com".try_into().unwrap();
	let user_id: OwnedUserId = "@user:example.com".try_into().unwrap();

	let mut aa = Vec::<u8>::new();
	aa.extend_from_slice(room_id.as_bytes());
	aa.push(0xFF);
	aa.push(0xFF);
	aa.extend_from_slice(user_id.as_bytes());

	let bb: (Option<OwnedRoomId>, Option<OwnedEventId>, Option<OwnedUserId>) =
		(Some(room_id), None, Some(user_id));

	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (Option<OwnedRoomId>, Option<OwnedEventId>, Option<OwnedUserId>) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(bb.0, cc.0);
	assert_eq!(None, cc.1);
	assert_eq!(bb.1, cc.1);
	assert_eq!(bb.2, cc.2);
}

#[test]
fn serde_tuple_option_none_none_none() {
	let aa = vec![0xFF, 0xFF];

	let bb: (Option<OwnedRoomId>, Option<OwnedEventId>, Option<OwnedUserId>) = (None, None, None);
	let bbs = serialize_to_vec(&bb).expect("failed to serialize tuple");
	assert_eq!(aa, bbs);

	let cc: (Option<OwnedRoomId>, Option<OwnedEventId>, Option<OwnedUserId>) =
		de::from_slice(&bbs).expect("failed to deserialize tuple");

	assert_eq!(None, cc.0);
	assert_eq!(bb, cc);
}
