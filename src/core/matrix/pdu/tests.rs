use super::Count;

#[test]
fn backfilled_parse() {
	let count: Count = "-987654".parse().expect("parse() failed");
	let backfilled = matches!(count, Count::Backfilled(_));

	assert!(backfilled, "not backfilled variant");
}

#[test]
fn saturating_inc_backward() {
	use slipstream::api::Direction;

	// Normal count
	let count = Count::Normal(10);
	let next = count.saturating_inc(Direction::Backward);
	assert_eq!(next, Count::Normal(9));

	// Transition to backfilled
	let count = Count::Normal(1);
	let next = count.saturating_inc(Direction::Backward);
	assert_eq!(next, Count::Normal(0));

	let count = Count::Normal(0);
	let next = count.saturating_inc(Direction::Backward);
	assert_eq!(next, Count::Backfilled(-1));

	// Minimum
	let count = Count::min();
	let next = count.saturating_inc(Direction::Backward);
	assert_eq!(next, Count::min());
}

/// `pdus`/`pdus_rev` in `service::rooms::timeline::data` are EXCLUSIVE of
/// their boundary and rely on this exact operation (`saturating_inc`) at
/// their call sites to become inclusive when needed (e.g.
/// `/members?at=...`). If this arithmetic ever drifts, that boundary
/// handling silently breaks — see the `TestSearch`/`/members?at=` regression
/// this test was added to guard against.
#[test]
fn saturating_inc_forward() {
	use slipstream::api::Direction;

	// Normal count
	let count = Count::Normal(10);
	let next = count.saturating_inc(Direction::Forward);
	assert_eq!(next, Count::Normal(11));

	// Backfilled stays Backfilled going forward even once non-negative —
	// the backfilled sequence only legitimately covers counts <= 0, so once
	// we step past zero the value must normalize into the Normal variant.
	let count = Count::Backfilled(-1);
	let next = count.saturating_inc(Direction::Forward);
	assert_eq!(next, Count::Backfilled(0));

	let count = Count::Backfilled(0);
	let next = count.saturating_inc(Direction::Forward);
	assert_eq!(next, Count::Normal(1));

	// Saturate at the largest valid normal count rather than overflowing into
	// values whose signed ordering no longer matches token ordering.
	let count = Count::max();
	let next = count.saturating_inc(Direction::Forward);
	assert_eq!(next, Count::max());
}

/// Documents the `Count` arithmetic that inclusive callers rely on when
/// compensating for the exclusive `pdus`/`pdus_rev` boundaries.
///
/// This is intentionally a low-level arithmetic test only; it does not
/// exercise the `/members?at=` integration path directly.
#[test]
fn saturating_inc_matches_boundary_compensation_arithmetic() {
	use slipstream::api::Direction;

	// pdus_rev(until) excludes `until`; a caller wanting `at` included as the
	// first (most recent) result must request pdus_rev(at + 1) so that
	// "everything strictly before at+1" == "everything up to and including at".
	let at = Count::Normal(42);
	let bumped_for_pdus_rev = at.saturating_inc(Direction::Forward);
	assert_eq!(bumped_for_pdus_rev, Count::Normal(43));

	// pdus(from) excludes `from`; a caller wanting `from` included as the
	// first (earliest) result must request pdus(from - 1).
	let from = Count::Normal(42);
	let bumped_for_pdus = from.saturating_inc(Direction::Backward);
	assert_eq!(bumped_for_pdus, Count::Normal(41));
}

#[test]
fn raw_id_normal_shorteventid_matches_bytes() {
	use super::{Id, RawId};

	let id = Id {
		shortroomid: 42,
		shorteventid: Count::Normal(12345),
	};
	let raw: RawId = id.into();

	// shorteventid() returns the offset-binary-encoded count bytes
	// (sign bit flipped for correct unsigned lexicographic sorting)
	let expected = Count::offset_binary_encoding(12345_i64.to_be_bytes());
	assert_eq!(raw.shorteventid(), expected);

	// as_ref()[8..] is the same 8 encoded bytes in the uniform 16-byte layout
	assert_eq!(&raw.as_ref()[8..], &expected);
}

#[test]
fn raw_id_backfilled_shorteventid_returns_count() {
	use super::{Id, RawId};

	let id = Id {
		shortroomid: 42,
		shorteventid: Count::Backfilled(-99),
	};
	let raw: RawId = id.into();

	// Uniform 16-byte layout: [room(8) | offset_binary_encoded_count(8)]
	assert_eq!(raw.as_ref().len(), 16);

	// shorteventid() returns the offset-binary-encoded count bytes
	let expected = Count::offset_binary_encoding((-99_i64).to_be_bytes());
	assert_eq!(raw.shorteventid(), expected);

	// as_ref()[8..] is exactly 8 bytes — the encoded count
	assert_eq!(raw.as_ref()[8..].len(), 8);
}

#[test]
fn raw_id_roundtrip_backfilled() {
	use super::{Id, RawId};

	let original = Id {
		shortroomid: 0xDEAD_BEEF,
		shorteventid: Count::Backfilled(-42),
	};
	let raw: RawId = original.into();
	let recovered: Id = raw.into();

	assert_eq!(recovered.shortroomid, original.shortroomid);
	assert_eq!(recovered.shorteventid, original.shorteventid);
}

// Golden compatibility tests for the `Pdu` JSON shape.
//
// The fixtures are literal stored/federation-form events in canonical JSON
// (sorted keys, compact). They pin field names, omitted optional fields and
// the exact bytes produced, so a change to the codec impl that would alter
// hashes, signatures or stored data fails here rather than in production.
mod golden {
	use slipstream::codec::{from_str, to_string, to_value};

	use super::super::{Pdu, RawJson};

	const ID_A: &str = "$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";
	const ID_B: &str = "$bbcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";
	const ID_C: &str = "$cbcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";

	fn message_fixture() -> String {
		format!(
			concat!(
				r#"{{"auth_events":["{b}"],"#,
				r#""content":{{"body":"héllo \"q\" 日本","msgtype":"m.text"}},"#,
				r#""depth":12,"event_id":"{a}","#,
				r#""hashes":{{"sha256":"aGFzaA"}},"origin_server_ts":1700000000000,"#,
				r#""prev_events":["{c}"],"room_id":"!room:example.org","#,
				r#""sender":"@alice:example.org","#,
				r#""signatures":{{"example.org":{{"ed25519:1":"c2ln"}}}},"#,
				r#""type":"m.room.message","unsigned":{{"age":5}}}}"#,
			),
			a = ID_A,
			b = ID_B,
			c = ID_C,
		)
	}

	fn state_fixture() -> String {
		format!(
			concat!(
				r#"{{"auth_events":["{b}"],"content":{{"membership":"join"}},"depth":3,"#,
				r#""event_id":"{a}","hashes":{{"sha256":"aGFzaA"}},"origin":"example.org","#,
				r#""origin_server_ts":1,"prev_events":["{c}"],"redacts":"{b}","#,
				r#""room_id":"!room:example.org","sender":"@alice:example.org","#,
				r#""state_key":"@alice:example.org","type":"m.room.member"}}"#,
			),
			a = ID_A,
			b = ID_B,
			c = ID_C,
		)
	}

	#[test]
	fn message_event_roundtrips_to_identical_bytes() {
		let fixture = message_fixture();
		let pdu: Pdu = from_str(&fixture).expect("fixture parses");

		assert_eq!(pdu.event_id.as_str(), ID_A);
		assert_eq!(pdu.depth, 12);
		assert_eq!(pdu.origin_server_ts, 1_700_000_000_000);
		assert_eq!(pdu.hashes.sha256, "aGFzaA");
		assert!(pdu.state_key.is_none() && pdu.redacts.is_none() && pdu.origin.is_none());
		assert!(!pdu.rejected, "rejected is runtime-only and defaults to false");

		assert_eq!(to_string(&pdu), fixture);
	}

	#[test]
	fn state_event_keeps_optional_fields() {
		let fixture = state_fixture();
		let pdu: Pdu = from_str(&fixture).expect("fixture parses");

		assert_eq!(pdu.state_key.as_deref(), Some("@alice:example.org"));
		assert_eq!(pdu.redacts.as_ref().map(|id| id.as_str()), Some(ID_B));
		assert_eq!(pdu.origin.as_ref().map(|o| o.as_str()), Some("example.org"));
		assert!(pdu.unsigned.is_none() && pdu.signatures.is_none());

		// Absent optional fields stay absent (no `"unsigned":null` etc.).
		assert_eq!(to_string(&pdu), fixture);
	}

	#[test]
	fn rejected_flag_is_never_serialized() {
		let mut pdu: Pdu = from_str(&state_fixture()).expect("fixture parses");
		pdu.rejected = true;

		assert!(
			to_value(&pdu)
				.as_object()
				.expect("object")
				.get("rejected")
				.is_none()
		);
		assert_eq!(to_string(&pdu), state_fixture());
	}

	#[test]
	fn non_canonical_input_is_reserialized_canonically() {
		// Same event as `state_fixture` with whitespace and shuffled keys. The
		// original text is not preserved; output is sorted and compact, which is
		// what hashing and signature checks operate on.
		let shuffled = format!(
			r#"{{ "type": "m.room.member", "sender": "@alice:example.org",
			"room_id": "!room:example.org", "redacts": "{b}", "prev_events": ["{c}"],
			"origin_server_ts": 1, "origin": "example.org",
			"hashes": {{"sha256": "aGFzaA"}}, "event_id": "{a}", "depth": 3,
			"state_key": "@alice:example.org",
			"content": {{ "membership": "join" }}, "auth_events": ["{b}"] }}"#,
			a = ID_A,
			b = ID_B,
			c = ID_C,
		);
		let pdu: Pdu = from_str(&shuffled).expect("parses");

		assert_eq!(to_string(&pdu), state_fixture());
	}

	#[test]
	fn missing_required_fields_are_rejected() {
		for field in ["sender", "type", "depth", "prev_events", "auth_events", "hashes"] {
			let value = to_value(&from_str::<Pdu>(&state_fixture()).expect("parses"));
			let mut object = value.as_object().expect("object").clone();
			assert!(object.remove(field).is_some(), "{field} present in fixture");
			let text = to_string(&slipstream::json::Value::Object(object));
			assert!(from_str::<Pdu>(&text).is_err(), "missing `{field}` must be an error");
		}
	}

	#[test]
	fn raw_replacement_equals_codec_string() {
		// `serialize_replacement` wraps the event in `RawJson::from_value`; it must
		// carry exactly the bytes the codec produces.
		let pdu: Pdu = from_str(&message_fixture()).expect("fixture parses");

		assert_eq!(RawJson::from_value(&pdu).get(), to_string(&pdu));
		assert_eq!(RawJson::from_value(&pdu).get(), message_fixture());
	}

	#[test]
	fn nested_raw_fields_survive_unchanged() {
		let pdu: Pdu = from_str(&message_fixture()).expect("fixture parses");

		assert_eq!(pdu.unsigned.as_ref().expect("unsigned").get(), r#"{"age":5}"#);
		assert_eq!(
			pdu.signatures.as_ref().expect("signatures").get(),
			r#"{"example.org":{"ed25519:1":"c2ln"}}"#
		);
		assert_eq!(pdu.content.get(), r#"{"body":"héllo \"q\" 日本","msgtype":"m.text"}"#);
	}
}

// Parity of the hand-written `Pdu` codec with the previous serde derive (see
// `../congruent`, `src/core/matrix/pdu.rs`): wrong types are errors, `null`
// `Option` fields mean `None`, `UInt` is capped at 2^53 - 1, `content` is
// required. Ordinary serialization intentionally emits canonical (sorted,
// compact) JSON instead of struct declaration order.
mod serde_parity {
	use slipstream::codec::{from_str, to_string};

	use super::super::Pdu;

	fn base(extra: &str) -> String {
		format!(
			concat!(
				r#"{{"auth_events":[],"content":{{}},"depth":1,"#,
				r#""event_id":"$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ","#,
				r#""hashes":{{"sha256":"x"}},"origin_server_ts":1,"prev_events":[],"#,
				r#""sender":"@a:example.org","type":"m.room.message"{extra}}}"#,
			),
			extra = extra,
		)
	}

	/// serde: a non-string `state_key` is a type error. The codec silently
	/// drops it, turning a state event into a timeline event.
	#[test]
	fn non_string_state_key_is_rejected() {
		assert!(from_str::<Pdu>(&base(r#","state_key":5"#)).is_err());
	}

	/// serde: `Option` fields accept `null` as `None` and omit them on output.
	#[test]
	fn null_optional_fields_are_none() {
		for field in ["room_id", "origin", "redacts", "unsigned", "signatures"] {
			let text = base(&format!(r#","{field}":null"#));
			let pdu: Pdu = from_str(&text).unwrap_or_else(|e| panic!("`{field}: null`: {e:?}"));
			assert!(!to_string(&pdu).contains(&format!(r#""{field}""#)), "{field} omitted");
		}
	}

	/// serde: `UInt` is limited to 2^53 - 1.
	#[test]
	fn depth_above_js_safe_integer_is_rejected() {
		let big = base("").replace(r#""depth":1"#, r#""depth":9007199254740993"#);
		assert!(from_str::<Pdu>(&big).is_err());
	}

	/// serde: `content` has no default, so a missing `content` is an error. The
	/// codec substitutes `{}`.
	#[test]
	fn missing_content_is_rejected() {
		let text = base("").replace(r#""content":{},"#, "");
		assert!(from_str::<Pdu>(&text).is_err());
	}

	#[test]
	fn uint_boundary_is_inclusive() {
		let max = base("").replace(r#""depth":1"#, r#""depth":9007199254740991"#);
		assert_eq!(
			from_str::<Pdu>(&max).expect("2^53 - 1 is valid").depth,
			9_007_199_254_740_991
		);
	}

	#[test]
	fn null_content_is_rejected() {
		let text = base("").replace(r#""content":{},"#, r#""content":null,"#);
		assert!(from_str::<Pdu>(&text).is_err());
	}
}
