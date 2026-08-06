use std::collections::HashSet;

use conduwuit::{Err, Result};

const MAX_JSON_SCAN_DEPTH: usize = 128;

/// MSC4499: Scan raw JSON bytes for duplicate keys within `verify_keys` and
/// `old_verify_keys` objects. Returns Err if any duplicate keys are found.
///
/// This must run on the raw bytes BEFORE `serde_json` deserialization, because
/// `serde_json` silently deduplicates (last-key-wins). Without this pre-scan,
/// a payload with `{"verify_keys": {"ed25519:foo": ..., "ed25519:foo": ...}}`
/// would be silently accepted with the second value winning.
pub(super) fn check_no_duplicate_json_keys(raw: &str) -> Result {
	// Scan the raw bytes for duplicate keys and count limits FIRST, before
	// full deserialization. This prevents memory exhaustion if a rogue server
	// sends 100,000 keys, since we reject it before allocating a JSON tree.
	let counts = scan_root_sections(raw.as_bytes())?;

	// MSC4499: "If a single key response payload contains more than 50 keys in its
	// verify_keys dictionary, receiving servers MUST treat the entire response
	// payload as malformed/hostile and reject it."
	if counts.verify_keys > 50 {
		return Err!(BadServerResponse("Too many keys in verify_keys (limit: 50)"));
	}

	// MSC4499: "If a single key response payload contains more than 1000 keys in
	// its old_verify_keys dictionary, receiving servers SHOULD treat the entire
	// response payload as malformed/hostile and reject it."
	// Note: We updated our quota to match MSC4499_RETIRED_KEY_CEILING keys total
	// to accommodate the "hostile" active-key spillover behavior, so the
	// old_verify_keys ceiling here must agree with the storage-layer eviction
	// target in mod.rs.
	if counts.old_verify_keys > super::MSC4499_RETIRED_KEY_CEILING {
		return Err!(BadServerResponse(
			"Too many keys in old_verify_keys (limit: {})",
			super::MSC4499_RETIRED_KEY_CEILING
		));
	}

	let value: serde_json::Value =
		serde_json::from_str(raw).map_err(|e| conduwuit::err!(BadServerResponse("{e}")))?;

	let Some(obj) = value.as_object() else {
		return Ok(());
	};

	// Cross-map collision: same key ID with different body across sections
	if let (Some(verify_keys), Some(old_verify_keys)) = (
		obj.get("verify_keys").and_then(|v| v.as_object()),
		obj.get("old_verify_keys").and_then(|v| v.as_object()),
	) {
		for (key_id, old_val) in old_verify_keys {
			if let Some(verify_val) = verify_keys.get(key_id) {
				let old_key = old_val.get("key").and_then(|v| v.as_str());
				let new_key = verify_val.get("key").and_then(|v| v.as_str());
				if old_key != new_key {
					return Err!(BadServerResponse(
						"Cross-map collision: key ID {key_id} has different bodies in \
						 verify_keys and old_verify_keys"
					));
				}
			}
		}
	}

	Ok(())
}

#[derive(Default)]
struct SectionCounts {
	verify_keys: usize,
	old_verify_keys: usize,
}

fn scan_root_sections(bytes: &[u8]) -> Result<SectionCounts> {
	let mut counts = SectionCounts::default();
	let mut i = skip_ws(bytes, 0);
	if bytes.get(i) != Some(&b'{') {
		return Ok(counts);
	}

	i = i.saturating_add(1);
	let mut seen_verify_keys = false;
	let mut seen_old_verify_keys = false;

	loop {
		i = skip_ws(bytes, i);
		match bytes.get(i) {
			| Some(b'"') => {},
			| Some(_) | None => return Ok(counts),
		}

		let (key, next) = parse_string(bytes, i)?;
		i = skip_ws(bytes, next);
		if bytes.get(i) != Some(&b':') {
			return Ok(counts);
		}

		i = skip_ws(bytes, i.saturating_add(1));
		if contains_escapes(key) {
			let key_str = std::str::from_utf8(key).unwrap_or("<invalid utf-8>");
			return Err!(BadServerResponse(
				"JSON key '{key_str}' contains illegal escape sequences"
			));
		}

		match key {
			| b"verify_keys" => {
				if seen_verify_keys {
					return Err!(BadServerResponse("Duplicate top-level verify_keys section"));
				}
				seen_verify_keys = true;

				if bytes.get(i) != Some(&b'{') {
					return Ok(counts);
				}

				let (count, end) = scan_object_for_duplicate_keys(bytes, i, key)?;
				counts.verify_keys = count;
				i = end;
			},
			| b"old_verify_keys" => {
				if seen_old_verify_keys {
					return Err!(BadServerResponse(
						"Duplicate top-level old_verify_keys section"
					));
				}
				seen_old_verify_keys = true;

				if bytes.get(i) != Some(&b'{') {
					return Ok(counts);
				}

				let (count, end) = scan_object_for_duplicate_keys(bytes, i, key)?;
				counts.old_verify_keys = count;
				i = end;
			},
			| _ => {
				i = skip_json_value(bytes, i, 0)?;
			},
		}

		i = skip_ws(bytes, i);
		match bytes.get(i) {
			| Some(b',') => i = i.saturating_add(1),
			| Some(_) | None => return Ok(counts),
		}
	}
}

/// Walk a JSON object's top-level keys and detect duplicates.
fn scan_object_for_duplicate_keys(
	bytes: &[u8],
	start: usize,
	section_name: &[u8],
) -> Result<(usize, usize)> {
	let mut seen_keys: HashSet<Vec<u8>> = HashSet::new();
	let mut i = start.saturating_add(1);

	loop {
		i = skip_ws(bytes, i);
		match bytes.get(i) {
			| Some(b'}') => return Ok((seen_keys.len(), i.saturating_add(1))),
			| Some(b'"') => {},
			| Some(_) | None => return Ok((seen_keys.len(), i)),
		}

		let (key, next) = parse_string(bytes, i)?;
		if contains_escapes(key) {
			let section = std::str::from_utf8(section_name).unwrap_or("<invalid>");
			let key_str = std::str::from_utf8(key).unwrap_or("<invalid utf-8>");
			return Err!(BadServerResponse(
				"JSON key '{key_str}' in {section} contains illegal escape sequences"
			));
		}
		if !seen_keys.insert(key.to_vec()) {
			let section = std::str::from_utf8(section_name).unwrap_or("<invalid>");
			let key_str = std::str::from_utf8(key).unwrap_or("<invalid utf-8>");
			return Err!(BadServerResponse("Duplicate JSON key '{key_str}' in {section}"));
		}

		i = skip_ws(bytes, next);
		if bytes.get(i) != Some(&b':') {
			return Ok((seen_keys.len(), i));
		}

		i = skip_json_value(bytes, i.saturating_add(1), 0)?;
		i = skip_ws(bytes, i);
		match bytes.get(i) {
			| Some(b',') => i = i.saturating_add(1),
			| Some(b'}') => return Ok((seen_keys.len(), i.saturating_add(1))),
			| Some(_) | None => return Ok((seen_keys.len(), i)),
		}
	}
}

fn skip_json_value(bytes: &[u8], start: usize, depth: usize) -> Result<usize> {
	if depth >= MAX_JSON_SCAN_DEPTH {
		return Err!(BadServerResponse("JSON nesting depth exceeds scan limit"));
	}

	let i = skip_ws(bytes, start);
	match bytes.get(i) {
		| Some(b'"') => parse_string(bytes, i).map(|(_, next)| next),
		| Some(b'{') => skip_object(bytes, i, depth.saturating_add(1)),
		| Some(b'[') => skip_array(bytes, i, depth.saturating_add(1)),
		| Some(b'-' | b'0'..=b'9') => Ok(skip_scalar(bytes, i)),
		| Some(b't') if bytes.get(i..i.saturating_add(4)) == Some(b"true") =>
			Ok(i.saturating_add(4)),
		| Some(b'f') if bytes.get(i..i.saturating_add(5)) == Some(b"false") =>
			Ok(i.saturating_add(5)),
		| Some(b'n') if bytes.get(i..i.saturating_add(4)) == Some(b"null") =>
			Ok(i.saturating_add(4)),
		| Some(_) | None => Ok(i),
	}
}

fn skip_object(bytes: &[u8], start: usize, depth: usize) -> Result<usize> {
	let mut i = start.saturating_add(1);
	loop {
		i = skip_ws(bytes, i);
		match bytes.get(i) {
			| Some(b'}') => return Ok(i.saturating_add(1)),
			| Some(b'"') => {},
			| Some(_) | None => return Ok(i),
		}

		let (_, next) = parse_string(bytes, i)?;
		i = skip_ws(bytes, next);
		if bytes.get(i) != Some(&b':') {
			return Ok(i);
		}

		i = skip_json_value(bytes, i.saturating_add(1), depth)?;
		i = skip_ws(bytes, i);
		match bytes.get(i) {
			| Some(b',') => i = i.saturating_add(1),
			| Some(b'}') => return Ok(i.saturating_add(1)),
			| Some(_) | None => return Ok(i),
		}
	}
}

fn skip_array(bytes: &[u8], start: usize, depth: usize) -> Result<usize> {
	let mut i = start.saturating_add(1);
	loop {
		i = skip_ws(bytes, i);
		match bytes.get(i) {
			| Some(b']') => return Ok(i.saturating_add(1)),
			| Some(_) => {
				i = skip_json_value(bytes, i, depth)?;
				i = skip_ws(bytes, i);
				match bytes.get(i) {
					| Some(b',') => i = i.saturating_add(1),
					| Some(b']') => return Ok(i.saturating_add(1)),
					| Some(_) | None => return Ok(i),
				}
			},
			| None => return Ok(i),
		}
	}
}

fn parse_string(bytes: &[u8], start: usize) -> Result<(&[u8], usize)> {
	if bytes.get(start) != Some(&b'"') {
		return Ok((&[], start));
	}

	let mut i = start.saturating_add(1);
	let string_start = i;
	while i < bytes.len() {
		match bytes[i] {
			| b'\\' => i = i.saturating_add(2),
			| b'"' => return Ok((&bytes[string_start..i], i.saturating_add(1))),
			| _ => i = i.saturating_add(1),
		}
	}

	Err!(BadServerResponse("Unterminated JSON string"))
}

fn skip_scalar(bytes: &[u8], start: usize) -> usize {
	let mut i = start;
	while let Some(byte) = bytes.get(i) {
		match byte {
			| b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t' => break,
			| _ => i = i.saturating_add(1),
		}
	}

	i
}

fn skip_ws(bytes: &[u8], start: usize) -> usize {
	let mut i = start;
	while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
		i = i.saturating_add(1);
	}

	i
}

/// Check if a key contains any backslashes (JSON escapes).
/// Matrix key IDs should never contain escapes, and allowing them
/// makes duplicate detection vulnerable to semantic bypasses.
fn contains_escapes(bytes: &[u8]) -> bool { bytes.contains(&b'\\') }

#[cfg(test)]
mod tests {
	use super::{MAX_JSON_SCAN_DEPTH, check_no_duplicate_json_keys};

	#[test]
	fn no_duplicates() {
		let json =
			r#"{"verify_keys": {"ed25519:a": {"key": "AAA"}, "ed25519:b": {"key": "BBB"}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_ok());
	}

	#[test]
	fn duplicate_in_verify_keys() {
		let json =
			r#"{"verify_keys": {"ed25519:a": {"key": "AAA"}, "ed25519:a": {"key": "BBB"}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_err());
	}

	#[test]
	fn duplicate_in_old_verify_keys() {
		let json = r#"{"old_verify_keys": {"ed25519:a": {"key": "AAA", "expired_ts": 1}, "ed25519:a": {"key": "BBB", "expired_ts": 2}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_err());
	}

	#[test]
	fn duplicate_top_level_section_is_rejected() {
		let json = r#"{"verify_keys": {}, "verify_keys": {}}"#;
		assert!(check_no_duplicate_json_keys(json).is_err());
	}

	#[test]
	fn nested_decoy_section_does_not_hide_root_duplicates() {
		let json = r#"{"unsigned":{"verify_keys":{"ed25519:a":{"key":"AAA"}}},"verify_keys":{"ed25519:a":{"key":"AAA"},"ed25519:a":{"key":"BBB"}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_err());
	}

	#[test]
	fn cross_map_collision() {
		let json = r#"{"verify_keys": {"ed25519:a": {"key": "AAA"}}, "old_verify_keys": {"ed25519:a": {"key": "BBB", "expired_ts": 1}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_err());
	}

	#[test]
	fn cross_map_same_body_is_legal() {
		let json = r#"{"verify_keys": {"ed25519:a": {"key": "AAA"}}, "old_verify_keys": {"ed25519:a": {"key": "AAA", "expired_ts": 1}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_ok());
	}

	#[test]
	fn rejects_escaped_key() {
		let json = r#"{"verify_keys": {"ed25519:\u0061": {"key": "BBB"}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_err());
	}

	#[test]
	fn rejects_escaped_top_level_section_name() {
		let json = r#"{"verify\u005fkeys": {"ed25519:a": {"key": "BBB"}}}"#;
		assert!(check_no_duplicate_json_keys(json).is_err());
	}

	#[test]
	fn rejects_excessive_nesting_depth() {
		let mut nested = String::new();
		for _ in 0..=MAX_JSON_SCAN_DEPTH {
			nested.push('[');
		}
		nested.push('0');
		for _ in 0..=MAX_JSON_SCAN_DEPTH {
			nested.push(']');
		}

		let json = format!(r#"{{"unsigned":{},"verify_keys":{{}}}}"#, nested);
		assert!(check_no_duplicate_json_keys(&json).is_err());
	}
}
