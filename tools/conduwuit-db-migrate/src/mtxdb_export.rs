//! Read-only conversion of the legacy RocksDB event and signing-key maps to
//! the `mtxdb-export/v1` files accepted by `mtxdb import`.

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	fs::{self, OpenOptions},
	io::{BufWriter, Write},
	path::{Path, PathBuf},
};

use rust_rocksdb::{DB, IteratorMode, Options};
use slipstream::{
	OwnedUserId, codec,
	json::{Object, Value},
};

const FORMAT: &str = "mtxdb-export/v1";
type ExportResult<T> = Result<T, Box<dyn std::error::Error>>;
type DbRow = (Vec<u8>, Vec<u8>);

#[derive(Clone)]
struct Event {
	id: String,
	room: String,
	outlier: bool,
	rejected: bool,
	soft_failed: bool,
	rejection_code: Option<u8>,
	soft_fail_code: Option<u8>,
	pdu: Value,
	prev: Value,
	auth: Value,
}

fn rows(db: &DB, name: &str) -> ExportResult<Vec<DbRow>> {
	let Some(cf) = db.cf_handle(name) else {
		return Ok(Vec::new());
	};
	db.iterator_cf(cf, IteratorMode::Start)
		.map(|row| {
			let (key, value) = row?;
			Ok((key.to_vec(), value.to_vec()))
		})
		.collect()
}

/// Local account map keys are full Matrix user IDs, whose server part is the
/// homeserver name used when the database was created.
fn infer_server_name(db: &DB) -> Result<String, Box<dyn std::error::Error>> {
	let Some(cf) = db.cf_handle("userid_password") else {
		return Err(
			"cannot infer server name: userid_password map is missing; pass an override".into()
		);
	};
	let mut names = std::collections::BTreeSet::new();
	for row in db.iterator_cf(cf, IteratorMode::Start) {
		let (key, _) = row?;
		if let Ok(raw_id) = std::str::from_utf8(&key) {
			if let Ok(user_id) = OwnedUserId::parse(raw_id) {
				names.insert(user_id.server_name().to_string());
			}
		}
	}
	match names.len() {
		| 1 => Ok(names.into_iter().next().expect("one inferred name")),
		| 0 =>
			Err("cannot infer server name: database has no local user IDs; pass an override"
				.into()),
		| _ => Err(format!(
			"cannot infer one server name: local user IDs use {}; pass an override",
			names.into_iter().collect::<Vec<_>>().join(", ")
		)
		.into()),
	}
}

fn parse_pdu(
	raw: &[u8],
	fallback_id: Option<&str>,
	outlier: bool,
	rejections: &HashMap<String, u8>,
	soft_fails: &HashMap<String, u8>,
	event_to_room: &mut HashMap<String, String>,
) -> Result<Event, String> {
	let mut pdu = Value::parse_bytes(raw).map_err(|error| error.to_string())?;
	let object = pdu.as_object_mut().ok_or("PDU JSON is not an object")?;
	let stored_id = object.get("event_id").and_then(Value::as_str);
	if let (Some(stored), Some(fallback)) = (stored_id, fallback_id)
		&& stored != fallback
	{
		return Err(format!("PDU event_id {stored} disagrees with map key {fallback}"));
	}
	let event_id = stored_id
		.or(fallback_id)
		.ok_or("event ID missing from PDU and map key")?
		.to_owned();
	let room = match object.get("room_id").and_then(Value::as_str) {
		Some(r) => {
			let room = r.to_owned();
			event_to_room.insert(event_id.clone(), room.clone());
			room
		},
		None => {
			let is_create = object.get("type").and_then(Value::as_str) == Some("m.room.create");
			let inferred = event_to_room
				.get(&event_id)
				.cloned()
				.or_else(|| {
					if is_create && event_id.starts_with('$') {
						let derived = format!("!{}", &event_id[1..]);
						Some(derived)
					} else {
						None
					}
				})
				.or_else(|| {
					if let Some(auths) = object.get("auth_events").and_then(Value::as_array) {
						for auth in auths {
							if let Some(id) = auth.as_str() {
								if let Some(r) = event_to_room.get(id) {
									return Some(r.clone());
								}
							}
						}
					}
					if let Some(prevs) = object.get("prev_events").and_then(Value::as_array) {
						for prev in prevs {
							if let Some(id) = prev.as_str() {
								if let Some(r) = event_to_room.get(id) {
									return Some(r.clone());
								}
							}
						}
					}
					None
				});
			match inferred {
				Some(r) => {
					object.insert("room_id".into(), Value::String(r.clone()));
					event_to_room.insert(event_id.clone(), r.clone());
					r
				},
				None => {
					eprintln!("Unresolvable PDU {event_id}: {}", String::from_utf8_lossy(raw));
					return Err("PDU has no string room_id and room could not be inferred".into());
				},
			}
		},
	};
	if !object.contains_key("event_id") {
		object.insert("event_id".into(), Value::String(event_id.clone()));
	}
	Ok(Event {
		rejected: rejections.contains_key(&event_id),
		soft_failed: soft_fails.contains_key(&event_id),
		rejection_code: rejections.get(&event_id).copied(),
		soft_fail_code: soft_fails.get(&event_id).copied(),
		id: event_id,
		room,
		outlier,
		prev: object
			.get("prev_events")
			.filter(|v| v.is_array())
			.cloned()
			.unwrap_or_else(|| Value::Array(Vec::new())),
		auth: object
			.get("auth_events")
			.filter(|v| v.is_array())
			.cloned()
			.unwrap_or_else(|| Value::Array(Vec::new())),
		pdu,
	})
}

fn safe_name(room_id: &str) -> String {
	room_id
		.as_bytes()
		.iter()
		.map(|byte| format!("{byte:02x}"))
		.collect()
}

fn hex(bytes: &[u8]) -> String {
	const DIGITS: &[u8; 16] = b"0123456789abcdef";
	let mut output = String::with_capacity(bytes.len().saturating_mul(2));
	for &byte in bytes {
		output.push(char::from(DIGITS[usize::from(byte >> 4)]));
		output.push(char::from(DIGITS[usize::from(byte & 0x0F)]));
	}
	output
}

fn event_line(out: &mut impl Write, event: &Event) -> Result<(), Box<dyn std::error::Error>> {
	let mut record = Object::new();
	record.insert("t".into(), Value::String("event".into()));
	record.insert("event_id".into(), Value::String(event.id.clone()));
	record.insert("prev_events".into(), event.prev.clone());
	record.insert("auth_events".into(), event.auth.clone());
	record.insert("outlier".into(), Value::Bool(event.outlier));
	record.insert("soft_failed".into(), Value::Bool(event.soft_failed));
	record.insert("rejected".into(), Value::Bool(event.rejected));
	if let Some(code) = event.soft_fail_code {
		record.insert("soft_fail_code".into(), codec::to_value(&code));
	}
	if let Some(code) = event.rejection_code {
		record.insert("rejection_code".into(), codec::to_value(&code));
	}
	record.insert("pdu".into(), event.pdu.clone());
	out.write_all(codec::to_string(&Value::Object(record)).as_bytes())?;
	out.write_all(b"\n")?;
	Ok(())
}

fn write_room(
	out_dir: &Path,
	server: &str,
	room: &str,
	mut events: Vec<Event>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
	events.sort_by(|a, b| a.id.cmp(&b.id));
	events = dependency_order(events);
	let path = out_dir.join(format!("room-{}.jsonl", safe_name(room)));
	let mut writer = BufWriter::new(
		OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&path)?,
	);
	let mut header = Object::new();
	header.insert("t".into(), Value::String("header".into()));
	header.insert("format".into(), Value::String(FORMAT.into()));
	header.insert("server".into(), Value::String(server.into()));
	header.insert("room_id".into(), Value::String(room.into()));
	writer.write_all(codec::to_string(&Value::Object(header)).as_bytes())?;
	writer.write_all(b"\n")?;
	for event in &events {
		event_line(&mut writer, event)?;
	}
	let mut end = Object::new();
	end.insert("t".into(), Value::String("end".into()));
	end.insert("events".into(), codec::to_value(&events.len()));
	writer.write_all(codec::to_string(&Value::Object(end)).as_bytes())?;
	writer.write_all(b"\n")?;
	writer.flush()?;
	Ok(path)
}

fn dependency_order(events: Vec<Event>) -> Vec<Event> {
	let ids: HashMap<String, usize> = events
		.iter()
		.enumerate()
		.map(|(index, event)| (event.id.clone(), index))
		.collect();
	let mut children = vec![Vec::new(); events.len()];
	let mut indegree = vec![0_usize; events.len()];
	for (index, event) in events.iter().enumerate() {
		let mut dependencies = HashSet::new();
		for value in event
			.prev
			.as_array()
			.into_iter()
			.flatten()
			.chain(event.auth.as_array().into_iter().flatten())
		{
			let id = value
				.as_str()
				.or_else(|| value.get("event_id").and_then(Value::as_str));
			if let Some(dependency) = id.and_then(|id| ids.get(id)).copied()
				&& dependency != index
				&& dependencies.insert(dependency)
			{
				children[dependency].push(index);
				indegree[index] += 1;
			}
		}
	}
	let mut ready: std::collections::BTreeSet<usize> = indegree
		.iter()
		.enumerate()
		.filter_map(|(index, &degree)| (degree == 0).then_some(index))
		.collect();
	let mut ordered = Vec::with_capacity(events.len());
	while let Some(index) = ready.pop_first() {
		for &child in &children[index] {
			indegree[child] -= 1;
			if indegree[child] == 0 {
				ready.insert(child);
			}
		}
		ordered.push(index);
	}
	// Keep cyclic or malformed leftovers so the importer can report its own
	// missing/auth findings rather than silently dropping source events.
	for (index, degree) in indegree.into_iter().enumerate() {
		if degree != 0 {
			ordered.push(index);
		}
	}
	let mut slots: Vec<Option<Event>> = events.into_iter().map(Some).collect();
	ordered
		.into_iter()
		.filter_map(|index| slots[index].take())
		.collect()
}

fn write_keys(db: &DB, out_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
	let mut writer = BufWriter::new(
		OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(out_path)?,
	);
	let mut count = 0;
	let mut records: BTreeMap<(String, String), Object> = BTreeMap::new();
	for (key, raw) in rows(db, "server_signingkeys")? {
		let key = std::str::from_utf8(&key)?;
		let Some((origin, historical)) = key
			.strip_suffix("\0historical")
			.map(|origin| (origin, true))
			.or(Some((key, false)))
		else {
			continue;
		};
		let stored: Value = Value::parse_bytes(&raw)
			.map_err(|error| format!("invalid server_signingkeys JSON for {origin}: {error}"))?;
		let valid_until = stored
			.get("valid_until_ts")
			.and_then(Value::as_u64)
			.ok_or_else(|| {
				format!(
					"server_signingkeys entry for {origin} has a missing or invalid \
					 valid_until_ts"
				)
			})?;
		for field in ["verify_keys", "old_verify_keys"] {
			let Some(keys) = stored.get(field).and_then(Value::as_object) else { continue };
			for (key_id, key_value) in keys {
				let Some(encoded_key) = key_value.get("key").and_then(Value::as_str) else {
					return Err(format!(
						"server_signingkeys entry for {origin}/{key_id} has no string key"
					)
					.into());
				};
				let retired = key_value.get("expired_ts").and_then(Value::as_u64);
				// A key present in several maps or rows keeps its retired entry so
				// the recorded retirement time is never lost; otherwise first wins.
				let id = (origin.to_owned(), key_id.to_owned());
				if records
					.get(&id)
					.is_some_and(|existing| existing.contains_key("expired_ts") || retired.is_none())
				{
					continue;
				}
				let mut record = Object::new();
				record.insert("t".into(), Value::String("server_keys".into()));
				record.insert("origin".into(), Value::String(origin.into()));
				record.insert("kind".into(), Value::String("permanent".into()));
				record.insert("key_id".into(), Value::String(key_id.clone()));
				record.insert("key".into(), Value::String(encoded_key.into()));
				record.insert("valid_until_ts".into(), codec::to_value(&valid_until));
				record.insert(
					"source".into(),
					Value::String(
						if historical { "legacy-history" } else { "legacy-rocksdb" }.into(),
					),
				);
				record.insert("ts_added_ms".into(), codec::to_value(&0_u64));
				if let Some(expired) = key_value.get("expired_ts").and_then(Value::as_u64) {
					record.insert("expired_ts".into(), codec::to_value(&expired));
				}
				records.insert(id, record);
			}
		}
	}
	for record in records.into_values() {
		writer.write_all(codec::to_string(&Value::Object(record)).as_bytes())?;
		writer.write_all(b"\n")?;
		count += 1;
	}
	writer.flush()?;
	if count == 0 {
		drop(writer);
		fs::remove_file(out_path)?;
	}
	Ok(count)
}

fn verdicts(db: &DB, map: &str) -> Result<HashMap<String, u8>, Box<dyn std::error::Error>> {
	let mut result = HashMap::new();
	for (event_id, value) in rows(db, map)? {
		let event_id = std::str::from_utf8(&event_id)
			.map_err(|error| format!("invalid event ID in {map}: {error}"))?;
		let [code] = value.as_slice() else {
			return Err(format!("{map} value for {event_id} is not one byte").into());
		};
		result.insert(event_id.to_owned(), *code);
	}
	Ok(result)
}

/// Exports legacy room events and cached remote signing keys as importer-ready
/// files. The RocksDB is opened read-only; the output directory must not exist.
pub fn export(
	rocks_path: &Path,
	out_dir: &Path,
	server_override: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
	if out_dir.exists() {
		return Err(
			format!("refusing to overwrite output directory: {}", out_dir.display()).into()
		);
	}
	let mut options = Options::default();
	options.create_if_missing(false);
	options.set_track_and_verify_wals_in_manifest(true);
	let names = DB::list_cf(&options, rocks_path)?;
	let db = DB::open_cf_for_read_only(&options, rocks_path, &names, false)?;
	let inferred_server = if server_override.is_none() {
		Some(infer_server_name(&db)?)
	} else {
		None
	};
	let server = server_override
		.or(inferred_server.as_deref())
		.expect("server name available");
	let rejections = verdicts(&db, "eventid_rejections")?;
	let soft_fails = verdicts(&db, "eventid_softfailed")?;

	let mut short_to_room: HashMap<[u8; 8], String> = HashMap::new();
	if let Some(cf) = db.cf_handle("roomid_shortroomid") {
		for row in db.iterator_cf(cf, IteratorMode::Start) {
			let (room_id, short_bytes) = row?;
			if short_bytes.len() == 8 {
				let mut short = [0u8; 8];
				short.copy_from_slice(&short_bytes);
				if let Ok(room_str) = std::str::from_utf8(&room_id) {
					short_to_room.insert(short, room_str.to_owned());
				}
			}
		}
	}

	let mut event_to_room: HashMap<String, String> = HashMap::new();
	let mut timeline_ids: HashMap<Vec<u8>, String> = HashMap::new();
	for (event_id, pdu_id) in rows(&db, "eventid_pduid")? {
		if let Ok(event_id_str) = std::str::from_utf8(&event_id) {
			timeline_ids.insert(pdu_id.clone(), event_id_str.to_owned());
			if pdu_id.len() >= 8 {
				let mut short = [0u8; 8];
				short.copy_from_slice(&pdu_id[0..8]);
				if let Some(room) = short_to_room.get(&short) {
					event_to_room.insert(event_id_str.to_owned(), room.clone());
				}
			}
		}
	}
	for (pdu_id, event_id) in rows(&db, "room_pducount_eventid")? {
		if let Ok(event_id_str) = std::str::from_utf8(&event_id) {
			timeline_ids.insert(pdu_id.clone(), event_id_str.to_owned());
			if pdu_id.len() >= 8 {
				let mut short = [0u8; 8];
				short.copy_from_slice(&pdu_id[0..8]);
				if let Some(room) = short_to_room.get(&short) {
					event_to_room.insert(event_id_str.to_owned(), room.clone());
				}
			}
		}
	}
	let mut events: BTreeMap<String, BTreeMap<String, Event>> = BTreeMap::new();
	let mut add = |event: Event| {
		events
			.entry(event.room.clone())
			.or_default()
			.entry(event.id.clone())
			.or_insert(event);
	};
	// Current layout: eventid_pdu is the source of truth; eventid_pduid marks
	// timeline events while records without it are outliers.
	let timeline_set: HashSet<String> = timeline_ids.values().cloned().collect();
	for (event_id, raw) in rows(&db, "eventid_pdu")? {
		let fallback = std::str::from_utf8(&event_id).ok();
		let event = parse_pdu(
			&raw,
			fallback,
			!fallback.is_some_and(|id| timeline_set.contains(id)),
			&rejections,
			&soft_fails,
			&mut event_to_room,
		)
		.map_err(|error| {
			format!("bad eventid_pdu row {}: {error}", String::from_utf8_lossy(&event_id))
		})?;
		add(event);
	}
	// Legacy layout: timeline PDUs were keyed by their packed PDU id.
	for (pdu_id, raw) in rows(&db, "pduid_pdu")? {
		let fallback = timeline_ids.get(&pdu_id).map(String::as_str);
		let event = parse_pdu(
			&raw,
			fallback,
			false,
			&rejections,
			&soft_fails,
			&mut event_to_room,
		)
		.map_err(|error| format!("bad pduid_pdu row {}: {error}", hex(&pdu_id)))?;
		add(event);
	}
	for (event_id, raw) in rows(&db, "eventid_outlierpdu")? {
		let fallback = std::str::from_utf8(&event_id).ok();
		let event = parse_pdu(
			&raw,
			fallback,
			true,
			&rejections,
			&soft_fails,
			&mut event_to_room,
		)
		.map_err(|error| {
			format!(
				"bad eventid_outlierpdu row {}: {error}",
				String::from_utf8_lossy(&event_id)
			)
		})?;
		add(event);
	}
	let exported_ids: HashSet<&str> = events
		.values()
		.flat_map(|room| room.keys().map(String::as_str))
		.collect();
	for event_id in rejections.keys().chain(soft_fails.keys()) {
		if !exported_ids.contains(event_id.as_str()) {
			return Err(
				format!("verdict exists for event {event_id}, but no PDU was exported").into()
			);
		}
	}
	if events.is_empty() {
		return Err("found no readable room PDUs in eventid_pdu, pduid_pdu, or \
		            eventid_outlierpdu"
			.into());
	}
	let stage = out_dir.with_extension(format!("exporting-{}", std::process::id()));
	fs::create_dir(&stage)?;
	let result = (|| -> Result<(), Box<dyn std::error::Error>> {
		let mut total = 0usize;
		for (room, by_id) in events {
			let count = by_id.len();
			write_room(&stage, server, &room, by_id.into_values().collect())?;
			total += count;
		}
		let keys = write_keys(&db, &stage.join("server-keys.jsonl"))?;
		fs::rename(&stage, out_dir)?;
		eprintln!(
			"Exported {total} room events and {keys} signing-key records to {}",
			out_dir.display()
		);
		Ok(())
	})();
	if result.is_err() {
		let _ = fs::remove_dir_all(&stage);
	}
	result
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn retired_key_keeps_expired_ts_over_stale_active_entry() {
		let dir = std::env::temp_dir().join(format!("mtxdb-export-keys-{}", std::process::id()));
		let _ = fs::remove_dir_all(&dir);
		let mut options = Options::default();
		options.create_if_missing(true);
		options.create_missing_column_families(true);
		let db = DB::open_cf(&options, &dir, ["server_signingkeys"]).unwrap();
		let cf = db.cf_handle("server_signingkeys").unwrap();
		// Legacy cumulative writer: same ID left in both maps after retirement.
		let stale = br#"{"valid_until_ts":10,"verify_keys":{"ed25519:a":{"key":"AAAA"}},"old_verify_keys":{"ed25519:a":{"key":"AAAA","expired_ts":5}}}"#;
		// Latest raw response no longer includes the key.
		let latest = br#"{"valid_until_ts":20,"verify_keys":{"ed25519:b":{"key":"BBBB"}}}"#;
		db.put_cf(cf, "example.org\0historical", stale).unwrap();
		db.put_cf(cf, "example.org", latest).unwrap();

		let out = dir.join("keys.jsonl");
		let count = write_keys(&db, &out).unwrap();
		let written = fs::read_to_string(&out).unwrap();
		drop(db);
		let _ = fs::remove_dir_all(&dir);

		assert_eq!(count, 2);
		let retired = written
			.lines()
			.find(|line| line.contains("ed25519:a"))
			.unwrap();
		assert!(retired.contains("\"expired_ts\":5"), "{retired}");
	}
}
