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
) -> Result<(Event, bool), String> {
	let mut pdu = Value::parse_bytes(raw).map_err(|error| error.to_string())?;
	let object = pdu.as_object_mut().ok_or("PDU JSON is not an object")?;
	let stored_id = object.get("event_id").and_then(Value::as_str);
	// Legacy indexes can outlive the PDU they pointed at. The stored PDU's
	// own event_id is authoritative; a differing map key is reported and the
	// event is treated as an outlier so it cannot corrupt timeline order.
	let id_mismatch = matches!(
		(stored_id, fallback_id),
		(Some(stored), Some(fallback)) if stored != fallback
	);
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
	Ok((Event {
		rejected: rejections.contains_key(&event_id),
		soft_failed: soft_fails.contains_key(&event_id),
		rejection_code: rejections.get(&event_id).copied(),
		soft_fail_code: soft_fails.get(&event_id).copied(),
		id: event_id,
		room,
		outlier: outlier || id_mismatch,
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
	}, id_mismatch))
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

fn union_verdicts(
	db: &DB,
	map: &str,
	verdicts: &mut HashMap<String, u8>,
	code: u8,
) -> ExportResult<usize> {
	let mut count = 0;
	for (event_id, _) in rows(db, map)? {
		let event_id = std::str::from_utf8(&event_id)
			.map_err(|error| format!("invalid event ID in {map}: {error}"))?;
		verdicts.entry(event_id.to_owned()).or_insert(code);
		count += 1;
	}
	Ok(count)
}

fn legacy_status_verdicts(
	db: &DB,
	map: &str,
	rejections: &mut HashMap<String, u8>,
	soft_fails: &mut HashMap<String, u8>,
) -> ExportResult<usize> {
	let mut count = 0;
	for (event_id, value) in rows(db, map)? {
		let event_id = std::str::from_utf8(&event_id)
			.map_err(|error| format!("invalid event ID in {map}: {error}"))?;
		let (kind, code) = match value.as_slice() {
			[1, code] => (1, *code),
			[2, code] => (2, *code),
			_ => return Err(format!("{map} value for {event_id} is not [kind, code]").into()),
		};
		let target = if kind == 1 { &mut *soft_fails } else { &mut *rejections };
		target.entry(event_id.to_owned()).or_insert(code);
		count += 1;
	}
	Ok(count)
}

/// Minimal reader for bincode (fixint, little-endian) rows. The legacy
/// `eventid_metadata` layouts are fixed, so reading them by hand avoids a
/// bincode dependency in this tool.
struct Raw<'a>(&'a [u8]);

impl Raw<'_> {
	fn take(&mut self, n: usize) -> Option<&[u8]> {
		let (head, tail) = self.0.split_at_checked(n)?;
		self.0 = tail;
		Some(head)
	}

	fn u8(&mut self) -> Option<u8> { self.take(1).map(|b| b[0]) }

	fn u32(&mut self) -> Option<u32> {
		Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
	}

	fn u64(&mut self) -> Option<u64> {
		Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
	}

	fn string(&mut self) -> Option<()> {
		let len = usize::try_from(self.u64()?).ok()?;
		std::str::from_utf8(self.take(len)?).ok().map(drop)
	}

	fn option(&mut self, mut inner: impl FnMut(&mut Self) -> Option<()>) -> Option<()> {
		match self.u8()? {
			| 0 => Some(()),
			| 1 => inner(self),
			| _ => None,
		}
	}

	fn flag(&mut self) -> Option<bool> {
		match self.u8()? {
			| 0 => Some(false),
			| 1 => Some(true),
			| _ => None,
		}
	}

	/// short_room_id, is_outlier, origin_server_ts, depth
	fn prefix(&mut self) -> Option<()> {
		self.u64()?;
		self.flag()?;
		self.u64()?;
		self.u64()?;
		Some(())
	}

	/// redacted_by, short_state_hash
	fn redacted_and_state(&mut self) -> Option<()> {
		self.option(Raw::string)?;
		self.option(|r| r.u64().map(drop))
	}

	/// deprecated_local_topo_depth, pdu_count
	fn topo_and_count(&mut self) -> Option<()> {
		self.u64()?;
		self.option(|r| r.u64().map(drop))
	}

	fn done(&self) -> Option<()> { self.0.is_empty().then_some(()) }
}

/// Verdict carried by one legacy `eventid_metadata` row: (rejection code,
/// soft-fail code). Layouts are tried newest first, each requiring the whole
/// row to be consumed, like `db_lt_21`. `None` means the row matches no known
/// layout.
fn legacy_metadata_row(raw: &[u8]) -> Option<(Option<u8>, Option<u8>)> {
	// v20: status enum (Pending, Accepted, Rejected(code), SoftFailed(code)).
	let v20 = || {
		let mut r = Raw(raw);
		r.prefix()?;
		let verdict = match r.u32()? {
			| 0 | 1 => (None, None),
			| 2 => (Some(u8::try_from(r.u32()?).ok()?), None),
			| 3 => (None, Some(u8::try_from(r.u32()?).ok()?)),
			| _ => return None,
		};
		r.redacted_and_state()?;
		r.topo_and_count()?;
		r.done()?;
		Some(verdict)
	};
	// v19: soft_failed and rejected booleans plus two reason strings.
	let v19 = || {
		let mut r = Raw(raw);
		r.prefix()?;
		let (soft, rejected) = (r.flag()?, r.flag()?);
		r.redacted_and_state()?;
		r.topo_and_count()?;
		r.string()?;
		r.string()?;
		r.done()?;
		Some((rejected.then_some(11), soft.then_some(3)))
	};
	// v18: booleans only, no topology or reason fields.
	let v18 = || {
		let mut r = Raw(raw);
		r.prefix()?;
		let (soft, rejected) = (r.flag()?, r.flag()?);
		r.redacted_and_state()?;
		r.done()?;
		Some((rejected.then_some(11), soft.then_some(3)))
	};
	// v18 plus the topological depth, written before pdu_count and the reason
	// strings existed. Common on outliers in v19 databases.
	let v18_topo = || {
		let mut r = Raw(raw);
		r.prefix()?;
		let (soft, rejected) = (r.flag()?, r.flag()?);
		r.redacted_and_state()?;
		r.u64()?;
		r.done()?;
		Some((rejected.then_some(11), soft.then_some(3)))
	};
	// v21+: no verdict fields; verdicts live in the independent maps.
	let current = || {
		let mut r = Raw(raw);
		r.prefix()?;
		r.redacted_and_state()?;
		r.topo_and_count()?;
		r.done()?;
		Some((None, None))
	};
	v20().or_else(v19).or_else(v18_topo).or_else(v18).or_else(current)
}

fn legacy_metadata_verdicts(
	db: &DB,
	rejections: &mut HashMap<String, u8>,
	soft_fails: &mut HashMap<String, u8>,
) -> ExportResult<(usize, usize)> {
	let mut rejected = 0;
	let mut soft_failed = 0;
	let mut unparsed = 0;
	for (event_id, raw) in rows(db, "eventid_metadata")? {
		let event_id = std::str::from_utf8(&event_id)
			.map_err(|error| format!("invalid event ID in eventid_metadata: {error}"))?;
		let Some((rejection, soft_fail)) = legacy_metadata_row(&raw) else {
			unparsed += 1;
			continue;
		};
		if let Some(code) = rejection {
			rejections.entry(event_id.to_owned()).or_insert(code);
			rejected += 1;
		}
		if let Some(code) = soft_fail {
			soft_fails.entry(event_id.to_owned()).or_insert(code);
			soft_failed += 1;
		}
	}
	if unparsed > 0 {
		eprintln!("warning: {unparsed} eventid_metadata rows matched no known layout");
	}
	Ok((rejected, soft_failed))
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
	let mut rejections = verdicts(&db, "eventid_rejections")?;
	let mut soft_fails = verdicts(&db, "eventid_softfailed")?;
	let event_status = legacy_status_verdicts(
		&db,
		"eventid_status",
		&mut rejections,
		&mut soft_fails,
	)?;
	let metadata_verdicts = legacy_metadata_verdicts(&db, &mut rejections, &mut soft_fails)?;
	let retired_rejections = union_verdicts(
		&db,
		"rejectedeventids",
		&mut rejections,
		11,
	)?;
	let admin_rejections = union_verdicts(
		&db,
		"adminrejectedeventids",
		&mut rejections,
		11,
	)?;
	let retired_soft_fails = union_verdicts(
		&db,
		"softfailedeventids",
		&mut soft_fails,
		3,
	)?;
	eprintln!(
		"Verdict sources: eventid_rejections={}, eventid_softfailed={}, eventid_status={}, eventid_metadata(rejected={}, soft_failed={}), rejectedeventids={}, adminrejectedeventids={}, softfailedeventids={}",
		verdicts(&db, "eventid_rejections")?.len(),
		verdicts(&db, "eventid_softfailed")?.len(),
		event_status,
		metadata_verdicts.0,
		metadata_verdicts.1,
		retired_rejections,
		admin_rejections,
		retired_soft_fails,
	);

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
	let mut id_mismatches = 0usize;
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
		let (event, mismatch) = parse_pdu(
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
		id_mismatches += usize::from(mismatch);
		add(event);
	}
	// Legacy layout: timeline PDUs were keyed by their packed PDU id.
	for (pdu_id, raw) in rows(&db, "pduid_pdu")? {
		// The PDU body is authoritative for its event ID. The eventid_pduid /
		// room_pducount_eventid indexes can be stale in old databases, so never
		// use a secondary index as the fallback identity. These rows are
		// timeline PDUs, so pass the fallback as None and keep outlier=false.
		let (event, mismatch) = parse_pdu(
			&raw,
			None,
			false,
			&rejections,
			&soft_fails,
			&mut event_to_room,
		)
		.map_err(|error| format!("bad pduid_pdu row {}: {error}", hex(&pdu_id)))?;
		id_mismatches += usize::from(mismatch);
		add(event);
	}
	for (event_id, raw) in rows(&db, "eventid_outlierpdu")? {
		let fallback = std::str::from_utf8(&event_id).ok();
		let (event, mismatch) = parse_pdu(
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
		id_mismatches += usize::from(mismatch);
		add(event);
	}
	if id_mismatches > 0 {
		eprintln!(
			"warning: {id_mismatches} PDU event IDs disagreed with their map keys; used the PDU event ID and treated the mismatched rows as outliers"
		);
	}
	let exported_ids: HashSet<&str> = events
		.values()
		.flat_map(|room| room.keys().map(String::as_str))
		.collect();
	let orphan_verdicts: HashSet<&str> = rejections
		.keys()
		.chain(soft_fails.keys())
		.map(String::as_str)
		.filter(|event_id| !exported_ids.contains(event_id))
		.collect();
	if !orphan_verdicts.is_empty() {
		eprintln!(
			"warning: dropping verdicts for {} events with no exported PDU (for example {})",
			orphan_verdicts.len(),
			orphan_verdicts.iter().next().expect("non-empty orphan set"),
		);
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

	fn meta_prefix(out: &mut Vec<u8>) {
		out.extend(7_u64.to_le_bytes());
		out.push(0);
		out.extend(1_u64.to_le_bytes());
		out.extend(2_u64.to_le_bytes());
	}

	fn meta_tail(out: &mut Vec<u8>) {
		out.push(0); // redacted_by
		out.push(0); // short_state_hash
	}

	#[test]
	fn legacy_metadata_layouts_yield_verdicts() {
		let mut v20 = Vec::new();
		meta_prefix(&mut v20);
		v20.extend(2_u32.to_le_bytes());
		v20.extend(7_u32.to_le_bytes());
		meta_tail(&mut v20);
		v20.extend(3_u64.to_le_bytes());
		v20.push(0);
		assert_eq!(legacy_metadata_row(&v20), Some((Some(7), None)));

		let mut v19 = Vec::new();
		meta_prefix(&mut v19);
		v19.extend([1, 0]);
		meta_tail(&mut v19);
		v19.extend(3_u64.to_le_bytes());
		v19.push(0);
		v19.extend(1_u64.to_le_bytes());
		v19.push(b'x');
		v19.extend(0_u64.to_le_bytes());
		assert_eq!(legacy_metadata_row(&v19), Some((None, Some(3))));

		let mut v18 = Vec::new();
		meta_prefix(&mut v18);
		v18.extend([0, 1]);
		meta_tail(&mut v18);
		assert_eq!(legacy_metadata_row(&v18), Some((Some(11), None)));

		let mut current = Vec::new();
		meta_prefix(&mut current);
		meta_tail(&mut current);
		current.extend(3_u64.to_le_bytes());
		current.push(0);
		assert_eq!(legacy_metadata_row(&current), Some((None, None)));

		assert_eq!(legacy_metadata_row(b"{}"), None);
	}

	#[test]
	fn legacy_metadata_v20_statuses_and_truncation() {
		let v20 = |tag: u32, code: Option<u32>| {
			let mut row = Vec::new();
			meta_prefix(&mut row);
			row.extend(tag.to_le_bytes());
			if let Some(code) = code {
				row.extend(code.to_le_bytes());
			}
			meta_tail(&mut row);
			row.extend(3_u64.to_le_bytes());
			row.push(0);
			row
		};
		assert_eq!(legacy_metadata_row(&v20(0, None)), Some((None, None)));
		assert_eq!(legacy_metadata_row(&v20(1, None)), Some((None, None)));
		assert_eq!(legacy_metadata_row(&v20(3, Some(2))), Some((None, Some(2))));
		assert_eq!(legacy_metadata_row(&v20(4, None)), None);

		let mut truncated = v20(2, Some(7));
		truncated.pop();
		assert_eq!(legacy_metadata_row(&truncated), None);
	}

	#[test]
	fn legacy_metadata_v18_with_topo_depth() {
		let mut row = Vec::new();
		meta_prefix(&mut row);
		row.extend([0, 1]); // soft_failed, rejected
		meta_tail(&mut row);
		row.extend(9_u64.to_le_bytes());
		assert_eq!(legacy_metadata_row(&row), Some((Some(11), None)));

		// Redacted variant: redacted_by = Some("$a").
		let mut redacted = Vec::new();
		meta_prefix(&mut redacted);
		redacted.extend([1, 0]);
		redacted.push(1);
		redacted.extend(2_u64.to_le_bytes());
		redacted.extend(b"$a");
		redacted.push(0);
		redacted.extend(9_u64.to_le_bytes());
		assert_eq!(legacy_metadata_row(&redacted), Some((None, Some(3))));
	}

	#[test]
	fn stored_pdu_id_wins_over_stale_index() {
		let raw = br#"{"event_id":"$stored","room_id":"!room:example.org","prev_events":[],"auth_events":[]}"#;
		let mut rooms = HashMap::new();
		let (event, mismatch) = parse_pdu(
			raw,
			Some("$stale"),
			false,
			&HashMap::new(),
			&HashMap::new(),
			&mut rooms,
		)
		.unwrap();
		assert!(mismatch);
		assert!(event.outlier);
		assert_eq!(event.id, "$stored");
	}
}
