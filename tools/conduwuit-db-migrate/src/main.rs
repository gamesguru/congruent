use std::{
	collections::HashSet,
	env,
	fs::{File, OpenOptions},
	io::{self, BufRead, BufReader, BufWriter, Write},
	path::{Path, PathBuf},
};

use redb::{Database, TableDefinition};
use rust_rocksdb::{DB, IteratorMode, Options};

mod mtxdb_export;

const FORMAT: &str = "conduwuit-metadata/v1";
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("conduwuit_metadata");

// These records are exported through the room and signing-key mtxdb JSONL
// formats, or are derived from those exports. Copying them into redb would
// leave stale legacy indexes beside the newly imported mtxdb data. The legacy
// state history and short-ID maps are skipped for the same reason: v23
// rebuilds the HAMT roots from the timeline, and mtxdb owns the short-ID index.
// The root handles and HAMT node maps stay out because v23 rewrites them.
const MTXDB_OWNED_MAPS: &[&str] = &[
	"eventid_outlierpdu",
	"eventid_pdu",
	"eventid_pduid",
	"eventid_rejections",
	"eventid_softfailed",
	"eventid_status",
	"pduid_pdu",
	"room_pducount_eventid",
	"room_pducount_eventid_backup",
	"roomid_depth_missingeventid",
	"roomid_missingeventid_depth",
	"roomid_pduleaves",
	"roomid_roothandle",
	"roomid_timestamp_pducount",
	"roomid_topologicalorder_pducount",
	"server_signingkeys",
	"shorteventid_authchain",
	"shorteventid_roothandle",
	"shorteventid_shortauthevents",
	"shorteventid_shortprevevents",
	"shorteventid_shortstatehash",
	"state_hamt_node_mtimes",
	"state_hamt_nodes",
	"state_hamt_root_lattices",
	"statehash_shortstatehash",
];

// Derived indexes and legacy inputs that are no longer needed. The mtxdb
// import builds its own short-ID index from event identities, v23 rebuilds
// state HAMTs from the timeline rather than the legacy statediff history or
// `roomid_shortstatehash` room selection, and search scans event bodies
// instead of the `tokenids` inverted index. `roomsynctoken_shortstatehash` is a
// sync-token cache keyed by legacy short IDs; v24 clears it, so copying it
// would only import stale entries.
//
// The room caches below are derived after v23 rebuilds state: membership,
// counts and server lists come from the state HAMT via
// `reconcile_membership`, while the DAG indexes and once-joined markers are
// derived from the event bodies during the same rebuild pass.
const SKIPPED_MAPS: &[&str] = &[
	"eventid_shorteventid",
	"referencedevents",
	"roomid_invitedcount",
	"roomid_joinedcount",
	"roomid_shortstatehash",
	"roomsynctoken_shortstatehash",
	"roomserverids",
	"roomuserid_joined",
	"roomuseroncejoinedids",
	"serverroomids",
	"shorteventid_eventid",
	"shortstatehash_statediff",
	"shortstatekey_statekey",
	"statekey_shortstatekey",
	"tofrom_relation",
	"tokenids",
	"userroomid_joined",
];

fn skipped(name: &str) -> bool { MTXDB_OWNED_MAPS.contains(&name) || SKIPPED_MAPS.contains(&name) }

fn usage() -> ! {
	eprintln!("usage: conduwuit-db-migrate export <rocksdb> <bundle.jsonl> [map ...]");
	eprintln!("       conduwuit-db-migrate export-mtxdb <rocksdb> <output-dir> [server-name]");
	eprintln!("       conduwuit-db-migrate import <bundle.jsonl> <new-metadata.redb>");
	eprintln!("       conduwuit-db-migrate compact <metadata.redb>");
	eprintln!("       conduwuit-db-migrate list <rocksdb>");
	eprintln!("       conduwuit-db-migrate stats <rocksdb>");
	std::process::exit(2);
}

fn composite_key(map: &[u8], key: &[u8]) -> io::Result<Vec<u8>> {
	let map_len = u32::try_from(map.len()).map_err(|_| {
		io::Error::new(io::ErrorKind::InvalidInput, "map name exceeds u32 length")
	})?;
	let capacity = 4_usize
		.checked_add(map.len())
		.and_then(|size| size.checked_add(key.len()))
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "composite key too large"))?;
	let mut output = Vec::with_capacity(capacity);
	// Must match src/database/engine/redb.rs::composite_key exactly.
	output.extend_from_slice(&map_len.to_be_bytes());
	output.extend_from_slice(map);
	output.extend_from_slice(key);
	Ok(output)
}

fn hex(bytes: &[u8]) -> String {
	const DIGITS: &[u8; 16] = b"0123456789abcdef";
	let mut output = String::with_capacity(bytes.len().saturating_mul(2));
	for byte in bytes {
		output.push(char::from(DIGITS[usize::from(byte >> 4)]));
		output.push(char::from(DIGITS[usize::from(byte & 0x0F)]));
	}
	output
}

fn unhex(input: &str) -> io::Result<Vec<u8>> {
	if !input.len().is_multiple_of(2) {
		return Err(io::Error::new(io::ErrorKind::InvalidData, "odd-length hex field"));
	}
	input
		.as_bytes()
		.as_chunks::<2>()
		.0
		.iter()
		.map(|pair| {
			let digit = |byte| match byte {
				| b'0'..=b'9' => Some(byte - b'0'),
				| b'a'..=b'f' => Some(byte - b'a' + 10),
				| b'A'..=b'F' => Some(byte - b'A' + 10),
				| _ => None,
			};
			let high = digit(pair[0])
				.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid hex field"))?;
			let low = digit(pair[1])
				.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid hex field"))?;
			Ok((high << 4) | low)
		})
		.collect()
}

fn json_line(writer: &mut impl Write, line: &str) -> io::Result<()> {
	writer.write_all(line.as_bytes())?;
	writer.write_all(b"\n")
}

fn export(
	rocks_path: &Path,
	bundle_path: &Path,
	requested: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
	if bundle_path.exists() {
		return Err(
			format!("refusing to overwrite existing bundle: {}", bundle_path.display()).into()
		);
	}
	let mut options = Options::default();
	options.create_if_missing(false);
	options.set_track_and_verify_wals_in_manifest(true);
	let names = DB::list_cf(&options, rocks_path)?;
	let selected: Vec<String> = if requested.is_empty() {
		names
			.iter()
			.filter(|name| name.as_str() != "default")
			.filter(|name| !skipped(name))
			.cloned()
			.collect()
	} else {
		for name in requested {
			if MTXDB_OWNED_MAPS.contains(&name.as_str()) {
				return Err(format!("{name} is owned by the mtxdb room/key import").into());
			}
			if SKIPPED_MAPS.contains(&name.as_str()) {
				return Err(format!("{name} is derived by the server, not migrated").into());
			}
			if !names.iter().any(|existing| existing == name) {
				return Err(format!("column family does not exist: {name}").into());
			}
		}
		requested.to_vec()
	};
	let mut unique_maps = HashSet::new();
	for map in &selected {
		if !map
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
		{
			return Err(format!(
				"column-family name cannot be represented in the JSONL manifest: {map}"
			)
			.into());
		}
		if !unique_maps.insert(map) {
			return Err(format!("column family selected more than once: {map}").into());
		}
	}

	let db = DB::open_cf_for_read_only(&options, rocks_path, &selected, false)?;
	let output = OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(bundle_path)?;
	let mut writer = BufWriter::new(output);
	let map_manifest = selected
		.iter()
		.map(|map| format!("\"{map}\""))
		.collect::<Vec<_>>()
		.join(",");
	json_line(
		&mut writer,
		&format!("{{\"type\":\"header\",\"format\":\"{FORMAT}\",\"maps\":[{map_manifest}]}}"),
	)?;
	let mut records = 0_u64;
	for map in &selected {
		let handle = db
			.cf_handle(map)
			.ok_or_else(|| format!("missing column family: {map}"))?;
		for item in db.iterator_cf(handle, IteratorMode::Start) {
			let (key, value) = item?;
			json_line(
				&mut writer,
				&format!(
					"{{\"type\":\"record\",\"map\":\"{map}\",\"key_hex\":\"{}\",\"value_hex\":\"\
					 {}\"}}",
					hex(&key),
					hex(&value),
				),
			)?;
			records = records.checked_add(1).ok_or("record count overflow")?;
		}
	}
	json_line(&mut writer, &format!("{{\"type\":\"end\",\"records\":{records}}}"))?;
	writer.flush()?;
	Ok(())
}

fn list(rocks_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
	let options = Options::default();
	for name in DB::list_cf(&options, rocks_path)? {
		let ownership = if name == "default" {
			"skipped"
		} else if MTXDB_OWNED_MAPS.contains(&name.as_str()) {
			"mtxdb"
		} else if SKIPPED_MAPS.contains(&name.as_str()) {
			"skipped (derived)"
		} else {
			"redb candidate"
		};
		println!("{ownership:<17}  {name}");
	}
	Ok(())
}

/// Count one composite-key map per entity.
///
/// Keys are `entity\xffrest` when `take_after_sep` is false and
/// `rest\xffentity` when it is true. RocksDB stores the keys raw, so this does
/// not depend on redb's serialization.
fn count_keys(
	db: &DB,
	cf: &str,
	take_after_sep: bool,
) -> Result<std::collections::HashMap<Vec<u8>, u64>, Box<dyn std::error::Error>> {
	let handle = db
		.cf_handle(cf)
		.ok_or_else(|| format!("missing column family: {cf}"))?;
	let mut counts = std::collections::HashMap::new();
	for item in db.iterator_cf(handle, IteratorMode::Start) {
		let (key, _) = item?;
		let Some(sep) = key.iter().position(|&byte| byte == 0xFF) else {
			continue;
		};
		let entity = if take_after_sep {
			&key[sep.saturating_add(1)..]
		} else {
			&key[..sep]
		};
		*counts.entry(entity.to_vec()).or_insert(0) += 1;
	}
	Ok(counts)
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
	if sorted.is_empty() {
		return 0;
	}
	let last = sorted.len().saturating_sub(1);
	let index = (last as f64 * fraction).round() as usize;
	sorted.get(index).copied().unwrap_or_default()
}

fn print_distribution(label: &str, mut values: Vec<u64>) {
	values.sort_unstable();
	let total: u64 = values.iter().sum();
	println!(
		"{label}: entities={}, total={}, min={}, p50={}, p90={}, p99={}, max={}",
		values.len(),
		total,
		values.first().copied().unwrap_or_default(),
		percentile(&values, 0.50),
		percentile(&values, 0.90),
		percentile(&values, 0.99),
		values.last().copied().unwrap_or_default(),
	);

	let buckets = [
		(0_u64, 0_u64),
		(1, 10),
		(11, 100),
		(101, 1_000),
		(1_001, 10_000),
		(10_001, 100_000),
		(100_001, u64::MAX),
	];
	let mut histogram = [0_u64; 7];
	for value in &values {
		let slot = buckets
			.iter()
			.position(|(low, high)| value >= low && value <= high)
			.unwrap_or(6);
		histogram[slot] = histogram[slot].saturating_add(1);
	}
	println!(
		"  histogram: 0={}, 1-10={}, 11-100={}, 101-1k={}, 1k-10k={}, 10k-100k={}, \
		 >100k={}",
		histogram[0],
		histogram[1],
		histogram[2],
		histogram[3],
		histogram[4],
		histogram[5],
		histogram[6],
	);
}

fn stats(rocks_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
	let options = Options::default();
	let all = DB::list_cf(&options, rocks_path)?;
	let all_names: Vec<&str> = all.iter().map(String::as_str).collect();
	// Read-only open replays WALs and crashes on some legacy stores (the
	// upstream read-only recovery path dereferences a null context). A
	// secondary instance reads the same files without that path.
	let secondary = std::env::temp_dir().join(format!(
		"conduwuit-db-stats-secondary-{}",
		std::process::id()
	));
	if secondary.exists() {
		std::fs::remove_dir_all(&secondary)?;
	}
	std::fs::create_dir_all(&secondary)?;
	let db = DB::open_cf_as_secondary(&options, rocks_path, &secondary, &all_names)?;

	let current = count_keys(&db, "roomuserid_joined", false)?;
	let once = count_keys(&db, "roomuseroncejoinedids", true)?;
	let servers = count_keys(&db, "roomserverids", false)?;

	print_distribution(
		"current members per room",
		current.values().copied().collect(),
	);
	print_distribution(
		"ever-joined per room",
		once.values().copied().collect(),
	);
	print_distribution(
		"joined servers per room",
		servers.values().copied().collect(),
	);

	let total_current: u64 = current.values().sum();
	let total_once: u64 = once.values().sum();
	let total_servers: u64 = servers.values().sum();
	let churn: u64 = once
		.iter()
		.map(|(room, ever)| ever.saturating_sub(*current.get(room).unwrap_or(&0)))
		.sum();
	println!(
		"totals: current_memberships={total_current}, ever_joined_pairs={total_once}, \
		 left_pairs={churn}, room_server_rows={total_servers}"
	);
	println!(
		"flat u32 payload: current={} bytes, reverse={} bytes, combined={} bytes",
		total_current.saturating_mul(4),
		total_current.saturating_mul(4),
		total_current.saturating_mul(8),
	);
	println!(
		"naive resident locators (10.7 B/key): current={} bytes, once={} bytes, servers={} \
		 bytes",
		total_current.saturating_mul(11),
		total_once.saturating_mul(11),
		total_servers.saturating_mul(11),
	);

	let mut rows: Vec<(Vec<u8>, u64, u64, u64)> = current
		.keys()
		.chain(once.keys())
		.chain(servers.keys())
		.collect::<std::collections::HashSet<_>>()
		.into_iter()
		.map(|room| {
			(
				room.clone(),
				*current.get(room).unwrap_or(&0),
				*once.get(room).unwrap_or(&0),
				*servers.get(room).unwrap_or(&0),
			)
		})
		.collect();
	rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)));

	println!("\ntop rooms by current members:");
	for (room, joined, ever, server_count) in rows.iter().take(20) {
		println!(
			"  {:<64} joined={joined:<8} ever={ever:<8} servers={server_count:<5} left={}",
			String::from_utf8_lossy(room),
			ever.saturating_sub(*joined),
		);
	}

	drop(db);
	std::fs::remove_dir_all(&secondary).ok();

	Ok(())
}

fn parse_record(line: &str) -> io::Result<(String, Vec<u8>, Vec<u8>)> {
	let body = line
		.strip_prefix("{\"type\":\"record\",\"map\":\"")
		.and_then(|body| body.strip_suffix("\"}"))
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed record JSONL"))?;
	let (map, rest) = body
		.split_once("\",\"key_hex\":\"")
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing map/key fields"))?;
	let (key, value) = rest
		.split_once("\",\"value_hex\":\"")
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing key/value fields"))?;
	if map.is_empty()
		|| !map
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
	{
		return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid map name"));
	}
	Ok((map.to_owned(), unhex(key)?, unhex(value)?))
}

fn temporary_path(destination: &Path) -> io::Result<PathBuf> {
	let parent = destination.parent().unwrap_or_else(|| Path::new("."));
	let name = destination
		.file_name()
		.ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidInput, "destination needs a filename")
		})?
		.to_string_lossy();
	Ok(parent.join(format!(".{name}.importing-{}", std::process::id())))
}

fn import(bundle_path: &Path, redb_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
	if redb_path.exists() {
		return Err(format!(
			"refusing to overwrite existing destination: {}",
			redb_path.display()
		)
		.into());
	}
	if let Some(parent) = redb_path.parent() {
		std::fs::create_dir_all(parent)?;
	}
	let temp_path = temporary_path(redb_path)?;
	if temp_path.exists() {
		return Err(
			format!("temporary import file already exists: {}", temp_path.display()).into()
		);
	}
	let result = (|| -> Result<(), Box<dyn std::error::Error>> {
		let mut reader = BufReader::new(File::open(bundle_path)?);
		let mut line = String::new();
		if reader.read_line(&mut line)? == 0 {
			return Err(
				io::Error::new(io::ErrorKind::InvalidData, "empty metadata bundle").into()
			);
		}
		let header = line.trim_end_matches(['\r', '\n']);
		let expected_prefix = format!("{{\"type\":\"header\",\"format\":\"{FORMAT}\",\"maps\":[");
		let map_list = header
			.strip_prefix(&expected_prefix)
			.and_then(|rest| rest.strip_suffix("]}"))
			.map(|names| {
				if names.is_empty() {
					return Ok(Vec::new());
				}
				names
					.split(',')
					.map(|name| {
						let name = name
							.strip_prefix('"')
							.and_then(|name| name.strip_suffix('"'))
							.ok_or_else(|| {
								io::Error::new(io::ErrorKind::InvalidData, "invalid map manifest")
							})?;
						if name.is_empty()
							|| !name
								.bytes()
								.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
						{
							return Err(io::Error::new(
								io::ErrorKind::InvalidData,
								"invalid map name in manifest",
							));
						}
						Ok(name.to_owned())
					})
					.collect::<io::Result<Vec<_>>>()
			})
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid bundle header"))?;
		let map_list = map_list?;
		let expected_maps: HashSet<String> = map_list.iter().cloned().collect();
		if expected_maps.len() != map_list.len() {
			return Err(
				io::Error::new(io::ErrorKind::InvalidData, "duplicate map in manifest").into()
			);
		}
		let map_count = map_list.len();
		let db = Database::create(&temp_path)?;
		let mut records = 0_u64;
		let mut end_count = None;
		let write_txn = db.begin_write()?;
		{
			let mut table = write_txn.open_table(TABLE)?;
			loop {
				line.clear();
				if reader.read_line(&mut line)? == 0 {
					break;
				}
				let current = line.trim_end_matches(['\r', '\n']);
				if let Some(count) = current
					.strip_prefix("{\"type\":\"end\",\"records\":")
					.and_then(|rest| rest.strip_suffix('}'))
					.and_then(|number| number.parse::<u64>().ok())
				{
					end_count = Some(count);
					break;
				}
				let (map, key, value) = parse_record(current)?;
				if !expected_maps.contains(&map) {
					return Err(io::Error::new(
						io::ErrorKind::InvalidData,
						"record map missing from manifest",
					)
					.into());
				}
				let composite = composite_key(map.as_bytes(), &key)?;
				table.insert(composite.as_slice(), value.as_slice())?;
				records = records.checked_add(1).ok_or("record count overflow")?;
			}
		}
		write_txn.commit()?;
		if end_count != Some(records) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"missing end record or record count mismatch",
			)
			.into());
		}
		line.clear();
		if reader.read_line(&mut line)? != 0 {
			return Err(
				io::Error::new(io::ErrorKind::InvalidData, "data follows end record").into()
			);
		}
		drop(db);
		if redb_path.exists() {
			return Err(
				format!("destination appeared during import: {}", redb_path.display()).into()
			);
		}
		std::fs::rename(&temp_path, redb_path)?;
		eprintln!(
			"Imported {records} records from {map_count} maps into {}",
			redb_path.display()
		);
		Ok(())
	})();
	if result.is_err() {
		let _ = std::fs::remove_file(&temp_path);
	}
	result
}

fn compact(redb_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
	let before = std::fs::metadata(redb_path)?.len();
	let mut db = Database::open(redb_path)?;
	let mut passes = 0_u64;
	while db.compact()? {
		passes = passes.saturating_add(1);
	}
	drop(db);
	let after = std::fs::metadata(redb_path)?.len();
	let reclaimed = before.saturating_sub(after);
	eprintln!(
		"Compacted {}: {before} -> {after} bytes (reclaimed {reclaimed}) in {passes} pass(es)",
		redb_path.display()
	);
	Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut args = env::args().skip(1);
	match args.next().as_deref() {
		| Some("export-mtxdb") => {
			let rocks = args.next().unwrap_or_else(|| usage());
			let output = args.next().unwrap_or_else(|| usage());
			let server = args.next();
			if args.next().is_some() {
				usage();
			}
			mtxdb_export::export(
				Path::new(&rocks),
				Path::new(&output),
				server.as_deref(),
			)
		},
		| Some("export") => {
			let rocks = args.next().unwrap_or_else(|| usage());
			let bundle = args.next().unwrap_or_else(|| usage());
			export(Path::new(&rocks), Path::new(&bundle), &args.collect::<Vec<_>>())
		},
		| Some("import") => {
			let bundle = args.next().unwrap_or_else(|| usage());
			let redb = args.next().unwrap_or_else(|| usage());
			if args.next().is_some() {
				usage();
			}
			import(Path::new(&bundle), Path::new(&redb))
		},
		| Some("compact") => {
			let redb = args.next().unwrap_or_else(|| usage());
			if args.next().is_some() {
				usage();
			}
			compact(Path::new(&redb))
		},
		| Some("list") => {
			let rocks = args.next().unwrap_or_else(|| usage());
			list(Path::new(&rocks))
		},
		| Some("stats") => {
			let rocks = args.next().unwrap_or_else(|| usage());
			if args.next().is_some() {
				usage();
			}
			stats(Path::new(&rocks))
		},
		| _ => usage(),
	}
}

#[cfg(test)]
mod tests {
	use super::{MTXDB_OWNED_MAPS, SKIPPED_MAPS};

	#[test]
	fn legacy_state_and_short_id_maps_are_rebuilt_or_skipped() {
		assert!(SKIPPED_MAPS.contains(&"roomid_shortstatehash"));
		assert!(SKIPPED_MAPS.contains(&"shortstatehash_statediff"));
		assert!(MTXDB_OWNED_MAPS.contains(&"roomid_roothandle"));
		assert!(MTXDB_OWNED_MAPS.contains(&"state_hamt_nodes"));
		assert!(SKIPPED_MAPS.contains(&"eventid_shorteventid"));
		assert!(SKIPPED_MAPS.contains(&"shortstatekey_statekey"));
		assert!(SKIPPED_MAPS.contains(&"tokenids"));
	}

	#[test]
	fn derived_room_caches_are_skipped() {
		assert!(SKIPPED_MAPS.contains(&"roomid_joinedcount"));
		assert!(SKIPPED_MAPS.contains(&"roomid_invitedcount"));
		assert!(SKIPPED_MAPS.contains(&"roomserverids"));
		assert!(SKIPPED_MAPS.contains(&"serverroomids"));
		assert!(SKIPPED_MAPS.contains(&"roomuserid_joined"));
		assert!(SKIPPED_MAPS.contains(&"userroomid_joined"));
		assert!(SKIPPED_MAPS.contains(&"roomuseroncejoinedids"));
		assert!(SKIPPED_MAPS.contains(&"tofrom_relation"));
		assert!(SKIPPED_MAPS.contains(&"referencedevents"));
	}
}
