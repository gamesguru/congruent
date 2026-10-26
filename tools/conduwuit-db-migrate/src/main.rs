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
// leave stale legacy indexes beside the newly imported mtxdb data.
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
	"roomid_shortstatehash",
	"roomid_timestamp_pducount",
	"roomid_topologicalorder_pducount",
	"server_signingkeys",
	"shorteventid_authchain",
	"shorteventid_roothandle",
	"shorteventid_shortauthevents",
	"shorteventid_shortprevevents",
	"shorteventid_shortstatehash",
	"shortstatehash_statediff",
	"state_hamt_node_mtimes",
	"state_hamt_nodes",
	"state_hamt_root_lattices",
	"statehash_shortstatehash",
];

fn usage() -> ! {
	eprintln!("usage: conduwuit-db-migrate export <rocksdb> <bundle.jsonl> [map ...]");
	eprintln!("       conduwuit-db-migrate export-mtxdb <rocksdb> <output-dir> [server-name] [--extra-rejected <ids-file>]... [--extra-soft-failed <ids-file>]...");
	eprintln!("       conduwuit-db-migrate import <bundle.jsonl> <new-metadata.redb>");
	eprintln!("       conduwuit-db-migrate list <rocksdb>");
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
			.filter(|name| !MTXDB_OWNED_MAPS.contains(&name.as_str()))
			.cloned()
			.collect()
	} else {
		for name in requested {
			if MTXDB_OWNED_MAPS.contains(&name.as_str()) {
				return Err(format!("{name} is owned by the mtxdb room/key import").into());
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
		} else {
			"redb candidate"
		};
		println!("{ownership}\t{name}");
	}
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut args = env::args().skip(1);
	match args.next().as_deref() {
		| Some("export-mtxdb") => {
			let rocks = args.next().unwrap_or_else(|| usage());
			let output = args.next().unwrap_or_else(|| usage());
			let mut server = None;
			let mut rejected = Vec::new();
			let mut soft_failed = Vec::new();
			while let Some(arg) = args.next() {
				if arg == "--extra-rejected" {
					rejected.push(args.next().unwrap_or_else(|| usage()));
				} else if arg == "--extra-soft-failed" {
					soft_failed.push(args.next().unwrap_or_else(|| usage()));
				} else if server.is_none() {
					server = Some(arg);
				} else {
					usage();
				}
			}
			let rejected: Vec<&Path> = rejected.iter().map(Path::new).collect();
			let soft_failed: Vec<&Path> = soft_failed.iter().map(Path::new).collect();
			mtxdb_export::export(
				Path::new(&rocks),
				Path::new(&output),
				server.as_deref(),
				&rejected,
				&soft_failed,
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
			import(Path::new(&bundle), Path::new(&redb))
		},
		| Some("list") => {
			let rocks = args.next().unwrap_or_else(|| usage());
			list(Path::new(&rocks))
		},
		| _ => usage(),
	}
}
