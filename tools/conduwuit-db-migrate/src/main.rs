use std::{env, fs::File, io::{self, BufReader, BufWriter, Read, Write}, path::Path};

use redb::{Database, TableDefinition};
use rust_rocksdb::{DB, IteratorMode, Options};

const MAGIC: &[u8; 8] = b"CDBMETA\0";
const VERSION: u32 = 1;
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("metadata");
const MIGRATION_MARKER: &[u8] = b"\0conduwuit/migration/rocksdb-metadata";

// Deliberately excludes event/state/DAG column families. Those are imported
// through mtxdb's native export format, not copied through this bridge.
const DEFAULT_METADATA_MAPS: &[&str] = &[
    "alias_roomid", "alias_userid", "aliasid_alias", "bannedroomids",
    "deviceleftid_userid", "disabledroomids", "email_localpart",
    "global", "id_appserviceregistrations", "keychangeid_userid",
    "keyid_key", "localpart_email", "logintoken_expiresatuserid",
    "openidtoken_expiresatuserid", "passwordresettoken_info",
    "presenceid_presence", "publicroomids", "pushkey_deviceid",
    "registrationtoken_info", "userdeviceid_metadata", "userid_avatarurl",
    "userid_displayname", "userid_password", "userid_rooms",
];

fn usage() -> ! {
    eprintln!("usage: conduwuit-db-migrate export <rocksdb> <bundle> [map ...]");
    eprintln!("       conduwuit-db-migrate import <bundle> <redb>");
    std::process::exit(2);
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn write_record(writer: &mut impl Write, map: &[u8], key: &[u8], value: &[u8]) -> io::Result<()> {
    for field in [map, key, value] {
        let len = u32::try_from(field.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "metadata field exceeds 4 GiB"))?;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(field)?;
    }
    Ok(())
}

fn read_field(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let len = read_u32(reader)? as usize;
    let mut field = vec![0; len];
    reader.read_exact(&mut field)?;
    Ok(field)
}

fn export(rocks_path: &Path, bundle_path: &Path, maps: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = Options::default();
    options.create_if_missing(false);
    let names = DB::list_cf(&options, rocks_path)?;
    let requested: Vec<&str> = if maps.is_empty() {
        DEFAULT_METADATA_MAPS.to_vec()
    } else {
        maps.iter().map(String::as_str).collect()
    };
    let selected: Vec<String> = requested
        .into_iter()
        .filter(|map| names.iter().any(|name| name == map))
        .map(str::to_owned)
        .collect();
    let db = DB::open_cf_for_read_only(&options, rocks_path, &selected, false)?;
    let mut writer = BufWriter::new(File::create(bundle_path)?);
    writer.write_all(MAGIC)?;
    writer.write_all(&VERSION.to_le_bytes())?;

    for map in selected {
        let handle = db.cf_handle(&map).ok_or_else(|| format!("missing column family: {map}"))?;
        for item in db.iterator_cf(handle, IteratorMode::Start) {
            let (key, value) = item?;
            write_record(&mut writer, map.as_bytes(), &key, &value)?;
        }
    }
    writer.flush()?;
    Ok(())
}

fn composite_key(map: &[u8], key: &[u8]) -> Result<Vec<u8>, io::Error> {
    let map_len = u32::try_from(map.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "map name exceeds 4 GiB"))?;
    let mut output = Vec::with_capacity(4 + map.len() + key.len());
    output.extend_from_slice(&map_len.to_le_bytes());
    output.extend_from_slice(map);
    output.extend_from_slice(key);
    Ok(output)
}

fn import(bundle_path: &Path, redb_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(File::open(bundle_path)?);
    let mut magic = [0; 8];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err("invalid metadata bundle magic".into());
    }
    if read_u32(&mut reader)? != VERSION {
        return Err("unsupported metadata bundle version".into());
    }

    let db = Database::create(redb_path)?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        loop {
            let map = match read_field(&mut reader) {
                Ok(field) => field,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error.into()),
            };
            let key = read_field(&mut reader)?;
            let value = read_field(&mut reader)?;
            let composite = composite_key(&map, &key)?;
            table.insert(composite.as_slice(), value.as_slice())?;
        }
        // The marker is committed in the same redb transaction as the data.
        // It lets the production database reject or resume a partial upgrade
        // instead of guessing from the presence of one user record.
        table.insert(MIGRATION_MARKER, VERSION.to_le_bytes().as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("export") => {
            let rocks = args.next().map_or_else(|| usage(), |value| value);
            let bundle = args.next().map_or_else(|| usage(), |value| value);
            export(Path::new(&rocks), Path::new(&bundle), &args.collect::<Vec<_>>())
        }
        Some("import") => {
            let bundle = args.next().map_or_else(|| usage(), |value| value);
            let redb = args.next().map_or_else(|| usage(), |value| value);
            import(Path::new(&bundle), Path::new(&redb))
        }
        _ => usage(),
    }
}
