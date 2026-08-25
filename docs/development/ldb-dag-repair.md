# Semi-Manual DAG Repair with `ldb`

RocksDB ships with a CLI tool called `ldb` that can inspect and mutate column families directly. This guide shows how to diagnose and fix timeline anachronisms caused by out-of-order outlier rescues without starting the server.

> **⚠ WARNING**: Back up your database before any manual mutation.
> ```bash
> cp -r /path/to/continuwuity_db /path/to/continuwuity_db.bak
> ```

## Prerequisites

```bash
# ldb is part of the rocksdb-tools package (or built from source)
apt install rocksdb-tools # Debian/Ubuntu
pacman -S rocksdb # Arch (includes ldb)

DB=/path/to/continuwuity_db # your database path
```

---

## 1. Inspect Column Families

List all column families in the database:

```bash
ldb list_column_families --db="$DB"
```

The relevant ones for DAG repair:

| Column Family | Key Format | Value | Purpose |
|---|---|---|---|
| `pduid_pdu` | `shortroomid(8B) || pducount(8B)` | PDU JSON | Timeline events (ordered) |
| `eventid_pduid` | event_id string | `shortroomid(8B) || pducount(8B)` | Event ID → timeline position |
| `eventid_outlierpdu` | event_id string | PDU JSON | Quarantined outlier events |
| `roomid_outliereventid` | `room_id || 0xFF || event_id` | event_id | Room → outlier index |
| `eventid_receivecount` | event_id string | u64 big-endian (8B) | Immutable receive order |

---

## 2. Find Your Room's Short ID

The timeline keys use a numeric `shortroomid`, not the string room ID.

```bash
# Look up shortroomid for a room
ldb get --db="$DB" \
  --column_family=roomid_shortroomid \
  --key="!yourRoomId:server.tld"
```

The value is an 8-byte big-endian integer. Note it as hex for prefix scans.

---

## 3. Scan Timeline Events

```bash
# Scan all timeline PDUs for a room (prefix = shortroomid bytes)
ldb scan --db="$DB" \
  --column_family=pduid_pdu \
  --from="<shortroomid_hex>" \
  --hex \
  --max_keys=50
```

Each key is `shortroomid(8B) || pducount(8B)`. The values are JSON. Look at `origin_server_ts` in each PDU to spot anachronisms (e.g., a January event appearing after April events).

---

### 4. Scan Outlier Events for a Room

If a rescue is successful, the events will be moved from the outlier tables to the timeline tables. In a healthy room, these tables should be empty.

```bash
# List outlier event IDs for a room
# Should return nothing if all are rescued
ldb scan --db="$DB" \
  --column_family=roomid_outliereventid \
  --from="!yourRoomId:server.tld\xff" \
  --max_keys=100
```

# Get a specific outlier's PDU JSON
ldb get --db="$DB" \
  --column_family=eventid_outlierpdu \
  --key='$eventId'
```

---

## 5. Check Receive Order

```bash
# Check when an event was first received
ldb get --db="$DB" \
  --column_family=eventid_receivecount \
  --key='$eventId' \
  --hex
```

The value is an 8-byte big-endian u64. Lower = received earlier.

---

## 6. Manual Reorder Process

If the admin commands aren't available (server won't start, etc.), you can reorder manually:

### Step 1: Export timeline PDUs

```bash
# Dump all PDUs for the room to a JSONL file
ldb scan --db="$DB" \
  --column_family=pduid_pdu \
  --from="<shortroomid_hex>" \
  --hex \
  > /tmp/room_pdus.jsonl
```

### Step 2: Sort externally

Sort by `origin_server_ts` (or use `prev_events` for a topological sort):

```bash
# Simple timestamp sort with jq
cat /tmp/room_pdus.jsonl | \
  jq -s 'sort_by(.origin_server_ts)' \
  > /tmp/room_pdus_sorted.json
```

### Step 3: Delete old timeline entries

```bash
# Delete each old pduid_pdu key
# WARNING: Do this for ALL keys with the room's shortroomid prefix
ldb delete --db="$DB" \
  --column_family=pduid_pdu \
  --key="<hex_key>"

# Also delete the reverse mapping
ldb delete --db="$DB" \
  --column_family=eventid_pduid \
  --key='$eventId'
```

### Step 4: Re-insert in sorted order

For each PDU in sorted order, construct a new key with an incrementing pducount and write it back:

```bash
# new_key = shortroomid(8B) || new_pducount(8B, big-endian)
ldb put --db="$DB" \
  --column_family=pduid_pdu \
  --key="<new_hex_key>" \
  --value='<pdu_json>'

# Update the reverse mapping
ldb put --db="$DB" \
  --column_family=eventid_pduid \
  --key='$eventId' \
  --value="<new_hex_key>"
```

> **⚠ IMPORTANT**: The new pducount values must be higher than the current
> global counter to avoid collisions. Check the `global` column family for the
> current counter value first.

---

## 7. Merge Outliers into Timeline

To manually rescue specific outliers:

```bash
# 1. Read the outlier PDU
ldb get --db="$DB" \
  --column_family=eventid_outlierpdu \
  --key='$eventId' > /tmp/outlier.json

# 2. Assign a new pducount and insert into pduid_pdu
ldb put --db="$DB" \
  --column_family=pduid_pdu \
  --key="<shortroomid_hex><new_pducount_hex>" \
  --value_file=/tmp/outlier.json

# 3. Create the reverse mapping
ldb put --db="$DB" \
  --column_family=eventid_pduid \
  --key='$eventId' \
  --value="<shortroomid_hex><new_pducount_hex>"

# 4. Remove from outlier tables
ldb delete --db="$DB" \
  --column_family=eventid_outlierpdu \
  --key='$eventId'

ldb delete --db="$DB" \
  --column_family=roomid_outliereventid \
  --key="!yourRoomId:server.tld\xff$eventId"
```

---

## 8. Preferred: Use Admin Commands

If the server can start, use the built-in commands instead:

```bash
# Fix a single room's timeline ordering
debug reorder-timeline !roomId:server.tld

# Full automated repair pipeline
debug heal-room !roomId:server.tld server.tld

# Rescue orphaned outliers
debug rescue-room !roomId:server.tld --all
```

The `heal-room` pipeline runs:
1. Rescue local outliers
2. Fetch missing events from federation
3. Rescue newly-fetched outliers
4. Reorder timeline (topological sort with origin_server_ts tiebreak)
5. Force-set room state from authoritative server
