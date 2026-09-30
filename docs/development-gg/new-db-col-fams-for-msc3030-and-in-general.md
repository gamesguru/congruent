In RocksDB there are no separate indexes — the **key prefix IS the index**. Let me check the key layouts:

Searched for "pduid_pdu|eventid_pduid|roomid_pduleaves|roomid_topo|shorteventid_short(prev|auth)"
Viewed maps.rs:1-540

Here's the full picture:

**Yes, querying by room is fast.** The two primary timeline CFs are keyed by `shortroomid` prefix:

| CF Name                            | Key Layout                                               | Index Type                                                                                 |
| ---------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `room_pducount_eventid`            | `[shortroomid: 8B][pdu_count: 8B]` → event_id            | `SEQUENTIAL` — prefix scan by shortroomid = all events in room, ordered by stream position |
| `roomid_topologicalorder_pducount` | `[shortroomid: 8B][depth: 8B][pdu_count: 8B]` → event_id | `SEQUENTIAL` — prefix scan by shortroomid = all events in room, ordered by topo depth      |

RocksDB sorts keys lexicographically, so scanning with a `shortroomid` prefix is a single seek + forward iteration — O(1) to start, O(n) to scan all events. It's as fast as any B-tree index.

**But** — `eventid_metadata`, `eventid_pdu`, `shorteventid_shortauthevents`, and `shorteventid_shortprevevents` are keyed by **event_id** or **shorteventid**, NOT by room. So:

- ✅ "Give me all events in room X" → fast prefix scan on `room_pducount_eventid`
- ✅ "Give me all events in room X in topo order" → fast prefix scan on `roomid_topologicalorder_pducount`
- ❌ "Give me auth_events for all events in room X" → must look up each `shorteventid_shortauthevents` individually (point reads)
- ❌ "Give me the PDU body for event $abc" → point read on `eventid_pdu` (fast per-event, but not a room-scoped scan)

This is exactly why `rebuild_state` extracts auth_events during Phase 1's topo scan — it piggybacks on the room-scoped sequential iteration to avoid 60K random point reads later.

One subtle point worth documenting explicitly: the _allocator_ for `shortroomid` and `shorteventid` is global and monotonic (`globals.next_count()`), but the _room-scoped behavior_ comes from how those IDs are used in RocksDB keys.

- `shortroomid` is attached to the room and becomes the prefix for timeline scans.
- `shorteventid` is just a compact event handle; it is not room-local and should not be treated as a per-room sequence.

So the invariant is really "room-scoped key prefixes over globally allocated short IDs", not "short IDs themselves are sequential within a room."

That also suggests a concrete storage improvement for auth/prev edges:

- `shorteventid_shortauthevents` and `shorteventid_shortprevevents` are currently keyed by `shorteventid` alone, so room-wide scans have to bounce through lots of unrelated event keys.
- A room-prefixed dual key, e.g. `shortroomid || shorteventid`, would cluster a room's edge lookups together and should improve cache locality and reduce random reads during room-local traversal.
- This would be an optimization for hot paths like state recovery, backfill, and auth-chain reconstruction. It should be documented as a performance-oriented storage change, not as a semantic requirement of the event model.

---

Good question — let me check what actually uses those tables:

Searched for "shorteventid_shortprevevents"
Searched for "shortid_shortauthchain"
Searched for "shortauthchain|auth_chain.*short|shorteventid.*authevents|shortauthevents"

**No, they're still needed.** `rebuild_state` bypasses them, but they're used in production hot paths:

| Table                          | Still used by                                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| `shorteventid_shortauthevents` | `auth_chain/mod.rs` — `get_auth_chain_inner()` for federation `/event_auth`, `/state_ids`, and live state res during `/send` |
| `shorteventid_shortprevevents` | `data.rs` — `get_shortprevevents()` for DAG traversal, outlier processing, `reindex.rs`                                      |

`rebuild_state` builds its **own** in-memory auth chains from `EventMeta` (RoaringBitmaps) and never touches those tables. But:

- **`auth_chain/mod.rs`** uses `shorteventid_shortauthevents` to compute auth chains on the fly for federation requests and live state resolution — this is the normal runtime path, not rebuild
- **`reindex.rs`** uses `shorteventid_shortprevevents` to rebuild DAG edges when shorteventids change
- **`outlier/mod.rs`** writes to both tables when storing outlier events

So `rebuild_state` is self-contained (metadata + RoaringBitmaps), but those tables are still the canonical persistent auth/prev edge stores for everything else in the server.

---

Searched for "tscount|timestamp_to_event|msc3030|roomid_ts"
Searched for "timestamp_to|jump.*date|origin_server_ts.*index|get_event_for_timestamp"
Searched for "timestamp_to_event|get_event_for_timestamp|TimestampToEvent"
Searched for "v1/rooms.*timestamp|rooms.*ts=|\_timestamp"

No. Continuwuity has **no** `roomid_tscount_pducount` index and **no** MSC3030 (`/timestamp_to_event`) endpoint.

Tuwunel added that index to support "jump to date" — the key is `[shortroomid][origin_server_ts][pdu_count]` so you can binary-search for the closest event to any timestamp.

Without it, continuwuity would have to do a full linear scan of the room's timeline to find the event closest to a given timestamp. The topo index sorts by `(depth, count)` and the stream index sorts by `(count)` — neither is sorted by timestamp.

Want me to look into adding it? It'd be a new column family + a migration to backfill it from existing events + the API endpoint.
