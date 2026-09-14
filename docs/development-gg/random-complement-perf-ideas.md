Viewed test_results.fails.jsonl:1-155

### Memory and Caches

The `Row cache` and `Shared cache` growing to exactly `191.91 MiB` and `191.53 MiB` is completely expected and extremely healthy. By default, `continuwuity` allocates exactly 50% of its available database memory capacity to the row cache and 50% to the block cache. These hitting ~192 MiB each simply means your database cache hit its configured ceiling (likely ~384 MiB total) and is now operating smoothly at capacity, evicting old items as needed via LRU.

### The `159` Queue Mystery

The reason every single offline destination has _exactly_ 159 active+queued items (e.g. `111 + 48 = 159`) is because Matrix outbound federation transactions batch up to **50 PDUs** and **100 EDUs** at a time, plus up to a small number of connection-level retries. You likely performed an action (or sent a message to a massive room) that broadcasted an identical payload to all 32 of those servers at once, causing each of their outbound queues to receive the exact same batch of federation traffic before failing and pausing.

---

### Test Failures Prioritization

Looking at your `test_results.fails.jsonl`, here are the most critical ones to investigate first:

**1. `TestFederationRoomsInvite` & `TestInboundCanReturnMissingEvents`**
Federation is the lifeblood of a Matrix server. If inviting users across federation or fetching missing events is broken, your server will frequently fork timelines or fail to join large rooms.

**2. `TestAsyncUpload` (Media)**
Basic media functionality seems to be completely failing (`Create_media`, `Upload_media`, `Download_media`). Users won't be able to send or view images/files.

**3. `TestRoomCreate` & `TestRoomCreationReportsEventsToMyself`**
If the server fails to properly report `m.room.create` events back to the client that created the room, the client UI will often hang or fail to open the room immediately after creating it.

**4. `TestJumpToDateEndpoint`**
This represents history pagination. If this endpoint fails, users won't be able to scroll back in their chat history reliably.

_(Features like `TestKnocking`, `TestThreadSubscriptions`, and `TestPushRuleRoomUpgrade` are less critical edge cases that you can defer until the core routing and media features are stable)._
