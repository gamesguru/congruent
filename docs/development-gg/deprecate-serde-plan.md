
2026/10/03 [Claude code in ./slipstream]

Yes, but only in part. Going by the grep you pasted, the serde use falls into four groups, and they differ a lot in how replaceable they are.

1. Replaceable now, with what slipstream has
This is the Matrix data path, and it's where the performance win is.
- Parsing and building JSON:
  - serde_json::from_str, from_slice, from_value, to_value, to_vec and json!
  - RawValue and to_raw_value
  - serde_json::Value and Map
  - They can move to rezzy's Value, the codec traits, Raw<T> and canonical_json.
- Where it shows up: pdu.rs, event/format.rs, event_auth.rs, the state_res tests, parse_incoming_pdu, sender.rs, timeline/*, read_receipt, bundled_aggregations, to_canonical_object and utils/json.rs.
- What's still needed: the traits already exist, but nothing implements them for continuwuity's own types.

2. Replaceable, but it needs a derive
- About 40 structs use #[derive(Serialize, Deserialize)] with #[serde(rename, default, skip_serializing_if, with)]. Examples are Pdu, PduBuilder, delayed_events, StateHashEntry, PublicReadReceipts, sync/mod.rs and the pdu_metadata types.
- Slipstream has no derive macros. Each struct need or a #[derive(codec::…)] macro.A derive macro could live in a separate proc-macrtself stays dependency-light. I'd recommend thatover hand-writing 40 impls.

3. Not worth replacing, because the framework requires serde                                                                   Config: config/mod.rs has about 300 #[serde(defauthrough figment and serde_saphyr for YAML and TOML.Config is read once at startup, so it isn't a hotkeep the slipstream bridge for Matrix types.The database layer: Json(..), deserialized() and nduwuit-database. The database crate's serializationformat is serde-based, so replacing it means rewrThird-party types: serde_regex, either::serde_untnd tracing types.
- The Event::get_content and get_unsigned serde bounds. The other agent's plan already says to leave these alone. They let any local serde struct be read from an event.

4. A mixed bag
- UserId and other types flowing through #[serde(wiway.
- The msc4500 and rooms/threads code mixes both andtime.

My recommendation
Drop serde from groups 1 and 2, which is the whole  it in group 3. Start with a derive macro(#[derive(codec::Serialize, Deserialize)] honouringlizing_if and with), since group 2 depends on it.

Do you want me to build that derive macro in a sepa repo, or hand-write the impls for the PDU typesfirst? Either way it's work in slipstream and contihas to be told.
