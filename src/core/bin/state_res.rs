use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
};

use ruma::{
    RoomVersionId, StateEventType, EventId, OwnedEventId,
    events::StateEventType::RoomMember,
};
use serde_json::Value;

// We need an async main because conduwuit's resolve is async
#[tokio::main]
async fn main() {
    let path = "/run/media/shane/shane4tb-ent/dags/remote-dag-l2xV0sd51lraysuRcsWVECge4NULaH3g-ou95vgDgiM-v12-grin.hu.jsonl";
    let file = File::open(path).expect("failed to open file");
    let reader = BufReader::new(file);

    let mut events = HashMap::new();
    let mut state_sets = vec![];
    let mut auth_chain_sets = vec![];

    for line in reader.lines() {
        let line = line.unwrap();
        let val: Value = serde_json::from_str(&line).unwrap();

        let event_id: OwnedEventId = val["event_id"].as_str().unwrap().try_into().unwrap();

        // Very basic parsing just to fetch it
        // To properly use conduwuit_core::matrix::state_res we need PduEvent
        // Wait, creating PduEvent is complex. Let's see if we can deserialize it.
        if let Ok(pdu) = serde_json::from_value::<conduwuit_core::PduEvent>(val.clone()) {
            events.insert(event_id.clone(), pdu);
        }
    }

    // Actually, setting up the exact state_sets from a DAG.jsonl is non-trivial.
    // The DAG jsonl from rezzy usually has all events. The extremities are the leaves.
    // I need to identify extremities to form state_sets.
}
