use std::fs::File;
use std::io::{BufRead, BufReader};
use ruma::{RoomVersionId, events::pdu::PduEvent};
use serde_json::Value;

fn main() {
    let file = File::open("/run/media/shane/shane4tb-ent/dags/remote-dag-BDSybzDpGyDxMHZzpN_unredacted.org-v10-unredacted.org-d1-23142.jsonl").unwrap();
    let reader = BufReader::new(file);
    let room_version = RoomVersionId::V10;

    for (i, line) in reader.lines().enumerate().take(5) {
        let line = line.unwrap();
        let mut json: Value = serde_json::from_str(&line).unwrap();

        let actual_id = json.get("event_id").unwrap().as_str().unwrap().to_string();

        // Let's use the same function from continuwuity
        // We'll just rely on ruma's PduEvent or similar to see if the ID matches.

        if let Ok(ruma_pdu) = serde_json::from_value::<PduEvent>(json.clone()) {
            println!("PDU {}: actual={}, ruma_id={}", i, actual_id, ruma_pdu.event_id);
        } else {
            // strip event_id and see what conduwuit / ruma says
            let mut obj = json.as_object_mut().unwrap();
            obj.remove("event_id");
            let mut pdu = json.clone();
            if let Ok(ruma_pdu) = serde_json::from_value::<PduEvent>(pdu) {
                println!("PDU {}: actual={}, ruma_id={}", i, actual_id, ruma_pdu.event_id);
            } else {
                println!("PDU {}: parse error", i);
            }
        }
    }
}
