use std::hint::black_box;
use std::time::Instant;

use mtx_slipstream::federation::raw_pdu::{canonical_to_bytes_without, parse_pdu_json};

const SKIP: &[&str] = &["signatures", "unsigned", "hashes"];

fn small_pdu() -> String {
    r#"{"auth_events":["$a1:example.com","$a2:example.com","$a3:example.com"],"content":{"body":"hello world, this is a typical message body","msgtype":"m.text"},"depth":4242,"hashes":{"sha256":"aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g"},"origin_server_ts":1700000000000,"prev_events":["$p1:example.com"],"room_id":"!room:example.com","sender":"@alice:example.com","signatures":{"example.com":{"ed25519:key1":"c2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJl"}},"type":"m.room.message","unsigned":{"age_ts":1700000000001}}"#.into()
}

/// A big state event with many members (keys deliberately unsorted).
fn big_pdu() -> String {
    let mut m = String::from(r#"{"type":"m.room.power_levels","state_key":"","sender":"@a:example.com","room_id":"!r:example.com","origin_server_ts":1700000000000,"depth":99,"content":{"users":{"#);
    for i in 0..400 { if i > 0 { m.push(','); } m += &format!(r#""@user{}:example.com":{}"#, (i * 7919) % 400, i % 100); }
    m += r#"},"events":{"#;
    for i in 0..60 { if i > 0 { m.push(','); } m += &format!(r#""m.custom.event{}":{}"#, (i * 31) % 60, i); }
    m += r#"},"ban":50,"kick":50,"redact":50,"state_default":50,"events_default":0,"users_default":0},"hashes":{"sha256":"abc"},"signatures":{"example.com":{"ed25519:k":"sig"}},"unsigned":{"age":1},"auth_events":[],"prev_events":[]}"#;
    m
}

/// Nested sync-like body with unicode escapes and strings needing escaping.
fn sync_body() -> String {
    let mut m = String::from(r#"{"next_batch":"s1_2_3","rooms":{"join":{"#);
    for r in 0..300 {
        if r > 0 { m.push(','); }
        m += &format!(r#""!room{r}:example.com":{{"timeline":{{"events":["#);
        for i in 0..20 {
            if i > 0 { m.push(','); }
            m += &format!(r#"{{"type":"m.room.message","content":{{"msgtype":"m.text","body":"msg {i} \"quoted\" café \n line — padding text for realism"}},"event_id":"$e{r}_{i}:example.com","sender":"@u{r}:example.com","origin_server_ts":{}}}"#, 1_700_000_000_000u64 + (r * 20 + i) as u64);
        }
        m += r#"],"limited":false}}"#;
    }
    m += "}}}";
    m
}

fn bench<F: FnMut() -> usize>(name: &str, bytes: usize, mut f: F) {
    for _ in 0..20 { black_box(f()); }
    // calibrate
    let t = Instant::now(); let mut n = 0u64;
    while t.elapsed().as_millis() < 150 { black_box(f()); n += 1; }
    let iters = n.max(5);
    let mut samples = Vec::new();
    for _ in 0..9 {
        let t = Instant::now();
        for _ in 0..iters { black_box(f()); }
        samples.push(t.elapsed().as_nanos() as f64 / iters as f64);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = samples[4];
    println!("  {name:<44} {:>12.2} µs   {:>8.1} MB/s", med / 1e3, bytes as f64 / med * 1e3);
}

fn slip_parse(src: &str) -> simd_json::OwnedValue {
    let mut b = src.as_bytes().to_vec();
    parse_pdu_json(&mut b).unwrap()
}

fn main() {
    for (label, src, canon) in [("small PDU", small_pdu(), true), ("big power_levels PDU", big_pdu(), true), ("sync body", sync_body(), false)] {
        let n = src.len();
        println!("\n== {label} ({n} bytes) ==");

        if canon {
            // correctness: both must agree on canonical output
            let a = canonical_to_bytes_without(&slip_parse(&src), SKIP).unwrap();
            let b = rezzy_json::write_raw_canonical_filtered(src.as_bytes(), |k| SKIP.contains(&k)).unwrap();
            let c = rezzy_json::write_string_value_filtered(&rezzy_json::Value::parse(&src).unwrap(), |k| SKIP.contains(&k)).unwrap();
            println!("  outputs equal: slip==rezzy_raw {}, rezzy_raw==rezzy_dom {}", &a[..] == b.as_bytes(), b == c);
        }

        println!(" parse only:");
        bench("slipstream  (simd-json to OwnedValue, +copy)", n, || { let v = slip_parse(&src); black_box(&v); 1 });
        bench("rezzy-json  Value::parse_bytes", n, || { let v = rezzy_json::Value::parse_bytes(src.as_bytes()).unwrap(); black_box(&v); 1 });

        println!(" serialize DOM (no filter):");
        let sv = slip_parse(&src);
        let rv = rezzy_json::Value::parse(&src).unwrap();
        bench("slipstream  canonical_to_bytes", n, || mtx_slipstream::federation::raw_pdu::canonical_to_bytes(&sv).unwrap().len());
        bench("rezzy-json  write_string_value", n, || rezzy_json::write_string_value(&rv).unwrap().len());

        if canon {
            println!(" end-to-end: raw bytes -> canonical (signature input):");
            bench("slipstream  parse + canonical_to_bytes_without", n, || canonical_to_bytes_without(&slip_parse(&src), SKIP).unwrap().len());
            bench("rezzy-json  write_raw_canonical_filtered (no DOM)", n, || rezzy_json::write_raw_canonical_filtered(src.as_bytes(), |k| SKIP.contains(&k)).unwrap().len());
            bench("rezzy-json  parse + write_string_value_filtered", n, || rezzy_json::write_string_value_filtered(&rezzy_json::Value::parse(&src).unwrap(), |k| SKIP.contains(&k)).unwrap().len());
        }
    }
}
