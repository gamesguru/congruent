use axum::extract::State;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use conduwuit::{Err, Result, err, info, utils::math::usize_from_f64};
use futures::StreamExt;
use ruma::OwnedEventId;
use serde::Serialize;
use xxhash_rust::xxh3;

/// Default active window: the W most recent events by topological depth.
const DEFAULT_WINDOW: usize = 5000;

/// Number of hash functions for the Bloom filter.
const BLOOM_K: u128 = 4;

/// Bits per element for ~5% false positive rate with k=4.
/// m = ceil(W * 6.235)
const BITS_PER_ELEMENT: f64 = 6.235;

/// Response for `GET
/// /_matrix/federation/unstable/org.matrix.msc0f01/room_digest/{roomId}`
///
/// Returns a compact Bloom filter digest of the server's event graph for
/// divergence detection (MSC0F01: Gossip-Based Federation Room Reconciliation).
#[derive(Serialize)]
pub(crate) struct RoomDigestResponse {
	pub digest: String,
	pub digest_type: String,
	pub digest_bits: u32,
	pub digest_window: u32,
	pub event_count: u64,
	pub extremity_event_ids: Vec<OwnedEventId>,
	pub depth_range: (u64, u64),
	pub origin_server_ts_range: (u64, u64),
}

/// Build an XXH3-128 double-hashed Bloom filter over a set of event IDs.
///
/// Uses the construction from MSC0F01:
///   h1 = XXH3-128(event_id, seed=0x00)
///   h2 = XXH3-128(event_id, seed=0x01)
///   position_i = (h1 + i * h2) mod m, for i in {0, 1, 2, 3}
///
/// Returns (base64url-encoded filter, bit count m).
fn build_xxh3_bloom(event_ids: &[OwnedEventId]) -> Result<(String, u32)> {
	let w = event_ids.len();
	let m_bits_f = (f64::from(u32::try_from(w).unwrap_or(u32::MAX)) * BITS_PER_ELEMENT)
		.ceil()
		.max(64.0);
	let m_bits = usize_from_f64(m_bits_f)?;
	let m_bytes = m_bits.div_ceil(8);
	let mut filter = vec![0_u8; m_bytes];

	let m = u128::try_from(m_bits).unwrap_or(u128::MAX);
	for event_id in event_ids {
		let bytes = event_id.as_bytes();
		let h1 = xxh3::xxh3_128_with_seed(bytes, 0x00);
		let h2 = xxh3::xxh3_128_with_seed(bytes, 0x01);

		for i in 0..BLOOM_K {
			let pos = h1
				.wrapping_add(i.wrapping_mul(h2))
				.checked_rem(m)
				.and_then(|p| usize::try_from(p).ok())
				.unwrap_or(0);
			filter[pos / 8] |= 1 << (pos % 8);
		}
	}

	Ok((URL_SAFE_NO_PAD.encode(&filter), u32::try_from(m_bits).unwrap_or(u32::MAX)))
}

/// Compute the ETag for a room digest.
///
/// ETag = XXH3-64(sorted(extremity_event_ids) || event_count)
///
/// Because the DAG is append-only, if extremities and count match,
/// the underlying event set is mathematically guaranteed identical.
fn compute_etag(extremities: &mut [OwnedEventId], event_count: u64) -> String {
	extremities.sort();
	let mut input = Vec::new();
	for eid in extremities.iter() {
		input.extend_from_slice(eid.as_bytes());
	}
	input.extend_from_slice(&event_count.to_le_bytes());
	let hash = xxh3::xxh3_64(&input);
	format!("\"xxh3:{hash:016x}\"")
}

/// # `GET /_matrix/federation/unstable/org.matrix.msc0f01/room_digest/{roomId}`
///
/// Returns a compact Bloom filter digest of the server's event graph for the
/// given room, enabling O(1) divergence detection between federated servers.
///
/// Part of MSC0F01: Gossip-Based Federation Room Reconciliation.
pub(crate) async fn get_room_digest_route(
	State(services): State<crate::State>,
	axum::extract::Path(room_id_str): axum::extract::Path<String>,
) -> Result<impl axum::response::IntoResponse> {
	let room_id = ruma::OwnedRoomId::try_from(room_id_str)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;

	// Verify we participate in this room
	if !services
		.rooms
		.state_cache
		.server_is_participant(services.globals.server_name(), &room_id)
		.await
	{
		return Err!(Request(Forbidden("This server is not participating in that room.")));
	}

	// Collect forward extremities
	let mut extremity_event_ids: Vec<OwnedEventId> = services
		.rooms
		.state
		.get_forward_extremities(&room_id)
		.collect()
		.await;

	// Reverse-walk the timeline to collect the active window of event IDs
	let pdus = services
		.rooms
		.timeline
		.pdus_rev(&room_id, std::ops::Bound::Unbounded);
	futures::pin_mut!(pdus);

	let mut window_event_ids: Vec<OwnedEventId> = Vec::with_capacity(DEFAULT_WINDOW);
	let mut event_count: u64 = 0;
	let mut min_depth = u64::MAX;
	let mut max_depth = 0_u64;
	let mut min_ts = u64::MAX;
	let mut max_ts = 0_u64;

	while let Some(Ok((_, pdu))) = pdus.next().await {
		event_count = event_count.saturating_add(1);

		let depth: u64 = pdu.depth.into();
		let ts: u64 = u64::from(pdu.origin_server_ts);

		if depth < min_depth {
			min_depth = depth;
		}
		if depth > max_depth {
			max_depth = depth;
		}
		if ts < min_ts {
			min_ts = ts;
		}
		if ts > max_ts {
			max_ts = ts;
		}

		// Only the first W events go into the Bloom filter
		if window_event_ids.len() < DEFAULT_WINDOW {
			window_event_ids.push(pdu.event_id.clone());
		}
	}

	// Handle empty room edge case
	if min_depth == u64::MAX {
		min_depth = 0;
	}
	if min_ts == u64::MAX {
		min_ts = 0;
	}

	info!(
		target: "msc0f01",
		room_id = %room_id,
		event_count = event_count,
		window = window_event_ids.len(),
		extremities = extremity_event_ids.len(),
		"Computed room digest"
	);

	// Build the Bloom filter
	let (digest, digest_bits) = build_xxh3_bloom(&window_event_ids)?;

	// Compute ETag for conditional request support
	let etag = compute_etag(&mut extremity_event_ids, event_count);

	let response = RoomDigestResponse {
		digest,
		digest_type: "xxh3_bloom".to_owned(),
		digest_bits,
		digest_window: u32::try_from(window_event_ids.len()).unwrap_or(u32::MAX),
		event_count,
		extremity_event_ids,
		depth_range: (min_depth, max_depth),
		origin_server_ts_range: (min_ts, max_ts),
	};

	Ok(([(http::header::ETAG, etag)], axum::Json(response)))
}

#[cfg(test)]
mod tests {
	use ruma::OwnedEventId;

	use super::{build_xxh3_bloom, compute_etag};

	#[test]
	fn bloom_filter_deterministic() {
		let ids: Vec<OwnedEventId> = vec![
			"$abc123:example.com".try_into().unwrap(),
			"$def456:example.com".try_into().unwrap(),
			"$ghi789:example.com".try_into().unwrap(),
		];
		let (digest1, bits1) = build_xxh3_bloom(&ids).unwrap();
		let (digest2, bits2) = build_xxh3_bloom(&ids).unwrap();
		assert_eq!(digest1, digest2, "Bloom filter must be deterministic");
		assert_eq!(bits1, bits2);
	}

	#[test]
	fn bloom_filter_sizing() {
		let ids: Vec<OwnedEventId> = (0..5000)
			.map(|i| format!("$event{i}:example.com").try_into().unwrap())
			.collect();
		let (_, bits) = build_xxh3_bloom(&ids).unwrap();
		// m = ceil(5000 * 6.235) = 31175
		assert_eq!(bits, 31175, "Filter size must match MSC0F01 spec");
	}

	#[test]
	fn etag_stability() {
		let mut ids: Vec<OwnedEventId> = vec![
			"$abc123:example.com".try_into().unwrap(),
			"$def456:example.com".try_into().unwrap(),
		];
		let etag1 = compute_etag(&mut ids.clone(), 81247);
		let etag2 = compute_etag(&mut ids, 81247);
		assert_eq!(etag1, etag2, "ETag must be stable for same inputs");
	}

	#[test]
	fn etag_changes_with_count() {
		let mut ids: Vec<OwnedEventId> = vec!["$abc123:example.com".try_into().unwrap()];
		let etag1 = compute_etag(&mut ids.clone(), 100);
		let etag2 = compute_etag(&mut ids, 101);
		assert_ne!(
			etag1, etag2,
			"ETag must change when event_count differs (Swiss cheese detection)"
		);
	}
}
