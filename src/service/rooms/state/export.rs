//! JSONL export of a room's durable state, for the mtxdb CLI to import.
//!
//! The export reads PDUs and timeline metadata from RocksDB, and authoritative
//! verdicts from mtxdb. Each line is one JSON object tagged by `"t"`, written
//! in dependency order so an importer can apply it as it streams:
//!
//! 1. `header`: format, server, room, and `timeline: true`.
//! 2. `event`: `event_id`, the `pdu` as stored, `prev_events`,
//!    `auth_events`, the `outlier`, `soft_failed` and `rejected` flags, and
//!    ordering: `stream_ordering` for timeline events, `arrival_ordering` for
//!    soft-failed events kept out of the timeline. Verdict codes and redaction
//!    metadata are included when present.
//! 3. `end`: event count. A file without an `end` line was truncated.
//!
//! Locators, short ids and auth-chain closures are not exported: they are
//! derived data, and the importer owns their on-disk format.

use std::{collections::HashSet, io::Write};

use conduwuit::{PduCount, Result, err, warn};
use conduwuit_mtxdb::export_schema::{self as schema, field, tag};
use futures::StreamExt;
use slipstream::{
	OwnedEventId, RoomId,
	json::{Object, Value},
};

use super::Service;

/// Format tag written to the `header` line.
pub const EXPORT_FORMAT: &str = schema::EXPORT_FORMAT;

/// What an export wrote.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExportCounts {
	pub events: u64,
}

fn line(out: &mut impl Write, record: Object) -> Result<()> {
	let text = slipstream::codec::to_string(&Value::Object(record));
	out.write_all(text.as_bytes())
		.and_then(|()| out.write_all(b"\n"))
		.map_err(|e| err!("export write failed: {e}"))
}

fn record(tag: &str) -> Object {
	let mut object = Object::new();
	object.insert(field::T.to_owned(), Value::String(tag.to_owned()));
	object
}

fn text(value: impl Into<String>) -> Value { Value::String(value.into()) }

fn id_list(pdu: &Object, key: &str) -> Value {
	match pdu.get(key) {
		| Some(Value::Array(ids)) => Value::Array(ids.clone()),
		| _ => Value::Array(Vec::new()),
	}
}

impl Service {
	/// Writes `room_id`'s durable state to `out` as JSONL. See the module
	/// documentation for the format.
	pub async fn export_room_jsonl<W: Write + Send>(
		&self,
		room_id: &RoomId,
		out: &mut W,
	) -> Result<ExportCounts> {
		const BATCH_SIZE: usize = 1024;

		let mut counts = ExportCounts::default();

		let mut events: Vec<(OwnedEventId, bool, Option<PduCount>)> = self
			.services
			.timeline
			.all_pdus(room_id)
			.map(|(count, pdu)| {
				(conduwuit::matrix::Event::event_id(&pdu).to_owned(), false, Some(count))
			})
			.collect()
			.await;
		let mut seen: HashSet<OwnedEventId> = events.iter().map(|(id, ..)| id.clone()).collect();
		let outliers: Vec<OwnedEventId> = self
			.services
			.outlier
			.room_stream(room_id)
			.map(|(id, _)| id)
			.collect()
			.await;
		for id in outliers {
			if seen.insert(id.clone()) {
				events.push((id, true, None));
			}
		}

		let mut header = record(tag::HEADER);
		header.insert(field::FORMAT.to_owned(), text(EXPORT_FORMAT));
		header
			.insert(field::SERVER.to_owned(), text(self.services.globals.server_name().as_str()));
		header.insert(field::ROOM_ID.to_owned(), text(room_id.as_str()));
		header.insert(field::TIMELINE.to_owned(), Value::Bool(true));
		line(out, header)?;

		for chunk in events.chunks(BATCH_SIZE) {
			let verdicts = self
				.services
				.pdu_metadata
				.get_verdicts(chunk.iter().map(|(id, ..)| id))?;

			for ((event_id, outlier, count), verdict) in chunk.iter().zip(verdicts) {
				let pdu = match self.services.timeline.get_pdu_json(event_id) {
					| Ok(pdu) => pdu,
					| Err(_) => match self.services.timeline.get_outlier_pdu_json(event_id) {
						| Ok(pdu) => pdu,
						| Err(error) => {
							warn!(%room_id, %event_id, ?error, "skipping event with no stored PDU");
							continue;
						},
					},
				};
				let mut event = record(tag::EVENT);
				event.insert(field::EVENT_ID.to_owned(), text(event_id.as_str()));
				event.insert(field::PREV_EVENTS.to_owned(), id_list(&pdu, field::PREV_EVENTS));
				event.insert(field::AUTH_EVENTS.to_owned(), id_list(&pdu, field::AUTH_EVENTS));
				event.insert(field::OUTLIER.to_owned(), Value::Bool(*outlier));
				if !*outlier {
					if let Some(count) = count {
						event.insert(
							field::STREAM_ORDERING.to_owned(),
							Value::from(count.into_signed()),
						);
					}
				}
				if let Some(arrival) = verdict.as_ref().and_then(|v| v.arrival_ordering) {
					let arrival = i64::try_from(arrival).map_err(|_| {
						err!("event {event_id}: arrival ordering {arrival} does not fit in i64")
					})?;
					event.insert(field::ARRIVAL_ORDERING.to_owned(), Value::from(arrival));
				}
				let soft_fail = verdict.as_ref().and_then(|v| v.soft_fail);
				event.insert(field::SOFT_FAILED.to_owned(), Value::Bool(soft_fail.is_some()));
				if let Some(code) = soft_fail {
					let code = crate::rooms::pdu_metadata::SoftFailCode::from_u8(code);
					event.insert(
						field::SOFT_FAIL_CODE.to_owned(),
						Value::from(u64::from(code.to_u8())),
					);
				}
				let rejection = verdict.as_ref().and_then(|v| v.rejection);
				event.insert(field::REJECTED.to_owned(), Value::Bool(rejection.is_some()));
				if let Some(code) = rejection {
					let code = crate::rooms::pdu_metadata::RejectionCode::from_u8(code);
					event.insert(
						field::REJECTION_CODE.to_owned(),
						Value::from(u64::from(code.to_u8())),
					);
				}
				if let Some(redacted_by) = verdict.as_ref().and_then(|v| v.redacted_by.as_ref()) {
					let redacted_by = std::str::from_utf8(redacted_by).map_err(|error| {
						err!("event {event_id}: invalid redaction event ID bytes: {error}")
					})?;
					let redacted_by = OwnedEventId::parse(redacted_by).map_err(|error| {
						err!("event {event_id}: invalid redaction event ID: {error}")
					})?;
					event.insert(field::REDACTED_BY.to_owned(), text(redacted_by.as_str()));
				}
				event.insert(field::PDU.to_owned(), Value::Object(pdu));
				line(out, event)?;
				counts.events = counts.events.saturating_add(1);
			}
		}

		let mut end = record(tag::END);
		end.insert(field::EVENTS.to_owned(), Value::from(counts.events));
		line(out, end)?;
		out.flush().map_err(|e| err!("export flush failed: {e}"))?;
		Ok(counts)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn id_list_copies_arrays_and_defaults_to_empty() {
		let mut pdu = Object::new();
		pdu.insert("prev_events".to_owned(), Value::Array(vec![text("$a")]));
		assert_eq!(id_list(&pdu, "prev_events"), Value::Array(vec![text("$a")]));
		assert_eq!(id_list(&pdu, "auth_events"), Value::Array(Vec::new()));
		pdu.insert("auth_events".to_owned(), text("not-an-array"));
		assert_eq!(id_list(&pdu, "auth_events"), Value::Array(Vec::new()));
	}

	#[test]
	fn lines_are_single_tagged_json_objects() {
		let mut buffer = Vec::new();
		line(&mut buffer, record("event")).unwrap();
		line(&mut buffer, record("end")).unwrap();

		let output = String::from_utf8(buffer).unwrap();
		let lines: Vec<&str> = output.lines().collect();
		assert_eq!(lines.len(), 2);
		assert!(lines[0].contains("\"t\":\"event\""));
		assert!(lines[1].contains("\"t\":\"end\""));
		assert!(output.ends_with('\n'));
	}

	/// The service keeps its own code enums (they carry retry policy), but the
	/// numbers and tags are the export format's. Every number must mean the same
	/// thing to the shared schema the importer reads.
	#[test]
	fn verdict_codes_match_the_shared_schema() {
		use crate::rooms::pdu_metadata::{RejectionCode, SoftFailCode};

		for number in 0..=u8::MAX {
			let ours = RejectionCode::from_u8(number);
			let shared = schema::RejectionCode::from_u8(number);
			assert_eq!(ours.to_u8(), shared.to_u8(), "rejection code {number}");
			assert_eq!(ours.tag(), shared.tag(), "rejection code {number}");

			let ours = SoftFailCode::from_u8(number);
			let shared = schema::SoftFailCode::from_u8(number);
			assert_eq!(ours.to_u8(), shared.to_u8(), "soft-fail code {number}");
			assert_eq!(ours.tag(), shared.tag(), "soft-fail code {number}");
		}
		for code in schema::RejectionCode::ALL {
			assert_eq!(RejectionCode::parse(code.tag()).map(RejectionCode::to_u8), {
				schema::RejectionCode::parse(code.tag()).map(schema::RejectionCode::to_u8)
			});
		}
		for code in schema::SoftFailCode::ALL {
			assert_eq!(SoftFailCode::parse(code.tag()).map(SoftFailCode::to_u8), {
				schema::SoftFailCode::parse(code.tag()).map(schema::SoftFailCode::to_u8)
			});
		}
	}
}
