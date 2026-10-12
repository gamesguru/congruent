//! JSONL export of a room's durable state, for the mtxdb CLI to import.
//!
//! The export is read-only and reads RocksDB directly; it never consults
//! mtxdb, so it works on a database that has only ever run on RocksDB. Each
//! line is one JSON object tagged by `"t"`, written in dependency order so an
//! importer can apply it as it streams:
//!
//! 1. `header`: format, server, room.
//! 2. `event`: `event_id`, the `pdu` as stored, `prev_events`,
//!    `auth_events`, and the `outlier`, `soft_failed` and `rejected` flags.
//!    Verdict codes and redaction metadata are included when present.
//! 3. `end`: event count. A file without an `end` line was truncated.
//!
//! Locators, short ids and auth-chain closures are not exported: they are
//! derived data, and the importer owns their on-disk format.

use std::{collections::HashSet, io::Write};

use conduwuit::{Result, err, warn};
use futures::StreamExt;
use slipstream::{
	OwnedEventId, RoomId,
	json::{Object, Value},
};

use super::Service;

/// Format tag written to the `header` line.
pub const EXPORT_FORMAT: &str = "mtxdb-export/v1";

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
	object.insert("t".to_owned(), Value::String(tag.to_owned()));
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
		let mut counts = ExportCounts::default();

		let mut events: Vec<(OwnedEventId, bool)> = self
			.services
			.timeline
			.all_pdus(room_id)
			.map(|(_, pdu)| (conduwuit::matrix::Event::event_id(&pdu).to_owned(), false))
			.collect()
			.await;
		let mut seen: HashSet<OwnedEventId> = events.iter().map(|(id, _)| id.clone()).collect();
		let outliers: Vec<OwnedEventId> = self
			.services
			.outlier
			.room_stream(room_id)
			.map(|(id, _)| id)
			.collect()
			.await;
		for id in outliers {
			if seen.insert(id.clone()) {
				events.push((id, true));
			}
		}

		let mut header = record("header");
		header.insert("format".to_owned(), text(EXPORT_FORMAT));
		header.insert("server".to_owned(), text(self.services.globals.server_name().as_str()));
		header.insert("room_id".to_owned(), text(room_id.as_str()));
		line(out, header)?;

		for (event_id, outlier) in &events {
			let pdu = match self.services.timeline.get_pdu_json(event_id).await {
				| Ok(pdu) => pdu,
				| Err(_) => match self.services.timeline.get_outlier_pdu_json(event_id).await {
					| Ok(pdu) => pdu,
					| Err(error) => {
						warn!(%room_id, %event_id, ?error, "skipping event with no stored PDU");
						continue;
					},
				},
			};
			let mut event = record("event");
			event.insert("event_id".to_owned(), text(event_id.as_str()));
			event.insert("prev_events".to_owned(), id_list(&pdu, "prev_events"));
			event.insert("auth_events".to_owned(), id_list(&pdu, "auth_events"));
			event.insert("outlier".to_owned(), Value::Bool(*outlier));
			event.insert(
				"soft_failed".to_owned(),
				Value::Bool(self.services.pdu_metadata.is_event_soft_failed(event_id)?),
			);
			let soft_fail_reason = self.services.pdu_metadata.get_soft_fail_reason(event_id)?;
			if let Some(reason) = soft_fail_reason {
				let code = crate::rooms::pdu_metadata::SoftFailCode::parse(&reason)
					.unwrap_or(crate::rooms::pdu_metadata::SoftFailCode::Unknown);
				event.insert("soft_fail_code".to_owned(), Value::from(u64::from(code.to_u8())));
			}
			event.insert(
				"rejected".to_owned(),
				Value::Bool(self.services.pdu_metadata.is_event_rejected(event_id)?),
			);
			if let Some(reason) = self.services.pdu_metadata.get_rejection_reason(event_id)? {
				let code = crate::rooms::pdu_metadata::RejectionCode::parse(&reason)
					.unwrap_or(crate::rooms::pdu_metadata::RejectionCode::Unknown);
				event.insert("rejection_code".to_owned(), Value::from(u64::from(code.to_u8())));
			}
			if let Ok(metadata) = self.services.timeline.get_event_metadata(event_id).await {
				if let Some(redacted_by) = metadata.redacted_by {
					event.insert("redacted_by".to_owned(), text(redacted_by.as_str()));
				}
			}
			event.insert("pdu".to_owned(), Value::Object(pdu));
			line(out, event)?;
			counts.events = counts.events.saturating_add(1);
		}

		let mut end = record("end");
		end.insert("events".to_owned(), Value::from(counts.events));
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
}
