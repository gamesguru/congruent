//! Domain adapter for per-event verdicts: rejection, soft-fail and redaction.
//!
//! One small record per event that carries any verdict, in a bucketed global
//! collection (the setters know only the event id). The codes are opaque bytes
//! here; their meaning stays with the caller.

use std::sync::Arc;

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::{event_record_id, event_verdict_collection_id},
	store::MtxdbStore,
};

/// Version byte of the verdict value encoding.
const VERDICT_VERSION: u8 = 1;
const HAS_REJECTION: u8 = 1;
const HAS_SOFT_FAIL: u8 = 1 << 1;
const HAS_REDACTION: u8 = 1 << 2;

/// Everything recorded about an event beyond its body and edges.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Verdict {
	pub rejection: Option<u8>,
	pub soft_fail: Option<u8>,
	/// Event id of the redaction that redacted this event.
	pub redacted_by: Option<Vec<u8>>,
}

impl Verdict {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.rejection.is_none() && self.soft_fail.is_none() && self.redacted_by.is_none()
	}
}

#[derive(Clone)]
pub struct VerdictAdapter {
	store: Arc<MtxdbStore>,
}

impl VerdictAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	/// The event's verdict, or `None` when it has none. A record written for a
	/// different event with the same 128-bit record id is a miss.
	pub fn get(&self, event_id: &[u8]) -> Result<Option<Verdict>, StorageError> {
		let node = event_record_id(event_id);
		self.store
			.get(ShardType::EventDag, &event_verdict_collection_id(&node), node)?
			.map_or(Ok(None), |bytes| {
				let (stored_event, verdict) = Self::decode(&bytes)?;
				Ok((stored_event == event_id).then_some(verdict))
			})
	}

	/// Read-modify-write the event's verdict atomically. The record is removed
	/// when `change` leaves it empty. A concurrent writer to the same bucket
	/// fails the commit, and the whole read-modify-write is redone.
	pub fn update<F>(&self, event_id: &[u8], mut change: F) -> Result<(), StorageError>
	where
		F: FnMut(&mut Verdict),
	{
		let node = event_record_id(event_id);
		let collection = event_verdict_collection_id(&node);
		let mut attempts = 0_u8;
		loop {
			let version = self
				.store
				.collection_version(ShardType::EventDag, &collection)?;
			let mut verdict = match self.store.get(ShardType::EventDag, &collection, node)? {
				| Some(bytes) => {
					let (stored_event, verdict) = Self::decode(&bytes)?;
					if stored_event != event_id {
						return Err(StorageError::Corrupt(
							"event record id collision in verdicts".to_owned(),
						));
					}
					verdict
				},
				| None => Verdict::default(),
			};
			change(&mut verdict);

			let tx = self.store.begin_transaction();
			tx.expect_collection_version(ShardType::EventDag, collection, version)?;
			if verdict.is_empty() {
				tx.remove(ShardType::EventDag, collection, node)?;
			} else {
				tx.put(
					ShardType::EventDag,
					collection,
					node,
					&Self::encode(event_id, &verdict)?,
				)?;
			}
			match tx.commit() {
				| Err(StorageError::StaleRead { .. }) if attempts < 3 => {
					attempts = attempts
						.checked_add(1)
						.expect("attempts is less than three");
				},
				| result => return result,
			}
		}
	}

	fn encode(event_id: &[u8], verdict: &Verdict) -> Result<Vec<u8>, StorageError> {
		let too_large = || StorageError::Corrupt("verdict field is too large".to_owned());
		let mut flags = 0_u8;
		if verdict.rejection.is_some() {
			flags |= HAS_REJECTION;
		}
		if verdict.soft_fail.is_some() {
			flags |= HAS_SOFT_FAIL;
		}
		if verdict.redacted_by.is_some() {
			flags |= HAS_REDACTION;
		}

		let mut out = vec![VERDICT_VERSION, flags];
		out.extend_from_slice(
			&u32::try_from(event_id.len())
				.map_err(|_| too_large())?
				.to_le_bytes(),
		);
		out.extend_from_slice(event_id);
		out.extend(verdict.rejection);
		out.extend(verdict.soft_fail);
		if let Some(redactor) = &verdict.redacted_by {
			out.extend_from_slice(
				&u32::try_from(redactor.len())
					.map_err(|_| too_large())?
					.to_le_bytes(),
			);
			out.extend_from_slice(redactor);
		}
		Ok(out)
	}

	fn decode(bytes: &[u8]) -> Result<(&[u8], Verdict), StorageError> {
		let malformed = || StorageError::Corrupt("malformed event verdict".to_owned());
		let take_len = |rest: &[u8]| -> Result<(usize, usize), StorageError> {
			let (length, _) = rest.split_first_chunk::<4>().ok_or_else(malformed)?;
			Ok((usize::try_from(u32::from_le_bytes(*length)).map_err(|_| malformed())?, 4))
		};

		let (&version, rest) = bytes.split_first().ok_or_else(malformed)?;
		if version != VERDICT_VERSION {
			return Err(StorageError::Corrupt("unknown event verdict version".to_owned()));
		}
		let (&flags, rest) = rest.split_first().ok_or_else(malformed)?;
		let (length, header) = take_len(rest)?;
		let rest = rest.get(header..).ok_or_else(malformed)?;
		if rest.len() < length {
			return Err(malformed());
		}
		let (event_id, mut rest) = rest.split_at(length);

		let mut verdict = Verdict::default();
		if flags & HAS_REJECTION != 0 {
			let (&code, tail) = rest.split_first().ok_or_else(malformed)?;
			verdict.rejection = Some(code);
			rest = tail;
		}
		if flags & HAS_SOFT_FAIL != 0 {
			let (&code, tail) = rest.split_first().ok_or_else(malformed)?;
			verdict.soft_fail = Some(code);
			rest = tail;
		}
		if flags & HAS_REDACTION != 0 {
			let (length, header) = take_len(rest)?;
			let tail = rest.get(header..).ok_or_else(malformed)?;
			verdict.redacted_by = Some(tail.get(..length).ok_or_else(malformed)?.to_vec());
		}
		Ok((event_id, verdict))
	}
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	use super::*;

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir()
			.join(format!("conduwuit-mtxdb-verdicts-{name}-{}", std::process::id()))
	}

	#[test]
	fn verdicts_merge_clear_and_survive_restart() {
		let path = test_path("restart");
		let _ = fs::remove_dir_all(&path);
		let event = b"$event:example.com";
		let other = b"$other:example.com";

		let () = {
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = VerdictAdapter::new(store.clone());
			assert!(adapter.get(event).unwrap().is_none());

			adapter.update(event, |v| v.rejection = Some(3)).unwrap();
			adapter.update(event, |v| v.soft_fail = Some(7)).unwrap();
			adapter
				.update(event, |v| v.redacted_by = Some(b"$redact:example.com".to_vec()))
				.unwrap();
			assert_eq!(
				adapter.get(event).unwrap(),
				Some(Verdict {
					rejection: Some(3),
					soft_fail: Some(7),
					redacted_by: Some(b"$redact:example.com".to_vec()),
				})
			);
			assert!(adapter.get(other).unwrap().is_none());

			// Clearing one verdict leaves the others.
			adapter.update(event, |v| v.soft_fail = None).unwrap();
			assert_eq!(adapter.get(event).unwrap().unwrap().soft_fail, None);
			assert_eq!(adapter.get(event).unwrap().unwrap().rejection, Some(3));
			store.sync(ShardType::EventDag).unwrap();
		};

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = VerdictAdapter::new(store);
		assert_eq!(adapter.get(event).unwrap().unwrap().rejection, Some(3));

		// Clearing the last verdict removes the record.
		adapter.update(event, |v| *v = Verdict::default()).unwrap();
		assert!(adapter.get(event).unwrap().is_none());
		fs::remove_dir_all(&path).unwrap();
	}

	#[test]
	fn decode_rejects_truncated_records() {
		let verdict = Verdict {
			rejection: Some(1),
			soft_fail: None,
			redacted_by: Some(b"$r:x".to_vec()),
		};
		let bytes = VerdictAdapter::encode(b"$e:x", &verdict).unwrap();
		assert_eq!(VerdictAdapter::decode(&bytes).unwrap().1, verdict);
		for cut in 0..bytes.len() {
			assert!(VerdictAdapter::decode(&bytes[..cut]).is_err(), "cut at {cut}");
		}
	}
}
