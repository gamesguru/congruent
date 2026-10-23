//! Domain adapter for per-event verdicts: rejection, soft-fail and redaction.
//!
//! One small record per event that carries any verdict, in a bucketed global
//! collection (the setters know only the event id). The codes are opaque bytes
//! here; their meaning stays with the caller.

use std::sync::Arc;

pub use mtxdb::interop::Verdict;
use mtxdb::{
	ShardType,
	interop::{decode_verdict, encode_verdict, event_record_id, event_verdict_collection_id},
};

use crate::{
	StorageError,
	keys::EVENT_LOCATOR_BUCKETS,
	store::{CollectionId, MtxdbStore, NodeId},
};

const MAX_UPDATE_RETRIES: u16 = 256;

#[derive(Clone)]
pub struct VerdictAdapter {
	store: Arc<MtxdbStore>,
}

impl VerdictAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store } }

	/// Whether any verdict record exists, for startup's incomplete-import
	/// check.
	pub fn has_any_records(&self) -> Result<bool, StorageError> {
		for bucket in 0..EVENT_LOCATOR_BUCKETS {
			let mut node = [0_u8; 16];
			node[..4].copy_from_slice(&bucket.to_le_bytes());
			let collection = event_verdict_collection_id(&node);
			if self.store.has_records(ShardType::EventDag, &collection)? {
				return Ok(true);
			}
		}
		Ok(false)
	}

	/// The event's verdict, or `None` when it has none. A record written for a
	/// different event with the same 128-bit record id is a miss.
	pub fn get(&self, event_id: &[u8]) -> Result<Option<Verdict>, StorageError> {
		let node = event_record_id(event_id);
		self.store
			.get(ShardType::EventDag, &event_verdict_collection_id(&node), node)?
			.map_or(Ok(None), |bytes| {
				let (stored_event, verdict) = decode_verdict(&bytes)?;
				Ok((stored_event == event_id).then_some(verdict))
			})
	}

	/// Batch-read verdicts for multiple event IDs. Groups by collection to
	/// issue one `get_many` per bucket instead of N single-key round trips.
	pub fn get_many(&self, event_ids: &[&[u8]]) -> Result<Vec<Option<Verdict>>, StorageError> {
		use std::collections::HashMap;

		let mut by_collection: HashMap<CollectionId, Vec<(usize, NodeId, Vec<u8>)>> =
			HashMap::new();

		for (idx, event_id) in event_ids.iter().enumerate() {
			let node = event_record_id(event_id);
			let collection = event_verdict_collection_id(&node);
			by_collection
				.entry(collection)
				.or_default()
				.push((idx, node, event_id.to_vec()));
		}

		let mut results = vec![None; event_ids.len()];

		for (collection, entries) in by_collection {
			let nodes: Vec<NodeId> = entries.iter().map(|(_, node, _)| *node).collect();
			let values = self
				.store
				.get_many(ShardType::EventDag, &collection, &nodes)?;

			for ((idx, _node, event_id), value) in entries.into_iter().zip(values) {
				let verdict = match value {
					| Some(bytes) => {
						let (stored_event, verdict) = decode_verdict(&bytes)?;
						(stored_event == event_id.as_slice()).then_some(verdict)
					},
					| None => None,
				};
				results[idx] = verdict;
			}
		}

		Ok(results)
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
		let mut attempts = 0_u16;
		loop {
			let tx = self.store.begin_transaction();
			let result = (|| {
				let (values, version) =
					tx.get_with_collection_version(ShardType::EventDag, collection, &[node])?;
				let mut verdict = match values.into_iter().next().flatten() {
					| Some(bytes) => {
						let (stored_event, verdict) = decode_verdict(&bytes)?;
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
				tx.expect_collection_version(ShardType::EventDag, collection, version)?;
				self.stage(&tx, event_id, &verdict)?;
				tx.commit()
			})();
			match result {
				| Err(error)
					if (error.is_stale_read() || error.is_would_block())
						&& attempts < MAX_UPDATE_RETRIES =>
				{
					attempts = attempts
						.checked_add(1)
						.expect("attempts is less than the retry limit");
					if error.is_would_block() {
						// A published group still awaiting materialization pins the
						// read boundary below the collection version; run recovery
						// so the versioned read can be certified.
						let _ = self.store.recover_pending_transactions();
					}
					std::thread::sleep(std::time::Duration::from_millis(2));
				},
				| result => return result,
			}
		}
	}

	/// Stage the event's whole verdict in `tx`, replacing any stored one, so an
	/// importer can commit verdicts together with other records (and the import
	/// marker) in one transaction. An empty verdict stages a removal. Staging
	/// the same verdict again is idempotent, so an interrupted import can be
	/// re-run.
	pub fn stage(
		&self,
		tx: &crate::store::MtxdbTransaction<'_>,
		event_id: &[u8],
		verdict: &Verdict,
	) -> Result<(), StorageError> {
		let node = event_record_id(event_id);
		let collection = event_verdict_collection_id(&node);
		if let Some(existing) = tx
			.get_many(ShardType::EventDag, collection, &[node])?
			.into_iter()
			.next()
			.flatten()
		{
			let (stored_event, _) = decode_verdict(&existing)?;
			if stored_event != event_id {
				return Err(StorageError::Corrupt(
					"event record id collision in verdicts".to_owned(),
				));
			}
		}
		if verdict.is_empty() {
			tx.remove(ShardType::EventDag, collection, node)
		} else {
			tx.put(ShardType::EventDag, collection, node, &encode_verdict(event_id, verdict)?)
		}
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
	fn has_any_records_observes_only_committed_verdicts() {
		let path = test_path("has-any-records");
		let _ = fs::remove_dir_all(&path);
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = VerdictAdapter::new(store.clone());
		let event = b"$event:example.com";

		assert!(!adapter.has_any_records().unwrap());
		let tx = store.begin_transaction();
		adapter
			.stage(&tx, event, &Verdict { rejection: Some(1), ..Verdict::default() })
			.unwrap();
		assert!(!adapter.has_any_records().unwrap());
		tx.commit().unwrap();
		assert!(adapter.has_any_records().unwrap());
		fs::remove_dir_all(&path).unwrap();
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
					outlier: false,
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
			outlier: false,
		};
		let bytes = encode_verdict(b"$e:x", &verdict).unwrap();
		let old_bytes: &[u8] =
			&[1, 5, 4, 0, 0, 0, b'$', b'e', b':', b'x', 1, 4, 0, 0, 0, b'$', b'r', b':', b'x'];
		assert_eq!(bytes.as_slice(), old_bytes);
		assert_eq!(decode_verdict(old_bytes).unwrap().1, verdict);
		assert_eq!(decode_verdict(&bytes).unwrap().1, verdict);
		for cut in 0..bytes.len() {
			assert!(decode_verdict(&bytes[..cut]).is_err(), "cut at {cut}");
		}
	}

	#[test]
	fn staged_verdicts_commit_replace_and_are_idempotent() {
		let path = test_path("stage");
		let _ = fs::remove_dir_all(&path);
		let event = b"$event:example.com";
		let cleared = b"$cleared:example.com";
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = VerdictAdapter::new(store.clone());
		adapter.update(cleared, |v| v.rejection = Some(1)).unwrap();

		let verdict = Verdict {
			rejection: Some(7),
			soft_fail: None,
			redacted_by: Some(b"$redact:example.com".to_vec()),
			outlier: false,
		};
		let tx = store.begin_transaction();
		adapter.stage(&tx, event, &verdict).unwrap();
		adapter.stage(&tx, cleared, &Verdict::default()).unwrap();
		assert!(adapter.get(event).unwrap().is_none(), "staged verdicts are not visible");
		tx.commit().unwrap();
		assert_eq!(adapter.get(event).unwrap(), Some(verdict));
		assert!(adapter.get(cleared).unwrap().is_none(), "an empty verdict removes the record");

		// Re-staging replaces rather than merges.
		let replacement = Verdict {
			rejection: None,
			soft_fail: Some(2),
			redacted_by: None,
			outlier: false,
		};
		for _ in 0..2 {
			let tx = store.begin_transaction();
			adapter.stage(&tx, event, &replacement).unwrap();
			tx.commit().unwrap();
			assert_eq!(adapter.get(event).unwrap(), Some(replacement.clone()));
		}

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(&path);
	}
}
