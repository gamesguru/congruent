use std::{path::PathBuf, sync::Arc};

use conduwuit_core::{
	Server,
	config::Config,
	log::{Log, LogLevelReloadHandles, capture},
};
use conduwuit_mtxdb::events::EventAdapter;
use futures::TryStreamExt;
use slipstream::{event_id, room_id};

use super::AuthChain;
use crate::Services;

static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct TempDbGuard {
	path: PathBuf,
	// Declared after `path` so the lock is released only once the database
	// directory has been removed.
	_lock: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for TempDbGuard {
	fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.path); }
}

async fn setup_test_services() -> (TempDbGuard, Arc<Server>, Arc<Services>) {
	// The test server drives HTTP via reqwest, which requires a TLS crypto
	// provider. `rustls` is a dev-dependency built with the `ring` feature (see
	// Cargo.toml), so this is unconditional and independent of which provider
	// the library's optional `ring`/`aws_lc_rs` features select for consumers.
	let _ = rustls::crypto::ring::default_provider().install_default();
	let lock = crate::DB_TEST_MUTEX.lock().await;
	let count = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let db_path =
		std::env::temp_dir().join(format!("conduwuit_auth_completeness_test_db_{count}"));
	let _ = std::fs::remove_dir_all(&db_path);

	let guard = TempDbGuard { path: db_path.clone(), _lock: lock };

	let config_raw = conduwuit::config::RawConfig::from_toml(&format!(
		r#"
        server_name = "test.conduwuit.local"
        database_path = "{}"
        "#,
		db_path.to_string_lossy().replace('\\', "/")
	))
	.expect("failed to parse test config");

	let config = Config::new(&config_raw).expect("failed to parse config");
	let runtime_handle = ();
	let server = Arc::new(Server::new(config, Some(&runtime_handle), Log {
		reload: LogLevelReloadHandles,
		capture: Arc::new(capture::State),
	}));

	let services = Services::build(server.clone()).expect("failed to build services");
	(guard, server, services)
}

#[conduwuit_macros::async_test]
async fn partial_auth_chain_is_rejected_then_recovers_after_parent_arrives() {
	let (guard, _server, services) = setup_test_services().await;
	let room = &room_id!("!auth-completeness:test.conduwuit.local");
	let child = &event_id!("$auth-child:test.conduwuit.local");
	let parent = &event_id!("$auth-parent:test.conduwuit.local");
	let good = &event_id!("$auth-good:test.conduwuit.local");
	let adapter = EventAdapter::new(services.mtxdb.clone());
	let reference = EventAdapter::new(Arc::new(
		conduwuit_mtxdb::store::MtxdbStore::open(guard.path.join("reference")).unwrap(),
	));
	services.rooms.short.get_or_create_shortroomid(room).await;
	for adapter in [&adapter, &reference] {
		adapter
			.write_event_with_edges(room.as_bytes(), good.as_bytes(), b"good", &[], &[])
			.unwrap();
		adapter
			.write_event_with_edges(room.as_bytes(), child.as_bytes(), b"child", &[], &[
				parent.as_bytes()
			])
			.unwrap();
	}
	reference
		.write_event_with_edges(room.as_bytes(), parent.as_bytes(), b"parent", &[], &[])
		.unwrap();
	let auth = &services.rooms.auth_chain;
	// Both roots are cold: an unavailable complete union must fall back to
	// checked walks, preserving the incomplete result rather than failing storage.
	let cold = auth
		.get_auth_chain(room, [good, child].into_iter())
		.await
		.unwrap();
	assert!(!cold.complete);
	cold.require_complete().unwrap_err();
	// Prime a complete root: completeness must still be false when another root has a gap.
	assert!(
		auth.get_auth_chain(room, std::iter::once(good))
			.await
			.unwrap()
			.complete
	);
	for roots in [vec![child], vec![good, child]] {
		let partial = auth
			.get_auth_chain(room, roots.iter().copied())
			.await
			.unwrap();
		assert!(!partial.complete);
		partial.require_complete().unwrap_err();
		auth.event_ids_iter(room, roots.iter().copied())
			.try_collect::<Vec<_>>()
			.await
			.unwrap_err();
	}
	let bitmap = auth
		.get_auth_chain_bitmap(room, std::iter::once(child))
		.await
		.unwrap();
	assert!(!bitmap.complete);
	bitmap.require_complete().unwrap_err();
	// No partial result may have entered either cache. Repeating after repair must succeed.
	adapter
		.write_event_with_edges(room.as_bytes(), parent.as_bytes(), b"parent", &[], &[])
		.unwrap();
	let mut expected = reference
		.complete_auth_chain_union(room.as_bytes(), &[good.as_bytes(), child.as_bytes()])
		.unwrap();
	expected.extend([good.as_bytes().to_vec(), child.as_bytes().to_vec()]);
	expected.sort();
	expected.dedup();
	for _ in 0..2 {
		let actual = auth
			.get_auth_chain(room, [good, child].into_iter())
			.await
			.unwrap()
			.require_complete()
			.unwrap();
		assert_eq!(actual.len(), expected.len());
		let emitted = auth
			.event_ids_iter(room, [good, child].into_iter())
			.try_collect::<Vec<_>>()
			.await
			.unwrap();
		let mut emitted: Vec<Vec<u8>> = emitted.iter().map(|id| id.as_bytes().to_vec()).collect();
		emitted.sort();
		assert_eq!(emitted, expected);
	}
	assert!(
		auth.get_auth_chain(room, std::iter::empty())
			.await
			.unwrap()
			.require_complete()
			.unwrap()
			.is_empty()
	);
}

#[test]
fn incomplete_empty_chain_is_not_a_successful_empty_response() {
	AuthChain { ids: Vec::<u64>::new(), complete: false }
		.require_complete()
		.unwrap_err();
	assert!(
		AuthChain { ids: Vec::<u64>::new(), complete: true }
			.require_complete()
			.unwrap()
			.is_empty()
	);
}
