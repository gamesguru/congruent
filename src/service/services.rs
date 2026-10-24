use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_lock::Mutex;
use conduwuit::{
	Result, Server, SyncRwLock, debug, debug_info, error, info, trace,
	utils::stream::{IterStream, ReadyExt},
	warn,
};
use database::Database;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::{
	account_data, admin, announcements, antispam, appservice, client, config, emergency,
	federation, firstrun, globals, key_backups, mailer,
	manager::Manager,
	media, moderation, password_reset, presence, pusher, registration_tokens, resolver, rooms,
	sending, server_keys,
	service::{self, Args, Map, Service},
	sync, threepid, transactions, uiaa, users,
};

pub struct Services {
	pub account_data: Arc<account_data::Service>,
	pub admin: Arc<admin::Service>,
	pub appservice: Arc<appservice::Service>,
	pub config: Arc<config::Service>,
	pub client: Arc<client::Service>,
	pub emergency: Arc<emergency::Service>,
	pub globals: Arc<globals::Service>,
	pub key_backups: Arc<key_backups::Service>,
	pub media: Arc<media::Service>,
	pub password_reset: Arc<password_reset::Service>,
	pub mailer: Arc<mailer::Service>,
	pub presence: Arc<presence::Service>,
	pub pusher: Arc<pusher::Service>,
	pub registration_tokens: Arc<registration_tokens::Service>,
	pub resolver: Arc<resolver::Service>,
	pub rooms: rooms::Service,
	pub federation: Arc<federation::Service>,
	pub firstrun: Arc<firstrun::Service>,
	pub sending: Arc<sending::Service>,
	pub server_keys: Arc<server_keys::Service>,
	pub sync: Arc<sync::Service>,
	pub transactions: Arc<transactions::Service>,
	pub threepid: Arc<threepid::Service>,
	pub uiaa: Arc<uiaa::Service>,
	pub users: Arc<users::Service>,
	pub moderation: Arc<moderation::Service>,
	pub announcements: Arc<announcements::Service>,
	pub antispam: Arc<antispam::Service>,

	manager: Mutex<Option<Arc<Manager>>>,
	pub(crate) service: Arc<Map>,
	pub server: Arc<Server>,
	pub db: Arc<Database>,
	pub mtxdb: Arc<conduwuit_mtxdb::store::MtxdbStore>,
}

impl Services {
	const INCOMPLETE_IMPORT_MESSAGE: &str = concat!(
		"This database has mtxdb data but no valid import marker, so the offline import ",
		"is incomplete or its marker was lost. Re-run `mtxdb import` with the complete ",
		"export set, or wipe the database and repeat the full offline conversion. ",
		"Refusing to start."
	);

	#[allow(clippy::cognitive_complexity)]
	pub fn build(server: Arc<Server>) -> Result<Arc<Self>> {
		let db = Database::open(&server)?;
		let mtxdb_path = server.config.database_path.join("mtxdb");
		let mtxdb =
			Arc::new(conduwuit_mtxdb::store::MtxdbStore::open(mtxdb_path).map_err(|error| {
				conduwuit::err!(Database(error!("Opening mtxdb sidecar: {error}")))
			})?);
		let service: Arc<Map> = Arc::new(SyncRwLock::new(BTreeMap::new()));
		macro_rules! build {
			($tyname:ty) => {{
				let built = <$tyname>::build(Args {
					db: &db,
					mtxdb: &mtxdb,
					server: &server,
					service: &service,
				})?;
				add_service(&service, built.clone(), built.clone());
				built
			}};
		}

		Ok(Arc::new(Self {
			// firstrun service should be built first so other services
			// can check first-run state
			firstrun: build!(firstrun::Service),
			account_data: build!(account_data::Service),
			admin: build!(admin::Service),
			appservice: build!(appservice::Service),
			resolver: build!(resolver::Service),
			client: build!(client::Service),
			config: build!(config::Service),
			emergency: build!(emergency::Service),
			globals: build!(globals::Service),
			key_backups: build!(key_backups::Service),
			media: build!(media::Service),
			password_reset: build!(password_reset::Service),
			mailer: build!(mailer::Service),
			presence: build!(presence::Service),
			pusher: build!(pusher::Service),
			registration_tokens: build!(registration_tokens::Service),
			rooms: rooms::Service {
				alias: build!(rooms::alias::Service),
				auth_chain: build!(rooms::auth_chain::Service),
				delayed_events: build!(rooms::delayed_events::Service),
				directory: build!(rooms::directory::Service),
				event_handler: build!(rooms::event_handler::Service),
				lazy_loading: build!(rooms::lazy_loading::Service),
				metadata: build!(rooms::metadata::Service),
				outlier: build!(rooms::outlier::Service),
				pdu_metadata: build!(rooms::pdu_metadata::Service),
				read_receipt: build!(rooms::read_receipt::Service),
				search: build!(rooms::search::Service),
				short: build!(rooms::short::Service),
				spaces: build!(rooms::spaces::Service),
				state: build!(rooms::state::Service),
				state_accessor: build!(rooms::state_accessor::Service),
				state_cache: build!(rooms::state_cache::Service),
				state_partial: build!(rooms::state_partial::Service),
				state_hamt: build!(rooms::state_hamt::Service),
				threads: build!(rooms::threads::Service),
				timeline: build!(rooms::timeline::Service),
				typing: build!(rooms::typing::Service),
				user: build!(rooms::user::Service),
				monitor: build!(rooms::monitor::Service),
				reconcile: build!(rooms::reconcile::Service),
			},
			federation: build!(federation::Service),
			sending: build!(sending::Service),
			server_keys: build!(server_keys::Service),
			sync: build!(sync::Service),
			threepid: build!(threepid::Service),
			transactions: build!(transactions::Service),
			uiaa: build!(uiaa::Service),
			users: build!(users::Service),
			moderation: build!(moderation::Service),
			announcements: build!(announcements::Service),
			antispam: build!(antispam::Service),

			manager: Mutex::new(None),
			service,
			server,
			db,
			mtxdb,
		}))
	}

	pub async fn start(self: &Arc<Self>) -> Result<Arc<Self>> {
		info!("Starting services...");

		self.admin.set_services(Some(Arc::clone(self)).as_ref());
		self.check_import_marker().await?;
		warn!(
			"Running database migrations... This may take a while depending on the database \
			 size."
		);
		super::migrations::migrations(self)
			.await
			.inspect_err(|e| error!("Migrations failed: {e}"))?;

		// Bound the crash-loss window and keep the shared WAL reclaiming. The
		// engine's background committer flushes within milliseconds; this
		// thread adds the pack/index checkpoint and segment reclaim barrier.
		if let Err(error) = self
			.mtxdb
			.start_maintenance(std::time::Duration::from_secs(30))
		{
			error!("Failed to start mtxdb maintenance: {error}");
		}

		// Initialize first-run state before listeners start accepting requests so
		// registration and banner checks cannot race the firstrun worker.
		self.firstrun.initialize_first_run_marker().await?;

		// Appservice tokens must resolve before the first request is served.
		self.appservice.initialize().await?;

		// Restore scheduled events before the listener can report ready. Otherwise
		// a restarted server may serve requests before the delayed-event worker
		// has loaded overdue sends from the database.
		self.rooms.delayed_events.initialize().await;

		info!("Starting service manager...");
		let manager = {
			let mut lock = self.manager.lock().await;
			let manager = Manager::new(self);
			_ = lock.insert(Arc::clone(&manager));
			manager
		};

		// reset dormant online/away statuses to offline on startup
		if self.server.config.allow_local_presence {
			info!("Local presence statuses will be reset in the background.");
		}

		info!("Starting service workers...");
		manager.start().await?;

		info!("Services startup complete.");

		Ok(Arc::clone(self))
	}

	/// mtxdb is the only store for events, verdicts and state, so a database
	/// whose events still live in RocksDB must be exported and imported before
	/// it can be served; otherwise every event would read as unrejected.
	/// A fresh install, with nothing in either store, is marked complete.
	async fn check_import_marker(&self) -> Result {
		use conduwuit_mtxdb::marker::{DATA_FORMAT, ImportMarker, MarkerAdapter};

		let marker = MarkerAdapter::new(self.mtxdb.clone());
		let db_error = |error: conduwuit_mtxdb::StorageError| {
			conduwuit::err!(Database(error!("Reading the mtxdb import marker: {error}")))
		};

		if let Some(found) = marker.get().map_err(db_error)? {
			if found.data_format != DATA_FORMAT {
				return Err(conduwuit::err!(Database(error!(
					"The mtxdb data format is {} but this server needs {DATA_FORMAT}. Export \
					 the database with the server version that wrote it and import it into this \
					 one.",
					found.data_format
				))));
			}
			return Ok(());
		}

		let has_legacy_events =
			self.db
				.deprecated("eventid_pdu")?
				.raw_keys()
				.next()
				.await
				.is_some() || (self.db.db.cf_exists("pduid_pdu")
				&& database::Map::open(&self.db.db, "pduid_pdu")?
					.raw_keys()
					.next()
					.await
					.is_some_and(|key| key.is_ok()))
				|| (self.db.db.cf_exists("eventid_outlierpdu")
					&& database::Map::open(&self.db.db, "eventid_outlierpdu")?
						.raw_keys()
						.next()
						.await
						.is_some_and(|key| key.is_ok()));
		let has_legacy_signing_keys = self
			.db
			.deprecated("server_signingkeys")?
			.raw_keys()
			.next()
			.await
			.is_some();
		let has_mtxdb_events = conduwuit_mtxdb::events::EventAdapter::new(self.mtxdb.clone())
			.has_any_events()
			.map_err(|error| {
				conduwuit::err!(Database(error!("Checking mtxdb event data: {error}")))
			})?;
		let has_mtxdb_verdicts =
			conduwuit_mtxdb::verdicts::VerdictAdapter::new(self.mtxdb.clone())
				.has_any_records()
				.map_err(|error| {
					conduwuit::err!(Database(error!("Checking mtxdb verdict data: {error}")))
				})?;
		let signing_keys =
			conduwuit_mtxdb::server_keys::ServerKeysAdapter::new(self.mtxdb.clone());
		let has_mtxdb_signing_keys = signing_keys.has_any_records().map_err(|error| {
			conduwuit::err!(Database(error!("Checking mtxdb signing-key data: {error}")))
		})? || signing_keys.has_any_native_records().map_err(
			|error| conduwuit::err!(Database(error!("Checking mtxdb signing-key data: {error}"))),
		)?;
		let has_mtxdb_short_ids = conduwuit_mtxdb::shortids::ShortIdAdapter::has_any_records(
			&self.mtxdb,
		)
		.map_err(|error| {
			conduwuit::err!(Database(error!("Checking mtxdb short id data: {error}")))
		})?;
		if has_legacy_events || has_legacy_signing_keys {
			return Err(conduwuit::err!(Database(error!(
				"This legacy database has no mtxdb import marker and still contains event or \
				 signing-key data in RocksDB. Export all rooms (`!admin yolo export-mtxdb \
				 --all`) and global signing keys (`!admin yolo export-mtxdb-keys`), import both \
				 with `mtxdb import`, then restart. Refusing to start."
			))));
		}

		if has_mtxdb_events || has_mtxdb_verdicts || has_mtxdb_signing_keys || has_mtxdb_short_ids
		{
			return Err(conduwuit::err!(Database(error!("{}", Self::INCOMPLETE_IMPORT_MESSAGE))));
		}

		if self.users.count().await > 0 {
			return Err(conduwuit::err!(Database(error!(
				"This database contains users but no mtxdb import marker and no event or \
				 signing-key data. Refusing to mark it as a fresh import; restore the complete \
				 export and run `mtxdb import`."
			))));
		}

		let tx = self.mtxdb.begin_transaction();
		marker
			.put(&tx, ImportMarker {
				data_format: DATA_FORMAT,
				events: 0,
				closures: 0,
			})
			.map_err(db_error)?;
		tx.commit().map_err(db_error)?;
		info!("Fresh database: marked mtxdb import complete.");
		Ok(())
	}

	pub async fn stop(&self) {
		info!("Shutting down services...");

		// Signal workers before the shutdown path can stall, so timers that
		// become due during teardown do not start new user-visible work.
		self.interrupt();

		// Report before anything can stall: the container may be killed mid-stop.
		self.db.log_write_stats();

		// Some service workers exit only after the server enters stopping state
		// and receives a shutdown signal. Interrupting alone is insufficient.
		if self.server.running() {
			self.server.shutdown().unwrap_or_else(error::default_log);
		}

		// set the server user as offline
		if self.server.config.allow_local_presence {
			_ = self
				.presence
				.ping_presence(
					&self.globals.server_user,
					&slipstream::presence::PresenceState::Offline,
				)
				.await;
		}

		self.interrupt();
		if let Some(manager) = self.manager.lock().await.as_ref() {
			manager.stop().await;
		}

		// Stop the periodic barrier before the final one, then record the index
		// coverage of everything written this session.
		self.mtxdb.stop_maintenance();
		if let Err(error) = self.mtxdb.checkpoint_all() {
			warn!("Failed to checkpoint mtxdb during shutdown: {error}");
		}
		for shard in conduwuit_mtxdb::ShardType::ALL {
			let pool = self.mtxdb.database().pool(shard);
			let stats = pool.stats();
			let checkpoint = pool.checkpoint_breakdown();
			info!(
				"mtxdb pool={} checkpoint_writes={} background_tails={} tail_waits={} \
				 delta_appends={} index_rebuilds={} last_checkpoint={checkpoint:?}",
				shard.as_str(),
				stats.checkpoint_writes,
				stats.checkpoint_tails_started,
				stats.syncs_waited_for_tail,
				stats.delta_appends,
				stats.index_rebuild_count,
			);
		}

		self.admin.set_services(None);

		debug_info!("Services shutdown complete.");
	}

	pub async fn poll(&self) -> Result<()> {
		if let Some(manager) = self.manager.lock().await.as_ref() {
			return manager.poll().await;
		}

		Ok(())
	}

	pub async fn clear_cache(&self) {
		use conduwuit::utils::stream::BroadbandExt;
		self.services()
			.broad_then(|service| async move {
				service.clear_cache().await;
			})
			.ready_for_each(|()| ())
			.await;
	}

	pub async fn memory_usage(&self) -> Result<String> {
		self.services()
			.map(Ok)
			.try_fold(String::new(), |mut out, service| async move {
				service.memory_usage(&mut out).await?;
				Ok(out)
			})
			.await
	}

	pub fn interrupt(&self) {
		warn!("Interrupting services...");
		for (name, (service, ..)) in self.service.read().iter() {
			if let Some(service) = service.upgrade() {
				debug!("Interrupting {name}");
				service.interrupt();
			}
		}
	}

	/// Iterate from snapshot of the services map
	fn services(&self) -> impl Stream<Item = Arc<dyn Service>> + Send {
		self.service
			.read()
			.values()
			.filter_map(|val| val.0.upgrade())
			.collect::<Vec<_>>()
			.into_iter()
			.stream()
	}

	#[inline]
	pub fn try_get<T>(&self, name: &str) -> Result<Arc<T>>
	where
		T: Any + Send + Sync + Sized,
	{
		service::try_get::<T>(&self.service, name)
	}

	#[inline]
	pub fn get<T>(&self, name: &str) -> Option<Arc<T>>
	where
		T: Any + Send + Sync + Sized,
	{
		service::get::<T>(&self.service, name)
	}
}

#[allow(clippy::needless_pass_by_value)]
fn add_service(map: &Arc<Map>, s: Arc<dyn Service>, a: Arc<dyn Any + Send + Sync>) {
	let name = s.name();
	let len = map.read().len();

	trace!("built service #{len}: {name:?}");
	map.write()
		.insert(name.to_owned(), (Arc::downgrade(&s), Arc::downgrade(&a)));
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
	};

	use conduwuit::{
		Server,
		config::Config,
		log::{Log, LogLevelReloadHandles, capture},
	};

	use super::Services;

	static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

	struct TestDb(std::path::PathBuf);

	impl Drop for TestDb {
		fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
	}

	#[conduwuit_macros::async_test]
	async fn startup_refuses_short_ids_without_import_marker() {
		let _serial = crate::DB_TEST_MUTEX.lock().await;
		let _ = rustls::crypto::ring::default_provider().install_default();
		let count = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
		let db_path = std::env::temp_dir().join(format!(
			"conduwuit_services_marker_test_{}_{}",
			std::process::id(),
			count
		));
		let _ = fs::remove_dir_all(&db_path);
		let _guard = TestDb(db_path.clone());

		let config_raw = conduwuit::config::RawConfig::from_toml(&format!(
			r#"
				server_name = "test.conduwuit.local"
				database_path = "{}"
				"#,
			db_path.to_string_lossy().replace('\\', "/")
		))
		.expect("failed to parse test config");
		let config = Config::new(&config_raw).expect("failed to validate test config");
		let runtime_handle = ();
		let server = Arc::new(Server::new(config, Some(&runtime_handle), Log {
			reload: LogLevelReloadHandles,
			capture: Arc::new(capture::State),
		}));
		let services = Services::build(server).expect("failed to build test services");
		conduwuit_mtxdb::shortids::ShortIdAdapter::new(services.mtxdb.clone())
			.expect("failed to open short-id adapter")
			.allocate_events(&[b"$partial:example.org"])
			.expect("failed to write short-id record");

		let error = services
			.check_import_marker()
			.await
			.expect_err("startup accepted short-id data without an import marker");
		assert!(
			error
				.to_string()
				.contains(Services::INCOMPLETE_IMPORT_MESSAGE)
		);
		assert!(
			conduwuit_mtxdb::marker::MarkerAdapter::new(services.mtxdb.clone())
				.get()
				.expect("failed to read import marker after startup refusal")
				.is_none(),
			"startup refusal must not mark incomplete data as imported"
		);
	}
}
