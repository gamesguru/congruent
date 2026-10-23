#![type_length_limit = "8192"]
#![allow(refining_impl_trait)]
#![allow(clippy::disallowed_macros)]
#![recursion_limit = "256"]

extern crate conduwuit_core as conduwuit;
extern crate conduwuit_database as database;

conduwuit_macros::introspect_crate! {}

mod manager;
mod migrations;
mod service;
pub mod services;
pub mod state;

pub mod account_data;
pub mod admin;
pub mod announcements;
pub mod antispam;
pub mod appservice;
pub mod client;
pub mod config;
pub mod emergency;
pub mod federation;
pub mod firstrun;
pub mod globals;
pub mod key_backups;
pub mod mailer;
pub mod media;
pub mod moderation;
pub mod password_reset;
pub mod presence;
pub mod pusher;
pub mod registration_tokens;
pub mod resolver;
pub mod rooms;
pub mod sending;
pub mod server_keys;
pub mod sync;
pub mod threepid;
pub mod transactions;
pub mod uiaa;
pub mod users;

pub(crate) use service::{Args, Dep, Service};

// RocksDB's default Env is process-global. Serializing tests that open full
// service databases prevents one test's teardown from disrupting another.
#[cfg(test)]
pub(crate) static DB_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub use crate::services::Services;

conduwuit::mod_ctor! {}
conduwuit::mod_dtor! {}
