#![type_length_limit = "16384"] //TODO: reduce me
#![recursion_limit = "512"] //TODO: reduce me
#![allow(clippy::toplevel_ref_arg)]
#![allow(clippy::disallowed_macros)]

extern crate conduwuit_core as conduwuit;
extern crate conduwuit_service as service;

conduwuit_macros::introspect_crate! {}

pub mod client;
pub(crate) mod hyper_router;
pub(crate) mod json_util;
pub(crate) mod msc2836;
pub mod router;
pub mod server;

pub mod admin;

pub(crate) use self::router::{Ruma, RumaResponse, State};

conduwuit::mod_ctor! {}
conduwuit::mod_dtor! {}
