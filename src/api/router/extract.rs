//! Transport extractor aliases kept at the router boundary.
//!
//! Handler modules import these names from here so the eventual router
//! replacement does not require changing every Matrix endpoint.

pub(crate) use axum::extract::{Path, Query, RawQuery, State};
pub(crate) use axum_client_ip::ClientIp;
pub(crate) use axum_extra::{TypedHeader, headers};
