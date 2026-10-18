use std::collections::BTreeMap;

use conduwuit::Result;
use slipstream::api::federation::discovery::get_server_version;

use crate::{Ruma, router::extract::State};

/// # `GET /_matrix/federation/v1/version`
///
/// Get version information on this server.
pub(crate) async fn get_server_version_route(
	State(services): State<crate::State>,
	_body: Ruma<get_server_version::v1::Request>,
) -> Result<get_server_version::v1::Response> {
	// MSC0501 capability discovery.
	let unstable_features = services
		.config
		.experimental_features
		.msc4521_enabled
		.then(|| {
			BTreeMap::from([
				("tk.nutra.msc0501.reconciliation".to_owned(), true),
				(conduwuit_service::rooms::reconcile::DIGEST_FEATURE.to_owned(), true),
			])
		});

	Ok(get_server_version::v1::Response {
		server: Some(get_server_version::v1::Server {
			name: Some(conduwuit::version::name().into()),
			version: Some(conduwuit::version::version().into()),
		}),
		unstable_features,
	})
}
