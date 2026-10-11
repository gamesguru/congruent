use super::fed::{FedDest, add_port_to_hostname, get_ip_with_port};

#[test]
fn ips_get_default_ports() {
	assert_eq!(
		get_ip_with_port("1.1.1.1"),
		Some(FedDest::Literal("1.1.1.1:8448".parse().unwrap()))
	);
	assert_eq!(
		get_ip_with_port("dead:beef::"),
		Some(FedDest::Literal("[dead:beef::]:8448".parse().unwrap()))
	);
}

#[test]
fn ips_keep_custom_ports() {
	assert_eq!(
		get_ip_with_port("1.1.1.1:1234"),
		Some(FedDest::Literal("1.1.1.1:1234".parse().unwrap()))
	);
	assert_eq!(
		get_ip_with_port("[dead::beef]:8933"),
		Some(FedDest::Literal("[dead::beef]:8933".parse().unwrap()))
	);
}

#[test]
fn hostnames_get_default_ports() {
	assert_eq!(
		add_port_to_hostname("example.com"),
		FedDest::Named(String::from("example.com"), ":8448".try_into().unwrap())
	);
}

#[test]
fn hostnames_keep_custom_ports() {
	assert_eq!(
		add_port_to_hostname("example.com:1337"),
		FedDest::Named(String::from("example.com"), ":1337".try_into().unwrap())
	);
}

#[conduwuit_macros::async_test]
async fn test_dns_resolution_integration() {
	// This is a minimal test that verifies the hickory-resolver can initialize
	// and perform a lookup. We can't easily run a full Server here, but we
	// can verify the configuration logic we use in Resolver::build.
	let (sys_conf, mut opts) = hickory_resolver::system_conf::read_system_conf().unwrap();
	opts.use_hosts_file = hickory_resolver::config::ResolveHosts::Always;

	let rt_prov = super::dns::SmolRuntimeProvider::new(conduwuit_core::RuntimeHandle::new());
	let mut builder = hickory_resolver::Resolver::builder_with_config(sys_conf, rt_prov);
	*builder.options_mut() = opts;
	let resolver = builder.build().unwrap();

	// Test resolving localhost, which should be in /etc/hosts on almost any system
	let result = resolver.lookup_ip("localhost").await;
	assert!(result.is_ok(), "Failed to resolve localhost: {:?}", result.err());

	// Test a custom host if provided via environment (used in CI)
	if let Ok(custom_host) = std::env::var("CONDUWUIT_TEST_DNS_HOST") {
		let result = resolver.lookup_ip(&custom_host).await;
		assert!(
			result.is_ok(),
			"Failed to resolve custom host {}: {:?}",
			custom_host,
			result.err()
		);
	}
}
