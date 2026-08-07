use std::{net::SocketAddr, sync::Arc, time::Duration};

use conduwuit::{Result, Server, err};
use futures::FutureExt;
use hickory_resolver::{TokioResolver, lookup_ip::LookupIp};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use super::cache::{Cache, CachedOverride};
use crate::{Dep, client};

pub struct Resolver {
	pub(crate) resolver: Arc<TokioResolver>,
	pub(crate) hooked: Arc<Hooked>,
	server: Arc<Server>,
	client: Dep<client::Service>,
}

pub(crate) struct Hooked {
	resolver: Arc<TokioResolver>,
	cache: Arc<Cache>,
	server: Arc<Server>,
	client: Dep<client::Service>,
}

type ResolvingResult = Result<Addrs, Box<dyn std::error::Error + Send + Sync>>;

impl Resolver {
	#[allow(clippy::as_conversions, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
	pub(crate) fn build(
		server: &Arc<Server>,
		cache: Arc<Cache>,
		client_resolver: Dep<client::Service>,
		client_hooked: Dep<client::Service>,
	) -> Result<Arc<Self>> {
		let config = &server.config;
		let (sys_conf, mut opts) = hickory_resolver::system_conf::read_system_conf()
			.map_err(|e| err!(error!("Failed to configure DNS resolver from system: {e}")))?;

		let mut conf = hickory_resolver::config::ResolverConfig::new();

		if let Some(domain) = sys_conf.domain() {
			conf.set_domain(domain.clone());
		}

		for sys_conf in sys_conf.search() {
			conf.add_search(sys_conf.clone());
		}

		for sys_conf in sys_conf.name_servers() {
			let mut ns = sys_conf.clone();

			if config.query_over_tcp_only {
				ns.protocol = hickory_resolver::proto::xfer::Protocol::Tcp;
			}

			ns.trust_negative_responses = !config.query_all_nameservers;

			conf.add_name_server(ns);
		}

		opts.cache_size = config.dns_cache_entries as usize;
		opts.preserve_intermediates = true;
		opts.negative_min_ttl = Some(Duration::from_secs(config.dns_min_ttl_nxdomain));
		opts.negative_max_ttl = opts.negative_min_ttl;
		opts.positive_min_ttl = Some(Duration::from_secs(config.dns_min_ttl));
		opts.positive_max_ttl = opts.positive_min_ttl;
		opts.timeout = Duration::from_secs(config.dns_timeout);
		opts.attempts = config.dns_attempts as usize;
		opts.try_tcp_on_error = config.dns_tcp_fallback;
		opts.num_concurrent_reqs = 3;
		opts.edns0 = true;
		opts.case_randomization = config.dns_case_randomization;
		opts.ip_strategy = match config.ip_lookup_strategy {
			| 1 => hickory_resolver::config::LookupIpStrategy::Ipv4Only,
			| 2 => hickory_resolver::config::LookupIpStrategy::Ipv6Only,
			| 3 => hickory_resolver::config::LookupIpStrategy::Ipv4AndIpv6,
			| 4 => hickory_resolver::config::LookupIpStrategy::Ipv6thenIpv4,
			| _ => hickory_resolver::config::LookupIpStrategy::Ipv4thenIpv6,
		};

		let rt_prov = hickory_resolver::proto::runtime::TokioRuntimeProvider::new();
		let conn_prov = hickory_resolver::name_server::TokioConnectionProvider::new(rt_prov);
		let mut builder = TokioResolver::builder_with_config(conf, conn_prov);
		*builder.options_mut() = opts;
		let resolver = Arc::new(builder.build());

		Ok(Arc::new(Self {
			resolver: resolver.clone(),
			hooked: Arc::new(Hooked {
				resolver,
				cache,
				server: server.clone(),
				client: client_hooked,
			}),
			server: server.clone(),
			client: client_resolver,
		}))
	}

	#[inline]
	pub fn clear_cache(&self) { self.resolver.clear_cache(); }

	pub async fn lookup_ip<N: hickory_resolver::IntoName>(
		&self,
		name: N,
	) -> core::result::Result<LookupIp, hickory_resolver::ResolveError> {
		let start = tokio::time::Instant::now();
		let result = self.resolver.lookup_ip(name).await;
		let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
		self.server
			.metrics
			.dns_requests_time
			.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);

		match &result {
			| Err(e) if !is_no_records_found(e) => {
				self.server
					.metrics
					.dns_requests_fail
					.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			},
			| Ok(_) => {
				self.server
					.metrics
					.dns_requests_success
					.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			},
			| _ => {},
		}

		result
	}

	pub async fn srv_lookup<N: hickory_resolver::IntoName>(
		&self,
		name: N,
	) -> core::result::Result<hickory_resolver::lookup::SrvLookup, hickory_resolver::ResolveError>
	{
		let start = tokio::time::Instant::now();
		let result = self.resolver.srv_lookup(name).await;
		let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
		self.server
			.metrics
			.dns_requests_time
			.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);

		match &result {
			| Err(e) if !is_no_records_found(e) => {
				self.server
					.metrics
					.dns_requests_fail
					.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			},
			| Ok(_) => {
				self.server
					.metrics
					.dns_requests_success
					.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			},
			| _ => {},
		}

		result
	}
}

impl Resolve for Resolver {
	fn resolve(&self, name: Name) -> Resolving {
		resolve_to_reqwest(
			self.server.clone(),
			self.resolver.clone(),
			Arc::clone(&self.client),
			name,
		)
		.boxed()
	}
}

impl Resolve for Hooked {
	fn resolve(&self, name: Name) -> Resolving {
		hooked_resolve(
			self.cache.clone(),
			self.server.clone(),
			self.resolver.clone(),
			Arc::clone(&self.client),
			name,
		)
		.boxed()
	}
}

#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(name = ?name.as_str())
)]
async fn hooked_resolve(
	cache: Arc<Cache>,
	server: Arc<Server>,
	resolver: Arc<TokioResolver>,
	client: Arc<client::Service>,
	name: Name,
) -> Result<Addrs, Box<dyn std::error::Error + Send + Sync>> {
	match cache.get_override(name.as_str()).await {
		| Ok(cached) if cached.valid() => cached_to_reqwest(cached, client).await,
		| Ok(CachedOverride { overriding, .. }) if overriding.is_some() =>
			resolve_to_reqwest(
				server,
				resolver,
				client,
				overriding
					.as_deref()
					.map(str::parse)
					.expect("overriding is set for this record")
					.expect("overriding is a valid internet name"),
			)
			.boxed()
			.await,

		| _ =>
			resolve_to_reqwest(server, resolver, client, name)
				.boxed()
				.await,
	}
}

fn is_valid_ip(ip: &std::net::IpAddr, client: &client::Service) -> bool {
	use ipaddress::IPAddress;
	if let Ok(parsed_ip) = IPAddress::parse(ip.to_string()) {
		client.valid_cidr_range(&parsed_ip)
	} else {
		false
	}
}

async fn resolve_to_reqwest(
	server: Arc<Server>,
	resolver: Arc<TokioResolver>,
	client: Arc<client::Service>,
	name: Name,
) -> ResolvingResult {
	use std::{io, io::ErrorKind::Interrupted, net::IpAddr};

	if let Ok(ip_addr) = name.as_str().parse::<IpAddr>() {
		let addrs: Addrs = Box::new(std::iter::once(SocketAddr::new(ip_addr, 0)));
		return Ok(addrs);
	}

	let handle_shutdown = || Box::new(io::Error::new(Interrupted, "Server shutting down"));
	let handle_results = |results: LookupIp| {
		let addrs: Addrs = Box::new(
			results
				.into_iter()
				.filter(move |ip| is_valid_ip(ip, &client))
				.map(|ip| SocketAddr::new(ip, 0)),
		);
		addrs
	};

	let start = tokio::time::Instant::now();
	let result = tokio::select! {
		results = resolver.lookup_ip(name.as_str()) => {
			match results {
				Ok(results) => {
					let res: ResolvingResult = Ok(handle_results(results));
					let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
					server
						.metrics
						.dns_requests_time
						.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);
					server
						.metrics
						.dns_requests_success
						.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
					res
				},
				Err(e) => {
					let res: ResolvingResult = Err(Box::new(e));
					res
				}
			}
		},
		() = server.until_shutdown() => {
			let res: ResolvingResult = Err(handle_shutdown());
			res
		},
	};

	if result.is_err() {
		let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
		server
			.metrics
			.dns_requests_time
			.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);
		// Don't count NoRecordsFound as a failure — it's a valid negative response
		if let Err(ref boxed_err) = result {
			if !boxed_err
				.downcast_ref::<hickory_resolver::ResolveError>()
				.is_some_and(is_no_records_found)
			{
				server
					.metrics
					.dns_requests_fail
					.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			}
		}
	}

	result
}

/// Check if a DNS resolve error is a NoRecordsFound (NXDOMAIN/NoError)
/// response. These are valid negative responses, not actual failures.
/// ServFail is explicitly excluded as it indicates a transient server error.
fn is_no_records_found(e: &hickory_resolver::ResolveError) -> bool {
	use hickory_resolver::{
		ResolveErrorKind::Proto,
		proto::{ProtoErrorKind, op::ResponseCode},
	};

	matches!(
		e.kind(),
		Proto(e) if matches!(
			e.kind(),
			ProtoErrorKind::NoRecordsFound {
				response_code: ResponseCode::NXDomain | ResponseCode::NoError,
				..
			}
		)
	)
}

async fn cached_to_reqwest(
	cached: CachedOverride,
	client: Arc<client::Service>,
) -> ResolvingResult {
	let addrs = cached
		.ips
		.into_iter()
		.filter(move |ip| is_valid_ip(ip, &client))
		.map(move |ip| SocketAddr::new(ip, cached.port));

	Ok(Box::new(addrs))
}
