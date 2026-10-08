use std::{
	future::Future,
	io,
	net::SocketAddr,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
	time::{Duration, Instant},
};

use async_io::Async;
use conduwuit::{Result, Server, err};
use futures::future::{Either, select};
use futures_io::{AsyncRead, AsyncWrite};
use hickory_resolver::{
	Resolver as HickoryResolver,
	lookup_ip::LookupIp,
	net::runtime::{DnsTcpStream, DnsUdpSocket, RuntimeProvider, Spawn, Time},
};

use super::cache::Cache;
use crate::{Dep, client};

#[derive(Clone)]
pub(crate) struct SmolRuntimeProvider {
	runtime: conduwuit_core::RuntimeHandle,
}

impl SmolRuntimeProvider {
	pub(crate) fn new(runtime: conduwuit_core::RuntimeHandle) -> Self { Self { runtime } }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SmolTime;

#[async_trait::async_trait]
impl Time for SmolTime {
	async fn delay_for(duration: Duration) { let _ = smol::Timer::after(duration).await; }

	async fn timeout<F: Future + Send + 'static>(
		duration: Duration,
		future: F,
	) -> io::Result<F::Output> {
		match select(Box::pin(future), Box::pin(smol::Timer::after(duration))).await {
			| Either::Left((output, _)) => Ok(output),
			| Either::Right((..)) =>
				Err(io::Error::new(io::ErrorKind::TimedOut, "future timed out")),
		}
	}
}

impl Spawn for SmolRuntimeProvider {
	fn spawn_bg(&mut self, future: impl Future<Output = ()> + Send + 'static) {
		let _ = self.runtime.spawn(future);
	}
}

impl RuntimeProvider for SmolRuntimeProvider {
	type Handle = Self;
	type Tcp = SmolTcpStream;
	type Timer = SmolTime;
	type Udp = SmolUdpSocket;

	fn create_handle(&self) -> Self::Handle { self.clone() }

	fn connect_tcp(
		&self,
		server_addr: SocketAddr,
		_bind_addr: Option<SocketAddr>,
		timeout: Option<Duration>,
	) -> Pin<Box<dyn Send + Future<Output = io::Result<Self::Tcp>>>> {
		Box::pin(async move {
			let duration = timeout.unwrap_or(Duration::from_secs(5));
			match select(
				Box::pin(async_net::TcpStream::connect(server_addr)),
				Box::pin(smol::Timer::after(duration)),
			)
			.await
			{
				| Either::Left((result, _)) => result.map(SmolTcpStream),
				| Either::Right((..)) =>
					Err(io::Error::new(io::ErrorKind::TimedOut, "TCP connect timed out")),
			}
		})
	}

	fn bind_udp(
		&self,
		local_addr: SocketAddr,
		_server_addr: SocketAddr,
	) -> Pin<Box<dyn Send + Future<Output = io::Result<Self::Udp>>>> {
		Box::pin(async move {
			let socket = std::net::UdpSocket::bind(local_addr)?;
			socket.set_nonblocking(true)?;
			Ok(SmolUdpSocket(Async::new(socket)?))
		})
	}
}

pub(crate) struct SmolTcpStream(async_net::TcpStream);

impl AsyncRead for SmolTcpStream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<io::Result<usize>> {
		Pin::new(&mut self.0).poll_read(cx, buf)
	}
}

impl AsyncWrite for SmolTcpStream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		Pin::new(&mut self.0).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.0).poll_flush(cx)
	}

	fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.0).poll_close(cx)
	}
}

impl DnsTcpStream for SmolTcpStream {
	type Time = SmolTime;
}

pub(crate) struct SmolUdpSocket(Async<std::net::UdpSocket>);

#[async_trait::async_trait]
impl DnsUdpSocket for SmolUdpSocket {
	type Time = SmolTime;

	fn poll_recv_from(
		&self,
		cx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<io::Result<(usize, SocketAddr)>> {
		loop {
			match self.0.get_ref().recv_from(buf) {
				| Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
					std::task::ready!(self.0.poll_readable(cx))?;
				},
				| result => return Poll::Ready(result),
			}
		}
	}

	fn poll_send_to(
		&self,
		cx: &mut Context<'_>,
		buf: &[u8],
		target: SocketAddr,
	) -> Poll<io::Result<usize>> {
		loop {
			match self.0.get_ref().send_to(buf, target) {
				| Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
					std::task::ready!(self.0.poll_writable(cx))?;
				},
				| result => return Poll::Ready(result),
			}
		}
	}
}

pub struct Resolver {
	pub(crate) resolver: Arc<HickoryResolver<SmolRuntimeProvider>>,
	server: Arc<Server>,
}

impl Resolver {
	#[allow(clippy::as_conversions, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
	pub(crate) fn build(
		server: &Arc<Server>,
		cache: Arc<Cache>,
		_client_resolver: Dep<client::Service>,
		_client_hooked: Dep<client::Service>,
	) -> Result<Arc<Self>> {
		let config = &server.config;
		let (sys_conf, mut opts) = hickory_resolver::system_conf::read_system_conf()
			.map_err(|e| err!(error!("Failed to configure DNS resolver from system: {e}")))?;

		let (domain, search, mut name_servers) = sys_conf.into_parts();

		for ns in &mut name_servers {
			if config.query_over_tcp_only {
				ns.connections = vec![hickory_resolver::config::ConnectionConfig::tcp()];
			}

			ns.trust_negative_responses = !config.query_all_nameservers;
		}

		let conf =
			hickory_resolver::config::ResolverConfig::from_parts(domain, search, name_servers);

		opts.cache_size = u64::from(config.dns_cache_entries);
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

		let rt_prov = SmolRuntimeProvider::new(server.runtime().clone());
		let mut builder = HickoryResolver::builder_with_config(conf, rt_prov);
		*builder.options_mut() = opts;
		let resolver = Arc::new(
			builder
				.build()
				.map_err(|e| err!(error!("Failed to build DNS resolver: {e}")))?,
		);

		Ok(Arc::new(Self {
			resolver: resolver.clone(),
			server: server.clone(),
		}))
	}

	#[inline]
	pub fn clear_cache(&self) { self.resolver.clear_cache(); }

	pub async fn lookup_ip<N: hickory_resolver::proto::rr::IntoName>(
		&self,
		name: N,
	) -> core::result::Result<LookupIp, hickory_resolver::net::NetError> {
		let start = Instant::now();
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

	pub async fn srv_lookup<N: hickory_resolver::proto::rr::IntoName>(
		&self,
		name: N,
	) -> core::result::Result<hickory_resolver::lookup::Lookup, hickory_resolver::net::NetError>
	{
		let start = Instant::now();
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

/// Check if a DNS resolve error is a NoRecordsFound (NXDOMAIN/NoError)
/// response. These are valid negative responses, not actual failures.
/// ServFail is explicitly excluded as it indicates a transient server error.
fn is_no_records_found(e: &hickory_resolver::net::NetError) -> bool {
	use hickory_resolver::{
		net::{DnsError, NetError},
		proto::op::ResponseCode,
	};

	matches!(
		e,
		NetError::Dns(DnsError::NoRecordsFound(records))
			if matches!(records.response_code, ResponseCode::NXDomain | ResponseCode::NoError)
	)
}
