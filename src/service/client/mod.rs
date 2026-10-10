use std::{
	collections::HashMap,
	pin::Pin,
	sync::{Arc, Mutex, PoisonError},
	task::{Context, Poll},
	time::{Duration, Instant},
};

use bytes::Bytes;
use conduwuit::{
	Config, Error, Result, config::proxy::ProxyConfig, implement, rt::TimeoutError, timeout,
	timeout_at, utils::IpCidr,
};
use futures_io::{AsyncRead, AsyncWrite};
use futures_rustls::{
	TlsConnector,
	rustls::{
		ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore, SignatureScheme,
		client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
		crypto::CryptoProvider,
		pki_types::{CertificateDer, ServerName, UnixTime},
	},
};
use http::{
	HeaderMap, HeaderValue, Method, Request, Response, StatusCode,
	header::{AUTHORIZATION, COOKIE, HOST, PROXY_AUTHORIZATION, WWW_AUTHENTICATE},
};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, client::conn::http1};
use url::Url;

use crate::{
	resolver::{self, Service as ResolverService, cache::CachedOverride},
	service,
};

const OVERFLOW_DEADLINE_GRACE: Duration = Duration::from_hours(8760);

fn request_deadline(total_timeout: Duration) -> Instant {
	Instant::now()
		.checked_add(total_timeout)
		.unwrap_or_else(|| {
			Instant::now()
				.checked_add(OVERFLOW_DEADLINE_GRACE)
				.expect("one-year deadline must fit in Instant")
		})
}

mod connector;
mod proxy;

#[derive(Clone)]
pub struct HttpClient {
	default_headers: HeaderMap,
	user_agent: Option<String>,
	tls: Arc<ClientConfig>,
	max_size: usize,
	connect_timeout: Duration,
	read_timeout: Duration,
	total_timeout: Duration,
	redirect_limit: usize,
	denylist_validation: bool,
	cidr_range_denylist: Vec<IpCidr>,
	resolver: Option<Arc<ResolverService>>,
	proxy: ProxyConfig,
	pool: Arc<Pool>,
}

type Sender = http1::SendRequest<Full<Bytes>>;

/// How long an idle keep-alive connection may be reused.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Idle connections kept per origin. Sized above the federation fetch
/// concurrency (`concurrency_scaled(2)`, 20 on a typical host), so a burst of
/// parallel fetches to one server reuses connections instead of dropping the
/// surplus and re-handshaking.
const POOL_MAX_IDLE_PER_HOST: usize = 64;

/// Connections are only interchangeable when everything that influenced how
/// they were established matches.
// Deliberately not `Debug`: `proxy` may embed credentials.
#[derive(Clone, Eq, Hash, PartialEq)]
struct PoolKey {
	tls: bool,
	authority: String,
	validate: bool,
	/// The full proxy URL (including any credentials) the connection was routed
	/// through, if any, so differently-authenticated proxies never share one.
	proxy: Option<String>,
}

struct Idle {
	sender: Sender,
	since: Instant,
}

/// Idle HTTP/1.1 keep-alive connections, shared by all clones of a client.
struct Pool {
	idle: Mutex<HashMap<PoolKey, Vec<Idle>>>,
	max_idle: usize,
}

impl Default for Pool {
	fn default() -> Self { Self::new(POOL_MAX_IDLE_PER_HOST) }
}

impl Pool {
	fn new(max_idle: usize) -> Self { Self { idle: Mutex::default(), max_idle } }

	fn take(&self, key: &PoolKey) -> Option<Sender> {
		let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
		let list = idle.get_mut(key)?;
		while let Some(entry) = list.pop() {
			if entry.since.elapsed() < POOL_IDLE_TIMEOUT && !entry.sender.is_closed() {
				return Some(entry.sender);
			}
		}

		None
	}

	fn put(&self, key: PoolKey, sender: Sender) {
		let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
		let list = idle.entry(key).or_default();
		list.retain(|entry| {
			entry.since.elapsed() < POOL_IDLE_TIMEOUT && !entry.sender.is_closed()
		});
		if list.len() < self.max_idle {
			list.push(Idle { sender, since: Instant::now() });
		}
	}
}

impl HttpClient {
	/// Execute using the client's own default response-body limit.
	pub async fn execute(&self, request: Request<Bytes>) -> Result<Response<Bytes>> {
		Box::pin(self.execute_with_limit(request, self.max_size)).await
	}

	/// Execute with an explicit response-body byte limit for this call.
	pub async fn execute_with_limit(
		&self,
		request: Request<Bytes>,
		max_size: usize,
	) -> Result<Response<Bytes>> {
		let url = parse_url(request.uri())?;
		// The total deadline is shared by every redirect hop, matching the
		// pre-migration reqwest clients.
		let deadline = request_deadline(self.total_timeout);
		match timeout_at(deadline, self.execute_hops(request, url, max_size)).await {
			| Ok(result) => result,
			| Err(TimeoutError) => Err(Error::HttpClientTimeout(
				format!("request exceeded total timeout of {:?}", self.total_timeout).into(),
			)),
		}
	}

	pub fn request<U: AsRef<str>>(&self, method: Method, url: U) -> RequestBuilder<'_> {
		RequestBuilder {
			client: self,
			method,
			url: url.as_ref().to_owned(),
			headers: HeaderMap::new(),
			body: Bytes::new(),
		}
	}

	pub fn get<U: AsRef<str>>(&self, url: U) -> RequestBuilder<'_> {
		self.request(Method::GET, url)
	}

	pub fn head<U: AsRef<str>>(&self, url: U) -> RequestBuilder<'_> {
		self.request(Method::HEAD, url)
	}

	pub fn post<U: AsRef<str>>(&self, url: U) -> RequestBuilder<'_> {
		self.request(Method::POST, url)
	}

	#[must_use]
	pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
		self.connect_timeout = connect_timeout;
		self
	}

	#[must_use]
	pub fn read_timeout(mut self, read_timeout: Duration) -> Self {
		self.read_timeout = read_timeout;
		self
	}

	#[must_use]
	pub fn total_timeout(mut self, total_timeout: Duration) -> Self {
		self.total_timeout = total_timeout;
		self
	}

	#[must_use]
	pub fn redirect_limit(mut self, redirect_limit: usize) -> Self {
		self.redirect_limit = redirect_limit;
		self
	}

	#[must_use]
	pub fn max_size(mut self, max_size: usize) -> Self {
		self.max_size = max_size;
		self
	}

	#[must_use]
	pub fn denylist_validation(mut self, denylist_validation: bool) -> Self {
		self.denylist_validation = denylist_validation;
		self
	}

	async fn execute_hops(
		&self,
		mut request: Request<Bytes>,
		mut url: Url,
		max_size: usize,
	) -> Result<Response<Bytes>> {
		apply_headers(&mut request, &self.default_headers, self.user_agent.as_deref())?;

		let mut redirects = 0;
		loop {
			let response = self.attempt(&url, &request, max_size).await?;
			let Some(location) = redirect_location(&response) else {
				return Ok(response);
			};

			let next = url.join(location).map_err(|error| {
				Error::HttpClient(format!("invalid redirect target: {error}").into())
			})?;
			if !can_follow_redirect(redirects, self.redirect_limit, &url, &next) {
				return Ok(response);
			}

			follow_redirect(&mut request, &mut url, next, response.status())?;
			redirects = redirects.saturating_add(1);
		}
	}

	/// Open a connection to the target of `url`, send `request`, and return the
	/// fully-collected response.
	async fn attempt(
		&self,
		url: &Url,
		request: &Request<Bytes>,
		max_size: usize,
	) -> Result<Response<Bytes>> {
		let target = Self::target(url)?;
		let tls = url.scheme() == "https";
		let authority = target.authority();
		let key = PoolKey {
			tls,
			authority: authority.clone(),
			validate: self.denylist_validation,
			proxy: self
				.proxy
				.proxy_url(url)
				.map(|proxy| proxy.as_str().to_owned()),
		};

		let mut reusable = self.pool.take(&key);
		loop {
			let reused = reusable.is_some();
			let mut sender = match reusable.take() {
				| Some(sender) => sender,
				| None => {
					let connected =
						timeout(self.connect_timeout, self.connect(url, &target, tls)).await;
					match connected {
						| Ok(result) => result?,
						| Err(TimeoutError) =>
							return Err(Error::HttpClientConnect(
								format!(
									"connection to {authority} timed out after {:?}",
									self.connect_timeout
								)
								.into(),
							)),
					}
				},
			};

			let prepared = prepare_request(request, url, self.needs_absolute_form(url))?;
			// Methods that are safe to replay if the connection fails after the request
			// may have reached the peer. Anything else is only retried when hyper
			// reports the request was never written.
			let replay_safe = matches!(
				*request.method(),
				Method::GET
					| Method::HEAD | Method::PUT
					| Method::DELETE
					| Method::OPTIONS
					| Method::TRACE
			);
			let request_closes = wants_close(request.headers());
			let read = timeout(self.read_timeout, async {
				let response = match sender.try_send_request(prepared).await {
					| Ok(response) => response,
					| Err(failure) => {
						// A pooled connection may have been closed by the peer while idle.
						let unsent = failure.message().is_some();
						let error = failure.into_error();
						let stale = unsent
							|| (replay_safe
								&& (error.is_canceled()
									|| error.is_closed() || error.is_incomplete_message()));
						return Err((
							stale,
							Error::HttpClient(
								format!("failed to send request to {authority}: {error}").into(),
							),
						));
					},
				};
				collect_body(response, max_size)
					.await
					.map_err(|(transient, error)| (transient && replay_safe && reused, error))
			})
			.await;

			match read {
				| Ok(Ok(response)) => {
					let reusable = !request_closes && connection_reusable(&response);
					if reusable && !sender.is_closed() {
						self.pool.put(key, sender);
					}

					return Ok(response);
				},
				| Ok(Err((stale, error))) =>
					if reused && stale {
						// Retry once on a fresh connection.
						continue;
					} else {
						return Err(error);
					},
				| Err(TimeoutError) =>
					return Err(Error::HttpClientTimeout(
						format!(
							"response from {authority} exceeded read timeout of {:?}",
							self.read_timeout
						)
						.into(),
					)),
			}
		}
	}

	/// Dial `target` (directly or through the configured proxy), wrap it in TLS
	/// where required, and start an HTTP/1 client connection.
	async fn connect(
		&self,
		url: &Url,
		target: &proxy::Target,
		tls: bool,
	) -> Result<http1::SendRequest<Full<Bytes>>> {
		let stream = self.open_stream(url, target, tls).await?;
		let (sender, connection) =
			http1::handshake(connector::SmolIo(stream))
				.await
				.map_err(|error| {
					Error::HttpClient(format!("HTTP handshake failed: {error}").into())
				})?;
		smol::spawn(async move {
			let _ = connection.await;
		})
		.detach();
		Ok(sender)
	}

	async fn open_stream(&self, url: &Url, target: &proxy::Target, tls: bool) -> Result<Stream> {
		let Some(proxy_url) = self.proxy.proxy_url(url).cloned() else {
			return self
				.wrap_tls(self.open_tcp(target, self.denylist_validation).await?, target, tls)
				.await;
		};

		let scheme = proxy::Scheme::parse(&proxy_url)?;
		let port = proxy::port(&proxy_url, scheme)?;
		let host = unbracket(
			proxy_url
				.host_str()
				.ok_or_else(|| Error::HttpClient("proxy URL has no host".into()))?,
		);
		// The proxy address itself is admin-configured and trusted, so it is
		// never denylist-checked; the denylist applies to the request target.
		let stream = self.open_tcp(&proxy::Target { host, port }, false).await?;

		let stream = match scheme {
			| proxy::Scheme::Http if !tls => stream,
			// CONNECT hands target resolution to the proxy, so the IP range
			// denylist cannot be enforced on this path.
			| proxy::Scheme::Http => proxy::http_connect(stream, target, &proxy_url).await?,
			| proxy::Scheme::Socks5 | proxy::Scheme::Socks5h => {
				// Local-resolve (`socks5`) validates the target through the
				// denylist below; remote-resolve (`socks5h`) cannot, for the
				// same reason as CONNECT.
				let port = target.port;
				let validate = self.denylist_validation;
				proxy::socks5_connect(stream, &proxy_url, scheme, target, |host| async move {
					self.resolve_host(&host, port, validate)
						.await
						.map(|address| address.ip())
				})
				.await?
			},
		};

		self.wrap_tls(stream, target, tls).await
	}

	async fn wrap_tls(
		&self,
		stream: async_net::TcpStream,
		target: &proxy::Target,
		tls: bool,
	) -> Result<Stream> {
		if !tls {
			return Ok(Stream::Plain(stream));
		}

		let stream = TlsConnector::from(self.tls.clone())
			.connect(server_name(&target.host)?, stream)
			.await
			.map_err(|error| {
				Error::HttpClient(
					format!("TLS handshake with {} failed: {error}", target.host).into(),
				)
			})?;
		Ok(Stream::Tls(Box::new(stream)))
	}

	/// Resolve the (possibly proxied) host through the configured resolver and
	/// open a TCP connection to the first allowed address.
	async fn open_tcp(
		&self,
		target: &proxy::Target,
		validate: bool,
	) -> Result<async_net::TcpStream> {
		let address = self
			.resolve_host(&target.host, target.port, validate)
			.await?;
		async_net::TcpStream::connect(address)
			.await
			.map_err(|error| {
				Error::HttpClientConnect(
					format!("failed to connect to {}: {error}", target.authority()).into(),
				)
			})
	}

	fn target(url: &Url) -> Result<proxy::Target> {
		let host = url
			.host_str()
			.ok_or_else(|| Error::HttpClient("request URL has no host".into()))?;
		let port = url
			.port_or_known_default()
			.ok_or_else(|| Error::HttpClient("request URL has no port".into()))?;
		Ok(proxy::Target { host: unbracket(host), port })
	}

	/// Resolve `host` through the configured resolver (never system DNS) and,
	/// when `validate` is set, keep only addresses outside the IP range
	/// denylist.
	async fn resolve_host(
		&self,
		host: &str,
		port: u16,
		validate: bool,
	) -> Result<std::net::SocketAddr> {
		if let Ok(ip) = host.parse::<std::net::IpAddr>() {
			self.check_denied(&ip, validate, host)?;
			return Ok(std::net::SocketAddr::new(ip, port));
		}

		let mut candidates = Vec::new();
		let resolver = self.resolver()?;
		match resolver.cache.get_override(host).await {
			| Ok(over) if over.valid() => candidates.extend(over.ips.iter().copied()),
			| Ok(CachedOverride { overriding: Some(overriding), .. }) =>
				self.lookup(&overriding, &mut candidates).await?,
			| _ => self.lookup(host, &mut candidates).await?,
		}

		for ip in candidates {
			if self.check_denied(&ip, validate, host).is_ok() {
				return Ok(std::net::SocketAddr::new(ip, port));
			}
		}

		Err(Error::HttpClient(
			format!(
				"no allowed address found for {host:?} (empty DNS answer or denied by \
				 ip_range_denylist)"
			)
			.into(),
		))
	}

	async fn lookup(&self, host: &str, candidates: &mut Vec<std::net::IpAddr>) -> Result<()> {
		let lookup = self
			.resolver()?
			.resolver
			.lookup_ip(host.to_owned())
			.await
			.map_err(|error| {
				Error::HttpClient(format!("failed to resolve {host:?}: {error}").into())
			})?;
		candidates.extend(lookup);
		Ok(())
	}

	fn resolver(&self) -> Result<&Arc<ResolverService>> {
		self.resolver
			.as_ref()
			.ok_or_else(|| Error::HttpClient("no DNS resolver configured for this client".into()))
	}

	fn check_denied(&self, ip: &std::net::IpAddr, validate: bool, host: &str) -> Result<()> {
		if validate
			&& self
				.cidr_range_denylist
				.iter()
				.any(|cidr| cidr.contains(ip))
		{
			return Err(Error::HttpClient(
				format!("refusing to connect to {ip} for {host:?}: denied by ip_range_denylist")
					.into(),
			));
		}
		Ok(())
	}

	/// Plain HTTP through an HTTP proxy must use an absolute-form request line.
	fn needs_absolute_form(&self, url: &Url) -> bool {
		self.proxy
			.proxy_url(url)
			.is_some_and(|proxy| proxy.scheme() == "http" && url.scheme() == "http")
	}
}

/// A connected stream, already wrapped in TLS where the scheme requires it.
enum Stream {
	Plain(async_net::TcpStream),
	Tls(Box<futures_rustls::client::TlsStream<async_net::TcpStream>>),
}

impl AsyncRead for Stream {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut [u8],
	) -> Poll<std::io::Result<usize>> {
		match self.get_mut() {
			| Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
			| Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
		}
	}
}

impl AsyncWrite for Stream {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		match self.get_mut() {
			| Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
			| Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
		}
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		match self.get_mut() {
			| Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
			| Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
		}
	}

	fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		match self.get_mut() {
			| Self::Plain(stream) => Pin::new(stream).poll_close(cx),
			| Self::Tls(stream) => Pin::new(stream).poll_close(cx),
		}
	}
}

pub struct RequestBuilder<'a> {
	client: &'a HttpClient,
	method: Method,
	url: String,
	headers: HeaderMap,
	body: Bytes,
}

impl RequestBuilder<'_> {
	#[must_use]
	pub fn header<V: AsRef<str>>(mut self, name: http::header::HeaderName, value: V) -> Self {
		if let Ok(value) = HeaderValue::try_from(value.as_ref()) {
			self.headers.insert(name, value);
		}
		self
	}

	#[must_use]
	pub fn bearer_auth(self, token: &str) -> Self {
		self.header(AUTHORIZATION, format!("Bearer {token}"))
	}

	#[must_use]
	pub fn body<B: Into<Bytes>>(mut self, body: B) -> Self {
		self.body = body.into();
		self
	}

	pub async fn send(self) -> Result<Response<Bytes>> {
		let mut request = Request::builder()
			.method(self.method)
			.uri(self.url)
			.body(self.body)?;
		*request.headers_mut() = self.headers;
		Box::pin(self.client.execute(request)).await
	}
}

#[derive(Clone)]
pub struct Service {
	pub default: Arc<HttpClient>,
	pub url_preview: Arc<HttpClient>,
	pub extern_media: Arc<HttpClient>,
	pub well_known: Arc<HttpClient>,
	pub federation: Arc<HttpClient>,
	pub synapse: Arc<HttpClient>,
	pub sender: Arc<HttpClient>,
	pub appservice: Arc<HttpClient>,
	pub pusher: Arc<HttpClient>,
	pub cidr_range_denylist: Vec<IpCidr>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;
		let resolver = args.require::<resolver::Service>("resolver");
		let base = || base(config, &resolver);
		Ok(Arc::new(Self {
			default: Arc::new(base()?),
			url_preview: Arc::new(
				base()?
					.total_timeout(Duration::from_secs(config.url_preview_timeout))
					.redirect_limit(3)
					.denylist_validation(true),
			),
			extern_media: Arc::new(base()?.redirect_limit(3).denylist_validation(true)),
			well_known: Arc::new(
				base()?
					.connect_timeout(Duration::from_secs(config.well_known_conn_timeout))
					.read_timeout(Duration::from_secs(config.well_known_timeout))
					.total_timeout(Duration::from_secs(config.well_known_timeout))
					.redirect_limit(4)
					.denylist_validation(true),
			),
			federation: Arc::new(
				base()?
					.connect_timeout(Duration::from_secs(config.federation_conn_timeout))
					.read_timeout(Duration::from_secs(config.federation_timeout))
					.total_timeout(Duration::from_secs(
						config
							.federation_timeout
							.saturating_add(config.federation_conn_timeout),
					))
					.redirect_limit(3)
					.denylist_validation(true),
			),
			synapse: Arc::new({
				let synapse_timeout = config.federation_timeout.saturating_mul(6);
				base()?
					.connect_timeout(Duration::from_secs(config.federation_conn_timeout))
					.read_timeout(Duration::from_secs(synapse_timeout))
					.total_timeout(Duration::from_secs(
						synapse_timeout.saturating_add(config.federation_conn_timeout),
					))
					.redirect_limit(3)
					.denylist_validation(true)
			}),
			sender: Arc::new(
				base()?
					.connect_timeout(Duration::from_secs(config.federation_conn_timeout))
					.read_timeout(Duration::from_secs(config.sender_timeout))
					.total_timeout(Duration::from_secs(config.sender_timeout))
					.redirect_limit(2)
					.denylist_validation(true),
			),
			appservice: Arc::new(
				base()?
					.connect_timeout(Duration::from_secs(5))
					.read_timeout(Duration::from_secs(config.appservice_timeout))
					.total_timeout(Duration::from_secs(config.appservice_timeout))
					.redirect_limit(2)
					// Appservices commonly target loopback, and the pre-migration clients
					// never denylist-checked them.
					.denylist_validation(false),
			),
			pusher: Arc::new(
				base()?
					.connect_timeout(Duration::from_secs(config.pusher_conn_timeout))
					.total_timeout(Duration::from_secs(config.pusher_timeout))
					.redirect_limit(2)
					.denylist_validation(true),
			),
			cidr_range_denylist: config.ip_range_denylist.clone(),
		}))
	}

	fn name(&self) -> &str { service::make_name(std::module_path!()) }
}

fn base(config: &Config, resolver: &Arc<ResolverService>) -> Result<HttpClient> {
	let mut roots = RootCertStore::empty();
	roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|root| {
		rustls::pki_types::TrustAnchor {
			subject: root.subject.to_vec().into(),
			subject_public_key_info: root.spki.to_vec().into(),
			name_constraints: root
				.name_constraints
				.map(|constraints| constraints.to_vec().into()),
		}
	}));
	let mut tls = ClientConfig::builder()
		.with_root_certificates(roots)
		.with_no_client_auth();
	if config.allow_invalid_tls_certificates_yes_i_know_what_the_fuck_i_am_doing_with_this_and_i_know_this_is_insecure
	{
		// Only the opt-in verifier needs the provider's signature schemes; fall back to
		// ring when no process default was installed so normal builds never depend on it.
		let provider = CryptoProvider::get_default()
			.cloned()
			.unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
		tls.dangerous()
			.set_certificate_verifier(Arc::new(AcceptAnyCertificate { provider }));
	}

	Ok(HttpClient {
		default_headers: HeaderMap::new(),
		user_agent: Some(config.user_agent.clone()),
		tls: Arc::new(tls),
		max_size: config.max_request_size,
		connect_timeout: Duration::from_secs(config.request_conn_timeout),
		read_timeout: Duration::from_secs(config.request_timeout),
		total_timeout: Duration::from_secs(config.request_total_timeout),
		redirect_limit: 6,
		denylist_validation: false,
		resolver: Some(Arc::clone(resolver)),
		cidr_range_denylist: config.ip_range_denylist.clone(),
		proxy: config.proxy.clone(),
		pool: Arc::default(),
	})
}

/// Accepts any server certificate. Installed only when the operator opts in via
/// the invalid-certificate configuration flag.
#[derive(Debug)]
struct AcceptAnyCertificate {
	provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AcceptAnyCertificate {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &ServerName<'_>,
		_ocsp_response: &[u8],
		_now: UnixTime,
	) -> Result<ServerCertVerified, TlsError> {
		Ok(ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(
		&self,
		_message: &[u8],
		_cert: &CertificateDer<'_>,
		_dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, TlsError> {
		Ok(HandshakeSignatureValid::assertion())
	}

	fn verify_tls13_signature(
		&self,
		_message: &[u8],
		_cert: &CertificateDer<'_>,
		_dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, TlsError> {
		Ok(HandshakeSignatureValid::assertion())
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		self.provider
			.signature_verification_algorithms
			.supported_schemes()
	}
}

/// Strip the brackets an IPv6 literal carries in a URL host or authority.
fn unbracket(host: &str) -> String {
	host.strip_prefix('[')
		.and_then(|host| host.strip_suffix(']'))
		.unwrap_or(host)
		.to_owned()
}

/// The value for a synthesized `Host` header: the URL host (already bracketed
/// for IPv6 literals) plus the port when it is not the scheme default.
fn host_header(url: &Url) -> Result<String> {
	let host = url
		.host_str()
		.ok_or_else(|| Error::HttpClient("request URL has no host".into()))?;
	let Some(port) = url.port_or_known_default() else {
		return Ok(host.to_owned());
	};
	let default = match url.scheme() {
		| "http" | "ws" => Some(80),
		| "https" | "wss" => Some(443),
		| "ftp" => Some(21),
		| _ => None,
	};
	if default == Some(port) {
		Ok(host.to_owned())
	} else {
		Ok(format!("{host}:{port}"))
	}
}

/// The URI hyper writes on the wire for a direct (or tunneled) connection.
fn origin_form(url: &Url) -> String {
	let path = if url.path().is_empty() { "/" } else { url.path() };
	match url.query() {
		| Some(query) => format!("{path}?{query}"),
		| None => path.to_owned(),
	}
}

fn server_name(host: &str) -> Result<ServerName<'static>> {
	ServerName::try_from(host.to_owned()).map_err(|error| {
		Error::HttpClient(format!("invalid server name {host:?}: {error}").into())
	})
}

fn parse_url(uri: &http::Uri) -> Result<Url> {
	Url::parse(&uri.to_string())
		.map_err(|error| Error::HttpClient(format!("invalid request URL: {error}").into()))
}

fn is_http_scheme(scheme: &str) -> bool { matches!(scheme, "http" | "https") }

fn apply_headers(
	request: &mut Request<Bytes>,
	default_headers: &HeaderMap,
	user_agent: Option<&str>,
) -> Result<()> {
	for (name, value) in default_headers {
		if !request.headers().contains_key(name) {
			request.headers_mut().insert(name, value.clone());
		}
	}
	if let Some(user_agent) = user_agent {
		if !request.headers().contains_key(http::header::USER_AGENT) {
			request
				.headers_mut()
				.insert(http::header::USER_AGENT, HeaderValue::try_from(user_agent)?);
		}
	}
	Ok(())
}

/// Rewrite the request for the wire: set the URI to origin-form (or leave it
/// absolute when talking to an HTTP proxy), and synthesize `Host` from the
/// current hop's URL whenever it is absent.
fn prepare_request(
	request: &Request<Bytes>,
	url: &Url,
	absolute_form: bool,
) -> Result<Request<Full<Bytes>>> {
	let mut request = request.clone().map(Full::new);
	*request.uri_mut() = if absolute_form {
		url.as_str()
			.parse()
			.map_err(|error| Error::HttpClient(format!("invalid request URI: {error}").into()))?
	} else {
		origin_form(url)
			.parse()
			.map_err(|error| Error::HttpClient(format!("invalid request URI: {error}").into()))?
	};
	if !request.headers().contains_key(HOST) {
		request
			.headers_mut()
			.insert(HOST, HeaderValue::try_from(host_header(url)?)?);
	}
	Ok(request)
}

/// Whether `headers` carry a `Connection: close` token.
fn wants_close(headers: &HeaderMap) -> bool {
	headers
		.get_all(http::header::CONNECTION)
		.iter()
		.filter_map(|value| value.to_str().ok())
		.any(|value| {
			value
				.split(',')
				.any(|token| token.trim().eq_ignore_ascii_case("close"))
		})
}

/// Whether the connection that produced `response` may serve another request.
fn connection_reusable(response: &Response<Bytes>) -> bool {
	response.version() == http::Version::HTTP_11 && !wants_close(response.headers())
}

/// Collect the response body. On failure the flag says whether the cause was
/// the connection failing (retryable) rather than the body exceeding `max_size`.
async fn collect_body(
	response: Response<Incoming>,
	max_size: usize,
) -> std::result::Result<Response<Bytes>, (bool, Error)> {
	let (parts, body) = response.into_parts();
	let body = http_body_util::Limited::new(body, max_size)
		.collect()
		.await
		.map_err(|error| {
			let too_large = error.is::<http_body_util::LengthLimitError>();
			(
				!too_large,
				Error::HttpClient(format!("failed to read response body: {error}").into()),
			)
		})?
		.to_bytes();
	Ok(Response::from_parts(parts, body))
}

fn redirect_location(response: &Response<Bytes>) -> Option<&str> {
	if !matches!(
		response.status(),
		StatusCode::MOVED_PERMANENTLY
			| StatusCode::FOUND
			| StatusCode::SEE_OTHER
			| StatusCode::PERMANENT_REDIRECT
			| StatusCode::TEMPORARY_REDIRECT
	) {
		return None;
	}
	response
		.headers()
		.get(http::header::LOCATION)
		.and_then(|value| value.to_str().ok())
}

/// Whether the redirect from `previous` to `next` should be followed: within
/// the hop limit, both endpoints http(s), and never downgrading TLS.
fn can_follow_redirect(redirects: usize, limit: usize, previous: &Url, next: &Url) -> bool {
	redirects < limit
		&& is_http_scheme(previous.scheme())
		&& is_http_scheme(next.scheme())
		&& !(previous.scheme() == "https" && next.scheme() == "http")
}

/// Advance `request` to the redirect target: rewrite the method and drop the
/// body for the statuses that require it, strip credentials on a cross-origin
/// hop, and re-synthesize `Host` for the next hop.
fn follow_redirect(
	request: &mut Request<Bytes>,
	url: &mut Url,
	next: Url,
	status: StatusCode,
) -> Result<()> {
	if !same_origin(url, &next) {
		for name in [AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION, WWW_AUTHENTICATE] {
			request.headers_mut().remove(name);
		}
	}

	*url = next;
	request.headers_mut().remove(HOST);

	if matches!(
		status,
		StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND | StatusCode::SEE_OTHER
	) {
		for name in [
			http::header::TRANSFER_ENCODING,
			http::header::CONTENT_ENCODING,
			http::header::CONTENT_TYPE,
			http::header::CONTENT_LENGTH,
		] {
			request.headers_mut().remove(name);
		}
		if request.method() != Method::HEAD {
			*request.method_mut() = Method::GET;
		}
		*request.body_mut() = Bytes::new();
	}

	Ok(())
}

fn same_origin(previous: &Url, next: &Url) -> bool {
	previous.scheme() == next.scheme()
		&& previous.host_str() == next.host_str()
		&& previous.port_or_known_default() == next.port_or_known_default()
}

#[inline]
#[must_use]
#[implement(Service)]
pub fn valid_cidr_range(&self, ip: &std::net::IpAddr) -> bool {
	self.cidr_range_denylist
		.iter()
		.all(|cidr| !cidr.contains(ip))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn overflowing_timeout_gets_far_future_deadline() {
		let now = Instant::now();
		let deadline = request_deadline(Duration::MAX);

		assert!(deadline > now);
		assert!(deadline.duration_since(now) > Duration::from_hours(24));
	}

	#[test]
	fn redirect_location_accepts_supported_redirects() {
		for status in [
			StatusCode::MOVED_PERMANENTLY,
			StatusCode::FOUND,
			StatusCode::SEE_OTHER,
			StatusCode::TEMPORARY_REDIRECT,
			StatusCode::PERMANENT_REDIRECT,
		] {
			let response = Response::builder()
				.status(status)
				.header(http::header::LOCATION, "/next")
				.body(Bytes::new())
				.expect("valid redirect response");

			assert_eq!(redirect_location(&response), Some("/next"));
		}
	}

	#[test]
	fn post_redirect_drops_body_for_301_302_303() {
		for status in [StatusCode::MOVED_PERMANENTLY, StatusCode::FOUND, StatusCode::SEE_OTHER] {
			let mut request = Request::builder()
				.method(Method::POST)
				.uri("https://example.org/start")
				.header(http::header::CONTENT_TYPE, "application/json")
				.header(http::header::CONTENT_LENGTH, "4")
				.body(Bytes::from_static(b"body"))
				.expect("valid request");
			let mut previous = Url::parse("https://example.org/start").expect("valid URL");
			let next = Url::parse("https://example.org/next").expect("valid URL");

			follow_redirect(&mut request, &mut previous, next, status).expect("valid redirect");
			assert_eq!(request.method(), Method::GET, "{status}");
			assert!(request.body().is_empty(), "{status}");
			assert!(!request.headers().contains_key(http::header::CONTENT_TYPE), "{status}");
			assert!(!request.headers().contains_key(http::header::CONTENT_LENGTH), "{status}");
		}
	}

	#[test]
	fn post_redirect_keeps_method_and_body_for_307_308() {
		for status in [StatusCode::TEMPORARY_REDIRECT, StatusCode::PERMANENT_REDIRECT] {
			let mut request = Request::builder()
				.method(Method::POST)
				.uri("https://example.org/start")
				.body(Bytes::from_static(b"body"))
				.expect("valid request");
			let mut previous = Url::parse("https://example.org/start").expect("valid URL");
			let next = Url::parse("https://example.org/next").expect("valid URL");

			follow_redirect(&mut request, &mut previous, next, status).expect("valid redirect");
			assert_eq!(request.method(), Method::POST, "{status}");
			assert_eq!(request.body(), &Bytes::from_static(b"body"), "{status}");
		}
	}

	#[test]
	fn head_redirect_stays_head_for_303() {
		let mut request = Request::builder()
			.method(Method::HEAD)
			.uri("https://example.org/start")
			.body(Bytes::new())
			.expect("valid request");
		let mut previous = Url::parse("https://example.org/start").expect("valid URL");
		let next = Url::parse("https://example.org/next").expect("valid URL");

		follow_redirect(&mut request, &mut previous, next, StatusCode::SEE_OTHER)
			.expect("valid redirect");
		assert_eq!(request.method(), Method::HEAD);
		assert!(request.body().is_empty());
	}

	#[test]
	fn cross_origin_redirect_strips_credentials() {
		let mut request = Request::builder()
			.method(Method::GET)
			.uri("https://example.org/start")
			.header(AUTHORIZATION, "Bearer secret")
			.header(COOKIE, "session=1")
			.header(HOST, "example.org")
			.body(Bytes::new())
			.expect("valid request");
		let mut previous = Url::parse("https://example.org/start").expect("valid URL");
		let next = Url::parse("https://other.example/next").expect("valid URL");

		follow_redirect(&mut request, &mut previous, next, StatusCode::FOUND)
			.expect("valid redirect");
		assert!(!request.headers().contains_key(AUTHORIZATION));
		assert!(!request.headers().contains_key(COOKIE));
		assert!(!request.headers().contains_key(HOST), "Host is re-synthesized per hop");
		assert_eq!(previous.host_str(), Some("other.example"));
	}

	#[test]
	fn same_origin_redirect_keeps_credentials() {
		let mut request = Request::builder()
			.method(Method::GET)
			.uri("https://example.org/start")
			.header(AUTHORIZATION, "Bearer secret")
			.header(COOKIE, "session=1")
			.body(Bytes::new())
			.expect("valid request");
		let mut previous = Url::parse("https://example.org/start").expect("valid URL");
		let next = Url::parse("https://example.org/next").expect("valid URL");

		follow_redirect(&mut request, &mut previous, next, StatusCode::FOUND)
			.expect("valid redirect");
		assert!(request.headers().contains_key(AUTHORIZATION));
		assert!(request.headers().contains_key(COOKIE));
	}

	#[test]
	fn redirect_hop_limit_and_downgrade_are_enforced() {
		let https = Url::parse("https://example.org/a").expect("valid URL");
		let http = Url::parse("http://example.org/a").expect("valid URL");
		let peer = Url::parse("https://peer.example/b").expect("valid URL");
		let ftp = Url::parse("ftp://example.org/f").expect("valid URL");

		assert!(can_follow_redirect(0, 6, &https, &peer));
		assert!(can_follow_redirect(5, 6, &https, &peer));
		assert!(!can_follow_redirect(6, 6, &https, &peer), "hop limit exhausted");
		assert!(!can_follow_redirect(0, 6, &https, &http), "https downgraded to http");
		assert!(can_follow_redirect(0, 6, &http, &https), "http upgraded to https");
		assert!(!can_follow_redirect(0, 6, &https, &ftp), "non-http scheme");
	}
}

#[cfg(test)]
mod pool_tests {
	use std::{
		net::SocketAddr,
		sync::atomic::{AtomicUsize, Ordering},
	};

	use futures::{
		future::join_all,
		io::{AsyncReadExt, AsyncWriteExt},
	};

	use super::*;

	#[derive(Clone, Copy)]
	enum Behavior {
		/// Answer every request and keep the connection open.
		KeepAlive,
		/// Answer with `Connection: close` and hang up.
		Close,
		/// Advertise keep-alive, answer, then hang up anyway (stale when idle).
		HangUpAfterResponse,
		/// On a reused connection, hang up without answering every 7th request.
		DropReusedEverySeventh,
		/// Answer the first request on a connection, then hang up on the second
		/// without answering it.
		DropSecondOnConnection,
	}

	struct TestServer {
		addr: SocketAddr,
		accepted: Arc<AtomicUsize>,
		served: Arc<AtomicUsize>,
	}

	const BODY: &[u8] = b"pong";

	/// The tests share smol's global executor, so running them in parallel makes
	/// them starve each other (and wrecks the benchmark). Serialize them.
	static SERIAL: Mutex<()> = Mutex::new(());

	fn serial() -> std::sync::MutexGuard<'static, ()> {
		SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
	}

	fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
		haystack
			.windows(needle.len())
			.position(|window| window == needle)
	}

	async fn serve(
		mut stream: async_net::TcpStream,
		behavior: Behavior,
		served: Arc<AtomicUsize>,
	) {
		let mut buffer = Vec::new();
		let mut chunk = [0_u8; 1024];
		let mut on_connection = 0_usize;
		loop {
			let end = loop {
				if let Some(position) = find(&buffer, b"\r\n\r\n") {
					break position.checked_add(4).expect("request header offset fits");
				}

				match stream.read(&mut chunk).await {
					| Ok(0) | Err(_) => return,
					| Ok(read) => buffer.extend_from_slice(&chunk[..read]),
				}
			};
			buffer.drain(..end);

			let total = served
				.fetch_add(1, Ordering::SeqCst)
				.checked_add(1)
				.expect("served request count fits");
			if matches!(behavior, Behavior::DropReusedEverySeventh)
				&& on_connection > 0
				&& total.is_multiple_of(7)
			{
				return;
			}

			if matches!(behavior, Behavior::DropSecondOnConnection) && on_connection > 0 {
				return;
			}

			on_connection = on_connection
				.checked_add(1)
				.expect("connection request count fits");
			let connection = if matches!(behavior, Behavior::Close) {
				"Connection: close\r\n"
			} else {
				""
			};
			let response =
				format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{connection}\r\n", BODY.len());
			if stream.write_all(response.as_bytes()).await.is_err()
				|| stream.write_all(BODY).await.is_err()
				|| stream.flush().await.is_err()
			{
				return;
			}

			if matches!(behavior, Behavior::Close | Behavior::HangUpAfterResponse) {
				return;
			}
		}
	}

	fn start_server(behavior: Behavior) -> TestServer {
		let listener = smol::block_on(async_net::TcpListener::bind("127.0.0.1:0"))
			.expect("bind test server");
		let addr = listener.local_addr().expect("local addr");
		let accepted = Arc::new(AtomicUsize::new(0));
		let served = Arc::new(AtomicUsize::new(0));

		let (accepted_count, served_count) = (Arc::clone(&accepted), Arc::clone(&served));
		smol::spawn(async move {
			while let Ok((stream, _)) = listener.accept().await {
				accepted_count.fetch_add(1, Ordering::SeqCst);
				smol::spawn(serve(stream, behavior, Arc::clone(&served_count))).detach();
			}
		})
		.detach();

		TestServer { addr, accepted, served }
	}

	fn test_client(pool: Pool) -> HttpClient {
		let tls = ClientConfig::builder()
			.with_root_certificates(RootCertStore::empty())
			.with_no_client_auth();

		HttpClient {
			default_headers: HeaderMap::new(),
			user_agent: None,
			tls: Arc::new(tls),
			max_size: 1 << 20,
			connect_timeout: Duration::from_secs(5),
			read_timeout: Duration::from_secs(5),
			total_timeout: Duration::from_secs(60),
			redirect_limit: 6,
			denylist_validation: false,
			cidr_range_denylist: Vec::new(),
			resolver: None,
			proxy: ProxyConfig::None,
			pool: Arc::new(pool),
		}
	}

	async fn ping(client: &HttpClient, addr: SocketAddr) -> Result<()> {
		let response = client.get(format!("http://{addr}/ping")).send().await?;
		assert_eq!(response.status(), StatusCode::OK);
		assert_eq!(response.body().as_ref(), BODY);
		Ok(())
	}

	#[test]
	fn sequential_requests_share_one_connection() {
		let _serial = serial();
		let server = start_server(Behavior::KeepAlive);
		let client = test_client(Pool::default());

		smol::block_on(async {
			for _ in 0..100 {
				ping(&client, server.addr).await.expect("request succeeds");
			}
		});

		assert_eq!(server.served.load(Ordering::SeqCst), 100);
		assert_eq!(server.accepted.load(Ordering::SeqCst), 1, "connection was reused");
	}

	#[test]
	fn connection_close_is_never_reused() {
		let _serial = serial();
		let server = start_server(Behavior::Close);
		let client = test_client(Pool::default());

		smol::block_on(async {
			for _ in 0..5 {
				ping(&client, server.addr).await.expect("request succeeds");
			}
		});

		assert_eq!(server.accepted.load(Ordering::SeqCst), 5);
	}

	#[test]
	fn stale_pooled_connections_are_retried() {
		let _serial = serial();
		let server = start_server(Behavior::HangUpAfterResponse);
		let client = test_client(Pool::default());

		smol::block_on(async {
			for _ in 0..25 {
				ping(&client, server.addr)
					.await
					.expect("stale connection is skipped or retried");
			}
		});

		assert_eq!(server.served.load(Ordering::SeqCst), 25);
	}

	#[test]
	fn survives_server_dropping_reused_connections_under_load() {
		let _serial = serial();
		let server = start_server(Behavior::DropReusedEverySeventh);
		let client = test_client(Pool::default());

		smol::block_on(async {
			for _ in 0..700 {
				ping(&client, server.addr)
					.await
					.expect("dropped reused connection is retried on a fresh one");
			}
		});

		assert!(server.accepted.load(Ordering::SeqCst) > 1, "drops forced reconnects");
	}

	#[test]
	fn non_replayable_requests_are_not_retried_after_connection_loss() {
		let _serial = serial();
		let server = start_server(Behavior::DropSecondOnConnection);
		let client = test_client(Pool::default());
		let url = format!("http://{}/ping", server.addr);

		smol::block_on(async {
			let first = client.request(Method::POST, &url).send().await;
			assert!(first.is_ok(), "first request opens the connection");

			// The pooled connection is dropped by the server mid-request. A POST may
			// already have been processed, so it must surface the error, not replay.
			let second = client.request(Method::POST, &url).send().await;
			assert!(second.is_err(), "POST must not be silently replayed");
		});

		assert_eq!(server.served.load(Ordering::SeqCst), 2, "no replay reached the server");

		// A GET in the same situation is replayed on a fresh connection.
		let server = start_server(Behavior::DropSecondOnConnection);
		let url = format!("http://{}/ping", server.addr);
		smol::block_on(async {
			client.get(&url).send().await.expect("first GET");
			client
				.get(&url)
				.send()
				.await
				.expect("GET is retried on a fresh connection");
		});
	}

	#[test]
	fn request_connection_close_is_not_pooled() {
		let _serial = serial();
		let server = start_server(Behavior::KeepAlive);
		let client = test_client(Pool::default());
		let url = format!("http://{}/ping", server.addr);

		smol::block_on(async {
			for _ in 0..3 {
				client
					.get(&url)
					.header(http::header::CONNECTION, "close")
					.send()
					.await
					.expect("request succeeds");
			}
		});

		assert_eq!(server.accepted.load(Ordering::SeqCst), 3);
	}

	#[test]
	fn concurrent_workers_reuse_idle_connections() {
		let _serial = serial();
		const WORKERS: usize = 8;
		const PER_WORKER: usize = 250;

		let server = start_server(Behavior::KeepAlive);
		let client = test_client(Pool::default());

		smol::block_on(async {
			let tasks = (0..WORKERS).map(|_| {
				let client = client.clone();
				let addr = server.addr;
				smol::spawn(async move {
					for _ in 0..PER_WORKER {
						ping(&client, addr).await.expect("request succeeds");
					}
				})
			});
			join_all(tasks).await;
		});

		assert_eq!(server.served.load(Ordering::SeqCst), WORKERS * PER_WORKER);
		assert!(
			server.accepted.load(Ordering::SeqCst) <= WORKERS,
			"each worker settles on one pooled connection (the cap bounds idle, not active, \
			 connections)"
		);
	}

	#[test]
	fn heavy_concurrency_stays_correct() {
		let _serial = serial();
		const WORKERS: usize = 64;
		const PER_WORKER: usize = 40;

		let server = start_server(Behavior::KeepAlive);
		let client = test_client(Pool::default());

		smol::block_on(async {
			let tasks = (0..WORKERS).map(|_| {
				let client = client.clone();
				let addr = server.addr;
				smol::spawn(async move {
					for _ in 0..PER_WORKER {
						ping(&client, addr).await.expect("request succeeds");
					}
				})
			});
			join_all(tasks).await;
		});

		assert_eq!(server.served.load(Ordering::SeqCst), WORKERS * PER_WORKER);
	}

	/// Run `REQUESTS` sequential requests and return the elapsed time and the
	/// number of connections the server accepted.
	fn timed_run(max_idle: usize, requests: usize) -> (Duration, usize) {
		let server = start_server(Behavior::KeepAlive);
		let client = test_client(Pool::new(max_idle));
		let started = Instant::now();
		smol::block_on(async {
			for _ in 0..requests {
				ping(&client, server.addr).await.expect("request succeeds");
			}
		});

		(started.elapsed(), server.accepted.load(Ordering::SeqCst))
	}

	#[test]
	#[allow(clippy::cast_precision_loss)]
	fn pooling_is_faster_than_reconnecting() {
		let _serial = serial();
		const REQUESTS: usize = 1500;
		const ROUNDS: usize = 3;

		// Best-of-N on each side keeps scheduler noise from deciding the result.
		let mut pooled = Duration::MAX;
		let mut unpooled = Duration::MAX;
		for _ in 0..ROUNDS {
			let (elapsed, connections) = timed_run(POOL_MAX_IDLE_PER_HOST, REQUESTS);
			assert_eq!(connections, 1, "pooled run reuses a single connection");
			pooled = pooled.min(elapsed);

			let (elapsed, connections) = timed_run(0, REQUESTS);
			assert_eq!(connections, REQUESTS, "unpooled run reconnects every time");
			unpooled = unpooled.min(elapsed);
		}

		let rate = |elapsed: Duration| {
			f64::from(u32::try_from(REQUESTS).expect("request count fits in u32"))
				/ elapsed.as_secs_f64()
		};
		eprintln!(
			"pooled:   {pooled:?} ({:.0} req/s, 1 connection)
unpooled: {unpooled:?} ({:.0} req/s, {REQUESTS} connections)
speedup:  {:.2}x",
			rate(pooled),
			rate(unpooled),
			unpooled.as_secs_f64() / pooled.as_secs_f64(),
		);

		assert!(pooled < unpooled, "reusing connections must beat reconnecting");
	}
}
