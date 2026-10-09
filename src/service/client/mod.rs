use std::{
	pin::Pin,
	sync::Arc,
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
		ClientConfig, CryptoProvider, DigitallySignedStruct, Error as TlsError, RootCertStore,
		SignatureScheme,
		client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
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
	resolver: Arc<ResolverService>,
	proxy: ProxyConfig,
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
		let deadline = Instant::now() + self.total_timeout;
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

			if redirects >= self.redirect_limit || !is_http_scheme(url.scheme()) {
				return Ok(response);
			}

			let next = url.join(location).map_err(|error| {
				Error::HttpClient(format!("invalid redirect target: {error}").into())
			})?;
			if !is_http_scheme(next.scheme()) {
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

		let connected = timeout(self.connect_timeout, self.connect(url, &target, tls)).await;
		let mut sender = match connected {
			| Ok(result) => result?,
			| Err(TimeoutError) =>
				return Err(Error::HttpClientConnect(
					format!(
						"connection to {authority} timed out after {:?}",
						self.connect_timeout
					)
					.into(),
				)),
		};

		let request = prepare_request(request, url, self.needs_absolute_form(url))?;
		let read = timeout(self.read_timeout, async {
			let response = sender.send_request(request).await.map_err(|error| {
				Error::HttpClient(
					format!("failed to send request to {authority}: {error}").into(),
				)
			})?;
			collect_body(response, max_size).await
		})
		.await;

		match read {
			| Ok(result) => result,
			| Err(TimeoutError) => Err(Error::HttpClientTimeout(
				format!(
					"response from {authority} exceeded read timeout of {:?}",
					self.read_timeout
				)
				.into(),
			)),
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
				.wrap_tls(self.open_tcp(target).await?, target, tls)
				.await;
		};

		let scheme = proxy::Scheme::parse(&proxy_url)?;
		let port = proxy::port(&proxy_url, scheme)?;
		let host = unbracket(
			proxy_url
				.host_str()
				.ok_or_else(|| Error::HttpClient("proxy URL has no host".into()))?,
		);
		let stream = self.open_tcp(&proxy::Target { host, port }).await?;

		let stream = match scheme {
			| proxy::Scheme::Http if !tls => stream,
			| proxy::Scheme::Http => proxy::http_connect(stream, target).await?,
			| proxy::Scheme::Socks5 | proxy::Scheme::Socks5h => {
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
	async fn open_tcp(&self, target: &proxy::Target) -> Result<async_net::TcpStream> {
		let address = self
			.resolve_host(&target.host, target.port, self.denylist_validation)
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
		match self.resolver.cache.get_override(host).await {
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
			.resolver
			.resolver
			.lookup_ip(host.to_owned())
			.await
			.map_err(|error| {
				Error::HttpClient(format!("failed to resolve {host:?}: {error}").into())
			})?;
		candidates.extend(lookup);
		Ok(())
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
	let provider = CryptoProvider::get_default()
		.cloned()
		.ok_or_else(|| Error::HttpClient("no rustls crypto provider is configured".into()))?;
	let mut tls = ClientConfig::builder_with_provider(Arc::clone(&provider))
		.with_root_certificates(roots)
		.with_no_client_auth();
	if config.allow_invalid_tls_certificates_yes_i_know_what_the_fuck_i_am_doing_with_this_and_i_know_this_is_insecure
	{
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
		resolver: Arc::clone(resolver),
		cidr_range_denylist: config.ip_range_denylist.clone(),
		proxy: config.proxy.clone(),
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

async fn collect_body(response: Response<Incoming>, max_size: usize) -> Result<Response<Bytes>> {
	let (parts, body) = response.into_parts();
	let body = http_body_util::Limited::new(body, max_size)
		.collect()
		.await
		.map_err(|error| {
			Error::HttpClient(format!("response body exceeds limit: {error}").into())
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
