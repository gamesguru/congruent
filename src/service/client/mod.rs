use std::sync::Arc;

use bytes::Bytes;
use conduwuit::{Config, Result, implement, utils::IpCidr};
use futures_rustls::{
	TlsConnector,
	rustls::{ClientConfig, RootCertStore, pki_types::ServerName},
};
use http::{HeaderMap, Request, Response, header::HOST};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use url::Url;

use crate::{resolver, service};

mod connector;

#[derive(Clone)]
pub struct HttpClient {
	default_headers: HeaderMap,
	user_agent: Option<String>,
	tls: Arc<ClientConfig>,
	max_size: usize,
}

impl HttpClient {
	pub async fn execute(&self, mut request: Request<Bytes>) -> Result<Response<Bytes>> {
		let url = Url::parse(&request.uri().to_string())
			.map_err(|error| conduwuit::Error::HttpClient(error.to_string().into()))?;
		let host = url
			.host_str()
			.ok_or_else(|| conduwuit::Error::HttpClient("request URL has no host".into()))?;
		let port = url
			.port_or_known_default()
			.ok_or_else(|| conduwuit::Error::HttpClient("request URL has no port".into()))?;

		for (name, value) in &self.default_headers {
			if !request.headers().contains_key(name) {
				request.headers_mut().insert(name, value.clone());
			}
		}
		if let Some(user_agent) = &self.user_agent {
			if !request.headers().contains_key(http::header::USER_AGENT) {
				request
					.headers_mut()
					.insert(http::header::USER_AGENT, http::HeaderValue::try_from(user_agent)?);
			}
		}
		if !request.headers().contains_key(HOST) {
			request
				.headers_mut()
				.insert(HOST, http::HeaderValue::try_from(host)?);
		}

		let stream = async_net::TcpStream::connect((host, port)).await?;
		let sender = if url.scheme() == "https" {
			let server_name = ServerName::try_from(host.to_owned())
				.map_err(|error| conduwuit::Error::HttpClient(error.to_string().into()))?;
			let stream = TlsConnector::from(self.tls.clone())
				.connect(server_name, stream)
				.await
				.map_err(|error| conduwuit::Error::HttpClient(error.to_string().into()))?;
			let (sender, connection) = http1::handshake(connector::SmolIo(stream))
				.await
				.map_err(|error| conduwuit::Error::HttpClient(error.to_string().into()))?;
			smol::spawn(async move {
				let _ = connection.await;
			})
			.detach();
			sender
		} else {
			let (sender, connection) = http1::handshake(connector::SmolIo(stream))
				.await
				.map_err(|error| conduwuit::Error::HttpClient(error.to_string().into()))?;
			smol::spawn(async move {
				let _ = connection.await;
			})
			.detach();
			sender
		};

		let response = sender
			.send_request(request.map(Full::new))
			.await
			.map_err(|error| conduwuit::Error::HttpClient(error.to_string().into()))?;
		let (parts, body) = response.into_parts();
		let body = http_body_util::Limited::new(body, self.max_size)
			.collect()
			.await
			.map_err(|error| {
				conduwuit::Error::HttpClient(
					format!("response body exceeds limit: {error}").into(),
				)
			})?
			.to_bytes();
		Ok(Response::from_parts(parts, body))
	}

	pub fn request(&self, method: http::Method, url: impl AsRef<str>) -> RequestBuilder<'_> {
		RequestBuilder {
			client: self,
			method,
			url: url.as_ref().to_owned(),
			headers: HeaderMap::new(),
			body: Bytes::new(),
		}
	}

	pub fn get(&self, url: impl AsRef<str>) -> RequestBuilder<'_> {
		self.request(http::Method::GET, url)
	}

	pub fn head(&self, url: impl AsRef<str>) -> RequestBuilder<'_> {
		self.request(http::Method::HEAD, url)
	}

	pub fn post(&self, url: impl AsRef<str>) -> RequestBuilder<'_> {
		self.request(http::Method::POST, url)
	}
}

pub struct RequestBuilder<'a> {
	client: &'a HttpClient,
	method: http::Method,
	url: String,
	headers: HeaderMap,
	body: Bytes,
}

impl RequestBuilder<'_> {
	pub fn header(mut self, name: http::header::HeaderName, value: impl AsRef<str>) -> Self {
		if let Ok(value) = http::HeaderValue::try_from(value.as_ref()) {
			self.headers.insert(name, value);
		}
		self
	}

	pub fn bearer_auth(self, token: &str) -> Self {
		self.header(http::header::AUTHORIZATION, &format!("Bearer {token}"))
	}

	pub fn body(mut self, body: impl Into<Bytes>) -> Self {
		self.body = body.into();
		self
	}

	pub fn form<T: serde::Serialize + ?Sized>(mut self, form: &T) -> Self {
		if let Ok(body) = serde_urlencoded::to_string(form) {
			self.headers.insert(
				http::header::CONTENT_TYPE,
				http::HeaderValue::from_static("application/x-www-form-urlencoded"),
			);
			self.body = Bytes::from(body);
		}
		self
	}

	pub async fn send(self) -> Result<Response<Bytes>> {
		let mut request = Request::builder()
			.method(self.method)
			.uri(self.url)
			.body(self.body)?;
		*request.headers_mut() = self.headers;
		self.client.execute(request).await
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
		let _ = resolver;
		let make = || base(config).map(Arc::new);
		Ok(Arc::new(Self {
			default: make()?,
			url_preview: make()?,
			extern_media: make()?,
			well_known: make()?,
			federation: make()?,
			synapse: make()?,
			sender: make()?,
			appservice: make()?,
			pusher: make()?,
			cidr_range_denylist: config.ip_range_denylist.clone(),
		}))
	}

	fn name(&self) -> &str { service::make_name(std::module_path!()) }
}

fn base(config: &Config) -> Result<HttpClient> {
	let mut roots = RootCertStore::empty();
	roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
	let tls = ClientConfig::builder()
		.with_root_certificates(roots)
		.with_no_client_auth();
	Ok(HttpClient {
		default_headers: HeaderMap::new(),
		user_agent: Some(config.user_agent.clone()),
		tls: Arc::new(tls),
		max_size: config.max_request_size.saturating_mul(10),
	})
}

#[inline]
#[must_use]
#[implement(Service)]
pub fn valid_cidr_range(&self, ip: &std::net::IpAddr) -> bool {
	self.cidr_range_denylist
		.iter()
		.all(|cidr| !cidr.contains(ip))
}
