use std::future::Future;

use conduwuit::{Error, Result, err};
use futures::{AsyncReadExt, AsyncWriteExt};
use futures_io::{AsyncRead, AsyncWrite};
use url::Url;

/// A destination for an outbound request: an un-bracketed host (a DNS name or
/// an IP literal) plus the port to dial.
#[derive(Clone, Debug)]
pub(super) struct Target {
	pub(super) host: String,
	pub(super) port: u16,
}

impl Target {
	pub(super) fn authority(&self) -> String {
		if self.host.parse::<std::net::Ipv6Addr>().is_ok() {
			format!("[{}]:{}", self.host, self.port)
		} else {
			format!("{}:{}", self.host, self.port)
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Scheme {
	Http,
	Socks5,
	Socks5h,
}

impl Scheme {
	pub(super) fn parse(url: &Url) -> Result<Self> {
		match url.scheme() {
			| "http" => Ok(Self::Http),
			| "socks5" => Ok(Self::Socks5),
			| "socks5h" => Ok(Self::Socks5h),
			| scheme => Err(err!(HttpClient(
				"unsupported proxy scheme {scheme:?} (supported: http, socks5, socks5h)"
			))),
		}
	}

	/// Whether the proxy resolves the target hostname itself, so the target
	/// must not be resolved locally.
	pub(super) const fn resolves_target(self) -> bool { matches!(self, Self::Socks5h) }
}

pub(super) fn port(url: &Url, scheme: Scheme) -> Result<u16> {
	let fallback = matches!(scheme, Scheme::Socks5 | Scheme::Socks5h).then_some(1080);
	url.port_or_known_default()
		.or(fallback)
		.ok_or_else(|| err!(HttpClient("proxy URL {url:?} has no port and no known default")))
}

/// Send an HTTP `CONNECT` request and wait for a `2xx` response, leaving the
/// stream positioned at the start of the tunneled byte stream.
pub(super) async fn http_connect<S>(mut stream: S, target: &Target, proxy: &Url) -> Result<S>
where
	S: AsyncRead + AsyncWrite + Unpin,
{
	let authority = target.authority();
	let credentials = proxy_authorization(proxy);
	let request =
		format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n{credentials}\r\n");
	stream
		.write_all(request.as_bytes())
		.await
		.map_err(|error| {
			Error::HttpClient(format!("proxy CONNECT write failed: {error}").into())
		})?;
	stream.flush().await.map_err(|error| {
		Error::HttpClient(format!("proxy CONNECT flush failed: {error}").into())
	})?;

	let response = read_header(&mut stream).await?;
	let status = response
		.split_whitespace()
		.nth(1)
		.and_then(|code| code.parse::<u16>().ok())
		.ok_or_else(|| err!(HttpClient("malformed proxy CONNECT response: {response:?}")))?;

	if !(200..300).contains(&status) {
		return Err(err!(HttpClient("proxy CONNECT to {authority} failed with status {status}")));
	}

	Ok(stream)
}

/// `Proxy-Authorization: Basic` header line (with trailing CRLF) built from the
/// proxy URL's userinfo, or an empty string when it has none.
fn proxy_authorization(proxy: &Url) -> String {
	use base64::{Engine, engine::general_purpose::STANDARD};

	if proxy.username().is_empty() && proxy.password().is_none() {
		return String::new();
	}

	let userinfo = format!("{}:{}", proxy.username(), proxy.password().unwrap_or_default());
	format!("Proxy-Authorization: Basic {}\r\n", STANDARD.encode(userinfo))
}

/// Perform a SOCKS5 (RFC 1928) CONNECT through `stream`. `resolve` is used only
/// by the local-resolve (`socks5`) variant and must return an allowed address.
pub(super) async fn socks5_connect<S, F, Fut>(
	mut stream: S,
	proxy: &Url,
	scheme: Scheme,
	target: &Target,
	resolve: F,
) -> Result<S>
where
	S: AsyncRead + AsyncWrite + Unpin,
	F: FnOnce(String) -> Fut,
	Fut: Future<Output = Result<std::net::IpAddr>>,
{
	let mut methods = vec![0x00_u8];
	if !proxy.username().is_empty() || proxy.password().is_some() {
		methods.push(0x02);
	}

	let mut greeting = Vec::with_capacity(2_usize.saturating_add(methods.len()));
	greeting.push(0x05);
	greeting.push(u8::try_from(methods.len()).expect("at most two methods"));
	greeting.extend_from_slice(&methods);
	socks_io(stream.write_all(&greeting).await)?;

	let mut choice = [0_u8; 2];
	socks_io(stream.read_exact(&mut choice).await)?;
	if choice[0] != 0x05 {
		return Err(err!(HttpClient(
			"proxy returned SOCKS version {:#04x}, expected 0x05",
			choice[0]
		)));
	}

	match choice[1] {
		| 0x00 => {},
		| 0x02 => authenticate(&mut stream, proxy).await?,
		| 0xFF =>
			return Err(err!(HttpClient(
				"proxy rejected all offered SOCKS5 authentication methods"
			))),
		| method =>
			return Err(err!(HttpClient(
				"proxy selected unsupported SOCKS5 authentication method {method:#04x}"
			))),
	}

	let address = if scheme.resolves_target() {
		Address::Domain(target.host.clone())
	} else {
		Address::parse(resolve(target.host.clone()).await?)
	};

	let mut request = vec![0x05, 0x01, 0x00];
	address.encode(&mut request);
	request.extend_from_slice(&target.port.to_be_bytes());
	socks_io(stream.write_all(&request).await)?;
	socks_io(stream.flush().await)?;

	let mut header = [0_u8; 4];
	socks_io(stream.read_exact(&mut header).await)?;
	if header[0] != 0x05 {
		return Err(err!(HttpClient(
			"proxy returned SOCKS version {:#04x} during CONNECT, expected 0x05",
			header[0]
		)));
	}
	if header[1] != 0x00 {
		return Err(err!(HttpClient("SOCKS5 CONNECT failed: {}", reply_message(header[1]))));
	}

	// Consume the bound address so the stream is left at the tunneled bytes.
	let mut bound = [0_u8; 2];
	match header[3] {
		| 0x01 => socks_io(stream.read_exact(&mut [0_u8; 4]).await)?,
		| 0x04 => socks_io(stream.read_exact(&mut [0_u8; 16]).await)?,
		| 0x03 => {
			socks_io(stream.read_exact(&mut bound).await)?;
			let mut host = vec![0_u8; usize::from(bound[1])];
			socks_io(stream.read_exact(&mut host).await)?;
		},
		| address_type =>
			return Err(err!(HttpClient(
				"proxy returned unknown SOCKS5 address type {address_type:#04x}"
			))),
	}
	socks_io(stream.read_exact(&mut bound).await)?;

	Ok(stream)
}

async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
	stream: &mut S,
	proxy: &Url,
) -> Result<()> {
	let username = proxy.username();
	let password = proxy.password().unwrap_or_default();
	if username.is_empty() {
		return Err(err!(HttpClient(
			"proxy requires username/password authentication but none was provided"
		)));
	}

	let mut request = vec![0x01];
	push_len_prefixed(&mut request, username.as_bytes())?;
	push_len_prefixed(&mut request, password.as_bytes())?;
	socks_io(stream.write_all(&request).await)?;
	socks_io(stream.flush().await)?;

	let mut response = [0_u8; 2];
	socks_io(stream.read_exact(&mut response).await)?;
	if response[1] != 0x00 {
		return Err(err!(HttpClient("proxy username/password authentication failed")));
	}

	Ok(())
}

fn push_len_prefixed(buffer: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
	let length = u8::try_from(bytes.len())
		.map_err(|_| err!(HttpClient("proxy credential exceeds 255 bytes")))?;
	buffer.push(length);
	buffer.extend_from_slice(bytes);
	Ok(())
}

#[derive(Clone, Debug)]
enum Address {
	Ipv4([u8; 4]),
	Ipv6([u8; 16]),
	Domain(String),
}

impl Address {
	fn parse(host: std::net::IpAddr) -> Self {
		match host {
			| std::net::IpAddr::V4(ip) => Self::Ipv4(ip.octets()),
			| std::net::IpAddr::V6(ip) => Self::Ipv6(ip.octets()),
		}
	}

	fn encode(&self, buffer: &mut Vec<u8>) {
		match self {
			| Self::Ipv4(octets) => {
				buffer.push(0x01);
				buffer.extend_from_slice(octets);
			},
			| Self::Ipv6(octets) => {
				buffer.push(0x04);
				buffer.extend_from_slice(octets);
			},
			| Self::Domain(host) => {
				buffer.push(0x03);
				// The host came from a parsed URL, so it cannot exceed 255 bytes.
				let length = u8::try_from(host.len()).expect("URL host fits in one byte");
				buffer.push(length);
				buffer.extend_from_slice(host.as_bytes());
			},
		}
	}
}

fn reply_message(reply: u8) -> &'static str {
	match reply {
		| 0x01 => "general failure",
		| 0x02 => "connection not allowed by ruleset",
		| 0x03 => "network unreachable",
		| 0x04 => "host unreachable",
		| 0x05 => "connection refused by destination",
		| 0x06 => "TTL expired",
		| 0x07 => "command not supported",
		| 0x08 => "address type not supported",
		| _ => "unknown error",
	}
}

fn socks_error(error: &std::io::Error) -> Error {
	Error::HttpClient(format!("SOCKS5 proxy I/O error: {error}").into())
}

fn socks_io<T>(result: std::io::Result<T>) -> Result<T> {
	result.map_err(|error| socks_error(&error))
}

/// Read from `stream` until a blank line, returning the status line and headers
/// as lossy UTF-8. Used for HTTP proxy responses.
async fn read_header<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> Result<String> {
	let mut buffer = Vec::with_capacity(256);
	let mut byte = [0_u8; 1];
	loop {
		stream.read_exact(&mut byte).await.map_err(|error| {
			Error::HttpClient(format!("proxy CONNECT read failed: {error}").into())
		})?;
		buffer.push(byte[0]);
		if buffer.ends_with(b"\r\n\r\n") || buffer.ends_with(b"\n\n") {
			break;
		}
		if buffer.len() > 16 * 1024 {
			return Err(err!(HttpClient("proxy CONNECT response header exceeds 16 KiB")));
		}
	}

	let end = buffer
		.windows(2)
		.position(|window| window == b"\r\n")
		.unwrap_or(buffer.len());
	Ok(String::from_utf8_lossy(&buffer[..end]).into_owned())
}

#[cfg(test)]
mod tests {
	use std::{
		pin::Pin,
		task::{Context, Poll},
	};

	use super::*;

	/// An in-memory `AsyncRead + AsyncWrite` that replays a scripted response
	/// and records everything written, for exercising the proxy handshakes.
	struct Scripted {
		incoming: std::collections::VecDeque<u8>,
		written: Vec<u8>,
	}

	impl Scripted {
		fn new(script: &[u8]) -> Self {
			Self {
				incoming: script.iter().copied().collect(),
				written: Vec::new(),
			}
		}

		fn written(&self) -> &[u8] { &self.written }
	}

	impl AsyncRead for Scripted {
		fn poll_read(
			mut self: Pin<&mut Self>,
			_cx: &mut Context<'_>,
			buffer: &mut [u8],
		) -> Poll<std::io::Result<usize>> {
			let count = self.incoming.len().min(buffer.len());
			for slot in &mut buffer[..count] {
				*slot = self.incoming.pop_front().expect("count <= incoming length");
			}
			Poll::Ready(Ok(count))
		}
	}

	impl AsyncWrite for Scripted {
		fn poll_write(
			mut self: Pin<&mut Self>,
			_cx: &mut Context<'_>,
			buffer: &[u8],
		) -> Poll<std::io::Result<usize>> {
			self.written.extend_from_slice(buffer);
			Poll::Ready(Ok(buffer.len()))
		}

		fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}

		fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}
	}

	const CONNECT_OK_IPV4_BOUND: [u8; 10] = [0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];

	#[test]
	fn proxy_authorization_from_userinfo() {
		let url = Url::parse("http://user:pass@proxy.example:3128").expect("valid URL");
		assert_eq!(proxy_authorization(&url), "Proxy-Authorization: Basic dXNlcjpwYXNz\r\n");

		let url = Url::parse("http://proxy.example:3128").expect("valid URL");
		assert!(proxy_authorization(&url).is_empty());
	}

	#[test]
	fn http_connect_succeeds_and_sends_authority() {
		let stream = Scripted::new(b"HTTP/1.1 200 Connection Established\r\n\r\n");
		let proxy = Url::parse("http://proxy.example:3128").expect("valid URL");
		let target = Target {
			host: "matrix.example".into(),
			port: 8448,
		};

		let stream =
			smol::block_on(http_connect(stream, &target, &proxy)).expect("CONNECT succeeds");
		let request = std::str::from_utf8(stream.written()).expect("ASCII request");
		assert!(request.starts_with("CONNECT matrix.example:8448 HTTP/1.1\r\n"), "{request}");
		assert!(request.contains("Host: matrix.example:8448\r\n"), "{request}");
		assert!(!request.contains("Proxy-Authorization"), "no userinfo configured");
	}

	#[test]
	fn http_connect_includes_proxy_authorization_from_userinfo() {
		let stream = Scripted::new(b"HTTP/1.1 200 Connection Established\r\n\r\n");
		let proxy = Url::parse("http://user:pass@proxy.example:3128").expect("valid URL");
		let target = Target {
			host: "matrix.example".into(),
			port: 8448,
		};

		let stream =
			smol::block_on(http_connect(stream, &target, &proxy)).expect("CONNECT succeeds");
		let request = std::str::from_utf8(stream.written()).expect("ASCII request");
		assert!(request.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"), "{request}");
	}

	#[test]
	fn http_connect_rejects_non_2xx_response() {
		let stream = Scripted::new(b"HTTP/1.1 403 Forbidden\r\n\r\n");
		let proxy = Url::parse("http://proxy.example:3128").expect("valid URL");
		let target = Target {
			host: "matrix.example".into(),
			port: 8448,
		};

		let result = smol::block_on(http_connect(stream, &target, &proxy));
		assert!(result.is_err());
	}

	#[test]
	fn socks5h_sends_domain_and_skips_local_resolution() {
		let mut script = vec![0x05, 0x00];
		script.extend_from_slice(&CONNECT_OK_IPV4_BOUND);
		let stream = Scripted::new(&script);
		let proxy = Url::parse("socks5h://proxy.example:1080").expect("valid URL");
		let target = Target {
			host: "matrix.example".into(),
			port: 8448,
		};

		let stream = smol::block_on(socks5_connect(
			stream,
			&proxy,
			Scheme::Socks5h,
			&target,
			|host| async move { panic!("socks5h must not resolve locally, got {host:?}") },
		))
		.expect("CONNECT succeeds");

		let written = stream.written();
		// Greeting: version, one method offered (no auth).
		assert_eq!(&written[0..3], &[0x05, 0x01, 0x00]);
		// CONNECT: version, connect command, reserved, domain address type.
		let request = &written[3..];
		assert_eq!(&request[0..4], &[0x05, 0x01, 0x00, 0x03]);
		assert_eq!(request[4], 14, "length of matrix.example");
		assert_eq!(&request[5..19], b"matrix.example");
		assert_eq!(&request[19..21], &8448_u16.to_be_bytes());
	}

	#[test]
	fn socks5_local_resolve_uses_resolver_and_sends_ip() {
		let mut script = vec![0x05, 0x00];
		script.extend_from_slice(&CONNECT_OK_IPV4_BOUND);
		let stream = Scripted::new(&script);
		let proxy = Url::parse("socks5://proxy.example:1080").expect("valid URL");
		let target = Target {
			host: "matrix.example".into(),
			port: 8448,
		};

		let stream = smol::block_on(socks5_connect(
			stream,
			&proxy,
			Scheme::Socks5,
			&target,
			|host| async move {
				assert_eq!(host, "matrix.example");
				Ok("203.0.113.9".parse().expect("valid IP"))
			},
		))
		.expect("CONNECT succeeds");

		let written = stream.written();
		assert_eq!(&written[0..3], &[0x05, 0x01, 0x00]);
		let request = &written[3..];
		assert_eq!(&request[0..4], &[0x05, 0x01, 0x00, 0x01]);
		assert_eq!(&request[4..8], &[203, 0, 113, 9]);
		assert_eq!(&request[8..10], &8448_u16.to_be_bytes());
	}

	#[test]
	fn socks5_offers_and_performs_username_password_authentication() {
		let mut script = vec![
			0x05, 0x02, // method choice: username/password
			0x01, 0x00, // authentication succeeded
		];
		script.extend_from_slice(&CONNECT_OK_IPV4_BOUND);
		let stream = Scripted::new(&script);
		let proxy = Url::parse("socks5://user:pass@proxy.example:1080").expect("valid URL");
		let target = Target {
			host: "matrix.example".into(),
			port: 8448,
		};

		let stream = smol::block_on(socks5_connect(
			stream,
			&proxy,
			Scheme::Socks5,
			&target,
			|_| async move { Ok("203.0.113.9".parse().expect("valid IP")) },
		))
		.expect("CONNECT succeeds");

		let written = stream.written();
		// Greeting offers no-auth and username/password.
		assert_eq!(&written[0..4], &[0x05, 0x02, 0x00, 0x02]);
		// Authentication request: version, length-prefixed user and password.
		assert_eq!(&written[4..6], &[0x01, 0x04]);
		assert_eq!(&written[6..10], b"user");
		assert_eq!(written[10], 0x04);
		assert_eq!(&written[11..15], b"pass");
		// The CONNECT request follows.
		assert_eq!(written[15], 0x05);
	}
}
