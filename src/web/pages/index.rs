use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};

pub(crate) async fn index(_request: Request<Incoming>) -> Response<Full<Bytes>> {
	Response::builder()
		.header(http::header::CONTENT_TYPE, "text/html; charset=utf-8")
		.body(Full::from(Bytes::from_static(
			b"<!doctype html><html><head><meta charset=\"utf-8\"><title>Continuwuity</title></head><body><h1>Continuwuity</h1><p>Matrix homeserver running.</p></body></html>",
		)))
		.expect("static response headers are valid")
}
