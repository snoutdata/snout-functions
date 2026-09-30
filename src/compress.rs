//! Response compression, by the rules Deno's own HTTP server applies.
//!
//! Deno's HTTP server compresses a response when
//! the request accepts gzip or brotli, the response's content type is compressible
//! (`compressible.rs`), it has no `content-encoding` or `content-range` already, its
//! `cache-control` does not say `no-transform`, and it is not known to be under 64 bytes. That
//! is the behaviour clients of a Deno-served function already see. Here the same decision is made once, for every
//! response this process sends, and a worker's own server is never asked to compress (its request
//! arrives without `accept-encoding`), so nothing is compressed twice.
//!
//! Each chunk of a body is compressed and flushed as it passes, so a response the function
//! streams still arrives in pieces, compressed.
//!
//! The 64-byte floor applies to this process's own answers and never to a function's: a
//! function's reply is compressed whatever its length (a 10-byte `status 500` included), as a
//! runtime that re-serves it as a stream of unknown length would. A worker's own server sends a
//! length here, so a proxied reply carries `FromWorker` and the length is not asked.

use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use http_body_util::combinators::BoxBody;
use hyper::header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue, VARY};
use hyper::{Method, Response};

use crate::compressible::is_content_compressible;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
	None,
	Gzip,
	Brotli,
}

/// What the request accepts, preferring brotli where the weights are equal.
pub fn requested(accept_encoding: Option<&str>) -> Encoding {
	let Some(accept) = accept_encoding else { return Encoding::None };
	let mut best = (Encoding::None, 0.0_f32);
	for item in accept.split(',') {
		let mut parts = item.split(';');
		let name = parts.next().unwrap_or("").trim().to_ascii_lowercase();
		let weight = parts
			.find_map(|p| p.trim().strip_prefix("q=").and_then(|q| q.trim().parse::<f32>().ok()))
			.unwrap_or(1.0);
		let encoding = match name.as_str() {
			"br" => Encoding::Brotli,
			"gzip" => Encoding::Gzip,
			_ => continue,
		};
		if weight > 0.0 && (weight > best.1 || (weight == best.1 && encoding == Encoding::Brotli)) {
			best = (encoding, weight);
		}
	}
	best.0
}

/// Marks a reply proxied from a worker, whose length does not count against the floor.
#[derive(Clone, Copy)]
pub struct FromWorker;

fn compressible(headers: &HeaderMap, from_worker: bool) -> bool {
	let Some(content_type) = headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) else {
		return false;
	};
	if !is_content_compressible(content_type) || headers.contains_key(CONTENT_ENCODING) || headers.contains_key(CONTENT_RANGE) {
		return false;
	}
	let no_transform = headers
		.get_all(CACHE_CONTROL)
		.iter()
		.filter_map(|v| v.to_str().ok())
		.any(|v| v.split(',').any(|d| d.trim().eq_ignore_ascii_case("no-transform")));
	if no_transform {
		return false;
	}
	if from_worker {
		return true;
	}
	let short = headers
		.get(CONTENT_LENGTH)
		.and_then(|v| v.to_str().ok())
		.and_then(|v| v.parse::<u64>().ok())
		.is_some_and(|length| length < 64);
	!short
}

/// The reply, compressed if the request and the response both allow it.
pub fn reply(response: Response<BoxBody<Bytes, Error>>, encoding: Encoding, method: &Method) -> Response<BoxBody<Bytes, Error>> {
	let from_worker = response.extensions().get::<FromWorker>().is_some();
	if encoding == Encoding::None || !compressible(response.headers(), from_worker) {
		return response;
	}
	let (mut parts, body) = response.into_parts();
	let headers = &mut parts.headers;
	let varies = headers
		.get_all(VARY)
		.iter()
		.filter_map(|v| v.to_str().ok())
		.any(|v| v.split(',').any(|d| d.trim().eq_ignore_ascii_case("accept-encoding") || d.trim() == "*"));
	if !varies {
		headers.append(VARY, HeaderValue::from_static("Accept-Encoding"));
	}
	// A strong validator names exact bytes, and these are not those bytes any more.
	if let Some(etag) = headers.get(ETAG).and_then(|v| v.to_str().ok())
		&& !etag.starts_with("W/")
		&& let Ok(weak) = HeaderValue::from_str(&format!("W/{etag}"))
	{
		headers.insert(ETAG, weak);
	}
	headers.remove(CONTENT_LENGTH);
	headers.insert(CONTENT_ENCODING, HeaderValue::from_static(if encoding == Encoding::Brotli { "br" } else { "gzip" }));
	if method == Method::HEAD {
		return Response::from_parts(parts, body);
	}
	let encoder = match encoding {
		Encoding::Brotli => Encoder::Brotli(Box::new(brotli::CompressorWriter::new(Vec::new(), 4096, 4, 22))),
		_ => Encoder::Gzip(flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default())),
	};
	Response::from_parts(parts, BoxBody::new(Compressed { inner: body, encoder: Some(encoder) }))
}

enum Encoder {
	Gzip(flate2::write::GzEncoder<Vec<u8>>),
	Brotli(Box<brotli::CompressorWriter<Vec<u8>>>),
}

impl Encoder {
	/// Compress a chunk and flush, so what the function has sent so far is on its way.
	fn chunk(&mut self, data: &[u8]) -> std::io::Result<Bytes> {
		match self {
			Encoder::Gzip(e) => {
				e.write_all(data)?;
				e.flush()?;
				Ok(Bytes::from(std::mem::take(e.get_mut())))
			}
			Encoder::Brotli(e) => {
				e.write_all(data)?;
				e.flush()?;
				Ok(Bytes::from(std::mem::take(e.get_mut())))
			}
		}
	}

	fn finish(self) -> std::io::Result<Bytes> {
		match self {
			Encoder::Gzip(e) => e.finish().map(Bytes::from),
			Encoder::Brotli(e) => Ok(Bytes::from(e.into_inner())),
		}
	}
}

struct Compressed {
	inner: BoxBody<Bytes, Error>,
	encoder: Option<Encoder>,
}

impl Body for Compressed {
	type Data = Bytes;
	type Error = Error;

	fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
		let this = self.get_mut();
		loop {
			let Some(encoder) = this.encoder.as_mut() else { return Poll::Ready(None) };
			match Pin::new(&mut this.inner).poll_frame(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
				Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
					Ok(data) => match encoder.chunk(&data) {
						Ok(out) if out.is_empty() => continue,
						Ok(out) => return Poll::Ready(Some(Ok(Frame::data(out)))),
						Err(error) => return Poll::Ready(Some(Err(error.into()))),
					},
					// Trailers do not survive compression: Deno's server drops them too.
					Err(_) => continue,
				},
				Poll::Ready(None) => {
					let Some(encoder) = this.encoder.take() else { return Poll::Ready(None) };
					return match encoder.finish() {
						Ok(out) if out.is_empty() => Poll::Ready(None),
						Ok(out) => Poll::Ready(Some(Ok(Frame::data(out)))),
						Err(error) => Poll::Ready(Some(Err(error.into()))),
					};
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn what_a_request_accepts() {
		assert_eq!(requested(None), Encoding::None);
		assert_eq!(requested(Some("gzip, deflate")), Encoding::Gzip);
		assert_eq!(requested(Some("gzip, deflate, br")), Encoding::Brotli);
		assert_eq!(requested(Some("gzip, deflate, br, zstd")), Encoding::Brotli);
		assert_eq!(requested(Some("br;q=0.5, gzip")), Encoding::Gzip);
		assert_eq!(requested(Some("gzip;q=0, identity")), Encoding::None);
		assert_eq!(requested(Some("identity")), Encoding::None);
	}

	#[test]
	fn what_a_response_allows() {
		let mut headers = HeaderMap::new();
		headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
		assert!(compressible(&headers, false));
		headers.insert(CONTENT_LENGTH, HeaderValue::from_static("63"));
		assert!(!compressible(&headers, false));
		// A function's reply: no floor, whatever its length.
		assert!(compressible(&headers, true));
		headers.insert(CONTENT_LENGTH, HeaderValue::from_static("64"));
		assert!(compressible(&headers, false));
		headers.insert(CACHE_CONTROL, HeaderValue::from_static("public, no-transform"));
		assert!(!compressible(&headers, false));
		assert!(!compressible(&headers, true));
		let mut stream = HeaderMap::new();
		stream.insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
		assert!(!compressible(&stream, true));
	}
}
