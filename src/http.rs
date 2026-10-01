//! The port the front door calls: route a request to its function's worker and proxy it there.
//!
//! What reaches here has already been authorised at the front door, which writes the project
//! (`x-snoutdata-ref`) from the hostname it resolved; this layer never re-decides who may call
//! what. It does decide, per request, how long the
//! request may take: the whole of the project's wall-clock limit, from the moment it arrives.
//!
//! It does ask one thing of every request: that it carry the front door's secret
//! (`DOOR_HEADER`). The project is named by a header, so a request that reached this port some
//! other way, a function calling the runtime on an address its network allows, could name any
//! project it liked, and run there past that project's keys. The secret is the fleet's, held by
//! the front doors and this process and never by a worker: it is removed before a request is
//! proxied, and a worker's environment is its manifest's.

use std::convert::Infallible;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::time::{Instant, Sleep};

use crate::manifest::{self, Manifests};
use crate::supervisor::{Key, Lease, Refusal, Supervisor};

pub const HEALTH_PATH: &str = "/_snoutpod/health";
/// The header the front door proves itself with, and the variable its value is read from.
pub const DOOR_HEADER: &str = "x-snoutdata-door";
pub const DOOR_ENV: &str = "SNOUT_FUNCTIONS_DOOR_SECRET";

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Reply = Response<BoxBody<Bytes, Error>>;

pub struct State {
	pub manifests: Manifests,
	pub supervisor: Arc<Supervisor>,
	/// None only where no secret was configured, which is logged at start.
	pub door: Option<Vec<u8>>,
}

/// Whether a request carries the door's secret, compared in time that does not depend on
/// where the two first differ.
pub fn through_door(presented: Option<&[u8]>, door: &[u8]) -> bool {
	let Some(presented) = presented else { return false };
	if presented.len() != door.len() {
		return false;
	}
	presented.iter().zip(door).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

pub async fn serve(listener: TcpListener, state: Arc<State>) {
	loop {
		let (stream, _) = match listener.accept().await {
			Ok(accepted) => accepted,
			Err(error) => {
				eprintln!("accept: {error}");
				continue;
			}
		};
		let _ = stream.set_nodelay(true);
		tokio::spawn(connection(stream, state.clone()));
	}
}

/// A project's own process: the front process is the only caller, on a socket in a directory only
/// the two of them can reach (router.rs).
pub async fn serve_unix(listener: UnixListener, state: Arc<State>) {
	loop {
		match listener.accept().await {
			Ok((stream, _)) => {
				tokio::spawn(connection(stream, state.clone()));
			}
			Err(error) => eprintln!("accept: {error}"),
		}
	}
}

async fn connection<S>(stream: S, state: Arc<State>)
where
	S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
	let service = service_fn(move |request| {
		let state = state.clone();
		async move { Ok::<_, Infallible>(route(request, state).await) }
	});
	let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).with_upgrades().await;
}

async fn route(request: Request<Incoming>, state: Arc<State>) -> Reply {
	let encoding = crate::compress::requested(request.headers().get(hyper::header::ACCEPT_ENCODING).and_then(|v| v.to_str().ok()));
	let method = request.method().clone();
	crate::compress::reply(answer(request, state).await, encoding, &method)
}

async fn answer(request: Request<Incoming>, state: Arc<State>) -> Reply {
	if request.uri().path() == HEALTH_PATH {
		return text(StatusCode::OK, "ok\n");
	}
	if let Some(door) = &state.door
		&& !through_door(request.headers().get(DOOR_HEADER).map(|v| v.as_bytes()), door)
	{
		return refuse(StatusCode::FORBIDDEN, "This request did not arrive through the front door.", None);
	}
	let Some(project) = request.headers().get("x-snoutdata-ref").and_then(|v| v.to_str().ok()).map(str::to_owned) else {
		return refuse(StatusCode::BAD_REQUEST, "This request did not arrive through the front door.", Some("Call https://<ref>.api.snoutdata.com/functions/v1/<name>"));
	};
	if !manifest::valid_ref(&project) {
		return refuse(StatusCode::BAD_REQUEST, "This request did not arrive through the front door.", None);
	}
	let Some(name) = manifest::function_name(request.uri().path()).map(str::to_owned) else {
		return refuse(StatusCode::NOT_FOUND, "No function was named in this request.", Some("The path is /functions/v1/<name>"));
	};
	let manifest = state.manifests.get(&project);
	let Some((manifest, deployed)) = manifest.and_then(|m| {
		let deployed = m.functions.iter().find(|f| f.name == name).cloned()?;
		Some((m, deployed))
	}) else {
		// The same answer for "this project has deployed nothing" and "not that one".
		return refuse(StatusCode::NOT_FOUND, &format!("There is no function called {name} in this project."), Some(&format!("Deploy it with: snoutdata functions deploy {name}")));
	};
	let digest = deployed.digest.clone();
	if !manifest::valid_digest(&digest) {
		return refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some("its deployment is not valid"));
	}

	// The request's own deadline starts now, and covers starting the worker too.
	let limits = manifest.limits_for(&deployed);
	let deadline = Instant::now() + Duration::from_millis(limits.wall_ms);
	let bundle: PathBuf = state.manifests.bundle_dir(&digest);
	let key = Key::new(&project, &name, &digest, &manifest.env, limits);
	let lease = match tokio::time::timeout_at(deadline, state.supervisor.lease(key, bundle, &manifest.env, deployed.concurrency)).await {
		Err(_) => return limit_refusal("wall clock"),
		Ok(Err(Refusal::Full)) => {
			let mut reply = refuse(StatusCode::SERVICE_UNAVAILABLE, "This host is running as many functions as it can.", Some("Try again in a moment."));
			reply.headers_mut().insert("retry-after", hyper::header::HeaderValue::from_static("1"));
			return reply;
		}
		Ok(Err(Refusal::Start(error))) => {
			eprintln!("{project}/{name}: {error}");
			return refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some(&error));
		}
		Ok(Ok(lease)) => lease,
	};
	proxy(request, lease, deadline).await
}

async fn proxy(mut request: Request<Incoming>, lease: Lease, deadline: Instant) -> Reply {
	let worker = lease.entry.worker.clone();
	let stream = match tokio::time::timeout_at(deadline, UnixStream::connect(&worker.socket)).await {
		Err(_) => return limit_refusal("wall clock"),
		Ok(Err(_)) => return stopped(&lease),
		Ok(Ok(stream)) => stream,
	};
	let (mut sender, connection) = match tokio::time::timeout_at(deadline, hyper::client::conn::http1::handshake(TokioIo::new(stream))).await {
		Err(_) => return limit_refusal("wall clock"),
		Ok(Err(_)) => return stopped(&lease),
		Ok(Ok(pair)) => pair,
	};
	tokio::spawn(async move {
		let _ = connection.with_upgrades().await;
	});
	// Compressing is this layer's (compress.rs), so the worker's server is never asked to.
	request.headers_mut().remove(hyper::header::ACCEPT_ENCODING);
	// The door's secret is the one thing a function must never be handed.
	request.headers_mut().remove(DOOR_HEADER);
	// Only the path and query travel: the worker's server knows nothing of our host and port.
	let path = request.uri().path_and_query().map(|p| p.as_str().to_owned()).unwrap_or_else(|| "/".into());
	*request.uri_mut() = path.parse::<Uri>().unwrap_or_default();
	let answered = tokio::time::timeout_at(deadline, sender.send_request(request)).await;
	let response = match answered {
		Err(_) => return limit_refusal("wall clock"),
		Ok(Err(_)) => return stopped(&lease),
		Ok(Ok(response)) => response,
	};
	let (parts, body) = response.into_parts();
	let body = Deadline { inner: body, sleep: Box::pin(tokio::time::sleep_until(deadline)), _lease: Some(lease) };
	let mut reply = Response::from_parts(parts, BoxBody::new(body));
	reply.extensions_mut().insert(crate::compress::FromWorker);
	reply
}

/// A response body that ends when the request's wall clock does, and holds the request's lease
/// until then, so the worker counts as busy for exactly as long as it is sending.
struct Deadline {
	inner: Incoming,
	sleep: Pin<Box<Sleep>>,
	_lease: Option<Lease>,
}

impl Body for Deadline {
	type Data = Bytes;
	type Error = Error;

	fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
		let this = self.get_mut();
		if this.sleep.as_mut().poll(cx).is_ready() {
			this._lease = None;
			return Poll::Ready(Some(Err("the function's wall-clock limit was reached".into())));
		}
		match Pin::new(&mut this.inner).poll_frame(cx) {
			Poll::Ready(None) => {
				this._lease = None;
				Poll::Ready(None)
			}
			Poll::Ready(Some(frame)) => Poll::Ready(Some(frame.map_err(Into::into))),
			Poll::Pending => Poll::Pending,
		}
	}
}

fn stopped(lease: &Lease) -> Reply {
	match lease.entry.worker.stopped_because() {
		Some(reason) if reason == "memory" || reason == "CPU" => limit_refusal(&reason),
		Some(reason) => refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some(&format!("its worker was stopped: {reason}"))),
		None => refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some("its worker stopped")),
	}
}

/// A function that ran out of something hears which, in the runtime's own words, so it can be
/// fixed, rather than one sentence for every limit.
fn limit_refusal(which: &str) -> Reply {
	refuse(StatusCode::INTERNAL_SERVER_ERROR, "This function could not be run.", Some(&format!("it reached its {which} limit")))
}

pub fn refuse(status: StatusCode, message: &str, hint: Option<&str>) -> Reply {
	let body = serde_json::json!({ "message": message, "hint": hint });
	let mut reply = Response::new(full(body.to_string()));
	*reply.status_mut() = status;
	reply.headers_mut().insert("content-type", hyper::header::HeaderValue::from_static("application/json"));
	reply
}

pub fn text(status: StatusCode, body: &'static str) -> Reply {
	let mut reply = Response::new(full(body.to_owned()));
	*reply.status_mut() = status;
	reply.headers_mut().insert("content-type", hyper::header::HeaderValue::from_static("text/plain"));
	reply
}

fn full(body: String) -> BoxBody<Bytes, Error> {
	Full::new(Bytes::from(body)).map_err(|never| match never {}).boxed()
}

#[cfg(test)]
mod tests {
	use super::through_door;

	#[test]
	fn only_the_door_secret_passes() {
		let door = b"0123456789abcdef";
		assert!(through_door(Some(b"0123456789abcdef"), door));
		assert!(!through_door(None, door));
		assert!(!through_door(Some(b""), door));
		assert!(!through_door(Some(b"0123456789abcdeF"), door));
		assert!(!through_door(Some(b"0123456789abcde"), door));
		assert!(!through_door(Some(b"0123456789abcdef0"), door));
	}
}
