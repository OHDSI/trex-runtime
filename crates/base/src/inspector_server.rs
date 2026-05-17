// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

// Below code bits are cherry-picked from
// `https://github.com/denoland/deno/blob/v1.37.2/runtime/inspector_server.rs`.

// Alias for the future `!` type.
use bytes::Bytes;
use core::convert::Infallible as Never;
use deno_core::futures::channel::mpsc;
use deno_core::futures::channel::mpsc::UnboundedReceiver;
use deno_core::futures::channel::mpsc::UnboundedSender;
use deno_core::futures::channel::oneshot;
use deno_core::futures::future;
use deno_core::futures::future::Future;
use deno_core::futures::prelude::*;
use deno_core::futures::select;
use deno_core::futures::stream::StreamExt;
use deno_core::futures::task::Poll;
use deno_core::serde_json;
use deno_core::serde_json::json;
use deno_core::serde_json::Value;
use deno_core::unsync::spawn;
use deno_core::url::Url;
use deno_core::InspectorMsg;
use deno_core::InspectorSessionChannels;
use deno_core::InspectorSessionKind;
use deno_core::InspectorSessionProxy;
use deno_core::JsRuntime;
use enum_as_inner::EnumAsInner;
use fastwebsockets::Frame;
use fastwebsockets::OpCode;
use fastwebsockets::WebSocket;
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn;
use hyper_util::server::graceful::GracefulShutdown;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::pin;
use std::process;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, EnumAsInner)]
pub enum InspectorOption {
  Inspect(SocketAddr),
  WithBreak(SocketAddr),
  WithWait(SocketAddr),
}

impl InspectorOption {
  pub fn socket_addr(&self) -> SocketAddr {
    match self {
      Self::Inspect(addr) | Self::WithBreak(addr) | Self::WithWait(addr) => {
        *addr
      }
    }
  }
}

#[derive(Clone)]
pub struct Inspector {
  pub option: InspectorOption,
  pub server: Arc<InspectorServer>,
}

impl Inspector {
  pub fn from_option(option: InspectorOption) -> Self {
    const INSPECTOR_NAME: &str = "sb-edge-runtime-inspector";

    Self {
      option,
      server: Arc::new(InspectorServer::new(
        option.socket_addr(),
        INSPECTOR_NAME,
      )),
    }
  }

  pub fn should_wait_for_session(&self) -> bool {
    matches!(
      self.option,
      InspectorOption::WithBreak(..) | InspectorOption::WithWait(..)
    )
  }
}

/// Websocket server that is used to proxy connections from
/// devtools to the inspector.
pub struct InspectorServer {
  pub host: SocketAddr,

  _register_inspector_tx: UnboundedSender<InspectorInfo>,
  force_disconnect_tx: UnboundedSender<Uuid>,
  shutdown_server_tx: Option<oneshot::Sender<()>>,
  thread_handle: Option<thread::JoinHandle<()>>,
}

impl InspectorServer {
  pub fn new(host: SocketAddr, name: &'static str) -> Self {
    let (register_inspector_tx, register_inspector_rx) =
      mpsc::unbounded::<InspectorInfo>();
    let (force_disconnect_tx, force_disconnect_rx) = mpsc::unbounded::<Uuid>();

    let (shutdown_server_tx, shutdown_server_rx) = oneshot::channel();

    let thread_handle = thread::spawn(move || {
      let rt = tokio::runtime::Builder::new_current_thread()
        .thread_name("sb-inspector")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

      let local = tokio::task::LocalSet::new();
      local.block_on(
        &rt,
        server(
          host,
          register_inspector_rx,
          force_disconnect_rx,
          shutdown_server_rx,
          name,
        ),
      )
    });

    Self {
      host,
      _register_inspector_tx: register_inspector_tx,
      force_disconnect_tx,
      shutdown_server_tx: Some(shutdown_server_tx),
      thread_handle: Some(thread_handle),
    }
  }

  pub fn register_inspector(
    &self,
    module_url: String,
    js_runtime: &mut JsRuntime,
    wait_for_session: bool,
  ) {
    let inspector = js_runtime.inspector();
    let session_sender = inspector.get_session_sender();
    let deregister_rx = inspector.add_deregister_handler();
    let info = InspectorInfo::new(
      self.host,
      session_sender,
      deregister_rx,
      module_url,
      wait_for_session,
    );
    // If the server thread has exited (e.g. the port failed to bind), the
    // receiver is dropped and this send fails. Don't take down the worker —
    // debugging is just unavailable.
    if let Err(err) = self._register_inspector_tx.unbounded_send(info) {
      warn!(%err, "inspector server unavailable; debug registration ignored");
    }
  }

  /// Tell the server thread that the worker behind `module_url` is gone and
  /// any attached DevTools WebSocket should be closed immediately. We can't
  /// rely on the worker's own deregister handler to fire when a supervisor
  /// kill leaves the worker thread stuck inside V8 — the runtime drop that
  /// would normally trigger deregistration never happens. Driving the close
  /// from here breaks the deadlock.
  pub fn force_disconnect_url(&self, module_url: &str) {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, module_url.as_bytes());
    let _ = self.force_disconnect_tx.unbounded_send(uuid);
  }
}

impl Drop for InspectorServer {
  fn drop(&mut self) {
    if let Some(shutdown_server_tx) = self.shutdown_server_tx.take() {
      // The receiver lives on the server thread. If that thread has already
      // exited (e.g. the port failed to bind, or shutdown raced with bind
      // failure), the send fails — that's fine, the thread is already gone.
      let _ = shutdown_server_tx.send(());
    }

    if let Some(thread_handle) = self.thread_handle.take() {
      let _ = thread_handle.join();
    }
  }
}

// Needed so hyper can use non Send futures
#[derive(Clone)]
struct LocalExecutor;

impl<Fut> hyper::rt::Executor<Fut> for LocalExecutor
where
  Fut: Future + 'static,
  Fut::Output: 'static,
{
  fn execute(&self, fut: Fut) {
    deno_core::unsync::spawn(fut);
  }
}

fn handle_ws_request(
  req: http::Request<hyper::body::Incoming>,
  inspector_map_rc: Rc<RefCell<HashMap<Uuid, InspectorInfo>>>,
) -> http::Result<http::Response<BoxBody<Bytes, Infallible>>> {
  let (parts, body) = req.into_parts();
  let req = http::Request::from_parts(parts, ());

  let maybe_uuid = req
    .uri()
    .path()
    .strip_prefix("/ws/")
    .and_then(|s| Uuid::parse_str(s).ok());

  if maybe_uuid.is_none() {
    return http::Response::builder()
      .status(http::StatusCode::BAD_REQUEST)
      .body("Malformed inspector UUID".to_string().boxed());
  }

  // run in a block to not hold borrow to `inspector_map` for too long
  let (new_session_tx, deregistered_watch_rx) = {
    let inspector_map = inspector_map_rc.borrow();
    let maybe_inspector_info = inspector_map.get(&maybe_uuid.unwrap());

    if maybe_inspector_info.is_none() {
      return http::Response::builder()
        .status(http::StatusCode::NOT_FOUND)
        .body("Invalid inspector UUID".to_string().boxed());
    }

    let info = maybe_inspector_info.unwrap();
    (
      info.new_session_tx.clone(),
      info.deregistered_watch_rx.clone(),
    )
  };
  let (parts, _) = req.into_parts();
  let mut req = http::Request::from_parts(parts, body);

  let (resp, fut) = match fastwebsockets::upgrade::upgrade(&mut req) {
    Ok((resp, fut)) => (resp.map(BodyExt::boxed), fut),
    _ => {
      return http::Response::builder()
        .status(http::StatusCode::BAD_REQUEST)
        .body("Not a valid Websocket Request".to_string().boxed());
    }
  };

  // spawn a task that will wait for websocket connection and then pump messages between
  // the socket and inspector proxy
  spawn(async move {
    let websocket = match fut.await {
      Ok(w) => w,
      Err(err) => {
        error!(%err, "inspector server failed to upgrade to ws connection");
        return;
      }
    };

    // The 'outbound' channel carries messages sent to the websocket.
    let (outbound_tx, outbound_rx) = mpsc::unbounded();
    // The 'inbound' channel carries messages received from the websocket.
    let (inbound_tx, inbound_rx) = mpsc::unbounded();

    let inspector_session_proxy = InspectorSessionProxy {
      channels: InspectorSessionChannels::Regular {
        tx: outbound_tx,
        rx: inbound_rx,
      },
      kind: InspectorSessionKind::Blocking,
    };

    info!("debugger session started");
    let _ = new_session_tx.unbounded_send(inspector_session_proxy);
    pump_websocket_messages(
      websocket,
      inbound_tx,
      outbound_rx,
      deregistered_watch_rx,
    )
    .await;
  });

  Ok(resp)
}

fn handle_json_request(
  inspector_map: Rc<RefCell<HashMap<Uuid, InspectorInfo>>>,
  host: Option<String>,
) -> http::Result<http::Response<BoxBody<Bytes, Infallible>>> {
  let data = inspector_map
    .borrow()
    .values()
    .map(move |info| info.get_json_metadata(&host))
    .collect::<Vec<_>>();
  http::Response::builder()
    .status(http::StatusCode::OK)
    .header(http::header::CONTENT_TYPE, "application/json")
    .body(serde_json::to_string(&data).unwrap().boxed())
}

fn handle_json_version_request(
  version_response: Value,
) -> http::Result<http::Response<BoxBody<Bytes, Infallible>>> {
  http::Response::builder()
    .status(http::StatusCode::OK)
    .header(http::header::CONTENT_TYPE, "application/json")
    .body(serde_json::to_string(&version_response).unwrap().boxed())
}

async fn server(
  host: SocketAddr,
  register_inspector_rx: UnboundedReceiver<InspectorInfo>,
  force_disconnect_rx: UnboundedReceiver<Uuid>,
  mut shutdown_server_rx: oneshot::Receiver<()>,
  name: &str,
) {
  let inspector_map_ =
    Rc::new(RefCell::new(HashMap::<Uuid, InspectorInfo>::new()));

  let inspector_map = Rc::clone(&inspector_map_);
  let mut register_inspector_handler = pin!(register_inspector_rx
    .map(|info| {
      let ws_url = info.get_websocket_debugger_url(&info.host.to_string());
      info!(
        target = %info.url,
        wait_for_session = info.wait_for_session,
        url = %ws_url,
        "debugger listening (visit chrome://inspect to connect)"
      );
      // Stable UUIDs (derived from the service path) can re-register when a
      // worker restarts after a crash. Replacing the old entry is fine — the
      // previous worker is gone and its session_tx is dead. If a previous
      // worker was somehow still live, surface it as a warning so we can
      // tell debugging is now pointed at the newer instance.
      if let Some(prev) = inspector_map.borrow_mut().insert(info.uuid, info) {
        let still_alive = !prev.new_session_tx.is_closed();
        if still_alive {
          warn!(
            target = %prev.url,
            uuid = %prev.uuid,
            "inspector target re-registered while an older session is still attached; \
             new connections will be routed to the most recent worker"
          );
          // Tell any client attached to the previous worker that the session
          // is being replaced so their WS tears down cleanly.
          let _ = prev.deregistered_watch_tx.send(true);
        }
      }
    })
    .collect::<()>());

  let inspector_map = Rc::clone(&inspector_map_);
  let mut deregister_inspector_handler = pin!(future::poll_fn(|cx| {
    inspector_map.borrow_mut().retain(|_, info| {
      if info.deregister_rx.poll_unpin(cx) == Poll::Pending {
        true
      } else {
        let _ = info.deregistered_watch_tx.send(true);
        false
      }
    });
    Poll::<Never>::Pending
  })
  .fuse());

  let inspector_map = Rc::clone(&inspector_map_);
  let mut force_disconnect_handler = pin!(force_disconnect_rx
    .map(|uuid| {
      // Fire the entry's deregister watch — that's what attached pumps poll
      // to detect "your worker is gone, close the WS". We do NOT remove the
      // entry here: when the new worker for the same service path
      // re-registers (same v5 UUID), the register handler replaces it.
      // Removing here would race with that re-registration.
      if let Some(info) = inspector_map.borrow().get(&uuid) {
        let _ = info.deregistered_watch_tx.send(true);
      }
    })
    .collect::<()>());

  let json_version_response = json!({
      "Browser": name,
      "Protocol-Version": "1.3",
      "V8-Version": deno_core::v8::V8::get_version(),
  });

  let service_fn = hyper::service::service_fn({
    let inspector_map = inspector_map_.clone();
    let json_version_response = json_version_response.clone();

    move |req| {
      // If the host header can make a valid URL, use it
      let host = req
        .headers()
        .get("host")
        .and_then(|host| host.to_str().ok())
        .and_then(|host| Url::parse(&format!("http://{host}")).ok())
        .and_then(|url| match (url.host(), url.port()) {
          (Some(host), Some(port)) => Some(format!("{host}:{port}")),
          (Some(host), None) => Some(format!("{host}")),
          _ => None,
        });

      let resp = match (req.method(), req.uri().path()) {
        (&http::Method::GET, path) if path.starts_with("/ws/") => {
          handle_ws_request(req, inspector_map.clone())
        }
        (&http::Method::GET, "/json/version") => {
          handle_json_version_request(json_version_response.clone())
        }
        (&http::Method::GET, "/json") => {
          handle_json_request(inspector_map.clone(), host)
        }
        (&http::Method::GET, "/json/list") => {
          handle_json_request(inspector_map.clone(), host)
        }
        _ => http::Response::builder()
          .status(http::StatusCode::NOT_FOUND)
          .body(String::from("Not Found").boxed()),
      };

      future::ready(resp)
    }
  });

  let listener = match TcpListener::bind(host).await {
    Ok(listener) => listener,
    Err(err) => {
      error!(
        %host,
        %err,
        "cannot bind inspector server; debugging is disabled for this session \
         (rest of the runtime keeps running)"
      );
      return;
    }
  };

  info!(%host, "inspector server listening");

  let graceful = GracefulShutdown::new();
  let accept_fut = async {
    let executor = LocalExecutor;
    let conn_builder = conn::auto::Builder::new(executor.clone());

    loop {
      let (tcp, _) = match listener.accept().await {
        Ok(conn) => conn,
        Err(err) => {
          error!(%err);
          continue;
        }
      };

      let io = TokioIo::new(tcp);
      let conn =
        conn_builder.serve_connection_with_upgrades(io, service_fn.clone());
      let conn = graceful.watch(conn.into_owned());

      executor.execute(async move {
        let _ = conn.await;
      });
    }
  }
  .fuse();

  {
    let mut accept_fut = pin!(accept_fut);

    select! {
      _ = register_inspector_handler => {},
      _ = deregister_inspector_handler => unreachable!(),
      _ = force_disconnect_handler => {},
      _ = accept_fut => {},
      _ = shutdown_server_rx => {}
    }
  }

  graceful.shutdown().await;
}

/// The pump future takes care of forwarding messages between the websocket
/// and channels. It resolves when either side disconnects, ignoring any
/// errors.
///
/// The future proxies messages sent and received on a warp WebSocket
/// to a UnboundedSender/UnboundedReceiver pair. We need these "unbounded" channel ends to sidestep
/// Tokio's task budget, which causes issues when JsRuntimeInspector::poll_sessions()
/// needs to block the thread because JavaScript execution is paused.
///
/// This works because UnboundedSender/UnboundedReceiver are implemented in the
/// 'futures' crate, therefore they can't participate in Tokio's cooperative
/// task yielding.
async fn pump_websocket_messages(
  mut websocket: WebSocket<TokioIo<hyper::upgrade::Upgraded>>,
  inbound_tx: UnboundedSender<String>,
  mut outbound_rx: UnboundedReceiver<InspectorMsg>,
  deregistered_watch_rx: watch::Receiver<bool>,
) {
  // We can't rely on `watch::Receiver::wait_for` to wake this task reliably
  // from the deregister handler running on the same LocalSet, so check the
  // current watch value via `borrow()` on a short interval. The cost is
  // negligible — there's at most one pump per attached DevTools session.
  let mut deregister_check =
    tokio::time::interval(std::time::Duration::from_millis(250));
  // Skip the immediate fire so we don't waste a select! iteration up front.
  deregister_check.set_missed_tick_behavior(
    tokio::time::MissedTickBehavior::Skip,
  );
  let _ = deregister_check.tick().await;

  let mut server_initiated_close = false;
  'pump: loop {
    tokio::select! {
      // Messages from the inspector to the websocket. `None` means the
      // inspector dropped its sender — i.e. the JsRuntime is gone. That is
      // our most reliable "worker died" signal.
      msg = outbound_rx.next() => {
        match msg {
          Some(msg) => {
            let frame = Frame::text(msg.content.into_bytes().into());
            let _ = websocket.write_frame(frame).await;
          }
          None => {
            debug!("debugger session ended (worker terminated)");
            server_initiated_close = true;
            break 'pump;
          }
        }
      }
      msg = websocket.read_frame() => {
        match msg {
          Ok(msg) => {
            match msg.opcode {
              OpCode::Text => {
                if let Ok(s) = String::from_utf8(msg.payload.to_vec()) {
                  let _ = inbound_tx.unbounded_send(s);
                }
              }
              OpCode::Close => {
                debug!("debugger session ended (client closed)");
                break 'pump;
              }
              _ => {
                  // Ignore other messages.
              }
            }
          }

          Err(err) => {
            debug!(%err, "debugger session ended (read error)");
            break 'pump;
          }
        }
      }
      _ = deregister_check.tick() => {
        if *deregistered_watch_rx.borrow() {
          debug!("debugger session ended (deregistered)");
          server_initiated_close = true;
          break 'pump;
        }
      }
      else => {
        break 'pump;
      }
    }
  }

  if server_initiated_close {
    // 1001 "going away" matches the spec for an endpoint shutting down.
    let close = Frame::close(1001, b"worker terminated");
    let _ = websocket.write_frame(close).await;
  }
}

/// Inspector information that is sent from the isolate thread to the server
/// thread when a new inspector is created.
pub struct InspectorInfo {
  pub host: SocketAddr,
  pub uuid: Uuid,
  pub thread_name: Option<String>,
  pub new_session_tx: UnboundedSender<InspectorSessionProxy>,
  pub deregister_rx: oneshot::Receiver<()>,
  pub deregistered_watch_tx: watch::Sender<bool>,
  pub deregistered_watch_rx: watch::Receiver<bool>,
  pub url: String,
  pub wait_for_session: bool,
}

impl InspectorInfo {
  pub fn new(
    host: SocketAddr,
    new_session_tx: mpsc::UnboundedSender<InspectorSessionProxy>,
    deregister_rx: oneshot::Receiver<()>,
    url: String,
    wait_for_session: bool,
  ) -> Self {
    let (deregistered_watch_tx, deregistered_watch_rx) = watch::channel(false);

    // Derive a stable target UUID from the module URL so that a worker's
    // DevTools WebSocket URL survives crashes and restarts. The previous
    // behaviour minted a fresh v4 UUID per worker, which made every crash
    // invalidate the user's pasted ws:// URL.
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, url.as_bytes());

    Self {
      host,
      uuid,
      thread_name: thread::current().name().map(|n| n.to_owned()),
      new_session_tx,
      deregister_rx,
      deregistered_watch_tx,
      deregistered_watch_rx,
      url,
      wait_for_session,
    }
  }

  fn get_json_metadata(&self, host: &Option<String>) -> Value {
    let host_listen = format!("{}", self.host);
    let host = host.as_ref().unwrap_or(&host_listen);
    json!({
      "description": "EdgeRuntime",
      "devtoolsFrontendUrl": self.get_frontend_url(host),
      "faviconUrl": "https://supabase.com/favicon/favicon.ico",
      "id": self.uuid.to_string(),
      "title": self.get_title(),
      "type": "node",
      "url": self.url.to_string(),
      "webSocketDebuggerUrl": self.get_websocket_debugger_url(host),
    })
  }

  pub fn get_websocket_debugger_url(&self, host: &str) -> String {
    format!("ws://{}/ws/{}", host, &self.uuid)
  }

  fn get_frontend_url(&self, host: &str) -> String {
    format!(
            "devtools://devtools/bundled/js_app.html?ws={}/ws/{}&experiments=true&v8only=true",
            host, &self.uuid
        )
  }

  fn get_title(&self) -> String {
    format!(
      "EdgeRuntime{} [pid: {}]",
      self
        .thread_name
        .as_ref()
        .map(|n| format!(" - {n}"))
        .unwrap_or_default(),
      process::id(),
    )
  }
}
