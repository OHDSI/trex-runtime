use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use base_rt::DenoRuntimeDropToken;
use base_rt::DuplexStreamEntry;
use deno_core::AsyncRefCell;
use deno_core::AsyncResult;
use deno_core::CancelHandle;
use deno_core::CancelTryFuture;
use deno_core::OpState;
use deno_core::RcRef;
use deno_core::Resource;
use deno_core::ResourceId;
use deno_core::op2;
use deno_error::JsErrorBox;
use deno_net::ops::IpAddr;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing::span;

deno_core::extension!(
  runtime_net,
  middleware = |op| match op.name {
    "op_net_listen_tcp" => op.with_implementation_from(&op_net_listen()),
    "op_net_accept_tcp" => op.with_implementation_from(&op_net_accept()),
    "op_net_listen_tls" => op.with_implementation_from(&op_net_unsupported()),
    "op_net_listen_udp" => op.with_implementation_from(&op_net_unsupported()),
    "op_node_unstable_net_listen_udp" =>
      op.with_implementation_from(&op_net_unsupported()),
    "op_net_listen_unix" => op.with_implementation_from(&op_net_unsupported()),
    "op_net_listen_unixpacket" =>
      op.with_implementation_from(&op_net_unsupported()),
    "op_node_unstable_net_listen_unixpacket" =>
      op.with_implementation_from(&op_net_unsupported()),
    _ => op,
  }
);

/// Duplex stream resource for user worker connections.
pub struct TokioDuplexResource {
  id: usize,
  read: AsyncRefCell<io::ReadHalf<io::DuplexStream>>,
  write: AsyncRefCell<io::WriteHalf<io::DuplexStream>>,
  cancel_handle: CancelHandle,
}

impl TokioDuplexResource {
  pub fn new(stream: io::DuplexStream) -> Self {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let (read, write) = io::split(stream);

    Self {
      id: COUNTER.fetch_add(1, Ordering::SeqCst),
      read: AsyncRefCell::new(read),
      write: AsyncRefCell::new(write),
      cancel_handle: CancelHandle::default(),
    }
  }

  pub fn cancel_read_ops(&self) {
    self.cancel_handle.cancel()
  }

  async fn read(self: Rc<Self>, buf: &mut [u8]) -> Result<usize, JsErrorBox> {
    let cancel_handle = RcRef::map(self.clone(), |this| &this.cancel_handle);
    async {
      let read = RcRef::map(self, |this| &this.read);
      let mut read = read.borrow_mut().await;
      Pin::new(&mut *read)
        .read(buf)
        .await
        .map_err(JsErrorBox::from_err)
    }
    .try_or_cancel(cancel_handle)
    .await
  }

  async fn write(self: Rc<Self>, buf: &[u8]) -> Result<usize, JsErrorBox> {
    let write = RcRef::map(self, |this| &this.write);
    let mut write = write.borrow_mut().await;
    Pin::new(&mut *write)
      .write(buf)
      .await
      .map_err(JsErrorBox::from_err)
  }

  async fn shutdown(self: Rc<Self>) -> Result<(), JsErrorBox> {
    let write = RcRef::map(self, |this| &this.write);
    let mut write = write.borrow_mut().await;
    Pin::new(&mut *write)
      .shutdown()
      .await
      .map_err(JsErrorBox::from_err)
  }

  /// Reunites halves back into the original DuplexStream.
  pub fn into_inner(self) -> (usize, io::DuplexStream) {
    let read = self.read.into_inner();
    let write = self.write.into_inner();
    (self.id, read.unsplit(write))
  }
}

impl Resource for TokioDuplexResource {
  deno_core::impl_readable_byob!();
  deno_core::impl_writable!();

  fn name(&self) -> Cow<str> {
    "tokioDuplexStream".into()
  }

  fn shutdown(self: Rc<Self>) -> AsyncResult<()> {
    Box::pin(self.shutdown())
  }

  fn close(self: Rc<Self>) {
    self.cancel_read_ops();
  }
}

/// Marker resource for virtual TCP listeners in user workers.
#[derive(Debug, Clone, Default)]
struct ListenMarker(CancellationToken);

impl Drop for ListenMarker {
  fn drop(&mut self) {
    self.0.cancel();
  }
}

impl Resource for ListenMarker {}

/// Virtual TCP listen - creates a marker instead of real TCP socket.
#[op2]
#[serde]
pub fn op_net_listen(
  state: &mut OpState,
  #[serde] addr: IpAddr,
  _reuse_port: bool,
  _load_balanced: bool,
  _tcp_backlog: i32,
) -> Result<(ResourceId, IpAddr), crate::RuntimeError> {
  Ok((
    state.resource_table.add(ListenMarker::default()),
    IpAddr {
      hostname: addr.hostname,
      port: addr.port,
    },
  ))
}

/// Virtual TCP accept - waits for duplex stream from main worker.
#[op2(async)]
#[serde]
pub async fn op_net_accept(
  state: Rc<RefCell<OpState>>,
  #[smi] rid: ResourceId,
) -> Result<(ResourceId, IpAddr, IpAddr, Option<u32>), crate::RuntimeError> {
  let accept_token = state
    .borrow()
    .resource_table
    .get::<ListenMarker>(rid)
    .map(|m| m.0.clone())
    .map_err(crate::RuntimeError::Resource)?;

  // Retry acquiring receiver (may be held by concurrent accept)
  const MAX_RETRIES: u32 = 100;
  let (rx, runtime_token) = {
    let mut retry_count = 0;
    loop {
      let result = {
        let mut op_state = state.borrow_mut();

        let runtime_token = op_state
          .try_borrow::<DenoRuntimeDropToken>()
          .cloned()
          .ok_or_else(|| {
            crate::RuntimeError::Runtime(
              "runtime drop token not available".into(),
            )
          })?;

        op_state
          .try_take::<mpsc::UnboundedReceiver<DuplexStreamEntry>>()
          .map(|rx| (rx, runtime_token))
      };

      if let Some(pair) = result {
        break pair;
      }

      retry_count += 1;
      if retry_count >= MAX_RETRIES {
        return Err(crate::RuntimeError::Runtime(
          "connection receiver busy".into(),
        ));
      }

      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  };

  // Ensure receiver is restored on exit
  let mut rx = scopeguard::guard(rx, {
    let state = state.clone();
    move |value| {
      state
        .borrow_mut()
        .put::<mpsc::UnboundedReceiver<DuplexStreamEntry>>(value);
    }
  });

  let (stream, conn_token) = tokio::select! {
    ret = rx.recv() => ret,
    _ = accept_token.cancelled() => None,
    _ = runtime_token.clone().cancelled_owned() => None,
  }
  .ok_or_else(|| crate::RuntimeError::Runtime("listener closed".into()))?;

  let resource = TokioDuplexResource::new(stream);
  let id = resource.id;

  drop(rx);

  let mut op_state = state.borrow_mut();
  let rid = op_state.resource_table.add(resource);

  if let Some(token) = conn_token {
    // Cancel connection when worker terminates
    drop(base_rt::SUPERVISOR_RT.spawn({
      let token = token.clone();
      async move {
        let _span = span!(Level::DEBUG, "conn_lifetime", id);
        tokio::select! {
          _ = runtime_token.cancelled_owned() => {
            if !token.is_cancelled() {
              token.cancel();
            }
          }
          _ = token.cancelled() => {}
        }
      }
    }));

    if let Some(map) =
      op_state.try_borrow_mut::<HashMap<usize, CancellationToken>>()
    {
      map.insert(id, token);
    }
  }

  Ok((
    rid,
    IpAddr {
      hostname: "0.0.0.0".into(),
      port: 9999,
    },
    IpAddr {
      hostname: "0.0.0.0".into(),
      port: 8888,
    },
    None,
  ))
}

#[op2(fast)]
pub fn op_net_unsupported(
  _state: &mut OpState,
) -> Result<(), crate::RuntimeError> {
  Err(crate::RuntimeError::Runtime(
    "Operation not supported".into(),
  ))
}
