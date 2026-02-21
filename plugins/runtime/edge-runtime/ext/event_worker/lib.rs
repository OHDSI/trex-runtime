use crate::events::RawEvent;
use crate::events::WorkerEventWithMetadata;
use deno_core::OpState;
use deno_core::op2;
use deno_error::JsErrorBox;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::mpsc;

pub mod events;
pub mod js_interceptors;

#[op2(async)]
#[serde]
async fn op_event_accept(
  state: Rc<RefCell<OpState>>,
) -> Result<RawEvent, JsErrorBox> {
  let rx = {
    let mut op_state = state.borrow_mut();
    op_state.try_take::<mpsc::UnboundedReceiver<WorkerEventWithMetadata>>()
  };
  if rx.is_none() {
    return Err(JsErrorBox::generic("events worker receiver not available"));
  }
  let mut rx = rx.unwrap();

  let data = rx.recv().await;

  let mut op_state = state.borrow_mut();
  op_state.put::<mpsc::UnboundedReceiver<WorkerEventWithMetadata>>(rx);

  match data {
    Some(event) => Ok(RawEvent::Event(Box::new(event))),
    None => {
      op_state.waker.wake();
      Ok(RawEvent::Done)
    }
  }
}

deno_core::extension!(
  user_event_worker,
  ops = [op_event_accept],
  esm = ["event_worker.js"]
);
