//! V8 Isolate lifecycle management for safe concurrent access.
//!
//! Provides atomic guards to prevent race conditions between V8 operations
//! and runtime shutdown. The `IsolateLifecycle` struct manages a reference
//! count of active operations and prevents new operations from starting
//! once the isolate begins dropping.

use std::sync::atomic::{AtomicU32, Ordering};
use tokio_util::sync::CancellationToken;

/// The state of the isolate lifecycle.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolateState {
  /// Isolate is running and accepting operations.
  Running = 0,
  /// Isolate is being dropped, no new operations allowed.
  Dropping = 1,
  /// Isolate has been fully dropped.
  Dropped = 2,
}

impl IsolateState {
  fn from_u32(value: u32) -> Self {
    match value {
      0 => IsolateState::Running,
      1 => IsolateState::Dropping,
      2 => IsolateState::Dropped,
      _ => IsolateState::Dropped,
    }
  }
}

/// Manages the lifecycle of a V8 isolate to prevent TOCTOU race conditions.
///
/// This struct provides atomic access control to ensure that V8 operations
/// don't race with runtime shutdown. It maintains a count of active operations
/// and blocks new operations once dropping begins.
pub struct IsolateLifecycle {
  state: AtomicU32,
  active_operations: AtomicU32,
  drop_token: CancellationToken,
}

impl IsolateLifecycle {
  /// Creates a new IsolateLifecycle with the given drop token.
  pub fn new(drop_token: CancellationToken) -> Self {
    Self {
      state: AtomicU32::new(IsolateState::Running as u32),
      active_operations: AtomicU32::new(0),
      drop_token,
    }
  }

  /// Atomically try to enter an isolate operation.
  ///
  /// Returns `Some(IsolateGuard)` if the isolate is still running and the
  /// operation can proceed. Returns `None` if the isolate is dropping or
  /// has been dropped.
  ///
  /// The returned guard will decrement the active operation count when dropped.
  pub fn try_enter(&self) -> Option<IsolateGuard<'_>> {
    loop {
      let current = self.active_operations.load(Ordering::Acquire);

      // Check if we're still in running state
      if self.state.load(Ordering::Acquire) != IsolateState::Running as u32 {
        return None;
      }

      // Try to increment the operation count
      match self.active_operations.compare_exchange_weak(
        current,
        current + 1,
        Ordering::AcqRel,
        Ordering::Relaxed,
      ) {
        Ok(_) => {
          // Double-check state after incrementing (handles race with begin_drop)
          if self.state.load(Ordering::Acquire) != IsolateState::Running as u32
          {
            self.active_operations.fetch_sub(1, Ordering::Release);
            return None;
          }
          return Some(IsolateGuard { lifecycle: self });
        }
        Err(_) => continue,
      }
    }
  }

  /// Begin dropping the isolate.
  ///
  /// This transitions the state to Dropping, cancels the drop token, and
  /// waits for all active operations to complete before transitioning to Dropped.
  pub fn begin_drop(&self) {
    self
      .state
      .store(IsolateState::Dropping as u32, Ordering::Release);
    self.drop_token.cancel();

    // Spin-wait for active operations to complete
    let mut spin_count = 0u32;
    while self.active_operations.load(Ordering::Acquire) > 0 {
      spin_count = spin_count.wrapping_add(1);
      if spin_count.is_multiple_of(1000) {
        std::thread::yield_now();
      } else {
        std::hint::spin_loop();
      }
    }

    self
      .state
      .store(IsolateState::Dropped as u32, Ordering::Release);
  }

  /// Returns the current lifecycle state.
  pub fn state(&self) -> IsolateState {
    IsolateState::from_u32(self.state.load(Ordering::Acquire))
  }

  /// Returns whether the isolate is still running.
  pub fn is_running(&self) -> bool {
    self.state() == IsolateState::Running
  }

  /// Returns whether the isolate is being dropped or has been dropped.
  pub fn is_dropping(&self) -> bool {
    self.state() != IsolateState::Running
  }

  /// Returns a reference to the drop token.
  pub fn drop_token(&self) -> &CancellationToken {
    &self.drop_token
  }

  /// Returns the number of currently active operations.
  pub fn active_operation_count(&self) -> u32 {
    self.active_operations.load(Ordering::Acquire)
  }
}

/// A guard that represents an active operation on the isolate.
///
/// While this guard exists, the isolate will not complete its drop sequence.
/// When the guard is dropped, it decrements the active operation count.
pub struct IsolateGuard<'a> {
  lifecycle: &'a IsolateLifecycle,
}

impl Drop for IsolateGuard<'_> {
  fn drop(&mut self) {
    self
      .lifecycle
      .active_operations
      .fetch_sub(1, Ordering::Release);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  use std::thread;

  #[test]
  fn test_try_enter_returns_guard_when_running() {
    let token = CancellationToken::new();
    let lifecycle = IsolateLifecycle::new(token);

    let guard = lifecycle.try_enter();
    assert!(guard.is_some());
    assert_eq!(lifecycle.active_operation_count(), 1);

    drop(guard);
    assert_eq!(lifecycle.active_operation_count(), 0);
  }

  #[test]
  fn test_try_enter_returns_none_after_begin_drop() {
    let token = CancellationToken::new();
    let lifecycle = IsolateLifecycle::new(token);

    lifecycle.begin_drop();

    let guard = lifecycle.try_enter();
    assert!(guard.is_none());
    assert_eq!(lifecycle.state(), IsolateState::Dropped);
  }

  #[test]
  fn test_begin_drop_waits_for_active_guards() {
    use std::sync::atomic::AtomicBool;

    let token = CancellationToken::new();
    let lifecycle = Arc::new(IsolateLifecycle::new(token));
    let drop_started = Arc::new(AtomicBool::new(false));
    let guard_released = Arc::new(AtomicBool::new(false));

    // Start a thread that will acquire a guard and hold it
    let lc = lifecycle.clone();
    let ds = drop_started.clone();
    let gr = guard_released.clone();
    let guard_thread = thread::spawn(move || {
      let guard = lc.try_enter().unwrap();
      assert_eq!(lc.active_operation_count(), 1);

      // Wait until begin_drop has been called
      while !ds.load(Ordering::Acquire) {
        thread::yield_now();
      }

      // Hold the guard a bit longer to test blocking
      thread::sleep(std::time::Duration::from_millis(10));

      drop(guard);
      gr.store(true, Ordering::Release);
    });

    // Wait for guard to be acquired
    thread::sleep(std::time::Duration::from_millis(5));

    // Start dropping - this should block until guard is released
    let lc2 = lifecycle.clone();
    let drop_thread = thread::spawn(move || {
      drop_started.store(true, Ordering::Release);
      lc2.begin_drop();
    });

    guard_thread.join().unwrap();
    drop_thread.join().unwrap();

    assert!(guard_released.load(Ordering::Acquire));
    assert_eq!(lifecycle.state(), IsolateState::Dropped);
    assert_eq!(lifecycle.active_operation_count(), 0);
  }

  #[test]
  fn test_concurrent_try_enter() {
    let token = CancellationToken::new();
    let lifecycle = Arc::new(IsolateLifecycle::new(token));

    let mut handles = vec![];
    for _ in 0..10 {
      let lc = lifecycle.clone();
      handles.push(thread::spawn(move || {
        for _ in 0..100 {
          if let Some(guard) = lc.try_enter() {
            // Simulate some work
            std::hint::spin_loop();
            drop(guard);
          }
        }
      }));
    }

    for handle in handles {
      handle.join().unwrap();
    }

    assert_eq!(lifecycle.active_operation_count(), 0);
  }

  #[test]
  fn test_drop_token_is_cancelled_on_begin_drop() {
    let token = CancellationToken::new();
    let lifecycle = IsolateLifecycle::new(token.clone());

    assert!(!token.is_cancelled());
    lifecycle.begin_drop();
    assert!(token.is_cancelled());
  }
}
