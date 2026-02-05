use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use deno_core::unsync::sync::AtomicFlag;

/// Lifecycle phases for finer-grained state tracking and debugging.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecyclePhase {
  /// Runtime has been created but not yet initialized.
  Created = 0,
  /// Runtime is initializing (loading modules, etc).
  Initializing = 1,
  /// Runtime is running and processing requests.
  Running = 2,
  /// Runtime is terminating (shutting down gracefully).
  Terminating = 3,
  /// Runtime has been fully terminated.
  Terminated = 4,
}

impl LifecyclePhase {
  /// Convert from u32 to LifecyclePhase.
  pub fn from_u32(value: u32) -> Self {
    match value {
      0 => LifecyclePhase::Created,
      1 => LifecyclePhase::Initializing,
      2 => LifecyclePhase::Running,
      3 => LifecyclePhase::Terminating,
      4 => LifecyclePhase::Terminated,
      _ => LifecyclePhase::Terminated,
    }
  }
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeState {
  pub init: Arc<AtomicFlag>,
  pub evaluating_mod: Arc<AtomicFlag>,
  pub event_loop_completed: Arc<AtomicFlag>,
  pub terminated: Arc<AtomicFlag>,
  pub found_inspector_session: Arc<AtomicFlag>,
  pub mem_reached_half: Arc<AtomicFlag>,
  pub wall_clock_beforeunload_triggered: Arc<AtomicFlag>,
  pub drain_triggered: Arc<AtomicFlag>,
  /// Finer-grained lifecycle phase for debugging.
  phase: Arc<AtomicU32>,
}

impl RuntimeState {
  pub fn is_init(&self) -> bool {
    self.init.is_raised()
  }

  pub fn is_evaluating_mod(&self) -> bool {
    self.evaluating_mod.is_raised()
  }

  pub fn is_event_loop_completed(&self) -> bool {
    self.event_loop_completed.is_raised()
  }

  pub fn is_terminated(&self) -> bool {
    self.terminated.is_raised()
  }

  pub fn is_found_inspector_session(&self) -> bool {
    self.found_inspector_session.is_raised()
  }

  /// Get the current lifecycle phase.
  pub fn phase(&self) -> LifecyclePhase {
    LifecyclePhase::from_u32(self.phase.load(Ordering::Acquire))
  }

  /// Atomically transition to a new phase if currently in the expected phase.
  ///
  /// Returns `true` if the transition was successful, `false` if the current
  /// phase didn't match the expected phase.
  pub fn transition(
    &self,
    expected: LifecyclePhase,
    new: LifecyclePhase,
  ) -> bool {
    self
      .phase
      .compare_exchange(
        expected as u32,
        new as u32,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
  }

  /// Set the lifecycle phase unconditionally.
  pub fn set_phase(&self, phase: LifecyclePhase) {
    self.phase.store(phase as u32, Ordering::Release);
  }
}
