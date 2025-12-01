//! Threading utilities for dedicated-thread-per-isolate model.
//!
//! This module provides utilities for managing V8 isolates on dedicated OS threads,
//! following the Deno 2.5.6 threading model. Each V8 isolate runs on its own OS thread
//! with a current-thread Tokio runtime, ensuring isolates never migrate between threads
//! and eliminating the need for v8::Locker.
//!
//! # Threading Model
//!
//! ```text
//! Main Thread (multi-threaded Tokio)
//!     │
//!     └─► Worker Thread (dedicated OS thread)
//!             │
//!             └─► Current-Thread Tokio Runtime
//!                     │
//!                     └─► LocalSet (pins !Send futures)
//!                             │
//!                             └─► V8 Isolate (never migrates)
//! ```
//!
//! # Key Principles
//!
//! - **Thread Affinity**: Each isolate is bound to exactly one OS thread
//! - **No Migration**: Isolates never move between threads
//! - **No Locker**: Single-threaded access eliminates need for v8::Locker
//! - **HandleScope**: Use deno_core::scope!() for V8 operations
//! - **Cross-Thread**: Use v8::IsolateHandle for termination only

use std::future::Future;
use std::thread::JoinHandle;

use anyhow::{Context, Error};
use deno_core::v8;
use tokio::sync::oneshot;

/// Creates a current-thread Tokio runtime suitable for running a V8 isolate.
///
/// This creates a single-threaded runtime that will NOT use work-stealing or
/// multi-threaded execution. All tasks scheduled on this runtime will execute
/// on the same thread, which is required for V8 isolate safety.
///
/// # Arguments
///
/// * `name` - Thread name for debugging/profiling purposes
///
/// # Returns
///
/// A configured current-thread Tokio runtime
///
/// # Example
///
/// ```rust,ignore
/// let rt = create_current_thread_runtime("worker-1")?;
/// rt.block_on(async {
///     // All code here runs on single thread
/// });
/// ```
pub fn create_current_thread_runtime(name: &str) -> Result<tokio::runtime::Runtime, Error> {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .thread_name(name)
        .build()
        .context("failed to create current-thread runtime")
}

/// Executes a future on a LocalSet, pinning all spawned tasks to the current thread.
///
/// This is critical for V8 isolates because it ensures that all async operations
/// related to the isolate run on the same thread. Without LocalSet, Tokio might
/// try to schedule tasks on different threads in a multi-threaded runtime.
///
/// # Arguments
///
/// * `rt` - The current-thread Tokio runtime
/// * `fut` - The future to execute
///
/// # Returns
///
/// The output of the future
///
/// # Example
///
/// ```rust,ignore
/// let rt = create_current_thread_runtime("worker")?;
/// let result = block_on_local(&rt, async {
///     // This and all spawned tasks run on same thread
///     tokio::task::spawn_local(async { /* ... */ }).await
/// });
/// ```
pub fn block_on_local<F>(rt: &tokio::runtime::Runtime, fut: F) -> F::Output
where
    F: Future,
{
    let local = tokio::task::LocalSet::new();
    local.block_on(rt, fut)
}

/// Handle to a worker thread containing a V8 isolate.
///
/// This struct provides safe access to a V8 isolate running on a dedicated thread.
/// The `join_handle` allows waiting for the thread to complete, while the
/// `isolate_handle` provides thread-safe access for termination.
pub struct WorkerThread {
    /// OS thread handle for joining on shutdown
    pub join_handle: JoinHandle<Result<(), Error>>,

    /// Thread-safe handle for cross-thread termination
    pub isolate_handle: v8::IsolateHandle,
}

/// Spawns a dedicated OS thread for running a V8 isolate.
///
/// This function creates a new OS thread, initializes a current-thread Tokio runtime
/// on that thread, and executes the provided worker function. The worker function
/// should create a V8 isolate and return its handle along with a future that runs
/// the worker's event loop.
///
/// # Threading Safety
///
/// - The isolate is created on the worker thread
/// - The isolate never leaves the worker thread
/// - Cross-thread communication uses the returned IsolateHandle
/// - No v8::Locker is needed
///
/// # Arguments
///
/// * `name` - Thread name for debugging/profiling
/// * `worker_fn` - Function that creates isolate and returns (IsolateHandle, event_loop_future)
///
/// # Returns
///
/// A `WorkerThread` containing handles for the thread and isolate
///
/// # Example
///
/// ```rust,ignore
/// let worker = spawn_worker_thread(
///     "user-worker-1".to_string(),
///     || {
///         // This runs on the new dedicated thread
///         let mut js_runtime = JsRuntime::new(/* ... */);
///         let isolate_handle = js_runtime.v8_isolate().thread_safe_handle();
///
///         let event_loop = async move {
///             js_runtime.run_event_loop(/* ... */).await
///         };
///
///         Ok((isolate_handle, event_loop))
///     }
/// )?;
///
/// // Later, for termination:
/// worker.isolate_handle.terminate_execution();
/// worker.join_handle.join().expect("worker thread panicked");
/// ```
pub fn spawn_worker_thread<F, Fut>(
    name: String,
    worker_fn: F,
) -> Result<WorkerThread, Error>
where
    F: FnOnce() -> Result<(v8::IsolateHandle, Fut), Error> + Send + 'static,
    Fut: Future<Output = Result<(), Error>> + 'static,
{
    let (handle_tx, handle_rx) = oneshot::channel();

    let join_handle = std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            // Create the isolate and get its handle
            let (isolate_handle, worker_fut) = worker_fn()?;

            // Send the isolate handle back to the parent thread
            // This allows the parent to terminate the isolate if needed
            if handle_tx.send(isolate_handle).is_err() {
                return Err(anyhow::anyhow!("failed to send isolate handle to parent thread"));
            }

            // Create current-thread runtime on THIS thread
            let rt = create_current_thread_runtime(&format!("tokio-{}", name))?;

            // Execute the worker event loop on this thread
            // LocalSet ensures all spawned tasks stay on this thread
            block_on_local(&rt, worker_fut)
        })
        .context("failed to spawn worker thread")?;

    // Wait for the worker thread to send back the isolate handle
    let isolate_handle = handle_rx
        .blocking_recv()
        .context("failed to receive isolate handle from worker thread")?;

    Ok(WorkerThread {
        join_handle,
        isolate_handle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_create_current_thread_runtime() {
        let rt = create_current_thread_runtime("test-runtime").expect("failed to create runtime");

        // Verify we can execute async code
        let result = rt.block_on(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            42
        });

        assert_eq!(result, 42);
    }

    #[test]
    fn test_block_on_local() {
        let rt = create_current_thread_runtime("test-local").expect("failed to create runtime");

        let result = block_on_local(&rt, async {
            // Spawn a local task
            let task = tokio::task::spawn_local(async {
                100
            });

            task.await.expect("task failed")
        });

        assert_eq!(result, 100);
    }

    #[test]
    fn test_block_on_local_captures_thread_id() {
        let rt = create_current_thread_runtime("test-thread-id").expect("failed to create runtime");

        let outer_thread_id = Arc::new(Mutex::new(None));
        let inner_thread_id = Arc::new(Mutex::new(None));

        let outer_clone = outer_thread_id.clone();
        let inner_clone = inner_thread_id.clone();

        block_on_local(&rt, async move {
            *outer_clone.lock().unwrap() = Some(thread::current().id());

            tokio::task::spawn_local(async move {
                *inner_clone.lock().unwrap() = Some(thread::current().id());
            })
            .await
            .expect("task failed");
        });

        let outer = outer_thread_id.lock().unwrap().expect("outer thread id not set");
        let inner = inner_thread_id.lock().unwrap().expect("inner thread id not set");

        // Both should be the same thread
        assert_eq!(outer, inner, "LocalSet did not pin task to same thread");
    }

    #[test]
    fn test_spawn_worker_thread_basic() {
        let main_thread_id = thread::current().id();
        let worker_thread_id = Arc::new(Mutex::new(None));
        let worker_thread_id_clone = worker_thread_id.clone();

        let worker = spawn_worker_thread(
            "test-worker".to_string(),
            move || {
                // Capture the worker thread ID
                *worker_thread_id_clone.lock().unwrap() = Some(thread::current().id());

                // Create a mock isolate handle (we can't create real V8 isolate in simple test)
                // In real code, this would be js_runtime.v8_isolate().thread_safe_handle()
                let isolate_handle = unsafe {
                    // SAFETY: This is only for testing. In production, this comes from JsRuntime
                    std::mem::transmute::<usize, v8::IsolateHandle>(0xDEADBEEF)
                };

                let future = async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(())
                };

                Ok((isolate_handle, future))
            },
        );

        assert!(worker.is_ok(), "failed to spawn worker thread");

        let worker = worker.unwrap();

        // Verify different thread
        let worker_tid = worker_thread_id.lock().unwrap().expect("worker thread id not set");
        assert_ne!(
            main_thread_id, worker_tid,
            "worker should run on different thread"
        );

        // Wait for worker to complete
        let result = worker.join_handle.join();
        assert!(result.is_ok(), "worker thread panicked");
        assert!(result.unwrap().is_ok(), "worker returned error");
    }

    #[test]
    fn test_spawn_worker_thread_isolate_handle_sent() {
        let handle_value = 0xCAFEBABEusize;

        let worker = spawn_worker_thread(
            "test-handle".to_string(),
            move || {
                let isolate_handle = unsafe {
                    std::mem::transmute::<usize, v8::IsolateHandle>(handle_value)
                };

                let future = async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(())
                };

                Ok((isolate_handle, future))
            },
        );

        assert!(worker.is_ok(), "failed to spawn worker");

        let worker = worker.unwrap();

        // Verify we received the handle
        let received_handle_value = unsafe {
            std::mem::transmute::<v8::IsolateHandle, usize>(worker.isolate_handle)
        };

        assert_eq!(
            received_handle_value, handle_value,
            "isolate handle not transmitted correctly"
        );

        // Cleanup
        let _ = worker.join_handle.join();
    }

    #[test]
    fn test_spawn_worker_thread_error_propagation() {
        let worker = spawn_worker_thread(
            "test-error".to_string(),
            || -> Result<(v8::IsolateHandle, std::future::Ready<Result<(), Error>>), Error> {
                Err(anyhow::anyhow!("intentional test error"))
            },
        );

        // Worker creation should fail because worker_fn returned error
        assert!(worker.is_err(), "expected error from worker_fn");
    }
}
