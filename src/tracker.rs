//! A tracker that can spawn detached threads and stop one or all threads using interrupt on demand
use crossbeam_utils::Backoff;
use nix::sys::{pthread::Pthread, signal::Signal};
use parking_lot::{Condvar, Mutex};
use std::{
    collections::HashSet,
    io,
    marker::PhantomData,
    os::{raw::c_void, unix::thread::JoinHandleExt},
    sync::Arc,
    time::Duration,
};

use crate::helpers;

/// State data for RawInterruptibleTracker
#[derive(Debug, Clone)]
struct RawInterruptibleTrackerData {
    can_spawn: bool,
    new_thread: bool,
    threads: HashSet<Pthread>,
}

/// This mutex + condvar notification tracks threads and notifies when:
/// - The number of threads reach 0.
/// - A new thread has been added. In this case, interrupts should be retried.
#[derive(Debug)]
struct RawInterruptibleTracker {
    data: Mutex<RawInterruptibleTrackerData>,
    condvar: Condvar,
}

impl RawInterruptibleTracker {
    fn track(&self, thread: Pthread) -> Result<(), io::Error> {
        let mut guard = self.data.lock();
        if !guard.can_spawn {
            return Err(io::Error::other(
                "cannot spawn any threads, the tracker is shutting down",
            ));
        }
        guard.threads.insert(thread);
        guard.new_thread = true;
        self.condvar.notify_all();
        Ok(())
    }

    fn untrack(&self, thread: Pthread) {
        let mut guard = self.data.lock();
        guard.threads.remove(&thread);
        self.condvar.notify_all();
    }
}

/// This mutex + condvar notification notifies that a tracked thread has exited.
#[derive(Debug)]
struct RawInterruptibleExitTracker {
    exited: Mutex<bool>,
    condvar: Condvar,
}

/// This guard guarantees that:
/// - The hosting thread is alive if this guard is in scope.
/// - Only when this guard exits, the caller-supplied work has stopped, and the exit status is marked. If the exit status is unmarked, the object must be in scope.
#[derive(Debug)]
pub struct InterruptibleGuard {
    tracker: Arc<RawInterruptibleTracker>,
    exit: Arc<RawInterruptibleExitTracker>,
    _unsend: PhantomData<*mut c_void>,
}

impl InterruptibleGuard {
    fn new(
        tracker: Arc<RawInterruptibleTracker>,
        exit: Arc<RawInterruptibleExitTracker>,
    ) -> Result<Self, io::Error> {
        tracker.track(nix::sys::pthread::pthread_self())?;
        Ok(Self {
            tracker,
            exit,
            _unsend: PhantomData,
        })
    }
}

impl Drop for InterruptibleGuard {
    fn drop(&mut self) {
        let current = nix::sys::pthread::pthread_self();
        self.tracker.untrack(current);
        let mut exit_guard = self.exit.exited.lock();
        *exit_guard = true;
        self.exit.condvar.notify_all();
        drop(exit_guard);
    }
}

#[derive(Debug, Clone)]
pub struct InterruptibleHandle {
    thread: Pthread,
    exit: Arc<RawInterruptibleExitTracker>,
}

impl InterruptibleHandle {
    /// Stop the thread by sending an interrupt.
    pub fn stop(&self) {
        let backoff = Backoff::new();
        let mut snooze_interrupt_done = false;
        let mut exit_guard = self.exit.exited.lock();
        while !*exit_guard {
            if backoff.is_completed() {
                let _ = nix::sys::pthread::pthread_kill(self.thread, Signal::SIGUSR1);
                self.exit
                    .condvar
                    .wait_for(&mut exit_guard, Duration::from_millis(1));
            } else {
                if !snooze_interrupt_done {
                    let _ = nix::sys::pthread::pthread_kill(self.thread, Signal::SIGUSR1);
                    snooze_interrupt_done = true;
                }
                drop(exit_guard);
                backoff.snooze();
                // Perform additional snoozes if try_lock fails and backoff has not run out.
                loop {
                    if let Some(g) = self.exit.exited.try_lock() {
                        exit_guard = g;
                        break;
                    } else if backoff.is_completed() {
                        exit_guard = self.exit.exited.lock();
                        break;
                    } else {
                        backoff.snooze();
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct InterruptibleTracker {
    raw: Arc<RawInterruptibleTracker>,
}

impl InterruptibleTracker {
    /// Create a new tracker.
    ///
    /// # SAFETY
    /// - The user has to ensure a custom SIGUSR1 handler exists.
    pub unsafe fn new() -> Result<Self, io::Error> {
        unsafe { helpers::ensure_sigaction_exists()? };
        Ok(Self {
            raw: Arc::new(RawInterruptibleTracker {
                condvar: Condvar::new(),
                data: Mutex::new(RawInterruptibleTrackerData {
                    can_spawn: true,
                    new_thread: false,
                    threads: HashSet::new(),
                }),
            }),
        })
    }

    /// Spawns a new thread and return an interrupt handle for that thread.
    ///
    /// The handle is stable and should not interfere with another running thread sharing the same pthread_t.
    pub fn spawn(
        &self,
        f: impl FnOnce(InterruptibleHandle) + Send + 'static,
    ) -> Result<InterruptibleHandle, io::Error> {
        let exit = Arc::new(RawInterruptibleExitTracker {
            condvar: Condvar::new(),
            exited: Mutex::new(false),
        });
        // This channel reports if a thread has exited yet.
        let (spawn_tx, spawn_rx) = crossbeam_channel::bounded(1);
        let raw_tracker = self.raw.clone();
        let exit_clone = exit.clone();
        let thread = std::thread::Builder::new().spawn(move || {
            let _guard = match InterruptibleGuard::new(raw_tracker, exit_clone.clone()) {
                Ok(g) => {
                    let _ = spawn_tx.send(Ok(()));
                    g
                }
                Err(e) => {
                    let _ = spawn_tx.send(Err(e));
                    return;
                }
            };
            let handle = InterruptibleHandle {
                exit: exit_clone,
                thread: nix::sys::pthread::pthread_self(),
            };
            f(handle)
        })?;
        spawn_rx
            .recv()
            .map_err(|_e| io::Error::other("checking thread hung up"))??;
        Ok(InterruptibleHandle {
            exit,
            thread: thread.as_pthread_t(),
        })
    }

    /// Interrupts until all thread running on this tracker are stopped.
    pub fn stop(&self) {
        let mut g = self.raw.data.lock();
        let backoff = Backoff::new();
        let mut snooze_interrupt_done = false;
        // Do not allow threads to spawn any further
        g.can_spawn = false;
        while !g.threads.is_empty() {
            // Reset the backoff and re-allow snooze interrupts upon encountering a new thread.
            if g.new_thread {
                g.new_thread = false;
                snooze_interrupt_done = false;
                backoff.reset();
            }
            if backoff.is_completed() {
                // The threads interrupted here is guaranteed to be in scope.
                for thread in g.threads.iter() {
                    let _ = nix::sys::pthread::pthread_kill(*thread, Signal::SIGUSR1);
                }
                self.raw.condvar.wait_for(&mut g, Duration::from_millis(1));
            } else {
                // To avoid incessant syscalls, we only call pthread_kill once per snooze group.
                // The threads interrupted here is guaranteed to be in scope.
                if !snooze_interrupt_done {
                    for thread in g.threads.iter() {
                        let _ = nix::sys::pthread::pthread_kill(*thread, Signal::SIGUSR1);
                    }
                    snooze_interrupt_done = true;
                }
                drop(g);
                // Perform an out-of lock snooze, then reclaim the lock to continue checking.
                backoff.snooze();
                // Perform additional snoozes if try_lock fails and backoff has not run out.
                loop {
                    if let Some(_g) = self.raw.data.try_lock() {
                        g = _g;
                        break;
                    } else if backoff.is_completed() {
                        g = self.raw.data.lock();
                        break;
                    } else {
                        backoff.snooze();
                    }
                }
            }
        }
    }

    /// Creates a interruptible guard for the current thread and a handle that tries to stop the current thread.
    pub fn guard(&self) -> Result<(InterruptibleGuard, InterruptibleHandle), io::Error> {
        let exit = Arc::new(RawInterruptibleExitTracker {
            condvar: Condvar::new(),
            exited: Mutex::new(false),
        });
        let raw_tracker = self.raw.clone();
        let exit_clone = exit.clone();
        let guard = InterruptibleGuard::new(raw_tracker, exit_clone)?;
        Ok((
            guard,
            InterruptibleHandle {
                exit,
                thread: nix::sys::pthread::pthread_self(),
            },
        ))
    }
}
