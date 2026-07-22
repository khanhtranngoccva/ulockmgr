//! Library for implementing Linux passthrough file locking and interruption in FUSE.
//!
//! This is a rewrite of [ulockmgr](https://github.com/libfuse/libfuse/blob/fuse_2_9_bugfix/util/ulockmgr_server.c) with slightly different mechanisms. However, the architecture is practically the same - each lock owner ID will spawn a process which stores locked FDs.
//!
//! This crate also includes utilities for interrupting an in-progress operation.
//!
//! # Differences
//! - The owner processes use TTLs. These processes exit if they are not in use.
//! - The lifecycle of locking is more explicit. The register mechanism adds file descriptors, while the unregister mechanism removes files and interrupts all in-progress locks. These mechanisms are abstracted in the RAII [`Lockable`] type returned by [`LockManager::register`].
//! - Explicit interrupt handles are used, and interrupts block until the relevant threads have exited by retrying in intervals.
//!
//! # TODO
//! - Since this is used as a filesystem component, checked allocations may be desired.
use crate::messages::RequestSpawnOwner;
pub use crate::owner::{Lockable, Owner};
use crossbeam_channel::RecvTimeoutError;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::{
    io,
    os::fd::{AsFd, OwnedFd},
    path::Path,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

pub(crate) mod communication;
pub(crate) mod helpers;
pub(crate) mod messages;
pub(crate) mod owner;
pub(crate) mod owner_spawner;
pub(crate) mod processes;
pub mod tracker;
pub mod types;
pub(crate) mod waiter;

/// The main structure for managing locks.
pub struct LockManager {
    communication: Mutex<OwnedFd>,
    owners: Arc<DashMap<u64, Arc<Owner>>>,
    ttl: Duration,
    cleanup_thread: Option<JoinHandle<()>>,
    cleanup_tx: Option<crossbeam_channel::Sender<()>>,
}

impl LockManager {
    /// Create a lock manager instance using the provided server process path.
    ///
    /// # SAFETY
    /// - These requirements must be propagated to the caller:
    ///     - The end user program has to set up a signal handler for SIGUSR1 beforehand. Removing the handler may lead to crashing.
    ///     - This method calls sigaction under the hood. The callers must follow safety requirements of that function.
    pub unsafe fn from_exe(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let coordinator_fd = processes::spawn_coordinator_process(path)?;
        unsafe { Self::build(coordinator_fd) }
    }

    /// Create a lock manager instance using the communication FD.
    ///
    /// # SAFETY
    /// - These requirements must be propagated to the caller:
    ///     - The end user program has to set up a signal handler for SIGUSR1 beforehand. Removing the handler may lead to crashing.
    ///     - This method calls sigaction under the hood. The callers must follow safety requirements of that function.
    pub unsafe fn build(coordinator_fd: OwnedFd) -> Result<Self, io::Error> {
        let owners = Arc::new(DashMap::new());
        let (cleanup_tx, cleanup_rx) = crossbeam_channel::bounded(0);
        let cleanup_thread = thread::Builder::new()
            .name(String::from("lockmgr-clr"))
            .spawn({
                let owners = owners.clone();
                move || cleanup_thread(owners, cleanup_rx)
            })?;
        Ok(Self {
            communication: Mutex::new(coordinator_fd),
            owners,
            ttl: Duration::from_secs(5),
            cleanup_thread: Some(cleanup_thread),
            cleanup_tx: Some(cleanup_tx),
        })
    }

    /// Low-level implementation for spawning owner processes. The user should generally use the [`Self::register`] method.
    pub fn get_or_create_owner(
        &self,
        owner_id: u64,
        ttl: Option<Duration>,
    ) -> Result<Arc<Owner>, io::Error> {
        // Lock order: owner lock -> communication lock
        let mut new_owner = false;
        loop {
            let ttl = ttl.unwrap_or(self.ttl);
            let owner = match self.owners.get(&owner_id).map(|owner| owner.clone()) {
                Some(owner) => owner,
                None => self
                    .owners
                    .entry(owner_id)
                    .or_try_insert_with(|| {
                        let comm_guard = self.communication.lock();
                        let (_, mut owner_fds) = messages::send_request_with_fds(
                            comm_guard,
                            RequestSpawnOwner::new(ttl),
                        )??;
                        if owner_fds.len() != 1 {
                            return Err(io::Error::other("expected 1 owner FD"));
                        }
                        let owner = Arc::new(Owner::new(owner_fds.remove(0)));
                        new_owner = true;
                        Ok(owner)
                    })?
                    .clone(),
            };
            // If the owner is dead, remove it and try again. Should avoid doing that if ttl is deliberately set to 0
            let new_owner_with_zero_ttl = ttl == Duration::from_secs(0) && new_owner;
            if !new_owner_with_zero_ttl && owner.ping().is_err() {
                self.owners.remove(&owner_id);
                continue;
            }
            break Ok(owner);
        }
    }

    /// Registers a file descriptor with the corresponding owner process for the selected owner ID. If the owner process is dead, it will be restarted.
    pub fn register(&self, owner_id: u64, fd: impl AsFd) -> Result<Lockable, io::Error> {
        loop {
            let owner = self.get_or_create_owner(owner_id, None)?;
            match owner.register(fd.as_fd()) {
                Ok(lockable) => break Ok(lockable),
                // BrokenPipe is not enough to indicate that the owner is dead (maybe invalid data frames were receoved).
                Err(e) if e.kind() == io::ErrorKind::BrokenPipe && owner.ping().is_err() => {
                    continue;
                }
                Err(e) => return Err(e),
            };
        }
    }
}

impl Drop for LockManager {
    fn drop(&mut self) {
        drop(self.cleanup_tx.take());
        let _ = self.cleanup_thread.take().unwrap().join();
    }
}

/// This thread attempts to clean up in intervals.
fn cleanup_thread(
    owners: Arc<DashMap<u64, Arc<Owner>>>,
    cleanup_rx: crossbeam_channel::Receiver<()>,
) {
    loop {
        match cleanup_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(_) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {}
        }
        // Unused owner processes will exit when there are no file locks, we clean up on our side
        owners.retain(|_, owner| owner.ping().is_ok());
    }
}

/// Structures and functions used by the server binary, this does not need to be used by the filesystem authors
pub mod server {
    use super::*;

    pub use communication::hydrate_forked_fd;
    pub use owner::DaemonOwner;
    pub use owner_spawner::OwnerSpawner;
}
