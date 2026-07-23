use crate::{
    helpers::{self},
    messages::{
        self, Interrupt, LockCommand, Message, Ping, Register, RemoveInterrupt, Reply,
        ReplyLockCommand, ReplyLockCommandData, Replyable, Unregister,
    },
    tracker::{InterruptibleHandle, InterruptibleTracker},
    types::{LockAction, LockParams},
    waiter::Waiter,
};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use dashmap::{DashMap, Entry};
use nix::{fcntl::FcntlArg, sys::signal::Signal};
use parking_lot::{ArcRwLockReadGuard, Mutex, RawRwLock, RwLock};
use rustix::io::Errno;
use std::{
    io,
    os::fd::{AsFd, OwnedFd},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub(crate) type ExitLockGuard = ArcRwLockReadGuard<RawRwLock, bool>;

#[derive(Debug)]
struct FdData {
    /// Holds the file descriptor for the file that is being locked.
    descriptor: OwnedFd,
    /// Holds the exit lock. As long as this guard is held, the owner process should not exit unless a critical error occurred.
    _exit_lock: ExitLockGuard,
    /// Thread tracker for the instance.
    tracker: InterruptibleTracker,
}

#[derive(Debug)]
struct DaemonOwnerInner {
    /// The exit lock. As long as a guard of this is held, the owner process should not exit unless a critical error occurred.
    exit_lock: Arc<RwLock<bool>>,
    /// The next ID to be assigned to a file descriptor.
    next_id: AtomicU64,
    /// The map of file descriptors to their data.
    descriptors: DashMap<u64, Arc<FdData>>,
    /// The map of interrupts.
    interrupts: DashMap<u64, InterruptibleHandle>,
    /// Next ID for interrupts.
    interrupt_next_id: AtomicU64,
    /// The communication socket for the main FUSE processor.
    communication: Mutex<OwnedFd>,
}

impl DaemonOwnerInner {
    pub(crate) fn acquire_exit_lock(&self) -> Result<ExitLockGuard, io::Error> {
        let g = self.exit_lock.read_arc();
        if *g {
            // We treat owner exiting as a broken pipe error.
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "owner already exited",
            ));
        }
        Ok(g)
    }

    pub(crate) fn try_exit(&self) -> Result<(), io::Error> {
        let mut exit_lock = self.exit_lock.try_write().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ResourceBusy,
                "there are still registered files",
            )
        })?;
        *exit_lock = true;
        Ok(())
    }
}

// This runs in case of a fatal error or a panic, but is done just once.
impl Drop for DaemonOwnerInner {
    fn drop(&mut self) {
        // Cleaning up of the owner process is done by the kernel, so we don't need to take the hassle
        let _ = self
            .try_exit()
            .inspect_err(|e| log::error!("error exiting owner: {}", e));
    }
}

#[derive(Debug)]
pub struct DaemonOwner {
    /// The inner data structure for the managing thread.
    inner: Arc<DaemonOwnerInner>,
    /// The thread that runs the command loop.
    thread: Option<JoinHandle<Result<(), io::Error>>>,
    /// The timeout before a in-use check is performed.
    ttl: Duration,
    /// The channel to send a timeout refresh. When the TTL expires, the process will try to quit.
    ttl_send: Option<Sender<()>>,
    /// The channel to receive a timeout refresh.
    ttl_recv: Receiver<()>,
}

impl DaemonOwner {
    /// Creates a new owner structure to receive lock requests.
    pub fn new(communication: OwnedFd, ttl: Duration) -> Self {
        let (ttl_tx, ttl_rx) = crossbeam_channel::bounded(1);
        Self {
            inner: Arc::new(DaemonOwnerInner {
                exit_lock: Arc::new(RwLock::new(false)),
                next_id: AtomicU64::new(0),
                interrupt_next_id: AtomicU64::new(0),
                interrupts: DashMap::new(),
                descriptors: DashMap::new(),
                communication: Mutex::new(communication),
            }),
            thread: None,
            ttl,
            ttl_send: Some(ttl_tx),
            ttl_recv: ttl_rx,
        }
    }

    /// Starts the main loop. The main event loop thread stopping is not guaranteed.
    pub fn start(mut self) -> Result<(), io::Error> {
        let inner = self.inner.clone();
        let ttl_sender = self.ttl_send.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "daemon owner already started")
        })?;
        self.thread = Some(thread::spawn(move || {
            owner_receiving_loop(inner, ttl_sender)
        }));
        loop {
            let mut disconnected = false;
            match self.ttl_recv.recv_timeout(self.ttl) {
                // Refresh the TTL.
                Ok(_) => {
                    continue;
                }
                Err(RecvTimeoutError::Timeout) => {}
                // The main event loop is terminated.
                Err(RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                }
            };
            // If the thread is finished at any point, we should stop checking the exit boolean.
            let thread_is_finished = match self.thread.as_ref() {
                None => return Err(io::Error::other("thread is not assigned")),
                Some(thread) => thread.is_finished(),
            };
            if thread_is_finished || disconnected {
                log::info!("Thread finished or channel hung up");
                return self
                    .thread
                    .take()
                    .expect("thread is not assigned")
                    .join()
                    .map_err(|e| io::Error::other(format!("thread join error: {:?}", e)))?;
            }
            // Attempt to exit the owner process. Should not be possible if there are any lock references.
            if self.inner.try_exit().is_ok() {
                log::info!("Owner is unused for {:?}, exiting", self.ttl);
                return Ok(());
            }
        }
    }
}

fn owner_receiving_loop(
    inner: Arc<DaemonOwnerInner>,
    ttl_sender: Sender<()>,
) -> Result<(), io::Error> {
    // Only the filesystem daemon may signal to exit, and internal functions should not be interfered with unless they opt into it at specific cancellation points.
    let _guard = helpers::block_all_signals()?;
    loop {
        let request: (Message, OwnedFd) =
            match messages::receive_request(inner.communication.lock()) {
                Ok(request) => request,
                // Connection terminated
                Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                    return Ok(());
                }
                // Data from client is invalid. However, it is still recoverable.
                Err(e)
                    if e.kind() == io::ErrorKind::UnexpectedEof
                        || e.kind() == io::ErrorKind::InvalidInput
                        || e.kind() == io::ErrorKind::InvalidData =>
                {
                    let _ = ttl_sender.try_send(());
                    log::error!("invalid data received");
                    continue;
                }
                // Other errors
                Err(e) => {
                    return Err(e);
                }
            };
        let _ = ttl_sender.try_send(());
        match request {
            (Message::Ping(_), reply_fd) => {
                Ping::create_reply(reply_fd).reply(Ok(()));
            }
            (Message::Interrupt(interrupt), reply_fd) => {
                Interrupt::create_reply(reply_fd)
                    .reply(io_cvt(handle_interrupt(&inner, interrupt)));
            }
            (Message::RemoveInterrupt(interrupt), reply_fd) => {
                RemoveInterrupt::create_reply(reply_fd)
                    .reply(io_cvt(handle_interrupt_remove(&inner, interrupt)));
            }
            (Message::Register(register), reply_fd) => {
                Register::create_reply(reply_fd)
                    .reply(io_cvt(handle_fd_register(&inner, register)));
            }
            (Message::Unregister(unregister), reply_fd) => {
                Unregister::create_reply(reply_fd)
                    .reply(io_cvt(handle_fd_unregister(&inner, unregister)));
            }
            (Message::LockCommand(lock_command), reply_fd) => {
                handle_lock_command(
                    inner.clone(),
                    lock_command,
                    LockCommand::create_reply(reply_fd),
                );
            }
        }
    }
}

fn io_cvt<T>(result: Result<T, io::Error>) -> Result<T, Errno> {
    result.map_err(|e| match e.raw_os_error() {
        Some(errno) => Errno::from_raw_os_error(errno),
        None => match e.kind() {
            io::ErrorKind::NotFound => Errno::NOENT,
            io::ErrorKind::PermissionDenied => Errno::PERM,
            io::ErrorKind::ConnectionRefused => Errno::CONNREFUSED,
            io::ErrorKind::ConnectionReset => Errno::CONNRESET,
            io::ErrorKind::ConnectionAborted => Errno::CONNABORTED,
            io::ErrorKind::NotConnected => Errno::NOTCONN,
            io::ErrorKind::AddrInUse => Errno::ADDRINUSE,
            io::ErrorKind::AddrNotAvailable => Errno::ADDRNOTAVAIL,
            io::ErrorKind::TimedOut => Errno::TIMEDOUT,
            io::ErrorKind::WouldBlock => Errno::WOULDBLOCK,
            io::ErrorKind::AlreadyExists => Errno::EXIST,
            io::ErrorKind::InvalidInput => Errno::INVAL,
            io::ErrorKind::InvalidData => Errno::INVAL,
            io::ErrorKind::Deadlock => Errno::DEADLOCK,
            io::ErrorKind::Other => Errno::IO,
            _ => Errno::IO,
        },
    })
}

fn handle_interrupt(inner: &DaemonOwnerInner, interrupt: Interrupt) -> Result<(), io::Error> {
    let intr = inner
        .interrupts
        .get(&interrupt.id)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "no interrupts found with specified ID",
            )
        })?
        .clone();
    intr.stop();
    Ok(())
}

fn handle_interrupt_add(inner: &DaemonOwnerInner, handle: InterruptibleHandle) -> u64 {
    loop {
        let candidate_id = inner.interrupt_next_id.fetch_add(1, Ordering::AcqRel);
        match inner.interrupts.entry(candidate_id) {
            Entry::Vacant(entry) => {
                entry.insert(handle);
                break candidate_id;
            }
            Entry::Occupied(_entry) => {
                continue;
            }
        }
    }
}

fn handle_interrupt_remove(
    inner: &DaemonOwnerInner,
    interrupt: RemoveInterrupt,
) -> Result<(), io::Error> {
    let _ = inner.interrupts.remove(&interrupt.id).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no interrupts found with specified ID",
        )
    })?;
    Ok(())
}

fn handle_fd_register(inner: &DaemonOwnerInner, register: Register) -> Result<u64, io::Error> {
    let lock = inner.acquire_exit_lock()?;
    let fd_data = Arc::new(FdData {
        descriptor: register.fd,
        _exit_lock: lock,
        tracker: unsafe { InterruptibleTracker::new()? },
    });
    let id = loop {
        let id = inner.next_id.fetch_add(1, Ordering::AcqRel);
        match inner.descriptors.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(fd_data);
                break id;
            }
            Entry::Occupied(_entry) => {
                continue;
            }
        }
    };
    Ok(id)
}

fn handle_fd_unregister(inner: &DaemonOwnerInner, unregister: Unregister) -> Result<(), io::Error> {
    let (_id, fd_data) = inner
        .descriptors
        .remove(&unregister.id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file descriptor not found"))?;
    // Remove the notifier.
    fd_data.tracker.stop();
    // Wait until other threads decreased refcount to 1. When that happens, the fd is closed and all locks should release when fd_unregister ends, even if threads may continue running.
    Waiter {
        sleep_duration: Duration::from_millis(1),
    }
    .snooze_wait(|| Arc::strong_count(&fd_data) == 1);
    Ok(())
}

fn handle_lock_command(
    inner: Arc<DaemonOwnerInner>,
    lock_command: LockCommand,
    mut lock_reply: ReplyLockCommand,
) {
    let fd_data = match inner
        .descriptors
        .get(&lock_command.id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file descriptor not found"))
    {
        Ok(fd_data) => fd_data,
        Err(e) => {
            lock_reply.reply(io_cvt(Err(e)));
            return;
        }
    }
    .clone();
    match lock_command.action {
        LockAction::GetLockStatus => {
            let mut raw: libc::flock = lock_command.params.into();
            match nix::fcntl::fcntl(&fd_data.descriptor, FcntlArg::F_GETLK(&mut raw))
                .map_err(|e| e.into())
                .and_then(|_| raw.try_into().map(ReplyLockCommandData::Done))
            {
                Ok(lock_params) => {
                    lock_reply.reply(io_cvt(Ok(lock_params)));
                }
                Err(e) => {
                    lock_reply.reply(io_cvt(Err(e)));
                }
            };
        }
        LockAction::SetLockNonBlocking => {
            let raw: libc::flock = lock_command.params.into();
            match nix::fcntl::fcntl(&fd_data.descriptor, FcntlArg::F_SETLK(&raw))
                .map_err(|e| e.into())
                .and_then(|_| raw.try_into().map(ReplyLockCommandData::Done))
            {
                Ok(lock_params) => {
                    lock_reply.reply(io_cvt(Ok(lock_params)));
                }
                Err(e) => {
                    lock_reply.reply(io_cvt(Err(e)));
                }
            };
        }
        LockAction::SetLockBlocking => {
            let _ = fd_data
                .tracker
                .spawn({
                    let owner = inner.clone();
                    let fd_data = fd_data.clone();
                    move |handle| {
                        set_lock_blocking(
                            &owner,
                            handle,
                            &fd_data,
                            lock_command.params,
                            lock_reply,
                        );
                    }
                })
                .inspect_err(|e| log::error!("failed to spawn locking thread: {}", e));
        }
    }
}

fn set_lock_blocking(
    owner: &DaemonOwnerInner,
    interrupt: InterruptibleHandle,
    fd_data: &FdData,
    lock: LockParams,
    mut reply: ReplyLockCommand,
) {
    let raw: libc::flock = lock.into();
    match nix::fcntl::fcntl(&fd_data.descriptor, FcntlArg::F_SETLK(&raw)).map_err(io::Error::from) {
        Ok(_) => {
            reply.reply(io_cvt(Ok(ReplyLockCommandData::Done(lock))));
        }
        Err(e) if e.raw_os_error() == Some(libc::EAGAIN) => {
            // Pending operation, will be completed asynchronously - need to send the thread ID to the client first so that it can interrupt.
            let id = handle_interrupt_add(owner, interrupt);
            reply.reply(io_cvt(Ok(ReplyLockCommandData::Pending(id))));
        }
        Err(e) => {
            reply.reply(io_cvt(Err(e)));
        }
    };
    let res = {
        let _restore = match helpers::unblock_signal(Signal::SIGUSR1) {
            Ok(restore) => restore,
            Err(e) => {
                reply.reply(io_cvt(Err(e)));
                return;
            }
        };
        nix::fcntl::fcntl(&fd_data.descriptor, FcntlArg::F_SETLKW(&raw)).map_err(io::Error::from)
    };
    match res {
        Ok(_) => {
            reply.reply(io_cvt(Ok(ReplyLockCommandData::Done(lock))));
        }
        Err(e) => {
            reply.reply(io_cvt(Err(e)));
        }
    };
}

#[derive(Debug)]
struct OwnerInner {
    communication: Mutex<OwnedFd>,
}

/// A medium-level wrapper over owner processes
///
/// # Notes
/// - Please use the [`crate::LockManager`] type directly.
#[derive(Debug)]
pub struct Owner {
    inner: Arc<OwnerInner>,
}

#[derive(Debug)]
pub(crate) struct RawLockable {
    inner: Arc<OwnerInner>,
    id: u64,
}

/// A RAII handle that represents a resource to lock.
/// Dropping the handle causes all locks owned by the handle to be removed, and interrupts all pending locks.
#[derive(Debug)]
pub struct Lockable {
    inner: Arc<RawLockable>,
}

impl Drop for Lockable {
    fn drop(&mut self) {
        let _ = messages::send_request(
            self.inner.inner.communication.lock(),
            Unregister { id: self.inner.id },
        )
        .map_err(|e| log::error!("error unregistering lock with id {}: {}", self.inner.id, e));
    }
}

struct Interruptable<'a> {
    lockable: &'a RawLockable,
    id: u64,
}

impl<'a> Interruptable<'a> {
    fn new(lockable: &'a RawLockable, id: u64) -> Self {
        Self { lockable, id }
    }

    fn id(&self) -> u64 {
        self.id
    }
}

impl<'a> Drop for Interruptable<'a> {
    fn drop(&mut self) {
        let _ = messages::send_request(
            self.lockable.inner.communication.lock(),
            RemoveInterrupt { id: self.id },
        )
        .inspect_err(|e| log::error!("failed to remove interrupt ID {}: {}", self.id, e));
    }
}

struct Invoker<F>
where
    F: FnOnce(Result<LockParams, io::Error>) + Send + 'static,
{
    inner: Option<F>,
}

impl<F> Invoker<F>
where
    F: FnOnce(Result<LockParams, io::Error>) + Send + 'static,
{
    fn new(item: F) -> Self {
        Self { inner: Some(item) }
    }

    fn invoke(&mut self, r: Result<LockParams, io::Error>) {
        if let Some(f) = self.inner.take() {
            f(r)
        }
    }
}

impl<F> Drop for Invoker<F>
where
    F: FnOnce(Result<LockParams, io::Error>) + Send + 'static,
{
    fn drop(&mut self) {
        self.invoke(Err(io::Error::other("thread panicked")));
    }
}

impl RawLockable {
    fn get_lock(&self, params: LockParams) -> Result<LockParams, io::Error> {
        let _restore = helpers::block_all_signals()?;
        let res = messages::send_request(
            self.inner.communication.lock(),
            LockCommand {
                id: self.id,
                action: LockAction::GetLockStatus,
                params,
            },
        )??;
        match res {
            ReplyLockCommandData::Done(params) => Ok(params),
            ReplyLockCommandData::Pending(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "getlk should not return a pending operation",
            )),
        }
    }

    fn set_lock_nonblocking(&self, params: LockParams) -> Result<LockParams, io::Error> {
        let _restore = helpers::block_all_signals()?;
        let res = messages::send_request(
            self.inner.communication.lock(),
            LockCommand {
                id: self.id,
                action: LockAction::SetLockNonBlocking,
                params,
            },
        )??;
        match res {
            ReplyLockCommandData::Done(params) => Ok(params),
            ReplyLockCommandData::Pending(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "setlk should not return a pending operation",
            )),
        }
    }

    fn set_lock_blocking(&self, params: LockParams) -> Result<LockParams, io::Error> {
        let _restore = helpers::block_all_signals()?;
        let mut iterator = messages::send_request_iter(
            self.inner.communication.lock(),
            LockCommand {
                id: self.id,
                action: LockAction::SetLockBlocking,
                params,
            },
        )?;
        let interruptable = match iterator.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "set_lock_blocking: connection is closed",
            )
        })??? {
            ReplyLockCommandData::Done(params) => return Ok(params),
            ReplyLockCommandData::Pending(id) => Interruptable::new(self, id),
        };
        loop {
            let next_item = {
                // The interruption only has to deal within this syscall.
                let _restore = helpers::unblock_signal(Signal::SIGUSR1)?;
                iterator.next()
            };
            let _res = match next_item.ok_or_else(|| {
                log::debug!("no next item");
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "set_lock_blocking: connection is closed",
                )
            })? {
                Ok(Ok(ReplyLockCommandData::Done(params))) => return Ok(params),
                Ok(Ok(ReplyLockCommandData::Pending(_id))) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "setlkw should not return a pending operation twice",
                    ));
                }
                // Outer interrupted signal. We must send the thread ID to the lock server, then continue waiting.
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    messages::send_request(
                        self.inner.communication.lock(),
                        Interrupt {
                            id: interruptable.id(),
                        },
                    )??;
                    continue;
                }
                // Flatten the error. The inner interrupted signal should be dealt with here.
                Ok(Err(e)) => Err(e),
                Err(e) => Err(e),
            }?;
        }
    }
}

impl Lockable {
    /// Check the lock status with the specified parameters.
    pub fn get_lock(&self, params: LockParams) -> Result<LockParams, io::Error> {
        self.inner.get_lock(params)
    }

    /// Attempt to set a lock on the target file without locking.
    pub fn set_lock_nonblocking(&self, params: LockParams) -> Result<LockParams, io::Error> {
        self.inner.set_lock_nonblocking(params)
    }

    /// Wait and set a blocking lock on the file.
    ///
    /// To stop the locking, the signal `SIGUSR1` can be sent to the calling thread.
    pub fn set_lock_blocking(&self, params: LockParams) -> Result<LockParams, io::Error> {
        self.inner.set_lock_blocking(params)
    }

    /// Asynchronously set a blocking lock and return an interruptable handle.
    /// A callback will be invoked to inform the operation status.
    ///
    /// The pre-callback is used to register the interrupt handle in a FUSE unique ID to interrupt dispatcher map. Using the callback is preferred over using the returned handle, as this prevents the race condition case where the operation is finished, then the interrupt handle is added, causing a resource leak.
    ///
    /// The post-callback is used to notify when a request ended, and should reply to original requests in FUSE types. It will always be invoked - whether the pre-callback failed or the operation failed or panicked. The user should generally save the request's unique ID here.
    ///
    /// To stop the locking, [`InterruptibleHandle::stop`] can be called on the handle. This causes the post-callback to emit [`io::ErrorKind::Interrupted`].
    pub fn set_lock_blocking_detached(
        &self,
        params: LockParams,
        tracker: &InterruptibleTracker,
        pre: impl FnOnce(InterruptibleHandle) -> Result<(), io::Error> + Send + 'static,
        done: impl FnOnce(Result<LockParams, io::Error>) + Send + 'static,
    ) -> Result<InterruptibleHandle, io::Error> {
        let cloned_raw = self.inner.clone();
        let mut invoker = Invoker::new(done);
        tracker.spawn(move |handle| {
            match pre(handle) {
                Ok(_) => {}
                Err(e) => {
                    invoker.invoke(Err(e));
                    return;
                }
            };
            let res = cloned_raw.set_lock_blocking(params);
            invoker.invoke(res)
        })
    }
}

impl Owner {
    /// Create a new owner instance using the communication FD supplied.
    pub(crate) fn new(communication: OwnedFd) -> Self {
        Self {
            inner: Arc::new(OwnerInner {
                communication: Mutex::new(communication),
            }),
        }
    }

    /// Register a file for locking.
    ///
    /// # Notes
    /// - This method does not manage owner regeneration in case the underlying owner process exits.
    /// - The method [`LockManager::register`](crate::LockManager::register) should be used instead.
    pub fn register(&self, fd: impl AsFd) -> Result<Lockable, io::Error> {
        let _restore = helpers::block_all_signals()?;
        let cloned = fd.as_fd().try_clone_to_owned()?;
        let id = messages::send_request(self.inner.communication.lock(), Register { fd: cloned })??;
        Ok(Lockable {
            inner: Arc::new(RawLockable {
                inner: self.inner.clone(),
                id,
            }),
        })
    }

    /// Pings the client. If the client disconnects, [`io::ErrorKind::BrokenPipe`] is returned.
    pub fn ping(&self) -> Result<(), io::Error> {
        let _restore = helpers::block_all_signals()?;
        messages::send_request(self.inner.communication.lock(), Ping {})??;
        Ok(())
    }
}
