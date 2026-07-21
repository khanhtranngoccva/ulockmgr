use rustix::io::FdFlags;
use std::{
    io,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
};

/// Hydrates a fd that is borrowed by forking a process, and add the CLOEXEC flag.
///
/// # SAFETY
/// - To avoid race conditions, hydrate all file descriptors before doing anything else.
pub unsafe fn hydrate_forked_fd(fd: BorrowedFd<'static>) -> Result<OwnedFd, io::Error> {
    rustix::io::fcntl_setfd(fd, FdFlags::CLOEXEC)?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd.as_raw_fd()) })
}
