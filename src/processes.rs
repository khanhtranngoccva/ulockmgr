use std::{
    io,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
        unix::process::CommandExt,
    },
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use rustix::{
    io::FdFlags,
    net::{AddressFamily, SocketFlags, SocketType},
    process::WaitOptions,
    thread::Pid,
};

/// Clear `FD_CLOEXEC` after fork before exec.
/// This is needed to pass the file descriptor to a child process without risking descriptor leak.
///
/// # SAFETY
/// - Must ensure that the file descriptor is not closed before the command is executed.
unsafe fn clear_cloexec_in_pre_exec(command: &mut Command, fd: BorrowedFd<'_>) {
    let fd = fd.as_raw_fd();
    unsafe {
        command.pre_exec(move || {
            let fd = BorrowedFd::borrow_raw(fd);
            let flags = rustix::io::fcntl_getfd(fd)?;
            let flags = flags & !FdFlags::CLOEXEC;
            rustix::io::fcntl_setfd(fd, flags)?;
            Ok(())
        })
    };
}

fn default_stdio_type() -> Stdio {
    {
        #[cfg(debug_assertions)]
        {
            Stdio::inherit()
        }
        #[cfg(not(debug_assertions))]
        {
            Stdio::piped()
        }
    }
}

/// Spawn an owner process.
pub(crate) fn spawn_owner_process(ttl: Duration) -> Result<OwnedFd, io::Error> {
    let (parent_fd, child_fd) = rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::CLOEXEC,
        None,
    )?;
    let mut command = Command::new("/proc/self/exe");
    command.arg("owner");
    command.arg(child_fd.as_raw_fd().to_string());
    command.args([String::from("--ttl-secs"), ttl.as_secs().to_string()]);
    command.args([String::from("--ttl-nsecs"), ttl.subsec_nanos().to_string()]);
    command.stdout(default_stdio_type());
    command.stderr(default_stdio_type());
    unsafe { clear_cloexec_in_pre_exec(&mut command, child_fd.as_fd()) };
    let process = command.spawn()?;
    // Do not allow the process to become a zombie.
    rustix::process::waitpid(Some(Pid::from_child(&process)), WaitOptions::NOHANG)?;
    drop(child_fd);
    Ok(parent_fd)
}

/// Spawn a coordinator process.
pub(crate) fn spawn_coordinator_process(path: impl AsRef<Path>) -> Result<OwnedFd, io::Error> {
    let (parent_fd, child_fd) = rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::CLOEXEC,
        None,
    )?;
    let mut command = Command::new(path.as_ref());
    command.arg("coordinator");
    command.arg(child_fd.as_raw_fd().to_string());
    command.stdout(default_stdio_type());
    command.stderr(default_stdio_type());
    unsafe { clear_cloexec_in_pre_exec(&mut command, child_fd.as_fd()) };
    let process = command.spawn()?;
    // Do not allow the process to become a zombie.
    rustix::process::waitpid(Some(Pid::from_child(&process)), WaitOptions::NOHANG)?;
    drop(child_fd);
    Ok(parent_fd)
}
