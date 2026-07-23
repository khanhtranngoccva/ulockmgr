use std::{ffi::c_void, io, marker::PhantomData, mem::MaybeUninit, ptr};

use nix::sys::signal::{SigSet, Signal};

/// Converts Linux return integers to Result using the *-1 means error is in `errno`*  convention.
/// Non-error values are `Ok`-wrapped.
pub(crate) fn cvt<T: Into<i64> + Copy>(t: T) -> io::Result<T> {
    if t.into() == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(t)
    }
}

pub(crate) struct SigSetGuard {
    old: SigSet,
    unsend: PhantomData<*mut c_void>,
}

impl SigSetGuard {
    pub(crate) fn new(sigset: SigSet) -> Self {
        Self {
            old: sigset,
            unsend: PhantomData,
        }
    }
}

impl Drop for SigSetGuard {
    fn drop(&mut self) {
        let _ = nix::sys::signal::pthread_sigmask(
            nix::sys::signal::SigmaskHow::SIG_SETMASK,
            Some(&self.old),
            None,
        );
    }
}

pub(crate) fn unblock_signal(sig: Signal) -> io::Result<SigSetGuard> {
    let mut sigset = SigSet::empty();
    sigset.add(sig);
    let mut old_set = SigSet::empty();
    nix::sys::signal::pthread_sigmask(
        nix::sys::signal::SigmaskHow::SIG_UNBLOCK,
        Some(&sigset),
        Some(&mut old_set),
    )?;
    Ok(SigSetGuard::new(old_set))
}

pub(crate) fn block_all_signals() -> io::Result<SigSetGuard> {
    let sigset = SigSet::all();
    let mut old_set = SigSet::empty();
    nix::sys::signal::pthread_sigmask(
        nix::sys::signal::SigmaskHow::SIG_BLOCK,
        Some(&sigset),
        Some(&mut old_set),
    )?;
    Ok(SigSetGuard::new(old_set))
}

pub(crate) unsafe fn ensure_sigaction_exists() -> Result<(), io::Error> {
    let mut action = MaybeUninit::<libc::sigaction>::uninit();
    let _ = cvt(unsafe { libc::sigaction(libc::SIGUSR1, ptr::null_mut(), action.as_mut_ptr()) })?;
    let action = unsafe { action.assume_init() };
    match action.sa_sigaction {
        libc::SIG_DFL | libc::SIG_IGN => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the process must be able to handle signals",
            ));
        }
        // Either a 1-arg or 3-arg function, we can ignore it
        _ => {}
    }
    Ok(())
}
