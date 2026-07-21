use std::path::PathBuf;

use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal};
use ulockmgr::LockManager;

extern "C" fn sig_handler(_signal: i32) {}

#[test_log::test(rstest::fixture)]
#[once]
pub fn sigusr1() -> () {
    let sigaction = SigAction::new(
        SigHandler::Handler(sig_handler),
        SaFlags::empty(),
        SigSet::empty(),
    );
    unsafe { nix::sys::signal::sigaction(Signal::SIGUSR1, &sigaction) }
        .expect("should set sigaction");
}

#[test_log::test(rstest::fixture)]
pub fn server_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_ulockmgr"))
}

#[test_log::test(rstest::fixture)]
pub fn lock_manager(#[from(sigusr1)] _signal_handling: &(), server_path: PathBuf) -> LockManager {
    unsafe { LockManager::from_exe(server_path) }.expect("failed to build lock manager")
}
