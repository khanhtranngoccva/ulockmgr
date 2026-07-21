use anyhow::Context;
use clap::{Parser, Subcommand};
use nix::sys::signal::{SaFlags, SigAction, SigSet, Signal};
use std::{
    os::fd::{BorrowedFd, RawFd},
    time::Duration,
};
use ulockmgr::{server::hydrate_forked_fd, server::DaemonOwner, server::OwnerSpawner};

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Operation mode for the server.
    #[command(subcommand)]
    mode: OperationMode,
}

#[derive(Debug, Clone, Subcommand)]
enum OperationMode {
    Coordinator {
        /// The raw communication file descriptor to hydrate.
        fd: RawFd,
    },
    Owner {
        /// The raw communication file descriptor to hydrate.
        fd: RawFd,
        /// Time-to-live for the owner process. It will try to exit if no requests arrive within this timeframe.
        #[arg(long, default_value = "5")]
        ttl_secs: u64,
        #[arg(long, default_value = "0")]
        ttl_nsecs: u32,
    },
}

extern "C" fn sigusr1_handler(_signum: i32) {}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    env_logger::init();
    // Absorbs SIGUSR1 signals.
    unsafe {
        nix::sys::signal::sigaction(
            Signal::SIGUSR1,
            &SigAction::new(
                nix::sys::signal::SigHandler::Handler(sigusr1_handler),
                SaFlags::empty(),
                SigSet::empty(),
            ),
        )
    }
    .context("failed to set SIGUSR1 handler")?;
    match args.mode {
        OperationMode::Coordinator { fd } => {
            let fd = unsafe { hydrate_forked_fd(BorrowedFd::borrow_raw(fd)) }?;
            let owner_spawner = OwnerSpawner::new(fd);
            owner_spawner.run()?;
        }
        OperationMode::Owner {
            fd,
            ttl_secs,
            ttl_nsecs,
        } => {
            let fd = unsafe { hydrate_forked_fd(BorrowedFd::borrow_raw(fd)) }?;
            let owner = DaemonOwner::new(fd, Duration::new(ttl_secs, ttl_nsecs));
            owner.start()?;
        }
    }
    Ok(())
}
