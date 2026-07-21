use nix::{fcntl::FcntlArg, sys::signal::Signal};
use std::{
    fs::OpenOptions,
    io::{self, Write},
    os::unix::thread::JoinHandleExt,
    time::Duration,
};
use ulockmgr::{
    LockManager,
    tracker::InterruptibleTracker,
    types::{LockParams, LockType, LockWhence},
};

mod fixtures;

#[test_log::test(rstest::rstest)]
fn test_should_interrupt_lock_blocking_on_write_conflict(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
    #[values(LockType::Read, LockType::Write)] lock_type: LockType,
) {
    let temp_directory = tempfile::TempDir::new().expect("should create temp directory");
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(temp_directory.path().join("test_lock_file"))
        .expect("should create test file");
    file.write_all(b"Hello world!")
        .expect("should write to file");
    let lockable = lock_manager
        .register(1, &file)
        .expect("should register a file for locking");
    nix::fcntl::fcntl(
        &file,
        FcntlArg::F_SETLKW(
            &LockParams {
                lock_type: LockType::Write,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should lock for writing on main process");
    let waiting_thread = std::thread::spawn(move || {
        assert_eq!(
            lockable
                .set_lock_blocking(LockParams {
                    lock_type,
                    whence: LockWhence::Start,
                    pid: 0,
                    start: 0,
                    len: 0,
                })
                .expect_err("should wait and perform locking")
                .kind(),
            io::ErrorKind::Interrupted,
            "locking should be interrupted with SIGUSR1 on demand"
        );
    });
    std::thread::sleep(Duration::from_millis(5));
    while !waiting_thread.is_finished() {
        nix::sys::pthread::pthread_kill(waiting_thread.as_pthread_t(), Signal::SIGUSR1)
            .expect("should interrupt the thread");
        std::thread::sleep(Duration::from_millis(5));
    }
    waiting_thread
        .join()
        .expect("waiting thread test should succeed");
}

#[test_log::test(rstest::rstest)]
fn test_should_interrupt_lock_blocking_on_write_conflict_detached(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
    #[values(LockType::Read, LockType::Write)] lock_type: LockType,
) {
    use std::sync::Arc;

    let temp_directory = tempfile::TempDir::new().expect("should create temp directory");
    let tracker = unsafe { InterruptibleTracker::new().expect("should create tracker") };
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(temp_directory.path().join("test_lock_file"))
        .expect("should create test file");
    file.write_all(b"Hello world!")
        .expect("should write to file");
    let lockable = Arc::new(
        lock_manager
            .register(1, &file)
            .expect("should register a file for locking"),
    );
    nix::fcntl::fcntl(
        &file,
        FcntlArg::F_SETLKW(
            &LockParams {
                lock_type: LockType::Write,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should lock for writing on main process");
    let (res_tx, res_rx) = crossbeam_channel::bounded(1);
    let handle = lockable
        .set_lock_blocking_detached(
            LockParams {
                lock_type,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            },
            &tracker,
            move |_handle| Ok(()),
            move |result| {
                let _ = res_tx.send(result);
            },
        )
        .expect("should spawn detached lock task");
    handle.stop();
    assert_eq!(
        res_rx
            .recv()
            .expect("should receive message")
            .expect_err("locking should return an error")
            .kind(),
        io::ErrorKind::Interrupted,
        "should be interrupted by handle"
    )
}
