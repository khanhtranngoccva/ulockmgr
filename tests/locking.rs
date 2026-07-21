use nix::fcntl::FcntlArg;
use std::{
    fs::OpenOptions,
    io::{self, Write},
    time::Duration,
};
use ulockmgr::{
    LockManager,
    types::{LockParams, LockType, LockWhence},
};

mod fixtures;

#[test_log::test(rstest::rstest)]
fn test_should_lock_file(#[from(fixtures::lock_manager)] lock_manager: LockManager) {
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
    lockable
        .set_lock_blocking(LockParams {
            lock_type: LockType::Read,
            whence: LockWhence::Start,
            pid: 0,
            start: 0,
            len: 0,
        })
        .expect("should lock file for read");
    lockable
        .set_lock_blocking(LockParams {
            lock_type: LockType::Write,
            whence: LockWhence::Start,
            pid: 0,
            start: 0,
            len: 0,
        })
        .expect("should lock file for write");
    lockable
        .set_lock_blocking(LockParams {
            lock_type: LockType::Unlock,
            whence: LockWhence::Start,
            pid: 0,
            start: 0,
            len: 0,
        })
        .expect("should unlock file");
}

#[test_log::test(rstest::rstest)]
fn test_should_handle_lock_nonblocking_on_write_conflict(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
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
    .expect("should lock on main process");
    assert!(
        lockable
            .set_lock_nonblocking(LockParams {
                lock_type: LockType::Write,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect_err("should not lock on file because another process is occupying it")
            .kind()
            == io::ErrorKind::WouldBlock,
        "write -> write lock conflicts should return WouldBlock message"
    );
    assert!(
        lockable
            .set_lock_nonblocking(LockParams {
                lock_type: LockType::Read,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect_err("should not lock on file because another process is occupying it")
            .kind()
            == io::ErrorKind::WouldBlock,
        "write -> read lock conflicts should return WouldBlock message"
    );
}

#[test_log::test(rstest::rstest)]
fn test_should_handle_lock_nonblocking_on_read_conflict(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
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
                lock_type: LockType::Read,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should lock on main process");
    assert!(
        lockable
            .set_lock_nonblocking(LockParams {
                lock_type: LockType::Write,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect_err("should not lock on file because another process is occupying it")
            .kind()
            == io::ErrorKind::WouldBlock,
        "read -> write lock conflicts should return WouldBlock message"
    );
    lockable
        .set_lock_nonblocking(LockParams {
            lock_type: LockType::Read,
            whence: LockWhence::Start,
            pid: 0,
            start: 0,
            len: 0,
        })
        .expect("read -> read: should lock on file because the lock on the other process is a read lock");
}

#[test_log::test(rstest::rstest)]
fn test_should_handle_lock_upgrade_nonblocking_on_read_conflict(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
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
                lock_type: LockType::Read,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should lock for reading on main process");
    let waiting_thread = std::thread::spawn(move || {
        lockable
            .set_lock_nonblocking(LockParams {
                lock_type: LockType::Read,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect("should perform read locking immediately");
        assert_eq!(
            lockable
                .set_lock_nonblocking(LockParams {
                    lock_type: LockType::Write,
                    whence: LockWhence::Start,
                    pid: 0,
                    start: 0,
                    len: 0,
                })
                .expect_err("should wait and perform write locking")
                .kind(),
            io::ErrorKind::WouldBlock,
            "upgrading from read lock to write lock should block when there is another process occupying the read lock"
        );
    });
    waiting_thread
        .join()
        .expect("waiting thread should succeed");
}

#[test_log::test(rstest::rstest)]
fn test_should_handle_lock_blocking_on_write_conflict(
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
        lockable
            .set_lock_blocking(LockParams {
                lock_type,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect("should wait and perform locking");
    });
    std::thread::sleep(Duration::from_millis(5));
    nix::fcntl::fcntl(
        &file,
        FcntlArg::F_SETLKW(
            &LockParams {
                lock_type: LockType::Unlock,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should unlock on main process");
    waiting_thread
        .join()
        .expect("waiting thread should succeed");
}

#[test_log::test(rstest::rstest)]
fn test_should_handle_lock_blocking_on_read_conflict(
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
                lock_type: LockType::Read,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should lock for reading on main process");
    let waiting_thread = std::thread::spawn(move || {
        lockable
            .set_lock_blocking(LockParams {
                lock_type,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect("should wait and perform locking");
    });
    std::thread::sleep(Duration::from_millis(5));
    nix::fcntl::fcntl(
        &file,
        FcntlArg::F_SETLKW(
            &LockParams {
                lock_type: LockType::Unlock,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should unlock on main process");
    waiting_thread
        .join()
        .expect("waiting thread should succeed");
}

#[test_log::test(rstest::rstest)]
fn test_should_handle_lock_upgrade_blocking_on_read_conflict(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
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
                lock_type: LockType::Read,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should lock for reading on main process");
    let waiting_thread = std::thread::spawn(move || {
        lockable
            .set_lock_nonblocking(LockParams {
                lock_type: LockType::Read,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect("should perform read locking immediately");
        lockable
            .set_lock_blocking(LockParams {
                lock_type: LockType::Write,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            })
            .expect("should wait and perform write locking");
    });
    std::thread::sleep(Duration::from_millis(5));
    nix::fcntl::fcntl(
        &file,
        FcntlArg::F_SETLKW(
            &LockParams {
                lock_type: LockType::Unlock,
                whence: LockWhence::Start,
                pid: 0,
                start: 0,
                len: 0,
            }
            .into(),
        ),
    )
    .expect("should unlock on main process");
    waiting_thread
        .join()
        .expect("waiting thread should succeed");
}
