use std::{
    io::{self},
    sync::Arc,
    time::Duration,
};

use ulockmgr::LockManager;
mod fixtures;

#[test_log::test(rstest::rstest)]
fn test_should_create_manager(#[from(fixtures::sigusr1)] _sigaction: &()) -> Result<(), anyhow::Error> {
    let path = env!("CARGO_BIN_EXE_ulockmgr");
    let _lock_manager = unsafe { LockManager::from_exe(path) }?;
    Ok(())
}

#[test_log::test(rstest::rstest)]
fn test_should_create_owner(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
) -> Result<(), anyhow::Error> {
    let owner_id = 1;
    let owner = lock_manager
        .get_or_create_owner(owner_id, None)
        .expect("failed to create owner");
    owner.ping().expect("should be able to ping");
    Ok(())
}

#[test_log::test(rstest::rstest)]
fn test_should_create_zero_lived_owner(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
) -> Result<(), anyhow::Error> {
    let owner_id = 1;

    let owner = lock_manager
        .get_or_create_owner(owner_id, Some(Duration::from_secs(0)))
        .expect("failed to create owner");
    const ATTEMPTS: usize = 3;
    for attempt in 1..=ATTEMPTS {
        match owner.ping() {
            Ok(_) if attempt == ATTEMPTS => {}
            Ok(_) => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => return Ok(()),
            r => r.expect("failed to ping owner"),
        }
    }
    Ok(())
}

#[test_log::test(rstest::rstest)]
fn test_should_regenerate_owner(
    #[from(fixtures::lock_manager)] lock_manager: LockManager,
) -> Result<(), anyhow::Error> {
    let owner_id = 1;

    let owner = lock_manager
        .get_or_create_owner(owner_id, Some(Duration::from_secs(0)))
        .expect("failed to create owner");
    const ATTEMPTS: usize = 3;
    for attempt in 1..=ATTEMPTS {
        match owner.ping() {
            Ok(_) if attempt == ATTEMPTS => {}
            Ok(_) => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => return Ok(()),
            r => r.expect("failed to ping owner"),
        }
    }
    let new_owner = lock_manager
        .get_or_create_owner(owner_id, None)
        .expect("should regenerate owner");
    assert!(
        !Arc::ptr_eq(&new_owner, &owner),
        "pointers for owner structures must be different"
    );
    Ok(())
}
