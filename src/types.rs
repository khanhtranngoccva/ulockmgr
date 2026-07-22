//! Wrapper types for operating system locking
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(i32)]
pub enum LockAction {
    SetLockNonBlocking = libc::F_SETLK,
    SetLockBlocking = libc::F_SETLKW,
    GetLockStatus = libc::F_GETLK,
}

impl TryFrom<i32> for LockAction {
    type Error = std::io::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            libc::F_SETLK => Ok(LockAction::SetLockNonBlocking),
            libc::F_SETLKW => Ok(LockAction::SetLockBlocking),
            libc::F_GETLK => Ok(LockAction::GetLockStatus),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid lock command",
            )),
        }
    }
}

impl From<LockAction> for i32 {
    fn from(value: LockAction) -> i32 {
        match value {
            LockAction::SetLockNonBlocking => libc::F_SETLK,
            LockAction::SetLockBlocking => libc::F_SETLKW,
            LockAction::GetLockStatus => libc::F_GETLK,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(i32)]
pub enum LockType {
    Read = libc::F_RDLCK,
    Write = libc::F_WRLCK,
    Unlock = libc::F_UNLCK,
}

impl TryFrom<i32> for LockType {
    type Error = std::io::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            libc::F_RDLCK => Ok(LockType::Read),
            libc::F_WRLCK => Ok(LockType::Write),
            libc::F_UNLCK => Ok(LockType::Unlock),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid lock type",
            )),
        }
    }
}

impl From<LockType> for i16 {
    fn from(value: LockType) -> i16 {
        match value {
            LockType::Read => libc::F_RDLCK as i16,
            LockType::Write => libc::F_WRLCK as i16,
            LockType::Unlock => libc::F_UNLCK as i16,
        }
    }
}

impl From<LockType> for i32 {
    fn from(value: LockType) -> i32 {
        i16::from(value) as i32
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(i32)]
pub enum LockWhence {
    Start = libc::SEEK_SET,
    Current = libc::SEEK_CUR,
    End = libc::SEEK_END,
}

impl TryFrom<i32> for LockWhence {
    type Error = std::io::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            libc::SEEK_SET => Ok(LockWhence::Start),
            libc::SEEK_CUR => Ok(LockWhence::Current),
            libc::SEEK_END => Ok(LockWhence::End),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid lock whence",
            )),
        }
    }
}

impl From<LockWhence> for i16 {
    fn from(value: LockWhence) -> i16 {
        match value {
            LockWhence::Start => libc::SEEK_SET as i16,
            LockWhence::Current => libc::SEEK_CUR as i16,
            LockWhence::End => libc::SEEK_END as i16,
        }
    }
}

impl From<LockWhence> for i32 {
    fn from(value: LockWhence) -> i32 {
        i16::from(value) as i32
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LockParams {
    pub lock_type: LockType,
    pub whence: LockWhence,
    pub start: libc::off_t,
    pub len: libc::off_t,
    pub pid: libc::pid_t,
}

impl TryFrom<libc::flock> for LockParams {
    type Error = std::io::Error;

    fn try_from(value: libc::flock) -> Result<Self, Self::Error> {
        Ok(LockParams {
            lock_type: LockType::try_from(value.l_type as i32)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            whence: LockWhence::try_from(value.l_whence as i32)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            start: value.l_start,
            len: value.l_len,
            pid: value.l_pid,
        })
    }
}

impl From<LockParams> for libc::flock {
    fn from(value: LockParams) -> libc::flock {
        libc::flock {
            l_type: value.lock_type.into(),
            l_whence: value.whence.into(),
            l_start: value.start,
            l_len: value.len,
            l_pid: value.pid,
        }
    }
}
