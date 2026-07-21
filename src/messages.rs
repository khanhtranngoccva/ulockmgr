use rustix::{
    cmsg_space,
    io::Errno,
    net::{
        AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
        SendFlags, SocketFlags, SocketType,
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fmt::Debug,
    io::{self, IoSlice, IoSliceMut},
    marker::PhantomData,
    mem::MaybeUninit,
    ops::DerefMut,
    os::fd::{AsFd, OwnedFd},
    time::Duration,
};

use crate::types::{LockAction, LockParams};

type FetchedReplyResult<T> = Result<FetchedReply<T>, io::Error>;
type FetchedReply<T> = Result<T, io::Error>;

/// Trait to be implemented by the message types that can be replied to. It should allow creating a reply object for the message.
pub(crate) trait Replyable: Debug {
    type Reply: Reply;

    fn create_reply(fd: OwnedFd) -> Self::Reply;
}

/// Trait to be implemented by the message types that can be serialized.
pub(crate) trait Serializable: Debug {
    type Payload: Serialize + DeserializeOwned + Debug;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>);

    fn deserialize(payload: Self::Payload, fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized;
}

/// Trait to be implemented by reply types.
pub(crate) trait Reply: Debug {
    /// The required payload for the Ok reply.
    /// The user may send an Errno as an Err, which will be converted as a raw error code.
    type Type: Serialize + DeserializeOwned + Debug;

    fn raw_reply(&mut self) -> &mut RawReply;

    fn reply(&mut self, message: Result<Self::Type, Errno>)
    where
        Self: Sized,
    {
        self.reply_with_fds(message, vec![]);
    }

    fn reply_with_fds(&mut self, message: Result<Self::Type, Errno>, fds: Vec<OwnedFd>)
    where
        Self: Sized,
    {
        let transformed = message.map_err(|e| e.raw_os_error());
        let data = match bincode::serialize(&transformed) {
            Ok(data) => data,
            Err(_) => return,
        };
        self.raw_reply().reply(&data, fds);
    }
}

/// Structure of the raw message received from the socket.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(C)]
pub(crate) enum RawMessage {
    /// Checks the owner liveness.
    Ping {},
    /// Interrupts one thread of the owner process using the specified ID.
    Interrupt { id: u64 },
    /// Remove the interrupt handle with the specified ID from the tracker.
    RemoveInterrupt { id: u64 },
    /// Registers a new file descriptor for the owner process.
    /// The file descriptor is received as the second SCM_RIGHTS ancillary item.
    Register {},
    /// Unregisters a file descriptor from the owner process.
    Unregister { id: u64 },
    /// Performs a lock command.
    LockCommand {
        id: u64,
        action: LockAction,
        params: LockParams,
    },
}

/// Structure encoding the message to be sent from the filesystem daemon and received by the server owner process.
#[derive(Debug)]
pub enum Message {
    Ping(Ping),
    Interrupt(Interrupt),
    RemoveInterrupt(RemoveInterrupt),
    Register(Register),
    Unregister(Unregister),
    LockCommand(LockCommand),
}

impl Serializable for Message {
    type Payload = RawMessage;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        match self {
            Message::Ping(ping) => ping.serialize(),
            Message::Interrupt(interrupt) => interrupt.serialize(),
            Message::Register(register) => register.serialize(),
            Message::Unregister(unregister) => unregister.serialize(),
            Message::LockCommand(lock_command) => lock_command.serialize(),
            Message::RemoveInterrupt(remove_interrupt) => remove_interrupt.serialize(),
        }
    }

    fn deserialize(payload: Self::Payload, fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        Ok(match payload {
            RawMessage::Ping {} => Message::Ping(Ping::deserialize(payload, fds)?),
            RawMessage::Interrupt { .. } => {
                Message::Interrupt(Interrupt::deserialize(payload, fds)?)
            }
            RawMessage::Register {} => Message::Register(Register::deserialize(payload, fds)?),
            RawMessage::Unregister { .. } => {
                Message::Unregister(Unregister::deserialize(payload, fds)?)
            }
            RawMessage::LockCommand { .. } => {
                Message::LockCommand(LockCommand::deserialize(payload, fds)?)
            }
            RawMessage::RemoveInterrupt { .. } => {
                Message::RemoveInterrupt(RemoveInterrupt::deserialize(payload, fds)?)
            }
        })
    }
}

/// Structure checking the owner liveness.
#[derive(Debug)]
pub(crate) struct Ping {}

impl Serializable for Ping {
    type Payload = RawMessage;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        (RawMessage::Ping {}, vec![])
    }

    fn deserialize(payload: Self::Payload, _fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        match payload {
            RawMessage::Ping {} => Ok(Self {}),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid payload",
            )),
        }
    }
}

impl Replyable for Ping {
    type Reply = ReplyPing;

    fn create_reply(fd: OwnedFd) -> Self::Reply {
        ReplyPing {
            raw: RawReply::new(fd),
        }
    }
}

/// Structure encoding the interrupt message.
#[derive(Debug)]
pub(crate) struct Interrupt {
    pub id: u64,
}

impl Replyable for Interrupt {
    type Reply = ReplyInterrupt;

    fn create_reply(fd: OwnedFd) -> Self::Reply {
        ReplyInterrupt {
            raw: RawReply::new(fd),
        }
    }
}

impl Serializable for Interrupt {
    type Payload = RawMessage;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        (RawMessage::Interrupt { id: self.id }, vec![])
    }

    fn deserialize(payload: Self::Payload, _fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        match payload {
            RawMessage::Interrupt { id: thread_id } => Ok(Self { id: thread_id }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid payload",
            )),
        }
    }
}

/// Structure encoding the register message.
#[derive(Debug)]
pub(crate) struct Register {
    pub fd: OwnedFd,
}

impl Replyable for Register {
    type Reply = ReplyRegister;

    fn create_reply(fd: OwnedFd) -> Self::Reply {
        ReplyRegister {
            raw: RawReply::new(fd),
        }
    }
}

impl Serializable for Register {
    type Payload = RawMessage;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        (RawMessage::Register {}, vec![self.fd])
    }

    fn deserialize(payload: Self::Payload, mut fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        if fds.len() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid number of file descriptors",
            ));
        }
        match payload {
            RawMessage::Register {} => Ok(Self {
                fd: fds.pop().unwrap(),
            }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid payload",
            )),
        }
    }
}

/// Structure encoding the unregister message.
#[derive(Debug)]
pub(crate) struct Unregister {
    pub id: u64,
}

impl Replyable for Unregister {
    type Reply = ReplyUnregister;

    fn create_reply(fd: OwnedFd) -> Self::Reply {
        ReplyUnregister {
            raw: RawReply::new(fd),
        }
    }
}

impl Serializable for Unregister {
    type Payload = RawMessage;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        (RawMessage::Unregister { id: self.id }, vec![])
    }

    fn deserialize(payload: Self::Payload, _fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        match payload {
            RawMessage::Unregister { id } => Ok(Self { id }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid payload",
            )),
        }
    }
}

/// Structure encoding the lock command message.
#[derive(Debug)]
pub(crate) struct LockCommand {
    pub id: u64,
    pub action: LockAction,
    pub params: LockParams,
}

impl Replyable for LockCommand {
    type Reply = ReplyLockCommand;

    fn create_reply(fd: OwnedFd) -> Self::Reply {
        ReplyLockCommand {
            raw: RawReply::new(fd),
        }
    }
}

impl Serializable for LockCommand {
    type Payload = RawMessage;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        (
            RawMessage::LockCommand {
                id: self.id,
                action: self.action,
                params: self.params,
            },
            vec![],
        )
    }

    fn deserialize(payload: Self::Payload, _fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        match payload {
            RawMessage::LockCommand { id, action, params } => Ok(Self { id, action, params }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid payload",
            )),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RequestSpawnOwner {
    pub(crate) ttl: Duration,
}

impl RequestSpawnOwner {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl }
    }
}

impl Serializable for RequestSpawnOwner {
    type Payload = Duration;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        (self.ttl, vec![])
    }

    fn deserialize(payload: Self::Payload, _fds: Vec<OwnedFd>) -> Result<Self, io::Error> {
        Ok(Self { ttl: payload })
    }
}

impl Replyable for RequestSpawnOwner {
    type Reply = ReplyRequestSpawnOwner;

    fn create_reply(fd: OwnedFd) -> Self::Reply {
        ReplyRequestSpawnOwner {
            raw: RawReply::new(fd),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RemoveInterrupt {
    pub(crate) id: u64,
}

impl Serializable for RemoveInterrupt {
    type Payload = RawMessage;

    fn serialize(self) -> (Self::Payload, Vec<OwnedFd>) {
        (RawMessage::RemoveInterrupt { id: self.id }, vec![])
    }

    fn deserialize(payload: Self::Payload, _fds: Vec<OwnedFd>) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        match payload {
            RawMessage::RemoveInterrupt { id } => Ok(Self { id }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid payload",
            )),
        }
    }
}

impl Replyable for RemoveInterrupt {
    type Reply = ReplyRemoveInterrupt;

    fn create_reply(fd: OwnedFd) -> Self::Reply {
        ReplyRemoveInterrupt {
            raw: RawReply::new(fd),
        }
    }
}

/// Structure encoding the reply to be sent from the server owner process and received by the filesystem daemon.
#[derive(Debug)]
pub(crate) struct RawReply {
    channel: OwnedFd,
}

impl RawReply {
    fn new(fd: OwnedFd) -> Self {
        Self { channel: fd }
    }

    fn reply(&mut self, message: &[u8], fds: Vec<OwnedFd>) {
        let _ = send_raw_message(&mut self.channel, message, fds)
            .map_err(|e| log::error!("reply: {}", e));
    }
}

#[derive(Debug)]
pub(crate) struct ReplyPing {
    pub(crate) raw: RawReply,
}

impl Reply for ReplyPing {
    type Type = ();

    fn raw_reply(&mut self) -> &mut RawReply {
        &mut self.raw
    }
}

/// Structure encoding the reply to the interrupt message.
#[derive(Debug)]
pub(crate) struct ReplyInterrupt {
    pub(crate) raw: RawReply,
}

impl Reply for ReplyInterrupt {
    type Type = ();

    fn raw_reply(&mut self) -> &mut RawReply {
        &mut self.raw
    }
}

/// Structure encoding the reply to the register message.
#[derive(Debug)]
pub(crate) struct ReplyRegister {
    pub(crate) raw: RawReply,
}

impl Reply for ReplyRegister {
    type Type = u64;

    fn raw_reply(&mut self) -> &mut RawReply {
        &mut self.raw
    }
}

/// Structure encoding the reply to the unregister message.
#[derive(Debug)]
pub(crate) struct ReplyUnregister {
    pub(crate) raw: RawReply,
}

impl Reply for ReplyUnregister {
    type Type = ();

    fn raw_reply(&mut self) -> &mut RawReply {
        &mut self.raw
    }
}

/// Structure encoding the reply to the lock command message.
#[derive(Debug)]
pub(crate) struct ReplyLockCommand {
    pub(crate) raw: RawReply,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum ReplyLockCommandData {
    Pending(u64),
    Done(LockParams),
}

impl Reply for ReplyLockCommand {
    type Type = ReplyLockCommandData;

    fn raw_reply(&mut self) -> &mut RawReply {
        &mut self.raw
    }
}

#[derive(Debug)]
pub(crate) struct ReplyRequestSpawnOwner {
    pub(crate) raw: RawReply,
}

impl Reply for ReplyRequestSpawnOwner {
    type Type = ();

    fn raw_reply(&mut self) -> &mut RawReply {
        &mut self.raw
    }

    fn reply(&mut self, message: Result<Self::Type, Errno>) {
        if message.is_ok() {
            panic!(
                "RequestSpawnOwner must reply with an owned file descriptor using reply_with_fds"
            );
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReplyRemoveInterrupt {
    raw: RawReply,
}

impl Reply for ReplyRemoveInterrupt {
    type Type = ();

    fn raw_reply(&mut self) -> &mut RawReply {
        &mut self.raw
    }
}

#[derive(Debug)]
pub(crate) struct ReplyIterator<T>
where
    T: Reply,
{
    oneshot: OwnedFd,
    phantom: PhantomData<T>,
}

impl<R> ReplyIterator<R>
where
    R: Reply,
{
    pub fn new(fd: OwnedFd) -> Self {
        Self {
            oneshot: fd,
            phantom: PhantomData,
        }
    }
}

impl<R> Iterator for ReplyIterator<R>
where
    R: Reply,
{
    type Item = FetchedReplyResult<(R::Type, Vec<OwnedFd>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (raw, fds) = match receive_raw_message(&mut self.oneshot) {
            Ok(res) => res,
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => return None,
            Err(e) => return Some(Err(e)),
        };
        let deserialize_res = bincode::deserialize::<Result<<R as Reply>::Type, i32>>(&raw);
        let item = match deserialize_res {
            Ok(Ok(item)) => item,
            Ok(Err(e)) => return Some(Ok(Err(io::Error::from_raw_os_error(e)))),
            Err(e) => return Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
        };
        Some(Ok(Ok((item, fds))))
    }
}

/// Receives a message from a socket including owned file descriptors.
fn receive_raw_message(socket: &mut impl AsFd) -> io::Result<(Vec<u8>, Vec<OwnedFd>)> {
    let mut bytes_to_receive_buf = [0u8; size_of::<u64>()];
    let mut fds_to_receive_buf = [0u8; size_of::<u64>()];
    let mut first_iov = [
        IoSliceMut::new(&mut bytes_to_receive_buf),
        IoSliceMut::new(&mut fds_to_receive_buf),
    ];
    let first_iov_expected_len: usize = first_iov.iter().map(|iov| iov.len()).sum();
    let mut message = rustix::net::recvmsg(
        &socket,
        &mut first_iov,
        &mut RecvAncillaryBuffer::default(),
        RecvFlags::WAITALL | RecvFlags::CMSG_CLOEXEC | RecvFlags::PEEK,
    )?;
    if message.bytes == 0 {
        message = rustix::net::recvmsg(
            &socket,
            &mut first_iov,
            &mut RecvAncillaryBuffer::default(),
            RecvFlags::WAITALL | RecvFlags::CMSG_CLOEXEC | RecvFlags::PEEK,
        )?;
        if message.bytes == 0 {
            // EOF, end communication
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection terminated",
            ));
        }
    }
    if message.bytes != first_iov_expected_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short message received",
        ));
    }
    let bytes_to_receive = usize::try_from(u64::from_ne_bytes(bytes_to_receive_buf))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let fds_to_receive = usize::try_from(u64::from_ne_bytes(fds_to_receive_buf))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let mut buffer = vec![0u8; bytes_to_receive];
    let mut fds = Vec::with_capacity(fds_to_receive);
    let ancillary_space = cmsg_space!(ScmRights(fds_to_receive));
    let mut ancillary_buf = vec![MaybeUninit::<u8>::uninit(); ancillary_space];
    let mut ancillary = RecvAncillaryBuffer::new(&mut ancillary_buf);
    let mut second_iov = [
        IoSliceMut::new(&mut bytes_to_receive_buf),
        IoSliceMut::new(&mut fds_to_receive_buf),
        IoSliceMut::new(&mut buffer),
    ];
    let second_iov_expected_len: usize = second_iov.iter().map(|iov| iov.len()).sum();
    let mut message = rustix::net::recvmsg(
        &socket,
        &mut second_iov,
        &mut ancillary,
        RecvFlags::WAITALL | RecvFlags::CMSG_CLOEXEC,
    )?;
    if message.bytes == 0 {
        message = rustix::net::recvmsg(
            &socket,
            &mut second_iov,
            &mut ancillary,
            RecvFlags::WAITALL | RecvFlags::CMSG_CLOEXEC,
        )?;
        if message.bytes == 0 {
            // EOF, end communication
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection terminated",
            ));
        }
    }
    if message.bytes != second_iov_expected_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short message received",
        ));
    }
    for ancillary_msg in ancillary.drain() {
        match ancillary_msg {
            RecvAncillaryMessage::ScmRights(rights) => {
                for right in rights {
                    fds.push(right);
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected ancillary message",
                ));
            }
        }
    }
    if fds.len() != fds_to_receive {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "not enough files sent",
        ));
    }
    Ok((buffer, fds))
}

/// Sends a raw message to the socket. Drops the file descriptors after sending.
fn send_raw_message(
    socket: &mut impl AsFd,
    buffer: &[u8],
    fds: Vec<OwnedFd>,
) -> Result<(), io::Error> {
    let mut ancillary_buf = vec![MaybeUninit::<u8>::uninit(); cmsg_space!(ScmRights(fds.len()))];
    let mut ancillary = SendAncillaryBuffer::new(&mut ancillary_buf);
    let borrowed = fds.iter().map(|fd| fd.as_fd()).collect::<Vec<_>>();
    ancillary.push(rustix::net::SendAncillaryMessage::ScmRights(&borrowed));
    let buffer_length_bytes = (buffer.len() as u64).to_ne_bytes();
    let fd_length_bytes = (fds.len() as u64).to_ne_bytes();
    let iov = [
        IoSlice::new(&buffer_length_bytes),
        IoSlice::new(&fd_length_bytes),
        IoSlice::new(buffer),
    ];
    let sent = rustix::net::sendmsg(socket, &iov, &mut ancillary, SendFlags::NOSIGNAL)?;
    let expected_length: usize = iov.iter().map(|b| b.len()).sum();
    if sent != expected_length {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "short message sent",
        ));
    }
    Ok(())
}

/// Sends a request to the server owner process and returns exactly one reply.
pub(crate) fn send_request<R, F>(
    socket: impl DerefMut<Target = F>,
    request: R,
) -> FetchedReplyResult<<R::Reply as Reply>::Type>
where
    R: Serializable + Replyable,
    F: AsFd,
{
    let mut iter = send_request_iter(socket, request)?;
    iter.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "send_request: connection is closed",
        )
    })?
}

/// Sends a request to the server owner process and returns exactly one reply with its file descriptors.
pub(crate) fn send_request_with_fds<R, F>(
    socket: impl DerefMut<Target = F>,
    request: R,
) -> FetchedReplyResult<(<R::Reply as Reply>::Type, Vec<OwnedFd>)>
where
    R: Serializable + Replyable,
    F: AsFd,
{
    let mut iter = send_request_iter_with_fds(socket, request)?;
    let reply = iter.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "send_request_with_fds: connection is closed",
        )
    })??;
    Ok(reply)
}

/// Sends a request to the server owner process and returns a stream of replies. File descriptors are discarded.
pub(crate) fn send_request_iter<R, F>(
    socket: impl DerefMut<Target = F>,
    request: R,
) -> Result<impl Iterator<Item = FetchedReplyResult<<R::Reply as Reply>::Type>>, io::Error>
where
    R: Serializable + Replyable,
    F: AsFd,
{
    let iterator = send_request_iter_with_fds(socket, request)?
        .map(|res| res.map(|reply| reply.map(|(data, _fds)| data)));
    Ok(iterator)
}

/// Sends a request to the server owner process and returns a stream of replies with their file descriptors.
pub(crate) fn send_request_iter_with_fds<R, F>(
    mut socket: impl DerefMut<Target = F>,
    request: R,
) -> Result<ReplyIterator<R::Reply>, io::Error>
where
    R: Serializable + Replyable,
    F: AsFd,
{
    let (payload, mut fds) = request.serialize();
    let payload_bytes = bincode::serialize(&payload).map_err(io::Error::other)?;
    let (parent_oneshot, child_oneshot) = rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::CLOEXEC,
        None,
    )?;
    fds.insert(0, child_oneshot);
    let sock_inner = socket.deref_mut();
    // Child oneshot is dropped here to avoid deadlock.
    send_raw_message(sock_inner, &payload_bytes, fds)?;
    // If the socket is behind a mutex, this allows releasing the mutex early if the caller wants
    drop(socket);
    Ok(ReplyIterator::new(parent_oneshot))
}

/// Receive a request from the filesystem daemon, as well as return its reply FD. The caller should differentiate the OwnedFd object.
pub(crate) fn receive_request<R, F>(
    mut socket: impl DerefMut<Target = F>,
) -> Result<(R, OwnedFd), io::Error>
where
    R: Serializable,
    F: AsFd,
{
    let sock_inner = socket.deref_mut();
    let (buffer, mut fds) = receive_raw_message(sock_inner)?;
    // If the socket is behind a mutex, this allows releasing the mutex early if the caller wants
    drop(socket);
    if fds.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no file descriptors received",
        ));
    }
    let payload: <R as Serializable>::Payload =
        bincode::deserialize(&buffer).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let reply_fd = fds.remove(0);
    let message = R::deserialize(payload, fds)?;
    Ok((message, reply_fd))
}
