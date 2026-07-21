use std::{io, os::fd::OwnedFd};

use parking_lot::Mutex;

use crate::{
    messages::{Reply, Replyable, RequestSpawnOwner, receive_request},
    processes::{self},
};

/// Structure that spawns daemon owners on-demand.
pub struct OwnerSpawner {
    /// The communication socket for the main FUSE processor.
    communication: Mutex<OwnedFd>,
}

impl OwnerSpawner {
    pub fn new(communication: OwnedFd) -> Self {
        Self {
            communication: Mutex::new(communication),
        }
    }

    pub fn run(self) -> Result<(), io::Error> {
        loop {
            let (request, reply_oneshot): (RequestSpawnOwner, OwnedFd) =
                match receive_request(self.communication.lock()) {
                    Ok((request, reply_oneshot)) => (request, reply_oneshot),
                    Err(e) if e.kind() == io::ErrorKind::BrokenPipe => break,
                    Err(e) => return Err(e),
                };
            let mut reply = <RequestSpawnOwner as Replyable>::create_reply(reply_oneshot);
            let parent_fd = processes::spawn_owner_process(request.ttl)?;
            reply.reply_with_fds(Ok(()), vec![parent_fd]);
        }
        Ok(())
    }
}
