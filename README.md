Library for implementing Linux passthrough file locking in FUSE.

This is a rewrite of [ulockmgr](https://github.com/libfuse/libfuse/blob/fuse_2_9_bugfix/util/ulockmgr_server.c) with slightly different mechanisms. However, the architecture is practically the same - each lock owner ID will spawn a process which stores locked FDs.

# Differences
- The owner processes use TTLs. These processes exit if they are not in use.
- The lifecycle of locking is more explicit. The register mechanism adds file descriptors, while the unregister mechanism removes files and interrupts all in-progress locks. These mechanisms are abstracted in the RAII [`Lockable`] type returned by [`LockManager::register`].
- Explicit interrupt handles are used, and interrupts block until the relevant threads have exited by retrying in intervals.