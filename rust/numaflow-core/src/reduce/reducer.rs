//! Reduce module contains the implementation for Reduce operation.
//! ```text
//!                                  +-------------------+
//!                                  |                   |
//!                                  |     ISB Reader    |
//!                                  |                   |
//!                                  +--------+----------+
//!                                           |
//!                                           | Messages
//!                                           v
//! +------------------+            +---------+---------+
//! |                  |  Replays   |                   |
//! |       WAL        +----------->|    Persistent     |
//! | (Write-Ahead Log)|            |       Buffer      |    
//! |                  |<-----------+       Queue       |  
//! +------------------+  Persists  |                   |  
//!                                 |       (PBQ)       |
//!                                 +---------+---------+
//!                                           |
//!                                           | ReceiverStream<Message>
//!                                           v
//!                                 +---------+---------+
//!                                 |                   |
//!                                 |      Reducer      |
//!                                 |                   |
//!                                 +---+-----+-----+---+
//!                                     |     |     |
//!                                     |     |     | Window Multiplexer
//!                                     |     |     |
//!                                     v     v     v
//!                           +---------+ +---------+ +---------+
//!                           |         | |         | |         |
//!                           | Reduce  | | Reduce  | | Reduce  |
//!                           | Task 1  | | Task 2  | | Task N  |
//!                           |         | |         | |         |
//!                           +---------+ +---------+ +---------+
//!                                |          |           |
//!                                |          |           |
//!                                v          v           v
//!                           +---------------------------------+
//!                           |                                 |
//!                           |          ISB Writer             |
//!                           |                                 |
//!                           +---------------------------------+
//! ```
//!
//! ## Error Handling
//! Errors are propagated from the reduce tasks to the AlignedReduceActor through the error_tx channel.
//! AlignedReduceActor will cancel the token to signal the upstream to stop sending new messages and
//! exit with error. This is possible because the streaming reader is awaiting on the cancellation token
//! (read) while the write/cancel of the cancellation token is done by the Reduce Actor.
//!

/// Aligned Reduce for Fixed and Sliding Windows.
pub(crate) mod aligned;

/// Unaligned Reduce for Session and Accumulator Windows.
pub(crate) mod unaligned;

/// User-defined Reduce for Aligned and Unaligned Windows.
pub(crate) mod user_defined;

#[derive(Debug, Clone)]
pub(crate) enum WindowManager {
    /// Aligned window manager.
    Aligned(aligned::windower::AlignedWindowManager),
    /// Unaligned window manager.
    Unaligned(unaligned::windower::UnalignedWindowManager),
}

/// Reducer for Aligned and Unaligned Windows.
pub(crate) enum Reducer {
    Aligned(aligned::reducer::AlignedReducer),
    Unaligned(unaligned::reducer::UnalignedReducer),
}
