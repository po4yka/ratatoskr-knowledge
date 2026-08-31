//! Crash-safe admission, leased work, and terminal publication state for the primary event stream.

mod admission;
mod outbox;
mod work;

pub use admission::{
    AdmissionDisposition, PRIMARY_EVENT_SUBJECTS, PrimaryAdmissionError, PrimaryAdmissionStore,
};
pub use outbox::{OutboxEntry, TerminalOutbox, TerminalOutboxError};
pub use work::{AnalysisWork, AnalysisWorkState, WorkQueue, WorkQueueError};
