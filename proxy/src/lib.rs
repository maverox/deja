//! Proxy library for testing and extensions
//!
//! This library provides access to proxy internals for testing and buffer validation.

pub mod buffer;
pub mod correlation;

// Re-export commonly used types
pub use buffer::{PendingEventBuffer, QuarantinedEvents};
pub use correlation::TraceCorrelator;
