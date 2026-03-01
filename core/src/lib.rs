pub mod config;
pub mod control_api;
pub mod diff;
pub mod protocols;
pub mod recording;
pub mod replay;
pub mod runtime;
pub mod storage;
pub mod tls_mitm;

// Re-export trace context and spawning functions for easy access
pub use runtime::spawn::{spawn, spawn_blocking, get_blocking_trace_id};
pub use runtime::pool_interceptor::InterceptedConnection;

// Re-export generated protobuf modules
pub mod proto {
    pub mod deja {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/deja.v1.rs"));
        }
    }
}

pub use proto::deja::v1 as events;
