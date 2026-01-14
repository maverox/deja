pub mod diff;
pub mod hash;
pub mod protocols;
pub mod recording;
pub mod replay;
pub mod runtime;
pub mod tls_mitm;

// Re-export generated protobuf modules
pub mod proto {
    pub mod deja {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/deja.v1.rs"));
        }
    }
}

pub use proto::deja::v1 as events;
