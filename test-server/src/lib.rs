//! Test Server for Deja Proxy Integration Testing
//!
//! A lightweight gRPC server that makes HTTP calls to backends,
//! designed to test Deja proxy's recording, replay, and correlation features.

pub mod test_service {
    tonic::include_proto!("test_service.v1");
}

pub mod http_client;
pub mod mock_backend;
pub mod service;
