pub mod test_service {
    tonic::include_proto!("test_service.v1");
}

pub mod connector {
    tonic::include_proto!("connector");
}

pub mod connector_service;
pub mod db_pools;
pub mod http_client;
pub mod http_handlers;
pub mod jsonl_exporter;
pub mod mock_backend;
pub mod service;
