pub mod pubsub;
pub mod queue;
#[cfg(test)]
mod tests;

use pubsub_proto::pubsub::server::pub_sub_server::PubSubServer;
use tonic::transport::Server;

use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:10000".parse().unwrap();

    let svc = pubsub::PubSubService::new();

    Server::builder()
        .add_service(PubSubServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
