pub mod db;
pub mod error;
pub mod models;
pub mod postgres;
pub mod pubsub;
pub mod queue;
pub mod schema;
#[cfg(test)]
mod tests;

use pubsub_proto::pubsub::server::pub_sub_server::PubSubServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:10000".parse().unwrap();
    let svc = pubsub::PubSubService::new("").await?;

    Server::builder()
        .add_service(PubSubServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
