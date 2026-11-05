pub mod db;
pub mod error;
pub mod models;
pub mod postgres;
pub mod pubsub;
pub mod queue;
pub mod schema;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;

use clap::Parser;
use pubsub_proto::pubsub::{
    client::pub_sub_client_server::PubSubClientServer, server::pub_sub_server::PubSubServer,
};
use tonic::transport::Server;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "PUBSUB_LISTEN", default_value = "[::1]:10000")]
    listen: SocketAddr,
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    let mut svc = pubsub::PubSubService::new(&args.database_url).await?;
    svc.run_migrations()
        .map_err(|err| err as Box<dyn std::error::Error>)?;

    log::info!("listening on {}", args.listen);

    Server::builder()
        .add_service(PubSubServer::new(svc.clone()))
        .add_service(PubSubClientServer::new(svc))
        .serve(args.listen)
        .await?;

    Ok(())
}
