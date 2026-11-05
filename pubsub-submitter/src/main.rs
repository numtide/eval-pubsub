mod cli;
pub mod db;
pub mod error;
pub mod models;
pub mod postgres;
pub mod pubsub;
pub mod queue;
pub mod schema;
#[cfg(test)]
mod tests;

use clap::Parser as _;
use pubsub_proto::pubsub::server::pub_sub_server::PubSubServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(_) = std::env::var("RUST_LOG") {
        env_logger::init();
    } else {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .init();
    };
    let cli: cli::Cli = cli::Cli::parse();
    log::info!("running with config: {:?}", cli);

    let svc = pubsub::PubSubService::new(cli.database_url).await?;

    let server = PubSubServer::new(svc);
    for addr in cli.listen_address {
        Server::builder()
            .add_service(server.clone())
            .serve(addr)
            .await?;
    }

    Ok(())
}
