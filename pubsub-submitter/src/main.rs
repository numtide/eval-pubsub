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
use pubsub_proto::pubsub::{
    client::pub_sub_client_server::PubSubClientServer, server::pub_sub_server::PubSubServer,
};
use tokio::net::TcpListener;
use tonic::{service::Routes, transport::Server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(_) = std::env::var("RUST_LOG") {
        env_logger::init();
    } else {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .init();
    };
    let cli: cli::FinalConfig = cli::FinalConfig::parse()?;
    log::info!("running with config: {:?}", cli);

    let mut svc = pubsub::PubSubService::new(cli.database_url).await?;
    svc.run_migrations()?;

    let server = PubSubServer::new(svc.clone());
    let client_server = PubSubClientServer::new(svc);
    let mut routes = Routes::builder();
    routes.add_service(client_server.clone());
    routes.add_service(server.clone());
    let axum_router = routes.routes().into_axum_router();
    futures::future::join_all(cli.listen_address.into_iter().map(|addr| {
        let axum_router = axum_router.clone();
        async move { axum::serve(TcpListener::bind(addr).await?, axum_router).await }
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;

    Ok(())
}
