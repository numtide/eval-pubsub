use std::{net::SocketAddr, sync::LazyLock};

const DEFAULT_LISTEN_ADDRESSES: LazyLock<Vec<SocketAddr>> = LazyLock::new(|| {
    vec![
        "0.0.0.0:10000".parse().unwrap(),
        "[::]:10000".parse().unwrap(),
    ]
});

#[derive(clap::Parser, Debug, Eq, Clone, PartialEq)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long, env = "PUBSUB_LISTEN", default_values_t = (*DEFAULT_LISTEN_ADDRESSES).clone())]
    pub listen_address: Vec<SocketAddr>,
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,
}
