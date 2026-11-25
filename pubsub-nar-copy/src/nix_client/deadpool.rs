use std::path::PathBuf;

use deadpool::managed;
use tokio::net::UnixStream;

use crate::nix_client::{Error, NixClient};

pub struct Manager {
    socket_path: PathBuf,
}

impl Manager {
    pub fn builder<S, W>(socket_path: S) -> managed::PoolBuilder<Manager, W>
    where
        S: Into<PathBuf>,
        W: From<managed::Object<Manager>>,
    {
        managed::Pool::builder(Manager {
            socket_path: socket_path.into(),
        })
    }
}

impl managed::Manager for Manager {
    type Type = NixClient<UnixStream>;
    type Error = Error;

    async fn create(&self) -> Result<NixClient<UnixStream>, Error> {
        let socket = UnixStream::connect(&self.socket_path).await.unwrap();
        let nix_client = NixClient::handshake(socket).await?;
        Ok(nix_client)
    }

    async fn recycle(
        &self,
        _: &mut NixClient<UnixStream>,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<Error> {
        Ok(())
    }
}

pub type Pool = managed::Pool<Manager>;
