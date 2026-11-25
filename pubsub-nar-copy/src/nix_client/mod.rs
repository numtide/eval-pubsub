use std::{cmp::min, collections::HashSet, fmt, path::PathBuf};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    net::UnixStream,
};

use crate::{framed, wire};

use crate::{
    framed::{FramedNixReader, FramedNixWriter},
    wire::{Nar, NixError, Operation, StorePath, Trust, UnkeyedValidPathInfo},
};

pub mod deadpool;

pub use deadpool::Pool;

const MAGIC_1: u64 = 0x6e697863;
const MAGIC_2: u64 = 0x6478696f;
const PROTOCOL_VERSION: wire::ProtocolVersion = wire::ProtocolVersion::from_parts(1, 35);
const STDERR_LAST: u64 = 0x616c7473;
const STDERR_ERROR: u64 = 0x63787470;
const STDERR_NEXT: u64 = 0x6f6c6d67;
const STDERR_READ: u64 = 0x64617461;
const STDERR_WRITE: u64 = 0x64617416;
const STDERR_START_ACTIVITY: u64 = 0x53545254;
const STDERR_STOP_ACTIVITY: u64 = 0x53544f50;
const STDERR_RESULT: u64 = 0x52534c54;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error {0:?}")]
    Io(#[from] std::io::Error),
    #[error("other error \"{0}\"")]
    Other(String),
    #[error("framing error {0:?}")]
    Framed(#[from] framed::Error),
    #[error("daemon error {0:?}")]
    Daemon(wire::NixError),
    #[error("unsupported deprecated Read or Write log from the Nix daemon")]
    LogReadWrite(),
    #[error("unknown log type with ID {0}")]
    UnknownLog(u64),
}

pub struct NixClient<RW> {
    protocol_version: wire::ProtocolVersion,
    daemon_nix_version: String,
    trust: Trust,
    nix_socket_r: FramedNixReader<ReadHalf<RW>>,
    nix_socket_w: FramedNixWriter<WriteHalf<RW>>,
}

impl<RW> fmt::Debug for NixClient<RW> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NixClient")
            .field(
                "protocol_version",
                &format!(
                    "{:?} => {}",
                    &self.protocol_version,
                    &self.protocol_version.to_string()
                ),
            )
            .field("daemon_nix_version", &self.daemon_nix_version)
            .field("trust", &self.trust)
            .field("nix_socket_r", &"?")
            .field("nix_socket_w", &"?")
            .finish()
    }
}

impl NixClient<UnixStream> {
    pub fn default_path() -> PathBuf {
        PathBuf::from("/nix/var/nix/daemon-socket/socket")
    }

    pub async fn connect_unix_default() -> Result<UnixStream, Error> {
        let socket = Self::default_path();
        UnixStream::connect(socket).await.map_err(Into::into)
    }
}

impl<RW> NixClient<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub async fn handshake(mut nix_socket: RW) -> Result<Self, Error> {
        nix_socket.write_u64_le(MAGIC_1).await?;
        nix_socket.flush().await?;

        let magic = nix_socket.read_u64_le().await?;
        assert_eq!(magic, MAGIC_2);

        let daemon_protocol_version = nix_socket.read_u64_le().await?;
        let daemon_protocol_version = daemon_protocol_version.try_into().map_err(Error::Other)?;

        nix_socket.write_u64_le(PROTOCOL_VERSION.into()).await?;
        nix_socket.flush().await?;

        let protocol_version = min(daemon_protocol_version, PROTOCOL_VERSION);

        // send cpu
        nix_socket.write_u64_le(0).await?;
        // reserve space
        nix_socket.write_u64_le(0).await?;
        nix_socket.flush().await?;

        let (nix_socket_r, nix_socket_w) = tokio::io::split(nix_socket);
        let mut nix_socket_r = FramedNixReader::new(nix_socket_r);
        let nix_socket_w = FramedNixWriter::new(nix_socket_w);

        let daemon_nix_version = nix_socket_r.nom_read::<String>(protocol_version).await?;

        let trust = nix_socket_r
            .get_u64_le()
            .await
            .try_into()
            .map_err(Error::Other)?;

        let mut this = Self {
            protocol_version,
            daemon_nix_version,
            trust,
            nix_socket_r,
            nix_socket_w,
        };

        this.flush_logs().await?;

        log::debug!(
            "handshake concluded with protocol version {} and trust {:?}",
            protocol_version,
            trust
        );

        Ok(this)
    }

    async fn flush_logs(&mut self) -> Result<(), Error> {
        while self.read_log().await? {}

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        self.nix_socket_w.flush().await?;
        self.flush_logs().await?;

        Ok(())
    }

    async fn read_log(&mut self) -> Result<bool, Error> {
        let r#type = self.nix_socket_r.get_u64_le().await;

        match r#type {
            STDERR_LAST => Ok(false),
            STDERR_ERROR => Err(Error::Daemon(
                self.nix_socket_r.nom_read(self.protocol_version).await?,
            )),
            STDERR_NEXT => {
                println!("STDERR_NEXT");
                let msg: String = self.nix_socket_r.nom_read(self.protocol_version).await?;
                println!("log: {}", msg);
                Ok(true)
            }
            STDERR_READ => Err(Error::LogReadWrite()),
            STDERR_WRITE => Err(Error::LogReadWrite()),
            STDERR_START_ACTIVITY => {
                todo!("STDERR_START_ACTIVITY isn't supported yet");
            }
            STDERR_STOP_ACTIVITY => {
                todo!("STDERR_STOP_ACTIVITY isn't supported yet");
            }
            STDERR_RESULT => {
                todo!("STDERR_RESULT isn't supported yet");
            }
            _ => Err(Error::UnknownLog(r#type)),
        }
    }

    async fn write_operation(&mut self, operation: &wire::Operation) -> Result<(), Error> {
        log::debug!("sending operation {operation:?}");
        self.nix_socket_w
            .write(self.protocol_version, operation)
            .await?;

        Ok(())
    }

    pub async fn query_path_info(
        &mut self,
        store_path: &StorePath,
    ) -> Result<Option<UnkeyedValidPathInfo>, Error> {
        self.write_operation(&Operation::QueryPathInfo).await?;

        self.nix_socket_w
            .write(self.protocol_version, store_path)
            .await?;

        self.flush().await?;

        let success = self.nix_socket_r.get_u64_le().await;
        let success: bool = success > 0;

        if success {
            let path_info: UnkeyedValidPathInfo =
                self.nix_socket_r.nom_read(self.protocol_version).await?;
            Ok(Some(path_info))
        } else {
            Ok(None)
        }
    }

    pub async fn nar_from_path(&mut self, store_path: &StorePath) -> Result<Nar, Error> {
        self.write_operation(&Operation::NarFromPath).await?;

        self.nix_socket_w
            .write(self.protocol_version, store_path)
            .await?;

        self.flush().await?;

        let decoder: Nar = self.nix_socket_r.nom_read(self.protocol_version).await?;
        Ok(decoder)
    }

    pub async fn add_to_store_nar(
        &mut self,
        info: &wire::ValidPathInfo,
        repair: bool,
        dont_check_sigs: bool,
        nar: &Nar,
    ) -> Result<(), Error> {
        if self.protocol_version < wire::ProtocolVersion::from_parts(1, 23) {
            panic!("protocol version < 1.23 isn't supported for \"add_to_store_nar\"")
        }

        self.write_operation(&Operation::AddToStoreNar).await?;

        self.nix_socket_w.write(self.protocol_version, info).await?;

        self.nix_socket_w
            .write(self.protocol_version, &repair)
            .await?;

        self.nix_socket_w
            .write(self.protocol_version, &dont_check_sigs)
            .await?;

        let bytes = nar.bytes.as_slice();

        for chunk in bytes.chunks(512) {
            self.nix_socket_w
                .write(self.protocol_version, &wire::bytes::Unpadded(chunk))
                .await?;
        }
        self.nix_socket_w
            .write(self.protocol_version, &0u64)
            .await?;

        self.flush().await?;

        Ok(())
    }

    pub async fn query_path_from_hash_part(
        &mut self,
        hash: wire::StorePathHash,
    ) -> Result<Option<StorePath>, Error> {
        self.write_operation(&Operation::QueryPathFromHashPart)
            .await?;

        self.nix_socket_w
            .write(self.protocol_version, &hash)
            .await?;

        self.flush().await?;

        let opt_store_path: wire::OptStorePath =
            self.nix_socket_r.nom_read(self.protocol_version).await?;

        match opt_store_path {
            wire::OptStorePath::StorePath(store_path) => Ok(Some(store_path)),
            wire::OptStorePath::Nothing => Ok(None),
        }
    }

    pub async fn get_closure(
        &mut self,
        store_path: &wire::StorePath,
    ) -> Result<Option<HashSet<wire::StorePath>>, Error>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut stack = vec![store_path.clone()];
        let mut closure = HashSet::from_iter(stack.clone().into_iter());

        while let Some(item) = stack.pop() {
            let path_info = self.query_path_info(&item).await?;

            match path_info {
                Some(path_info) => {
                    for reference in path_info.references {
                        if !closure.contains(&reference) {
                            stack.push(reference.clone());
                            closure.insert(reference);
                        }
                    }
                }
                None => return Ok(None),
            }
        }

        Ok(Some(closure))
    }
}

mod tests {
    use pubsub_util::init_logger;

    use super::*;

    #[tokio::test]
    async fn handshake() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logger();
        let nix_socket = NixClient::connect_unix_default().await?;
        let nix_client = NixClient::handshake(nix_socket).await?;

        println!("nix_client: {:?}", nix_client);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_closure() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logger();
        let nix_socket = NixClient::connect_unix_default().await?;
        let mut nix_client = NixClient::handshake(nix_socket).await?;

        let store_path = StorePath::try_from(
            "/nix/store/7fppvgqxs3zxhvrpqnmwydn70bzgbnnx-rustc-wrapper-1.95.0",
        )?;

        let closure = nix_client.get_closure(&store_path).await?;
        match closure {
            Some(closure) => {
                for item in closure {
                    println!("- {}", item);
                }
            }
            None => panic!("failed to get closure"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn get_nar() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        init_logger();
        let nix_socket = NixClient::connect_unix_default().await?;
        let mut nix_client = NixClient::handshake(nix_socket).await?;

        let store_path = StorePath::try_from(
            "/nix/store/7fppvgqxs3zxhvrpqnmwydn70bzgbnnx-rustc-wrapper-1.95.0",
        )?;

        let path_info = nix_client.query_path_info(&store_path).await?;
        match path_info {
            Some(path_info) => {
                let nar = nix_client.nar_from_path(&store_path).await?;
                println!("length nar_bytes: {}", nar.bytes.len());

                let valid_path_info = wire::ValidPathInfo::new(store_path, path_info);

                nix_client
                    .add_to_store_nar(&valid_path_info, true, false, &nar)
                    .await?;

                nix_client
                    .add_to_store_nar(&valid_path_info, true, false, &nar)
                    .await?;

                Ok(())
            }
            None => panic!("couldn't fetch path_info"),
        }
    }
}
