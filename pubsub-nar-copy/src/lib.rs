use std::{cmp::min, error::Error, fmt, path::PathBuf};

use bytes::{BufMut as _, BytesMut};
use nom::{AsBytes, IResult, Input};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    net::UnixStream,
};

mod framed_reader;
mod wire;

use wire::ProtocolVersion;

use crate::{
    framed_reader::FramedNixReader,
    wire::{
        NixDeserialize, NixError, NixSerialize as _, Operation, StorePath, Trust,
        UnkeyedValidPathInfo,
    },
};

const MAGIC_1: u64 = 0x6e697863;
const MAGIC_2: u64 = 0x6478696f;
const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::from_parts(1, 35);
const STDERR_LAST: u64 = 0x616c7473;
const STDERR_ERROR: u64 = 0x63787470;
const STDERR_NEXT: u64 = 0x6f6c6d67;
const STDERR_READ: u64 = 0x64617461;
const STDERR_WRITE: u64 = 0x64617416;
const STDERR_START_ACTIVITY: u64 = 0x53545254;
const STDERR_STOP_ACTIVITY: u64 = 0x53544f50;
const STDERR_RESULT: u64 = 0x52534c54;

struct NixClient<RW> {
    protocol_version: ProtocolVersion,
    daemon_nix_version: String,
    trust: Trust,
    nix_socket_r: FramedNixReader<ReadHalf<RW>>,
    nix_socket_w: WriteHalf<RW>,
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
    async fn connect_unix_default() -> Result<UnixStream, Box<dyn Error + Send + Sync>> {
        let socket = Self::default_path();
        UnixStream::connect(socket).await.map_err(Into::into)
    }
}

impl<RW> NixClient<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn default_path() -> PathBuf {
        PathBuf::from("/nix/var/nix/daemon-socket/socket")
    }

    async fn handshake(mut nix_socket: RW) -> Result<Self, Box<dyn Error + Send + Sync>> {
        nix_socket.write_u64_le(MAGIC_1).await?;
        nix_socket.flush().await?;

        let magic = nix_socket.read_u64_le().await?;
        assert_eq!(magic, MAGIC_2);

        let daemon_protocol_version = nix_socket.read_u64_le().await?;
        let daemon_protocol_version = daemon_protocol_version.try_into()?;

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

        let daemon_nix_version = nix_socket_r
            .nom_read::<String>(protocol_version)
            .await
            .unwrap();

        println!("daemon_nix_version {}", daemon_nix_version);

        let trust = nix_socket_r.get_u64_le().await.try_into()?;

        let mut this = Self {
            protocol_version,
            daemon_nix_version,
            trust,
            nix_socket_r,
            nix_socket_w,
        };

        this.flush_logs().await?;

        Ok(this)
    }

    async fn flush_logs(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        RW: Send,
    {
        while self.read_log().await? {}

        Ok(())
    }

    async fn read_log(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>>
    where
        RW: Send,
    {
        let r#type = self.nix_socket_r.get_u64_le().await;

        match r#type {
            STDERR_LAST => {
                println!("STDERR_LAST");
                Ok(false)
            }
            STDERR_ERROR => {
                let error: NixError = self
                    .nix_socket_r
                    .nom_read(self.protocol_version)
                    .await
                    .unwrap();
                println!("STDERR_ERROR: {:?}", error);
                Ok(false)
            }
            STDERR_NEXT => {
                println!("STDERR_NEXT");
                let msg: String = self
                    .nix_socket_r
                    .nom_read(self.protocol_version)
                    .await
                    .unwrap();
                println!("log: {}", msg);
                Ok(true)
            }
            STDERR_READ => {
                println!("STDERR_READ");
                Ok(true)
            }
            STDERR_WRITE => {
                println!("STDERR_WRITE");
                Ok(true)
            }
            STDERR_START_ACTIVITY => {
                println!("STDERR_START_ACTIVITY");
                Ok(true)
            }
            STDERR_STOP_ACTIVITY => {
                println!("STDERR_STOP_ACTIVITY");
                Ok(true)
            }
            STDERR_RESULT => {
                println!("STDERR_RESULT");
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn query_path_info(
        &mut self,
        store_path: StorePath,
    ) -> Result<Option<UnkeyedValidPathInfo>, Box<dyn Error + Send + Sync>>
    where
        RW: Send,
    {
        let mut bytes_mut = BytesMut::new();

        Operation::QueryPathInfo.serialize(self.protocol_version, &mut bytes_mut);
        self.nix_socket_w.write_buf(&mut bytes_mut).await?;
        let mut store_path_buf = BytesMut::new();
        store_path.serialize(self.protocol_version, &mut store_path_buf);
        self.nix_socket_w.write_all_buf(&mut store_path_buf).await?;
        self.nix_socket_w.flush().await?;

        self.flush_logs().await?;

        let success = self.nix_socket_r.get_u64_le().await;
        println!("success: {:#x}", success);
        let success: bool = success > 0;
        println!("sent {} got {}", store_path, success);

        let mut buffer = BytesMut::new();

        if success {
            let path_info: UnkeyedValidPathInfo = self
                .nix_socket_r
                .nom_read(self.protocol_version)
                .await
                .unwrap();
            Ok(Some(path_info))
        } else {
            Ok(None)
        }
    }
}

mod tests {
    use super::*;

    #[tokio::test]
    async fn handshake() -> Result<(), Box<dyn Error + Send + Sync>> {
        let nix_socket = NixClient::connect_unix_default().await.unwrap();
        let mut nix_client = NixClient::handshake(nix_socket).await?;

        println!("nix_client: {:?}", nix_client);

        let path_info = nix_client
            .query_path_info(StorePath::try_from(
                "/nix/store/7fppvgqxs3zxhvrpqnmwydn70bzgbnnx-rustc-wrapper-1.95.0",
            )?)
            .await?;

        println!("path_info: {:?}", path_info);

        Ok(())
    }
}
