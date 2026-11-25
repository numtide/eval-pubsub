use bytes::{BufMut as _, BytesMut};
use pubsub_proto::pubsub::nar_copy::{self, nar_copy_server};
use pubsub_util::job_set::JobSet;
use std::sync::Arc;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UnixStream,
    sync::{RwLock, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    nix_client::{self, NixClient},
    wire::{self, ValidPathInfo},
};

impl From<wire::ValidPathInfo> for nar_copy::NarInfo {
    fn from(value: wire::ValidPathInfo) -> Self {
        nar_copy::NarInfo {
            store_path: (&value.path).into(),
            nar_hash: (&value.info.nar_hash).into(),
            nar_size: value.info.nar_size,
            reference: value
                .info
                .references
                .iter()
                .map(|store_path| store_path.into())
                .collect(),
            deriver: value.info.deriver.into(),
            signature: match value.info.signatures {
                Some(signatures) => signatures
                    .iter()
                    .map(|signature| signature.into())
                    .collect(),
                None => Vec::new(),
            },
            ca: value.info.ca,
        }
    }
}

impl TryFrom<nar_copy::NarInfo> for wire::ValidPathInfo {
    type Error = String;

    fn try_from(value: nar_copy::NarInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            path: value.store_path.try_into().map_err(|e| format!("{}", e))?,
            info: wire::UnkeyedValidPathInfo {
                deriver: match value.deriver {
                    Some(deriver) => deriver.try_into().map_err(|e| format!("{}", e))?,
                    None => wire::OptStorePath::Nothing,
                },
                nar_hash: value.nar_hash.try_into()?,
                references: value
                    .reference
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()
                    .map_err(|e| format!("{}", e))?,
                registration_time: 0.into(),
                nar_size: value.nar_size,
                ultimate: Some(false),
                signatures: if value.signature.is_empty() {
                    None
                } else {
                    Some(
                        value
                            .signature
                            .into_iter()
                            .map(TryInto::try_into)
                            .collect::<Result<_, _>>()?,
                    )
                },
                ca: value.ca,
            },
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Nix daemon error {0}")]
    NixError(#[from] nix_client::Error),
    #[error("no such hash {0:?}")]
    NoSuchHash(wire::StorePathHash),
    #[error("during value conversion {0:}")]
    TryFrom(String),
}

impl From<Error> for tonic::Status {
    fn from(value: Error) -> Self {
        match value {
            Error::NixError(error) => tonic::Status::internal(error.to_string()),
            Error::NoSuchHash(store_path_hash) => tonic::Status::not_found(format!(
                "no such hash {}",
                Into::<String>::into(store_path_hash)
            )),
            Error::TryFrom(error) => {
                tonic::Status::internal(format!("{} during value conversion", error))
            }
        }
    }
}

pub struct NarCopyService<RW> {
    nix_client: Arc<RwLock<NixClient<RW>>>,
    job_set: JobSet,
}

impl NarCopyService<UnixStream> {
    pub async fn new_default_socket() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let nix_socket = NixClient::connect_unix_default().await?;
        let nix_client = NixClient::handshake(nix_socket).await?;

        Ok(Self {
            nix_client: Arc::new(RwLock::new(nix_client)),
            job_set: JobSet::new(),
        })
    }
}

#[tonic::async_trait]
impl<RW> nar_copy_server::NarCopy for NarCopyService<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    async fn download_nar_info(
        &self,
        request: tonic::Request<nar_copy::DownloadNarInfoRequest>,
    ) -> std::result::Result<tonic::Response<nar_copy::DownloadNarInfoResponse>, tonic::Status>
    {
        let mut nix_client = self.nix_client.write().await;
        let hash: wire::StorePathHash = request.into_inner().hash.try_into().expect("valid hash");

        if let Some(path) = nix_client
            .query_path_from_hash_part(hash)
            .await
            .map_err(Into::<Error>::into)?
        {
            if let Some(info) = nix_client
                .query_path_info(&path)
                .await
                .map_err(Into::<Error>::into)?
            {
                Ok(tonic::Response::new(nar_copy::DownloadNarInfoResponse {
                    nar_info: Some((ValidPathInfo { path, info }).into()),
                }))
            } else {
                Err(Error::NoSuchHash(hash).into())
            }
        } else {
            Err(Error::NoSuchHash(hash).into())
        }
    }

    type DownloadNarStream = ReceiverStream<Result<nar_copy::DownloadNarResponse, tonic::Status>>;
    async fn download_nar(
        &self,
        request: tonic::Request<nar_copy::DownloadNarRequest>,
    ) -> std::result::Result<tonic::Response<Self::DownloadNarStream>, tonic::Status> {
        let mut nix_client = self.nix_client.write().await;
        let request = request.into_inner();
        let hash: wire::StorePathHash = request.hash.try_into().expect("valid hash");
        let chunk_size: u64 = request.chunk_size;

        if let Some(path) = nix_client
            .query_path_from_hash_part(hash)
            .await
            .map_err(Into::<Error>::into)?
        {
            let nar = nix_client
                .nar_from_path(&path)
                .await
                .map_err(Into::<Error>::into)?;

            let (tx, rx) = mpsc::channel(1);

            self.job_set
                .spawn(
                    "download_nar",
                    async move {
                        for (ix, chunk) in nar.bytes.chunks(chunk_size as usize).enumerate() {
                            tx.send(Ok(nar_copy::DownloadNarResponse {
                                nar_chunk: Some(nar_copy::NarChunk {
                                    offset: ix as u64 * chunk_size,
                                    body: chunk.to_vec(),
                                }),
                            }))
                            .await
                            .expect("successful send over the mpsc");
                        }
                    },
                    async {},
                )
                .await;

            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        } else {
            Err(Error::NoSuchHash(hash).into())
        }
    }

    async fn upload_nar(
        &self,
        request: tonic::Request<tonic::Streaming<nar_copy::UploadNarRequest>>,
    ) -> std::result::Result<tonic::Response<nar_copy::UploadNarResponse>, tonic::Status> {
        let mut request = request.into_inner();

        if let Ok(Some(nar_copy::UploadNarRequest {
            r#type: Some(nar_copy::upload_nar_request::Type::NarInfo(nar_info)),
        })) = request.message().await
        {
            let mut nix_client = self.nix_client.write().await;
            let nar_info: wire::ValidPathInfo = nar_info.try_into().map_err(Error::TryFrom)?;
            let mut nar_buf: BytesMut = BytesMut::new();
            nar_buf.resize(nar_info.info.nar_size as usize, 0);

            while let Ok(Some(message)) = request.message().await {
                match message.r#type {
                    Some(nar_copy::upload_nar_request::Type::NarChunk(nar_chunk)) => {
                        // TODO slow, copying and needless allocation
                        let start = nar_chunk.offset as usize;
                        let end = nar_chunk.offset as usize + nar_chunk.body.len();
                        let mut writer = nar_buf[start..end].writer();
                        std::io::copy(&mut nar_chunk.body.as_slice(), &mut writer)?;
                    }
                    _ => todo!(),
                }
            }

            let nar: Vec<u8> = nar_buf.into();
            let nar: wire::Nar = nar.into();
            nix_client
                .add_to_store_nar(&nar_info, false, false, &nar)
                .await
                .map_err(Into::<Error>::into)?;

            Ok(tonic::Response::new(nar_copy::UploadNarResponse {}))
        } else {
            todo!()
        }
    }
}
