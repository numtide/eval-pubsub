mod framed;
mod nix_client;
mod wire;

use std::{cell::RefCell, sync::Arc};

use pubsub_proto::pubsub::nar_copy::{self, nar_copy_server};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::RwLock,
};
use tokio_stream::wrappers::ReceiverStream;

use crate::{nix_client::NixClient, wire::ValidPathInfo};

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

struct NarCopyServer<RW> {
    nix_client: Arc<RwLock<NixClient<RW>>>,
}

#[tonic::async_trait]
impl<RW> nar_copy_server::NarCopy for NarCopyServer<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    async fn download_nar_info(
        &self,
        request: tonic::Request<nar_copy::DownloadNarInfoRequest>,
    ) -> std::result::Result<tonic::Response<nar_copy::DownloadNarInfoResponse>, tonic::Status>
    {
        let hash: wire::StorePathHash = request.into_inner().hash.try_into().expect("valid hash");

        let path = self
            .nix_client
            .write()
            .await
            .query_path_from_hash_part(hash)
            .await
            .unwrap()
            .unwrap();

        let info = self
            .nix_client
            .write()
            .await
            .query_path_info(&path)
            .await
            .unwrap()
            .unwrap();

        Ok(tonic::Response::new(nar_copy::DownloadNarInfoResponse {
            nar_info: Some((ValidPathInfo { path, info }).into()),
        }))
    }

    type DownloadNarStream = ReceiverStream<Result<nar_copy::DownloadNarResponse, tonic::Status>>;
    async fn download_nar(
        &self,
        request: tonic::Request<nar_copy::DownloadNarRequest>,
    ) -> std::result::Result<tonic::Response<Self::DownloadNarStream>, tonic::Status> {
        let request = request.into_inner();
        let hash: wire::StorePathHash = request.hash.try_into().expect("valid hash");
        let chunk_size: u64 = request.chunk_size;

        let path = self
            .nix_client
            .write()
            .await
            .query_path_from_hash_part(hash)
            .await
            .unwrap()
            .unwrap();

        let nar = self
            .nix_client
            .write()
            .await
            .nar_from_path(&path)
            .await
            .unwrap();

        for chunk in nar.bytes.chunks(chunk_size) {}
    }

    async fn upload_nar(
        &self,
        request: tonic::Request<tonic::Streaming<nar_copy::UploadNarRequest>>,
    ) -> std::result::Result<tonic::Response<nar_copy::UploadNarResponse>, tonic::Status> {
        todo!()
    }
}
