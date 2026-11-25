use std::cmp::min;

use async_stream::stream;
use bytes::BufMut;
use futures::Stream;
use pubsub_proto::pubsub::nar_copy::{self, nar_copy_client};
use tonic::transport::Channel;

use crate::wire;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("encountered an Io error {0:?}")]
    Io(#[from] std::io::Error),
    #[error("encountered a Grpc transport error {0:?}")]
    GrpcTransport(#[from] tonic::transport::Error),
    #[error("encountered a Grpc status error {0:?}")]
    GrpcStatus(#[from] tonic::Status),
    #[error("chunk missing at offset {offset}")]
    ChunkMissing { offset: u64 },
    #[error("chunk missing at offset {offset}")]
    ChunkOutOfOrder { offset: u64 },
}

#[derive(Clone, Debug)]
pub struct Client<C> {
    tonic_client: nar_copy_client::NarCopyClient<C>,
}

trait StatusIntoOption {
    type Item;
    fn into_option(self) -> Result<Option<Self::Item>, tonic::Status>;
}

impl<A> StatusIntoOption for Result<A, tonic::Status> {
    type Item = A;
    fn into_option(self) -> Result<Option<A>, tonic::Status> {
        match self {
            Ok(res) => Ok(Some(res)),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl Client<Channel> {
    pub async fn new<S>(address: S) -> Result<Self, Error>
    where
        S: AsRef<str>,
    {
        nar_copy_client::NarCopyClient::connect(address.as_ref().to_owned())
            .await
            .map(|tonic_client| Client { tonic_client })
            .map_err(Into::into)
    }

    pub async fn download_nar_info(
        &mut self,
        hash: wire::StorePathHash,
    ) -> Result<Option<wire::ValidPathInfo>, Error> {
        let response = self
            .tonic_client
            .download_nar_info(tonic::Request::new(nar_copy::DownloadNarInfoRequest {
                hash: (&hash).into(),
            }))
            .await
            .into_option()?;

        match response.and_then(|response| response.into_inner().nar_info) {
            Some(nar_info) => Ok(Some(nar_info.try_into().unwrap())),
            None => Ok(None),
        }
    }

    pub async fn download_nar(
        &mut self,
        hash: wire::StorePathHash,
    ) -> Result<Option<(wire::ValidPathInfo, wire::Nar)>, Error> {
        let chunk_size = 512;
        let nar_info = self.download_nar_info(hash).await?;

        match nar_info {
            Some(nar_info) => {
                let mut nar_buf = Vec::new();
                nar_buf.resize(nar_info.info.nar_size as usize, 0);

                let response = self
                    .tonic_client
                    .download_nar(tonic::Request::new(nar_copy::DownloadNarRequest {
                        chunk_size,
                        hash: Into::<String>::into(&hash),
                    }))
                    .await
                    .into_option()?;
                match response {
                    Some(mut response) => {
                        let stream = response.get_mut();

                        let mut ix = 0;
                        while let Some(next_message) = stream.message().await? {
                            match &next_message.nar_chunk {
                                Some(nar_chunk) => {
                                    log::debug!(
                                        "expecting chunk {} got {}",
                                        ix * chunk_size,
                                        nar_chunk.offset
                                    );
                                    if nar_chunk.offset != ix * chunk_size {
                                        return Err(Error::ChunkOutOfOrder {
                                            offset: nar_chunk.offset,
                                        });
                                    }

                                    let mut reader = &nar_chunk.body[..];
                                    let start = nar_chunk.offset as usize;
                                    let end = min(
                                        (nar_chunk.offset + chunk_size) as usize,
                                        nar_buf.len(),
                                    );
                                    let mut writer = &mut nar_buf[..][start..end];
                                    std::io::copy(&mut reader, &mut writer)?;
                                    ix += 1;
                                }
                                None => {
                                    return Err(Error::ChunkMissing {
                                        offset: ix * chunk_size,
                                    });
                                }
                            }
                        }

                        Ok(Some((nar_info, wire::Nar::from(nar_buf))))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    async fn nar_info_nar_stream(
        nar_info: wire::ValidPathInfo,
        nar: wire::Nar,
    ) -> impl Stream<Item = nar_copy::UploadNarRequest> {
        let chunk_size = 512;

        stream! {
            yield nar_copy::UploadNarRequest {
                r#type: Some(nar_copy::upload_nar_request::Type::NarInfo(nar_info.into()))
            };

            for (ix, chunk) in nar.bytes.chunks(chunk_size).enumerate() {
                log::debug!("sending nar chunk");
                yield nar_copy::UploadNarRequest {
                    r#type: Some(nar_copy::upload_nar_request::Type::NarChunk(
                        nar_copy::NarChunk {
                            offset: (ix * chunk_size) as u64,
                            body: chunk.to_owned()
                        }
                    ))
                }
            }
        }
    }

    pub async fn upload_nar(
        &mut self,
        nar_info: wire::ValidPathInfo,
        nar: wire::Nar,
    ) -> Result<(), Error> {
        self.tonic_client
            .upload_nar(Self::nar_info_nar_stream(nar_info, nar).await)
            .await?;

        Ok(())
    }
}
