use thiserror::Error;

#[derive(Error, Debug)]
pub enum GrpcError {
    #[error("status {0}")]
    Status(#[from] tonic::Status),
    #[error("transport error {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("field missing {0:?}")]
    FieldMissing(Vec<String>),
}

impl GrpcError {
    pub fn field_missing<I, S>(path: I) -> GrpcError
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        GrpcError::FieldMissing(path.into_iter().map(|s| s.as_ref().to_owned()).collect())
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("GRPC {0}")]
    Grpc(#[from] GrpcError),
}
