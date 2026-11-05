use thiserror::Error;

#[derive(Error, Debug)]
pub enum BuilderError {
    #[error("transport error {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error {0}")]
    Rpc(#[from] tonic::Status),
    #[error("io error {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid uuid {0}")]
    Uuid(#[from] uuid::Error),
    #[error("sysinfo error {0}")]
    Sysinfo(#[from] sysinfo::Error),
    #[error("field {field:?} missing")]
    FieldMissing { field: Vec<String> },
    #[error("submitter closed the job stream")]
    StreamClosed,
}

impl BuilderError {
    pub fn field_missing<S>(field: &[S]) -> Self
    where
        S: AsRef<str>,
    {
        Self::FieldMissing {
            field: field.iter().map(|x| x.as_ref().to_owned()).collect(),
        }
    }
}
