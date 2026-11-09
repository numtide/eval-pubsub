use diesel_async::pooled_connection::deadpool::PoolError;
use thiserror::Error;

pub trait ThroughProtocolError {
    type Value;
    fn through(self) -> Result<Self::Value, SubmitterError>;
}

impl<T, E> ThroughProtocolError for Result<T, E>
where
    E: Into<SubmitterError>,
{
    type Value = T;

    fn through(self) -> Result<Self::Value, SubmitterError> {
        match self {
            Ok(ok) => Ok(ok),
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("field {field:?} missing")]
    FieldMissing { field: Vec<String> },
    #[error("metadata {metadata:?} missing")]
    MetadataMissing { metadata: String },
    #[error("metadata {metadata:?} invalid")]
    MetadataInvalid { metadata: String },
}

impl ProtocolError {
    pub fn field_missing<'a, S>(field: &'a [S]) -> Self
    where
        S: AsRef<str>,
    {
        Self::FieldMissing {
            field: field
                .as_ref()
                .iter()
                .map(|x| x.as_ref().to_owned())
                .collect(),
        }
    }

    pub fn metadata_missing<'a, S>(metadata: S) -> Self
    where
        S: AsRef<str>,
    {
        Self::MetadataMissing {
            metadata: metadata.as_ref().to_owned(),
        }
    }

    pub fn metadata_invalid<'a, S>(metadata: S) -> Self
    where
        S: AsRef<str>,
    {
        Self::MetadataInvalid {
            metadata: metadata.as_ref().to_owned(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SubmitterError {
    #[error("protocol error {0}")]
    Protocol(#[from] ProtocolError),
    #[error("error sending job to builder {builder_uuid}")]
    JobSend { builder_uuid: uuid::Uuid },
    #[error("job {job_uuid} not found")]
    JobNotFound { job_uuid: uuid::Uuid },
    #[error("job output not found")]
    JobOutputNotFound { job_output_id: i64 },
    #[error("job {job_uuid} was picked twice")]
    JobPickedTwice { job_uuid: uuid::Uuid },
    #[error("database pool error {0}")]
    DatabasePool(#[from] PoolError),
    #[error("database error {0}")]
    Database(#[from] diesel::result::Error),
    #[error("timestamp error {0}")]
    Timestamp(#[from] prost_types::TimestampError),
    #[error("unregistered")]
    Unregistered {},
    #[error("serde error {0}")]
    SerdeError(#[from] serde_json::error::Error),
    #[error("sysinfo error {0}")]
    SysinfoError(#[from] sysinfo::Error),
    #[error("unknown error")]
    Unknown,
}

impl From<SubmitterError> for tonic::Status {
    fn from(value: SubmitterError) -> Self {
        match value {
            SubmitterError::Protocol(protocol_error) => protocol_error.into(),
            SubmitterError::JobSend { .. } => tonic::Status::internal(value.to_string()),
            SubmitterError::JobNotFound { .. } => tonic::Status::not_found(value.to_string()),
            SubmitterError::JobPickedTwice { .. } => tonic::Status::internal(value.to_string()),
            SubmitterError::DatabasePool(pool_error) => {
                tonic::Status::internal(pool_error.to_string())
            }
            SubmitterError::Database(error) => tonic::Status::internal(error.to_string()),
            SubmitterError::Unregistered {} => {
                tonic::Status::failed_precondition("must register first")
            }
            SubmitterError::Unknown => tonic::Status::internal("unknown error"),
            SubmitterError::SerdeError(error) => tonic::Status::internal(error.to_string()),
            SubmitterError::JobOutputNotFound { .. } => tonic::Status::internal(value.to_string()),
            SubmitterError::SysinfoError(error) => tonic::Status::internal(error.to_string()),
            SubmitterError::Timestamp(timestamp_error) => {
                tonic::Status::internal(timestamp_error.to_string())
            }
        }
    }
}

impl From<()> for SubmitterError {
    fn from(_value: ()) -> Self {
        SubmitterError::Unknown
    }
}

impl From<ProtocolError> for tonic::Status {
    fn from(value: ProtocolError) -> Self {
        match &value {
            ProtocolError::FieldMissing { field: _ } => {
                tonic::Status::invalid_argument(value.to_string())
            }
            ProtocolError::MetadataMissing { metadata: _ } => {
                tonic::Status::invalid_argument(value.to_string())
            }
            ProtocolError::MetadataInvalid { metadata: _ } => {
                tonic::Status::invalid_argument(value.to_string())
            }
        }
    }
}
