use std::{io, ops::Deref};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("field {field:?} missing")]
    FieldMissing { field: Vec<String> },
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
}

#[derive(Error, Debug)]
pub enum SubmitterError {
    #[error("protocol error {0}")]
    Protocol(#[from] ProtocolError),
    #[error("error sending job to builder {builder_uuid}")]
    JobSend { builder_uuid: uuid::Uuid },
    #[error("job {job_uuid} not found")]
    JobNotFound { job_uuid: uuid::Uuid },
    #[error("job {job_uuid} was picked twice")]
    JobPickedTwice { job_uuid: uuid::Uuid },
}

impl From<SubmitterError> for tonic::Status {
    fn from(value: SubmitterError) -> Self {
        match value {
            SubmitterError::Protocol(protocol_error) => protocol_error.into(),
            SubmitterError::JobSend { builder_uuid: _ } => {
                tonic::Status::internal(value.to_string())
            }
            SubmitterError::JobNotFound { job_uuid: _ } => {
                tonic::Status::not_found(value.to_string())
            }
            SubmitterError::JobPickedTwice { job_uuid: _ } => {
                tonic::Status::internal(value.to_string())
            }
        }
    }
}

impl From<ProtocolError> for tonic::Status {
    fn from(value: ProtocolError) -> Self {
        match &value {
            ProtocolError::FieldMissing { field: _ } => {
                tonic::Status::invalid_argument(value.to_string())
            }
        }
    }
}
