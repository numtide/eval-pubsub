use pubsub_proto::pubsub as proto;
use uuid::Uuid;

use crate::{
    error::{ProtocolError, SubmitterError},
    queue::Keyable,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildJob {
    pub derivation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalJob {
    Flake {
        flake_ref: String,
        attrs: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnonymousJob {
    EvalJob { eval_job: EvalJob },
    BuildJob { build_job: BuildJob },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub job_id: Uuid,
    pub anonymous_job: AnonymousJob,
}

impl Keyable for Job {
    type Key = Uuid;

    fn key(&self) -> Self::Key {
        self.job_id
    }
}

impl BuildJob {
    pub fn new<S>(derivation: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            derivation: derivation.as_ref().to_owned(),
        }
    }
}

impl EvalJob {
    pub fn new_flake<S1, S2, I>(flake_ref: S1, attrs: I) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        I: IntoIterator<Item = S2>,
    {
        Self::Flake {
            flake_ref: flake_ref.as_ref().to_owned(),
            attrs: attrs
                .into_iter()
                .map(|str| str.as_ref().to_owned())
                .collect(),
        }
    }
}

impl AnonymousJob {
    pub fn new_eval_job(eval_job: EvalJob) -> Self {
        Self::EvalJob { eval_job }
    }

    pub fn new_eval_job_flake<S1, S2, I>(flake_ref: S1, attrs: I) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        I: IntoIterator<Item = S2>,
    {
        Self::new_eval_job(EvalJob::new_flake(flake_ref, attrs))
    }

    pub fn new_build_job<S>(derivation: S) -> Self
    where
        S: AsRef<str>,
    {
        Self::BuildJob {
            build_job: BuildJob::new(derivation),
        }
    }
}

impl Job {
    pub fn new_eval_job(eval_job: EvalJob) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            anonymous_job: AnonymousJob::new_eval_job(eval_job),
        }
    }

    pub fn new_eval_job_flake<S1, S2, I>(flake_ref: S1, attrs: I) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        I: IntoIterator<Item = S2>,
    {
        Self {
            job_id: Uuid::new_v4(),
            anonymous_job: AnonymousJob::new_eval_job_flake(flake_ref, attrs),
        }
    }

    pub fn new_build_job<S>(derivation: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            job_id: Uuid::new_v4(),
            anonymous_job: AnonymousJob::new_build_job(derivation),
        }
    }

    pub fn new(anonymous_job: AnonymousJob) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            anonymous_job,
        }
    }
}

pub mod proto_conversion {
    use pubsub_proto::pubsub as proto;

    use crate::error::ProtocolError;

    impl From<super::EvalJob> for proto::common::EvalJob {
        fn from(value: super::EvalJob) -> Self {
            match value {
                super::EvalJob::Flake { flake_ref, attrs } => proto::common::EvalJob {
                    r#type: Some(proto::common::eval_job::Type::Flake(
                        proto::common::eval_job::Flake { flake_ref, attrs },
                    )),
                },
            }
        }
    }

    impl TryFrom<proto::common::EvalJob> for super::EvalJob {
        type Error = ProtocolError;

        fn try_from(value: proto::common::EvalJob) -> Result<Self, Self::Error> {
            match value.r#type {
                Some(proto::common::eval_job::Type::Flake(proto::common::eval_job::Flake {
                    flake_ref,
                    attrs,
                })) => Ok(super::EvalJob::Flake { flake_ref, attrs }),
                None => Err(ProtocolError::field_missing(&["type"])),
            }
        }
    }

    impl From<super::BuildJob> for proto::common::BuildJob {
        fn from(value: super::BuildJob) -> Self {
            let super::BuildJob { derivation } = value;
            proto::common::BuildJob { derivation }
        }
    }

    impl TryFrom<proto::common::BuildJob> for super::BuildJob {
        type Error = ProtocolError;

        fn try_from(value: proto::common::BuildJob) -> Result<Self, Self::Error> {
            let proto::common::BuildJob { derivation } = value;
            Ok(super::BuildJob { derivation })
        }
    }

    impl From<super::AnonymousJob> for proto::common::AnonymousJob {
        fn from(value: super::AnonymousJob) -> Self {
            match value {
                super::AnonymousJob::EvalJob { eval_job } => proto::common::AnonymousJob {
                    r#type: Some(proto::common::anonymous_job::Type::Eval(eval_job.into())),
                },
                super::AnonymousJob::BuildJob { build_job } => proto::common::AnonymousJob {
                    r#type: Some(proto::common::anonymous_job::Type::Build(build_job.into())),
                },
            }
        }
    }

    impl TryFrom<proto::common::AnonymousJob> for super::AnonymousJob {
        type Error = ProtocolError;

        fn try_from(value: proto::common::AnonymousJob) -> Result<Self, Self::Error> {
            match value.r#type {
                Some(proto::common::anonymous_job::Type::Eval(eval_job)) => {
                    let eval_job = eval_job.try_into()?;
                    Ok(super::AnonymousJob::EvalJob { eval_job })
                }
                Some(proto::common::anonymous_job::Type::Build(build_job)) => {
                    let build_job = build_job.try_into()?;
                    Ok(super::AnonymousJob::BuildJob { build_job })
                }
                None => Err(ProtocolError::field_missing(&["type"])),
            }
        }
    }

    impl From<super::Job> for proto::common::Job {
        fn from(value: super::Job) -> Self {
            let super::Job {
                job_id,
                anonymous_job,
            } = value;
            proto::common::Job {
                job_id: Some(job_id.into()),
                anonymous_job: Some(anonymous_job.into()),
            }
        }
    }

    impl TryFrom<proto::common::Job> for super::Job {
        type Error = ProtocolError;

        fn try_from(value: proto::common::Job) -> Result<Self, Self::Error> {
            match value {
                proto::common::Job { job_id: None, .. } => {
                    Err(ProtocolError::field_missing(&["job_id"]))
                }
                proto::common::Job {
                    anonymous_job: None,
                    ..
                } => Err(ProtocolError::field_missing(&["anonymous_job"])),
                proto::common::Job {
                    job_id: Some(job_id),
                    anonymous_job: Some(anonymous_job),
                } => {
                    let job_id = job_id.into();
                    let anonymous_job = anonymous_job.try_into()?;
                    Ok(super::Job {
                        job_id,
                        anonymous_job,
                    })
                }
            }
        }
    }

    impl From<super::Builder> for proto::common::Builder {
        fn from(value: super::Builder) -> Self {
            Self {
                information: Some(proto::common::BuilderInformation {
                    uuid: Some(value.uuid.into()),
                    system: value.system,
                    smt_core_count: value.smt_core_count,
                    total_memory: value.total_memory,
                    store_uri: value.store_uri,
                }),
                active: value.active,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Builder {
    pub uuid: Uuid,
    pub system: String,
    pub smt_core_count: i32,
    // TODO this really ought to be u64 but that would need a postgresl extension
    pub total_memory: u64,
    pub active: bool,
}

impl Builder {
    pub fn new_from_uuid_and_system<S>(uuid: &Uuid, system: S) -> Result<Self, SubmitterError>
    where
        S: AsRef<str>,
    {
        let sysinfo = sysinfo::System::new_with_specifics(
            sysinfo::RefreshKind::nothing()
                .with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()),
        )?;

        Ok(Builder {
            uuid: *uuid,
            system: system.as_ref().to_owned(),
            smt_core_count: std::thread::available_parallelism().unwrap().get() as i32,
            total_memory: sysinfo.total_memory(),
            active: false,
        })
    }
}

pub struct PickedJob {
    pub id: Uuid,
    pub builder_id: Uuid,
}

impl PickedJob {
    pub fn new(id: Uuid, builder_id: Uuid) -> Self {
        Self { id, builder_id }
    }
}
