use std::{net::SocketAddr, path::PathBuf};

use pubsub_client::{AnonymousJob, JobFilter};
use uuid::Uuid;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Data {
    Inline { anonymous_job: AnonymousJob },
    File { filepath: PathBuf },
}

fn parse_anonymous_jobs(val: &str) -> Result<Data, String> {
    if let Some(char) = val.chars().nth(0)
        && char == '@'
    {
        Ok(Data::File {
            filepath: PathBuf::from(val.chars().skip(1).collect::<String>()),
        })
    } else {
        Ok(Data::Inline {
            anonymous_job: serde_json::from_str(val).map_err(|e| e.to_string())?,
        })
    }
}

fn parse_job_filter(val: &str) -> Result<JobFilter, String> {
    match val {
        "pending" => Ok(JobFilter::Pending),
        "picked" => Ok(JobFilter::Picked),
        "Succeeded" => Ok(JobFilter::Succeeded),
        "Failed" => Ok(JobFilter::Failed),
        _ => Err(format!("unknown job filter type: {}", val)),
    }
}

#[derive(clap::Subcommand, PartialEq, Eq, Clone, Debug)]
pub enum Command {
    SubmitJobs {
        #[arg(value_parser=parse_anonymous_jobs)]
        jobs: Vec<Data>,
    },
    TailJobs {
        jobs: Vec<Uuid>,
        #[arg(short, long)]
        follow: bool,
        #[arg(short, long)]
        end: bool,
        #[arg(short, long, default_value_t = 2000)]
        number: i64,
    },
    WaitJobs {
        jobs: Vec<Uuid>,
    },
    ListJobs {
        page: i32,
        count: i32,
        #[arg(value_parser=parse_job_filter)]
        filter: Option<JobFilter>,
    },
    ListBuilders {
        page: i32,
        count: i32,
    },
}

#[derive(clap::Parser, Debug, Eq, Clone, PartialEq)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub endpoint: String,
    #[command(subcommand)]
    pub command: Command,
}
