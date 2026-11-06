use std::{net::SocketAddr, path::PathBuf};

use pubsub_client::AnonymousJob;
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

#[derive(clap::Subcommand, PartialEq, Eq, Clone, Debug)]
pub enum Command {
    SubmitJobs {
        #[arg(value_parser=parse_anonymous_jobs)]
        jobs: Vec<Data>,
    },
    TailJobs {
        jobs: Vec<Uuid>,
    },
    WaitJobs {
        jobs: Vec<Uuid>,
    },
    ListJobs {
        page: i32,
        count: i32,
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
