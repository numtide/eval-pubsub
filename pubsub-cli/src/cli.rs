use std::net::SocketAddr;

use pubsub_client::AnonymousJob;
use uuid::Uuid;

fn parse_anonymous_jobs(val: &str) -> Result<AnonymousJob, String> {
    let rules = serde_json::from_str(val).map_err(|e| e.to_string())?;
    Ok(rules)
}

#[derive(clap::Subcommand, PartialEq, Eq, Clone, Debug)]
pub enum Command {
    SubmitJobs {
        #[arg(value_parser=parse_anonymous_jobs)]
        jobs: Vec<AnonymousJob>,
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
