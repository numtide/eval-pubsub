pub mod error;
pub mod executor;
pub mod info;
pub mod output;
pub mod scheduler;
pub mod submitter;

use std::path::PathBuf;

use clap::Parser;
use tokio::sync::mpsc;

use crate::{executor::Executor, submitter::Submitter};

#[derive(Parser)]
struct Args {
    #[arg(
        long = "submitter",
        env = "PUBSUB_SUBMITTERS",
        value_delimiter = ',',
        required = true
    )]
    submitters: Vec<String>,
    #[arg(long, env = "PUBSUB_STATE_DIR")]
    state_dir: PathBuf,
    #[arg(long, env = "PUBSUB_STORE_URI")]
    store_uri: String,
    #[arg(long, env = "PUBSUB_SYSTEM", default_value_t = info::current_system())]
    system: String,
    #[arg(long, env = "PUBSUB_MAX_JOBS", default_value_t = 1)]
    max_jobs: usize,
    #[arg(long, env = "PUBSUB_NIX_EVAL_JOBS", default_value = "nix-eval-jobs")]
    nix_eval_jobs: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    let builder_uuid = info::load_or_create_uuid(&args.state_dir).await?;
    let builder_information = info::builder_information(builder_uuid, args.system, args.store_uri)?;

    log::info!("starting builder {}", builder_uuid);

    let (offers_sender, offers_receiver) = mpsc::channel(256);
    for uri in args.submitters {
        let submitter = Submitter::connect_lazy(uri, builder_uuid)?;
        tokio::spawn(submitter::run(
            submitter,
            builder_information.clone(),
            offers_sender.clone(),
        ));
    }
    drop(offers_sender);

    scheduler::run(
        Executor::new(args.nix_eval_jobs),
        args.max_jobs,
        offers_receiver,
    )
    .await;

    Ok(())
}
