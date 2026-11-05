use std::io::stdout;

use clap::Parser as _;
use futures::StreamExt as _;
use pubsub_client::Client;
use tokio::task::JoinSet;

mod cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(_) = std::env::var("RUST_LOG") {
        env_logger::init();
    } else {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .init();
    };

    let cli = cli::Cli::parse();

    log::info!("running with config {:?}", cli);

    let mut client = Client::new(cli.endpoint).await?;

    match cli.command {
        cli::Command::SubmitJobs { jobs } => {
            log::info!("submitting {} jobs", jobs.len());
            let mut uuids = Vec::with_capacity(jobs.len());
            for job in jobs {
                uuids.push(client.submit_job(job).await?);
            }
            for uuid in uuids {
                println!("{}", uuid);
            }
        }
        cli::Command::TailJobs { jobs } => {
            log::info!("tailing {} jobs", jobs.len());
            let mut streams = Vec::with_capacity(jobs.len());

            for job in jobs {
                streams.push(client.tail_job(job).await?)
            }

            let mut stream = futures::stream::select_all(streams);
            while let Some(item) = stream.next().await {
                match item {
                    Ok(line) => println!("{}", line), // TODO can we not use a formatting macro to just not use it?
                    Err(err) => log::error!("received GRPC error: {:?}", err),
                }
            }
        }
        cli::Command::WaitJobs { jobs } => {
            log::info!("waiting for {} jobs to conclude", jobs.len());
            let mut clients = Vec::with_capacity(jobs.len());

            for _ in 0..jobs.len() {
                clients.push(client.clone())
            }

            futures::future::join_all(
                jobs.into_iter()
                    .zip(clients)
                    .map(|(job, mut client)| async move { client.wait_job(job).await }),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        }
        cli::Command::ListJobs { page, count } => {
            let (jobs, total) = client.list_jobs(page, count).await?;

            println!("total: {}", total);
            for job in jobs {
                println!("{}", serde_json::to_string(&job)?);
            }
        }
        cli::Command::ListBuilders { page, count } => {
            let (builders, total) = client.list_builders(page, count).await?;

            println!("total: {}", total);
            for builder in builders {
                println!("{}", serde_json::to_string(&builder)?);
            }
        }
    }

    Ok(())
}
