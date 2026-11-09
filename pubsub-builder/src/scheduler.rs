use std::sync::Arc;

use time::UtcDateTime;
use tokio::sync::{Semaphore, mpsc};

use crate::{
    executor::{Conclusion, Executor},
    output::OutputSink,
    submitter::Offer,
};

pub async fn run(executor: Executor, max_jobs: usize, mut offers: mpsc::Receiver<Offer>) {
    let slots = Arc::new(Semaphore::new(max_jobs));

    while let Some(offer) = offers.recv().await {
        if !executor.supports(&offer.job) {
            log::debug!("ignoring unsupported job {}", offer.job_id);
            continue;
        }

        let slot = slots
            .clone()
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");

        if pick(&offer).await {
            let executor = executor.clone();
            tokio::spawn(async move {
                run_job(&executor, offer).await;
                drop(slot);
            });
        }
    }
}

async fn pick(offer: &Offer) -> bool {
    match offer.submitter.pick_job(offer.job_id).await {
        Ok(true) => {
            log::info!(
                "picked job {} from submitter {}",
                offer.job_id,
                offer.submitter.uri()
            );
            true
        }
        Ok(false) => {
            log::info!("job {} was not allowed to be picked", offer.job_id);
            false
        }
        Err(err) => {
            log::warn!("failed to pick job {}: {}", offer.job_id, err);
            false
        }
    }
}

async fn run_job(executor: &Executor, offer: Offer) {
    let output = OutputSink::new(offer.submitter.clone(), offer.job_id);
    let conclusion = executor.run(offer.job, &output).await;

    if let Conclusion::Failure(reason) = &conclusion {
        output
            .line(format!("job failed: {}", reason), UtcDateTime::now())
            .await;
    }

    match offer.submitter.conclude_job(offer.job_id, conclusion).await {
        Ok(()) => log::info!("concluded job {}", offer.job_id),
        Err(err) => log::error!("failed to conclude job {}: {}", offer.job_id, err),
    }
}
