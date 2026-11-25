use std::{panic, sync::Arc};

use tokio::{
    sync::RwLock,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone)]
pub struct JobSet {
    join_set: Arc<RwLock<JoinSet<()>>>,
}

impl JobSet {
    pub fn new() -> Self {
        JobSet {
            join_set: Arc::new(RwLock::new(JoinSet::new())),
        }
    }

    pub async fn spawn<S, F1, F2>(&self, name: S, future: F1, finally: F2) -> JoinHandle<F2::Output>
    where
        S: AsRef<str>,
        F1: Future<Output = ()> + Send + 'static,
        F2: Future<Output = ()> + Send + 'static,
    {
        let name = name.as_ref().to_owned();
        log::trace!("spawning background task with name {}", name);
        let join_set = self.join_set.clone();
        tokio::spawn(async move {
            join_set.write().await.spawn(async move {
                future.await;
                log::trace!("background task with name {} exiting", name);
                finally.await
            });
        })
    }

    pub async fn wait_shutdown(self) {
        let mut join_set = self.join_set.write().await;
        join_set.abort_all();
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(_) => {}
                Err(err) if err.is_cancelled() => {}
                Err(err) => panic::resume_unwind(err.into_panic()),
            };
        }
    }
}
