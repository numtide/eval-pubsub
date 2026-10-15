use std::sync::Arc;

use hyper_util::rt::TokioIo;
use pubsub_proto::pubsub::{
    self as proto, server::pub_sub_client::PubSubClient, server::pub_sub_server::PubSubServer,
};
use tempfile::NamedTempFile;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Notify, broadcast},
};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{
    Request,
    transport::{Channel, Endpoint, Server, Uri},
};
use tower::service_fn;
use uuid::Uuid;

use crate::pubsub;

async fn server_and_client_stub<Callback, F>(
    other: Callback,
    shutdown: Arc<Notify>,
) -> (impl Future<Output = ()>, PubSubClient<Channel>)
where
    Callback: FnOnce(pubsub::PubSubService) -> F,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let socket = NamedTempFile::new().unwrap();
    let socket = Arc::new(socket.into_temp_path());
    std::fs::remove_file(&*socket).unwrap();

    let uds = UnixListener::bind(&*socket).unwrap();
    let stream = UnixListenerStream::new(uds);

    let pubsub_service = pubsub::PubSubService::new();
    tokio::spawn(other(pubsub_service.clone()));
    let serve_future = async move {
        let result = Server::builder()
            .add_service(PubSubServer::new(pubsub_service))
            .serve_with_incoming_shutdown(stream, async { shutdown.notified().await })
            .await;
        // Server must be running fine...
        assert!(result.is_ok());
    };

    let socket = Arc::clone(&socket);
    // Connect to the server over a Unix socket
    // The URL will be ignored.
    let channel = Endpoint::try_from("http://any.url")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = Arc::clone(&socket);
            async move { UnixStream::connect(&*socket).await.map(TokioIo::new) }
        }))
        .await
        .unwrap();

    let client = PubSubClient::new(channel);

    (serve_future, client)
}

#[macro_export]
macro_rules! pubsub_submitter_test {
    ( $callback:expr, $client:expr ) => {
        let shutdown = Arc::new(Notify::new());

        let (serve_future, client) = server_and_client_stub($callback, shutdown.clone()).await;

        let request_future = async { ($client)(client).await };

        // Wait for completion, when the client request future completes
        tokio::select! {
            _ = serve_future => panic!("server returned first"),
            _ = request_future => shutdown.notify_one(),
        }
    };
}

// We do this somewhat complicated dance with Notify, to ensure that we test both the case
// where a job is already pending when a builder is first seen and when a builder has already
// been seen (and is listing jobs).
#[tokio::test]
async fn end_to_end() {
    let notify_submitted = Arc::new(Notify::new());
    let notify_listing = Arc::new(Notify::new());

    let eval_job_1 = pubsub::EvalJob {
        job_id: Uuid::new_v4(),
        r#type: pubsub::EvalJobType::Flake {
            flake_ref: "github:numtide/flake-utils".into(),
            attrs: ["evalJobs"].map(Into::into).into(),
        },
    };

    let eval_job_2 = pubsub::EvalJob {
        job_id: Uuid::new_v4(),
        r#type: pubsub::EvalJobType::Flake {
            flake_ref: "github:numtide/treefmt-nix".into(),
            attrs: ["evalJobs"].map(Into::into).into(),
        },
    };

    pubsub_submitter_test!(
        {
            let eval_job_1 = eval_job_1.clone();
            let eval_job_2 = eval_job_2.clone();
            let notify_submitted = notify_submitted.clone();
            let notify_listing = notify_listing.clone();

            async move |server| {
                server.submit_eval_job(eval_job_1).await.unwrap();
                notify_submitted.notify_one();
                notify_listing.notified().await;
                server.submit_eval_job(eval_job_2).await.unwrap();
            }
        },
        async |mut client: PubSubClient<Channel>| {
            notify_submitted.notified().await;

            let response = client
                .list_jobs(Request::new(proto::server::ListJobsRequest {
                    builder_information: Some(proto::server::BuilderInformation {
                        uuid: Some(Uuid::new_v4().into()),
                        system: String::from("x86_64-linux"),
                        smt_core_count: 32,
                        total_memory: 32 * 1024 * 1024 * 1024,
                    }),
                }))
                .await
                .unwrap();

            notify_listing.notify_one();

            let mut inbound_stream = response.into_inner();

            let proto::server::ListJobsResponse { job } = inbound_stream
                .message()
                .await
                .unwrap()
                .expect("didn't return a ListJobsResponse");
            let job_1 = job.expect("didn't return a job 1");
            assert_eq!(job_1, eval_job_1.into());

            let proto::server::ListJobsResponse { job } = inbound_stream
                .message()
                .await
                .unwrap()
                .expect("didn't return a ListJobsResponse");
            let job_2 = job.expect("didn't return a job 2");
            assert_eq!(job_2, eval_job_2.into());

            match client
                .pick_job(Request::new(proto::server::PickJobRequest {
                    job_id: job_1.job_id,
                }))
                .await
                .unwrap()
                .get_ref()
            {
                proto::server::PickJobResponse {
                    pick_response: Some(proto::server::pick_job_response::PickResponse::Allow(_)),
                } => {
                    println!("successfully picked a job")
                }
                proto::server::PickJobResponse {
                    pick_response: Some(proto::server::pick_job_response::PickResponse::Deny(_)),
                } => {
                    panic!("server denied the pick when an allow was expected")
                }
                proto::server::PickJobResponse {
                    pick_response: None,
                } => panic!("server didn't return a pick response"),
            };
        }
    );
}
