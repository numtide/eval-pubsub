use std::sync::Arc;

use hyper_util::rt::TokioIo;
use pubsub_proto::{self as proto, pub_sub_client::PubSubClient, pub_sub_server::PubSubServer};
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

async fn server_and_client_stub<'a, Callback>(
    other: Callback,
    shutdown: Arc<Notify>,
) -> (impl Future<Output = ()>, PubSubClient<Channel>)
where
    Callback: AsyncFnOnce(&mut pubsub::PubSubService) -> (),
{
    let socket = NamedTempFile::new().unwrap();
    let socket = Arc::new(socket.into_temp_path());
    std::fs::remove_file(&*socket).unwrap();

    let uds = UnixListener::bind(&*socket).unwrap();
    let stream = UnixListenerStream::new(uds);

    let mut pubsub_service = pubsub::PubSubService::new();
    other(&mut pubsub_service).await;
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

// The actual test is here
#[tokio::test]
async fn add_merchant_test() {
    let eval_job = pubsub::EvalJob {
        job_id: Uuid::new_v4(),
        r#type: pubsub::EvalJobType::Flake {
            flake_ref: "github:numtide/flake-utils".into(),
            attrs: ["evalJobs"].map(Into::into).into(),
        },
    };

    pubsub_submitter_test!(
        {
            let eval_job = eval_job.clone();
            async |server| {
                server.submit_eval_job(eval_job).await.unwrap();
            }
        },
        async |mut client: PubSubClient<Channel>| {
            let response = client
            .list_jobs(Request::new(proto::ListJobsRequest {
                builder_information: Some(proto::BuilderInformation {
                    uuid: Some(Uuid::new_v4().into()),
                    system: String::from("x86_64-linux"),
                    smt_core_count: 32,
                    total_memory: 32 * 1024 * 1024 * 1024,
                })
            }))
            .await
            .unwrap();

            let mut inbound_stream = response.into_inner();
            let proto::ListJobsResponse { job } = inbound_stream.message().await.unwrap().expect("didn't return a ListJobsResponse");
            let job = job.expect("didn't return a job");
            assert_eq!(job, eval_job.into());
            println!("{:?}", job);

            match client.pick_job(Request::new(proto::PickJobRequest { job_id: job.job_id })).await.unwrap().get_ref() {
                proto::PickJobResponse { pick_response: Some(proto::pick_job_response::PickResponse::Allow(_)) } => {
                    println!("successfully picked a job")
                },
                proto::PickJobResponse { pick_response: Some(proto::pick_job_response::PickResponse::Deny(_)) } => {
                    panic!("server denied the pick when an allow was expected")
                },
                proto::PickJobResponse { pick_response: None } => panic!("server didn't return a pick response"),
            };
        }
    );
}
