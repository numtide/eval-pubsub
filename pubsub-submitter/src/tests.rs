use std::{
    str::FromStr as _,
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum_core::response::IntoResponse;
use core::convert::Infallible;
use hyper_util::rt::TokioIo;
use pubsub_proto::pubsub::{
    self as proto,
    client::{
        pub_sub_client_client::PubSubClientClient, pub_sub_client_server::PubSubClientServer,
    },
    server::{pub_sub_client::PubSubClient, pub_sub_server::PubSubServer},
};
use tempfile::NamedTempFile;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::Notify,
};
use tokio_postgres::{Config, NoTls};
use tokio_stream::{StreamExt as _, wrappers::UnixListenerStream};
use tonic::{
    Request,
    body::Body,
    server::NamedService,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, Endpoint, Server, Uri},
};
use tower::{Service, service_fn};
use uuid::Uuid;

use crate::{
    models,
    postgres::config::ConfigString as _,
    pubsub::{self, PubSubService},
};

async fn connect_with_changes<S, T, Change, Run>(database_url: S, change: Change, run: Run) -> T
where
    Change: AsyncFnOnce(&mut Config) -> (),
    Run: AsyncFnOnce(&tokio_postgres::Client) -> T,
    S: AsRef<str>,
{
    let mut connection_config = Config::from_str(database_url.as_ref()).unwrap();

    change(&mut connection_config).await;

    {
        let shutdown = Arc::new(Notify::new());
        let (client, connection) =
            tokio_postgres::connect(&connection_config.to_key_value_string(), NoTls)
                .await
                .unwrap();

        {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = async move {
                        if let Err(e) = connection.await {
                            panic!("connection error: {}", e);
                        }
                    } => {}
                    _ = shutdown.notified() => {}
                }
            });
        }

        let res = run(&client).await;

        shutdown.notify_one();

        res
    }
}

async fn with_test_database<S, Fn, T>(database_url: S, cb: Fn) -> T
where
    S: AsRef<str>,
    Fn: AsyncFnOnce(&str) -> T,
{
    let database_url = database_url.as_ref();
    let db_uuid = Uuid::new_v4().to_string().replace('-', "_");
    let dbname = format!("test_db_{}", db_uuid);

    connect_with_changes(
        database_url,
        async |_config| {},
        async |client| {
            client
                .simple_query(format!("CREATE DATABASE {}", dbname).as_str())
                .await
                .unwrap();
        },
    )
    .await;

    let mut connection_string = Config::from_str(&database_url).unwrap();
    connection_string.dbname(&dbname);

    let ret = cb(&connection_string.to_key_value_string()).await;

    connect_with_changes(
        database_url,
        async |_config| {},
        async |client| {
            client
                .simple_query(format!("DROP DATABASE {}", dbname).as_str())
                .await
                .unwrap();
        },
    )
    .await;

    ret
}

struct StandardInterceptor {
    builder_uuid: Uuid,
}

impl StandardInterceptor {
    fn new(builder_uuid: Uuid) -> StandardInterceptor {
        StandardInterceptor { builder_uuid }
    }
}

impl Interceptor for StandardInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let metadata = request.metadata_mut();

        metadata.insert(
            "x-builder-uuid",
            self.builder_uuid.to_string().parse().unwrap(),
        );

        Ok(request)
    }
}

trait StubGrpc {
    type Service;
    type Server;
    type Client;

    async fn make_service(&self, database_url: &'_ str) -> Self::Service;
    async fn make_server(&self, service: Self::Service) -> Self::Server;
    async fn make_client(&self, channel: Channel) -> Self::Client;
    async fn wait_shutdown(&self, service: Self::Service) -> ();
}

async fn server_and_client_stub<G, Run, RunFuture, A>(stub_grpc: G, run: Run) -> A
where
    G: StubGrpc + Clone,
    G::Service: Clone + Send + Sync,
    G::Server: Send
        + Sync
        + Clone
        + NamedService
        + 'static
        + Service<http::Request<Body>, Error = Infallible>,
    <G::Server as Service<http::Request<Body>>>::Future: Send,
    <G::Server as Service<http::Request<Body>>>::Response: IntoResponse,
    Run: FnOnce(G::Service, G::Client) -> RunFuture,
    RunFuture: Future<Output = A> + Send + 'static,
{
    let shutdown = Arc::new(Notify::new());
    let socket = NamedTempFile::new().unwrap();
    let socket = Arc::new(socket.into_temp_path());
    std::fs::remove_file(&*socket).unwrap();

    let uds = UnixListener::bind(&*socket).unwrap();
    let stream = UnixListenerStream::new(uds);

    with_test_database(
        std::env::var("DATABASE_URL").unwrap(),
        async |database_url| {
            let service = stub_grpc.make_service(database_url).await;

            let serve_future = {
                let shutdown = shutdown.clone();
                let service = service.clone();
                let stub_grpc = stub_grpc.clone();
                async move {
                    let result = Server::builder()
                        .add_service(stub_grpc.make_server(service).await)
                        .serve_with_incoming_shutdown(stream, async { shutdown.notified().await })
                        .await;
                    // Server must be running fine...
                    assert!(result.is_ok());
                }
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

            // let client =
            //     PubSubClient::with_interceptor(channel, StandardInterceptor::new(builder_uuid));

            let run_future = run(service.clone(), stub_grpc.make_client(channel).await);

            tokio::select! {
                _ = serve_future => panic!("server returned first"),
                res = run_future => { shutdown.notify_one(); stub_grpc.wait_shutdown(service).await; res },
            }
        },
    )
    .await
}

#[derive(Copy, Clone)]
struct SubmitterGrpcStub {
    builder_uuid: Uuid,
}

impl StubGrpc for SubmitterGrpcStub {
    type Service = PubSubService;
    type Server = PubSubServer<PubSubService>;
    type Client = PubSubClient<InterceptedService<Channel, StandardInterceptor>>;

    async fn make_service(&self, database_url: &'_ str) -> Self::Service {
        let mut service = pubsub::PubSubService::new(database_url).unwrap();
        service.clear_db().await;
        service.run_migrations().unwrap();
        service
    }

    async fn make_server(&self, service: Self::Service) -> Self::Server {
        PubSubServer::new(service)
    }

    async fn make_client(&self, channel: Channel) -> Self::Client {
        PubSubClient::with_interceptor(channel, StandardInterceptor::new(self.builder_uuid))
    }

    async fn wait_shutdown(&self, service: Self::Service) -> () {
        service.wait_shutdown().await
    }
}

#[derive(Clone)]
struct ClientGrpcStub {
    service: PubSubService,
}

impl StubGrpc for ClientGrpcStub {
    type Service = PubSubService;
    type Server = PubSubClientServer<PubSubService>;
    type Client = PubSubClientClient<Channel>;

    async fn make_service(&self, _database_url: &'_ str) -> Self::Service {
        self.service.clone()
    }

    async fn make_server(&self, service: Self::Service) -> Self::Server {
        PubSubClientServer::new(service)
    }

    async fn make_client(&self, channel: Channel) -> Self::Client {
        PubSubClientClient::new(channel)
    }

    async fn wait_shutdown(&self, service: Self::Service) -> () {}
}

const JOB_1: LazyLock<models::AnonymousJob> = LazyLock::new(|| {
    models::AnonymousJob::new_eval_job_flake("github:numtide/flake-utils", ["evalJobs"])
});
const JOB_2: LazyLock<models::Job> =
    LazyLock::new(|| models::Job::new_eval_job_flake("github:numtide/treefmt-nix", ["evalJobs"]));

#[tokio::test]
async fn concluded_jobs_arent_pending() {
    pretty_env_logger::init();
    let builder_uuid = Uuid::new_v4();
    let job_1 = (*JOB_1).clone();

    server_and_client_stub(
        SubmitterGrpcStub { builder_uuid },
        async move |service, mut client| {
            server_and_client_stub(
                ClientGrpcStub { service },
                async move |_, mut client_client| {
                    client_client
                        .submit_job(Request::new(proto::client::SubmitJobRequest {
                            job: Some((*JOB_1).clone().into()),
                        }))
                        .await
                        .unwrap();

                    let response = client_client
                        .list_jobs(Request::new(proto::client::ListJobsRequest {
                            page: 0,
                            count: 100,
                        }))
                        .await
                        .unwrap();

                    let response = response.into_inner();
                    assert_eq!(response.total, 1);
                    assert_eq!(
                        response
                            .jobs
                            .into_iter()
                            .map(|j| j.anonymous_job)
                            .collect::<Option<Vec<_>>>(),
                        Some(vec![job_1.into()])
                    );
                },
            )
            .await;
        },
    )
    .await;
}

// We do this somewhat complicated dance with Notify, to ensure that we test both the case
// where a job is already pending when a builder is first seen and when a builder has already
// been seen (and is listing jobs).
#[tokio::test]
async fn end_to_end() {
    pretty_env_logger::init();
    let job_2_source = (*JOB_2).clone();
    let builder_uuid = Uuid::new_v4();

    server_and_client_stub(
        SubmitterGrpcStub { builder_uuid },
        async move |service, mut client| {
            service.submit_job(job_2_source.clone()).await.unwrap();

            server_and_client_stub(
                ClientGrpcStub { service },
                async move |_, mut client_client| {
                    client
                        .register(Request::new(proto::server::RegisterRequest {
                            builder_information: Some(proto::common::BuilderInformation {
                                uuid: Some(builder_uuid.into()),
                                system: String::from("x86_64-linux"),
                                smt_core_count: 32,
                                total_memory: 32 * 1024 * 1024 * 1024,
                            }),
                        }))
                        .await
                        .unwrap();

                    let response = client
                        .list_jobs(Request::new(proto::server::ListJobsRequest {}))
                        .await
                        .unwrap();

                    client_client
                        .submit_job(Request::new(proto::client::SubmitJobRequest {
                            job: Some((*JOB_1).clone().into()),
                        }))
                        .await
                        .unwrap();

                    let mut inbound_stream = response.into_inner();

                    let proto::server::ListJobsResponse { job } = inbound_stream
                        .message()
                        .await
                        .unwrap()
                        .expect("didn't return a ListJobsResponse");
                    let job_2 = job.expect("didn't return a job 2");
                    assert_eq!(
                        TryInto::<models::Job>::try_into(job_2).unwrap(),
                        job_2_source
                    );

                    let proto::server::ListJobsResponse { job } = inbound_stream
                        .message()
                        .await
                        .unwrap()
                        .expect("didn't return a ListJobsResponse");
                    let job_1 = job.expect("didn't return a job 1");
                    assert_eq!(
                        job_1
                            .anonymous_job
                            .expect("didn't return job_1.anonymous_job"),
                        (*JOB_1).clone().into()
                    );

                    match client
                        .pick_job(Request::new(proto::server::PickJobRequest {
                            job_id: job_1.job_id,
                        }))
                        .await
                        .unwrap()
                        .get_ref()
                    {
                        proto::server::PickJobResponse {
                            pick_response:
                                Some(proto::server::pick_job_response::PickResponse::Allow(_)),
                        } => {
                            println!("successfully picked a job")
                        }
                        proto::server::PickJobResponse {
                            pick_response:
                                Some(proto::server::pick_job_response::PickResponse::Deny(_)),
                        } => {
                            panic!("server denied the pick when an allow was expected")
                        }
                        proto::server::PickJobResponse {
                            pick_response: None,
                        } => panic!("server didn't return a pick response"),
                    };

                    match client
                        .pick_job(Request::new(proto::server::PickJobRequest {
                            job_id: job_1.job_id,
                        }))
                        .await
                        .unwrap()
                        .get_ref()
                    {
                        proto::server::PickJobResponse {
                            pick_response:
                                Some(proto::server::pick_job_response::PickResponse::Allow(_)),
                        } => panic!(
                            "server allowed a pick when the job has been already picked by us"
                        ),
                        proto::server::PickJobResponse {
                            pick_response:
                                Some(proto::server::pick_job_response::PickResponse::Deny(_)),
                        } => println!("successfully failed to double pick a job"),
                        proto::server::PickJobResponse {
                            pick_response: None,
                        } => panic!("server didn't return a pick response"),
                    }

                    tokio::join!(
                        async {
                            let tail_job_response = client_client
                                .tail_job(Request::new(proto::client::TailJobRequest {
                                    job_id: job_1.job_id,
                                }))
                                .await
                                .unwrap();
                            let job_output_stream = tail_job_response.into_inner();

                            println!("reading the stream");
                            let job_output = job_output_stream
                                .take(4)
                                .collect::<Result<Vec<_>, _>>()
                                .await
                                .unwrap();

                            assert_eq!(
                                vec![
                                    proto::client::TailJobResponse {
                                        line: "job 1 has been started".to_owned()
                                    },
                                    proto::client::TailJobResponse {
                                        line: "churnining....".to_owned()
                                    },
                                    proto::client::TailJobResponse {
                                        line: "almost done......".to_owned()
                                    },
                                    proto::client::TailJobResponse {
                                        line: "done!".to_owned()
                                    }
                                ],
                                job_output
                            );
                        },
                        async {
                            // TODO race condition?
                            // tokio::time::sleep(Duration::from_secs(4)).await;
                            client
                                .job_output(Request::new(proto::server::JobOutputRequest {
                                    job_id: job_1.job_id,
                                    output: vec![
                                        "job 1 has been started",
                                        "churnining....",
                                        "almost done......",
                                        "done!",
                                    ]
                                    .into_iter()
                                    .map(ToOwned::to_owned)
                                    .collect(),
                                }))
                                .await
                                .unwrap();
                        }
                    );

                    tokio::join!(
                        async {
                            client_client
                                .wait_job(Request::new(proto::client::WaitJobRequest {
                                    job_id: job_1.job_id,
                                }))
                                .await
                                .unwrap()
                        },
                        async {
                            match client
                                .conclude_job(Request::new(proto::server::ConcludeJobRequest {
                                    job_id: job_1.job_id,
                                }))
                                .await
                                .unwrap()
                                .get_ref()
                            {
                                proto::server::ConcludeJobResponse {} => println!("concluded job!"),
                            };
                        }
                    );

                    match client
                        .pick_job(Request::new(proto::server::PickJobRequest {
                            job_id: job_1.job_id,
                        }))
                        .await
                    {
                        Ok(_) => panic!("received unexpected allow/deny for a known concluded job"),
                        Err(status) if status.code() == tonic::Code::NotFound => {
                            println!(
                                "job was successfully reported as not found, since it's concluded"
                            )
                        }
                        Err(_) => panic!("received another error than \"not found\""),
                    }
                },
            )
            .await
        },
    )
    .await;
}
