use std::{collections::HashMap, path::Path, process::Stdio};

use pubsub_proto::pubsub as proto;
use serde::Deserialize;
use time::UtcDateTime;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncRead, BufReader},
    process::Command,
};

use super::Conclusion;
use crate::output::OutputSink;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NixEvalJobsLineWeak {
    attr_path: Vec<String>,
    drv_path: Option<String>,
    system: Option<String>,
    outputs: Option<HashMap<String, String>>,
    required_system_features: Option<Vec<String>>,
    store_dir: Option<String>,
    error: Option<String>,
}

#[derive(Debug)]
enum NixEvalJobsLine {
    Success {
        attr_path: Vec<String>,
        drv_path: String,
        system: String,
        outputs: HashMap<String, String>,
        required_system_features: Vec<String>,
        store_dir: String,
    },
    Failure {
        attr_path: Vec<String>,
        error: String,
    },
}

impl TryFrom<NixEvalJobsLineWeak> for NixEvalJobsLine {
    type Error = Vec<String>;

    fn try_from(value: NixEvalJobsLineWeak) -> Result<Self, Self::Error> {
        match value {
            NixEvalJobsLineWeak {
                attr_path,
                drv_path: Some(drv_path),
                system: Some(system),
                outputs: Some(outputs),
                required_system_features: Some(required_system_features),
                store_dir: Some(store_dir),
                error: _,
            } => Ok(NixEvalJobsLine::Success {
                attr_path,
                drv_path,
                system,
                outputs,
                required_system_features,
                store_dir,
            }),
            NixEvalJobsLineWeak {
                attr_path,
                drv_path: _,
                system: _,
                outputs: _,
                required_system_features: _,
                store_dir: _,
                error: Some(error),
            } => Ok(NixEvalJobsLine::Failure { attr_path, error }),
            NixEvalJobsLineWeak { attr_path, .. } => Err(attr_path),
        }
    }
}

pub async fn run(
    nix_eval_jobs: &Path,
    eval_job: proto::common::EvalJob,
    output: &OutputSink,
) -> Conclusion {
    match eval_job.r#type {
        Some(proto::common::eval_job::Type::Flake(flake)) => {
            run_flake(nix_eval_jobs, flake, output).await
        }
        None => Conclusion::Failure("eval job has no type".to_owned()),
    }
}

async fn run_flake(
    nix_eval_jobs: &Path,
    flake: proto::common::eval_job::Flake,
    output: &OutputSink,
) -> Conclusion {
    if flake.attrs.is_empty() {
        return Conclusion::Failure("no attributes to evaluate".to_owned());
    }

    let mut attrs = Vec::new();
    for attr in flake.attrs {
        let installable = format!("{}#{}", flake.flake_ref, attr);
        match evaluate(nix_eval_jobs, &installable, &[attr], output).await {
            Ok(results) => attrs.extend(results),
            Err(reason) => return Conclusion::Failure(reason),
        }
    }

    Conclusion::Success(proto::server::conclude_job_request::Result::Eval(
        proto::common::EvalResult { attrs },
    ))
}

async fn evaluate(
    nix_eval_jobs: &Path,
    installable: &str,
    prefix: &[String],
    output: &OutputSink,
) -> Result<Vec<proto::common::eval_result::Attr>, String> {
    let mut child = Command::new(nix_eval_jobs)
        .arg("--flake")
        .arg(installable)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| format!("failed to start {}: {}", nix_eval_jobs.display(), err))?;

    let stdout = child.stdout.take().expect("stdout is piped");

    let attrs = collect_attrs(stdout, prefix, output).await;

    let status = child
        .wait()
        .await
        .map_err(|err| format!("failed to wait for nix-eval-jobs: {}", err))?;

    if status.success() {
        Ok(attrs)
    } else {
        Err(format!(
            "nix-eval-jobs failed on {} ({})",
            installable, status
        ))
    }
}

async fn collect_attrs<R>(
    reader: R,
    prefix: &[String],
    output: &OutputSink,
) -> Vec<proto::common::eval_result::Attr>
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    let mut attrs = Vec::new();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("{}", line);
        match parse_line(prefix, &line) {
            Ok((attr, error)) => {
                output
                    .line(summarize(&attr), Some(&attr.attr_path), UtcDateTime::now())
                    .await;
                for line in error.iter().flat_map(|error| error.lines()) {
                    output
                        .line(line, Some(&attr.attr_path), UtcDateTime::now())
                        .await;
                }
                attrs.push(attr);
            }
            Err(err) => {
                log::warn!("unparseable nix-eval-jobs output {:?}: {}", line, err);
                output.line(line, None, UtcDateTime::now()).await;
            }
        }
    }

    attrs
}

fn parse_line(
    prefix: &[String],
    line: &str,
) -> Result<(proto::common::eval_result::Attr, Option<String>), serde_json::Error> {
    let line = serde_json::from_str::<NixEvalJobsLineWeak>(line)?.try_into();

    let (attr_path, result, error) = match line {
        Ok(NixEvalJobsLine::Success {
            attr_path,
            drv_path,
            system,
            outputs,
            required_system_features,
            store_dir,
        }) => (
            attr_path,
            proto::common::eval_result::attr::Result::Derivation(
                proto::common::eval_result::Derivation {
                    drv_path,
                    // TODO can system be null/missing?
                    system,
                    outputs,
                    required_system_features,
                    store_dir,
                },
            ),
            None,
        ),
        Ok(NixEvalJobsLine::Failure { attr_path, error }) => (
            attr_path,
            proto::common::eval_result::attr::Result::Failed(proto::common::eval_result::Failed {}),
            Some(error),
        ),
        Err(attr_path) => (
            attr_path,
            proto::common::eval_result::attr::Result::Failed(proto::common::eval_result::Failed {}),
            Some("nix-eval-jobs returned neither a derivation nor an error".to_owned()),
        ),
    };

    Ok((
        proto::common::eval_result::Attr {
            attr_path: prefix.iter().cloned().chain(attr_path).collect(),
            result: Some(result),
        },
        error,
    ))
}

fn summarize(attr: &proto::common::eval_result::Attr) -> String {
    let attr_path = attr.attr_path.join(".");

    match &attr.result {
        Some(proto::common::eval_result::attr::Result::Derivation(derivation)) => {
            format!("evaluated {}: {}", attr_path, derivation.drv_path)
        }
        Some(proto::common::eval_result::attr::Result::Failed(_)) | None => {
            format!("failed to evaluate {}:", attr_path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::common::eval_result::{Derivation, Failed, attr::Result as AttrResult};

    #[test]
    fn parse_derivation() {
        let (attr, error) = parse_line(
            &["evalJobs".to_owned()],
            r#"{
                 "attr": "hello",
                 "attrPath": [ "hello" ],
                 "drvPath": "/nix/store/x-hello.drv",
                 "system":"x86_64-linux",
                 "outputs": {
                   "out": "/nix/store/x-hello"
                 },
                 "requiredSystemFeatures": [
                   "kvm",
                   "nixos-test"
                 ],
                 "storeDir": "/nix/store"
               }"#,
        )
        .unwrap();

        assert_eq!(error, None);
        assert_eq!(attr.attr_path, ["evalJobs", "hello"]);
        assert_eq!(
            attr.result,
            Some(AttrResult::Derivation(Derivation {
                drv_path: "/nix/store/x-hello.drv".to_owned(),
                system: "x86_64-linux".to_owned(),
                outputs: HashMap::from([("out".to_owned(), "/nix/store/x-hello".to_owned())]),
                required_system_features: Vec::from(&["kvm".to_owned(), "nixos-test".to_owned()]),
                store_dir: "/nix/store".to_owned(),
            }))
        );
        assert_eq!(error, None);
    }

    #[test]
    fn parse_failure() {
        let (attr, error) = parse_line(
            &["evalJobs".to_owned()],
            r#"{"attr":"broken","attrPath":["broken"],"error":"error: boom"}"#,
        )
        .unwrap();

        assert_eq!(attr.result, Some(AttrResult::Failed(Failed {})));
        assert_eq!(error.as_deref(), Some("error: boom"));
    }
}
