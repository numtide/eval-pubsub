use std::{path::Path, process::Stdio};

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
struct NixEvalJobsLine {
    attr_path: Vec<String>,
    drv_path: Option<String>,
    system: Option<String>,
    error: Option<String>,
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
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| format!("failed to start {}: {}", nix_eval_jobs.display(), err))?;

    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");

    let (attrs, ()) = tokio::join!(
        collect_attrs(stdout, prefix, output),
        forward_lines(stderr, output)
    );

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
        match parse_line(prefix, &line) {
            Ok((attr, error)) => {
                output.line(summarize(&attr), UtcDateTime::now()).await;
                for line in error.iter().flat_map(|error| error.lines()) {
                    output.line(line, UtcDateTime::now()).await;
                }
                attrs.push(attr);
            }
            Err(err) => {
                log::warn!("unparseable nix-eval-jobs output {:?}: {}", line, err);
                output.line(line, UtcDateTime::now()).await;
            }
        }
    }

    attrs
}

async fn forward_lines<R>(reader: R, output: &OutputSink)
where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        output
            .line(strip_ansi_escapes(&line), UtcDateTime::now())
            .await;
    }
}

fn strip_ansi_escapes(line: &str) -> String {
    let mut stripped = String::with_capacity(line.len());
    let mut chars = line.chars();

    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.next() == Some('[') {
                chars.find(|c| ('@'..='~').contains(c));
            }
        } else {
            stripped.push(c);
        }
    }

    stripped
}

fn parse_line(
    prefix: &[String],
    line: &str,
) -> Result<(proto::common::eval_result::Attr, Option<String>), serde_json::Error> {
    let line: NixEvalJobsLine = serde_json::from_str(line)?;

    let (result, error) = match (line.error, line.drv_path) {
        (Some(error), _) => (
            proto::common::eval_result::attr::Result::Failed(proto::common::eval_result::Failed {}),
            Some(error),
        ),
        (None, Some(drv_path)) => (
            proto::common::eval_result::attr::Result::Derivation(
                proto::common::eval_result::Derivation {
                    drv_path,
                    system: line.system.unwrap_or_default(),
                },
            ),
            None,
        ),
        (None, None) => (
            proto::common::eval_result::attr::Result::Failed(proto::common::eval_result::Failed {}),
            Some("nix-eval-jobs returned neither a derivation nor an error".to_owned()),
        ),
    };

    Ok((
        proto::common::eval_result::Attr {
            attr_path: prefix.iter().cloned().chain(line.attr_path).collect(),
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
            r#"{"attr":"hello","attrPath":["hello"],"drvPath":"/nix/store/x-hello.drv","system":"x86_64-linux"}"#,
        )
        .unwrap();

        assert_eq!(attr.attr_path, ["evalJobs", "hello"]);
        assert_eq!(
            attr.result,
            Some(AttrResult::Derivation(Derivation {
                drv_path: "/nix/store/x-hello.drv".to_owned(),
                system: "x86_64-linux".to_owned(),
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

    #[test]
    fn strip_colors() {
        assert_eq!(
            strip_ansi_escapes("\u{1b}[31;1merror:\u{1b}[0m boom"),
            "error: boom"
        );
    }
}
