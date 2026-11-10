import json
import shutil

endpoint = "http://localhost:10000"

def list_jobs(page, count, filter=None):
    jobs = submitter.succeed(
        f'{__pubsub_cli} --endpoint http://localhost:10000 list-jobs {page} {count}'
        + f' --filter {filter}' if filter is not None else ""
    ).strip()
    return list(map(json.loads, jobs.splitlines()))

start_all()

submitter.wait_for_unit("pubsub-submitter.service")
submitter.wait_for_open_port(10000)
builder.stop_job("pubsub-builder.service")

try:
    with subtest("client submits an eval job"):
        job_uuid_pre = submitter.succeed(f'{__pubsub_cli} --endpoint http://localhost:10000 submit-jobs "@{__submit_job}"').strip()

    with subtest("pending filter works"):
        jobs = list_jobs(0, 100, "pending")
        print(jobs)
        assert len(jobs) == 1
        assert jobs[0]["job"]["uuid"] == job_uuid_pre

        jobs = list_jobs(0, 100, "picked")
        assert len(jobs) == 0

        jobs = list_jobs(0, 100, "failure")
        assert len(jobs) == 0

        jobs = list_jobs(0, 100, "success")
        assert len(jobs) == 0

    builder.start_job("pubsub-builder.service")
    builder.wait_for_unit("pubsub-builder.service")

    builder_uuid = builder.wait_until_succeeds("cat /var/lib/pubsub-builder/uuid", timeout=10).strip()

    with subtest("builder registers and subscribes"):
        builders = submitter.succeed(f'{__pubsub_cli} --endpoint http://localhost:10000 list-builders 0 100')
        builders = list(map(json.loads, builders.splitlines()[1:]))

        print("registered builders:")
        for builder in builders:
            uuid = builder["information"]["uuid"]
            active = builder["active"]
            print(f"- {uuid} -> {active}")

        assert builders[0]["information"]["uuid"] == builder_uuid, "builder UUID doesn't match expected value"
        assert builders[0]["active"], "builder isn't active"

    with subtest("client submits an eval job"):
        job_uuid_post = submitter.succeed(f'{__pubsub_cli} --endpoint http://localhost:10000 submit-jobs "@{__submit_job}"').strip()

    with subtest("builder evaluates and concludes the job"):
        submitter.succeed(f'{__pubsub_cli} --endpoint http://localhost:10000 wait-jobs {job_uuid_post} {job_uuid_pre}')
        jobs = list_jobs(0, 100)
        print(jobs)
        for job in jobs:
            assert "Succeeded" in job["status"]
            assert job["job"]["uuid"] in [job_uuid_pre, job_uuid_post]
            assert job["status"]["Succeeded"]["builder_uuid"] == builder_uuid

    with subtest("job output carries evaluation results and errors"):
        output = submitter.succeed(f"{__pubsub_cli} --endpoint http://localhost:10000 tail-jobs --end {job_uuid_pre}")
        print(output)
        output = list(map(lambda str: str.strip(), output.splitlines()))
        assert len(output) == 11
        assert f"evaluated evalJobs.hello: {__hello_drv_path}" in output
        assert "failed to evaluate evalJobs.broken:" in output
        assert "error: boom" in output

        output = submitter.succeed(f"{__pubsub_cli} --endpoint http://localhost:10000 tail-jobs --end {job_uuid_post}")
        print(output)
        output = list(map(lambda str: str.strip(), output.splitlines()))
        assert len(output) == 11
        assert f"evaluated evalJobs.hello: {__hello_drv_path}" in output
        assert "failed to evaluate evalJobs.broken:" in output
        assert "error: boom" in output
except Exception as exc:
    with subtest("dump pubsub database for further inspection"):
        submitter.succeed("runuser -u postgres -- pg_dump pubsub -f /tmp/dump.sql ")

        submitter.copy_from_machine("/tmp/dump.sql", "postgres")
        shutil.copy(submitter.out_dir / "postgres" / "dump.sql", "dump.sql")
    raise exc
finally:
    with subtest("dump pubsub database for further inspection"):
        submitter.succeed("runuser -u postgres -- pg_dump pubsub -f /tmp/dump.sql ")

        submitter.copy_from_machine("/tmp/dump.sql", "dump.sql")
