import json

def psql(query):
    return submitter.succeed(f"runuser -u postgres -- psql pubsub -tAc \"{query}\"").strip()

start_all()

submitter.wait_for_unit("pubsub-submitter.service")
submitter.wait_for_open_port(10000)
builder.wait_for_unit("pubsub-builder.service")

builder_uuid = builder.succeed("cat /var/lib/pubsub-builder/uuid").strip()

with subtest("builder registers and subscribes"):
    output = submitter.succeed(f'{__pubsub_cli} --endpoint http://localhost:10000 list-builders 0 100').splitlines()
    builders = list(map(json.loads, output[1:]))

    print("registered builders:")
    for builder in builders:
        uuid = builder["information"]["uuid"]
        active = builder["active"]
        print(f"- {uuid} -> {active}")

    assert builders[0]["information"]["uuid"] == builder_uuid, "builder UUID doesn't match expected value"
    assert builders[0]["active"], "builde isn't active"

with subtest("client submits an eval job"):
    job_uuid = submitter.succeed(f'{__pubsub_cli} --endpoint http://localhost:10000 submit-jobs "@{__submit_job}"').strip()

with subtest("builder evaluates and concludes the job"):
    submitter.succeed(f'{__pubsub_cli} --endpoint http://localhost:10000 wait-jobs {job_uuid}')
    submitter.succeed(
        f"{__pubsub_cli} --endpoint http://localhost:10000 list-jobs 0 100 | {__jq} '.status' | grep -q 'Succeeded'"
    )
    assert psql("select builder_id from concluded_eval_job") == builder_uuid

with subtest("job output carries evaluation results and errors"):
    output = submitter.succeed(f"{__pubsub_cli} --endpoint http://localhost:10000 tail-jobs --end {job_uuid}")
    print(output)
    output = list(map(lambda str: str.strip(), output.splitlines()))
    assert f"evaluated evalJobs.hello: {__hello_drv_path}" in output
    assert "failed to evaluate evalJobs.broken:" in output
    assert "error: boom" in output
