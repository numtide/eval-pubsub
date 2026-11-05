{
  pkgs,
  pubsub,
  lib ? pkgs.lib,
}:

let
  pubsub-submitter = lib.getExe' pubsub "pubsub-submitter";
  pubsub-builder = lib.getExe' pubsub "pubsub-builder";
  pubsub-cli = lib.getExe' pubsub "pubsub-cli";

  hello = {
    name = "hello";
    system = pkgs.stdenv.hostPlatform.system;
    builder = "/bin/sh";
    args = [
      "-c"
      "echo hello > $out"
    ];
  };

  helloDrvPath = builtins.unsafeDiscardStringContext (derivation hello).drvPath;

  flake = pkgs.writeTextDir "flake.nix" ''
    {
      outputs = _: {
        evalJobs = {
          hello = derivation ${pkgs.lib.generators.toPretty { } hello};
          broken = throw "boom";
        };
      };
    }
  '';

  submitJob = pkgs.writeText "submit-job.json" (
    builtins.toJSON {
      EvalJob.Flake = {
        flake_ref = "path:${flake}";
        attrs = [ "evalJobs" ];
      };
    }
  );
in

pkgs.testers.runNixOSTest {
  name = "pubsub-e2e";

  nodes = {
    submitter = {
      virtualisation.cores = 4;

      services.postgresql = {
        enable = true;
        ensureDatabases = [ "pubsub" ];
        ensureUsers = [
          {
            name = "pubsub";
            ensureDBOwnership = true;
          }
        ];
      };

      users.users.pubsub = {
        isSystemUser = true;
        group = "pubsub";
      };
      users.groups.pubsub = { };

      systemd.services.pubsub-submitter = {
        wantedBy = [ "multi-user.target" ];
        requires = [ "postgresql.target" ];
        after = [ "postgresql.target" ];
        environment = {
          DATABASE_URL = "host=/run/postgresql user=pubsub dbname=pubsub";
          PUBSUB_LISTEN = "[::]:10000";
          RUST_LOG = "debug";
        };
        serviceConfig = {
          User = "pubsub";
          ExecStart = pubsub-submitter;
        };
      };

      networking.firewall.allowedTCPPorts = [ 10000 ];
      environment.systemPackages = [ pkgs.grpcurl ];
    };

    builder = {
      nix.settings.experimental-features = [
        "nix-command"
        "flakes"
      ];

      system.extraDependencies = [ flake ];

      systemd.services.pubsub-builder = {
        wantedBy = [ "multi-user.target" ];
        wants = [ "network-online.target" ];
        after = [ "network-online.target" ];
        environment = {
          HOME = "/var/lib/pubsub-builder";
          RUST_LOG = "info";
        };
        serviceConfig = {
          StateDirectory = "pubsub-builder";
          ExecStart = pkgs.lib.escapeShellArgs [
            pubsub-builder
            "--submitter"
            "http://submitter:10000"
            "--state-dir"
            "/var/lib/pubsub-builder"
            "--store-uri"
            "ssh-ng://builder"
            "--nix-eval-jobs"
            "${lib.getExe pkgs.nix-eval-jobs}"
          ];
        };
      };
    };
  };

  testScript = ''
    def psql(query):
        return submitter.succeed(f"runuser -u postgres -- psql pubsub -tAc \"{query}\"").strip()

    start_all()

    submitter.wait_for_unit("pubsub-submitter.service")
    submitter.wait_for_open_port(10000)
    builder.wait_for_unit("pubsub-builder.service")

    builder_uuid = builder.succeed("cat /var/lib/pubsub-builder/uuid").strip()

    with subtest("builder registers and subscribes"):
        submitter.wait_until_succeeds(
            f"runuser -u postgres -- psql pubsub -tAc \"select active from builder where id = '{builder_uuid}'\" | grep -qx t",
            timeout=60,
        )

    with subtest("client submits an eval job"):
        submitter.succeed(
            '${pubsub-cli} --endpoint http://localhost:10000 submit-jobs "$(cat ${submitJob})"'
        )

    with subtest("builder evaluates and concludes the job"):
        submitter.wait_until_succeeds(
            "runuser -u postgres -- psql pubsub -tAc 'select count(*) from concluded_eval_job' | grep -qx 1",
            timeout=120,
        )
        assert psql("select builder_id from concluded_eval_job") == builder_uuid

    with subtest("job output carries evaluation results and errors"):
        output = psql("select line from job_output order by id")
        print(output)
        assert "evaluated evalJobs.hello: ${helloDrvPath}" in output.splitlines()
        assert "failed to evaluate evalJobs.broken:" in output
        assert "error: boom" in output
  '';
}
