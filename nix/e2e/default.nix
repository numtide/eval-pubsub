{
  pkgs,
  pubsub,
  lib ? pkgs.lib,
  nixosModules,
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
        package = pkgs.postgresql_18;
        ensureDatabases = [ "pubsub" ];
        ensureUsers = [
          {
            name = "pubsub";
            ensureDBOwnership = true;
          }
        ];
      };

      imports = [
        nixosModules."pubsubSubmitter"
      ];

      services.pubsub.submitter = {
        enable = true;
        openFirewall = true;
      };
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
          RUST_LOG = "pubsub_=trace";
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
    __pubsub_cli = "${pubsub-cli}"
    __jq = "${lib.getExe pkgs.jq}"
    __submit_job = "${submitJob}"
    __hello_drv_path = "${helloDrvPath}"

    ${builtins.readFile ./test.py}
  '';
}
