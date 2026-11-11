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
      imports = [
        nixosModules."pubsubSubmitter"
      ];

      virtualisation.cores = 4;

      services.postgresql = {
        enable = true;
        package = pkgs.postgresql_18;
        ensureDatabases = [ "pubsub-submitter" ];
        ensureUsers = [
          {
            name = "pubsub-submitter";
            ensureDBOwnership = true;
          }
        ];
      };

      services.pubsub.submitter = {
        enable = true;
        openFirewall = true;
        logLevel = "pubsub_=trace";
      };
    };

    builder = {
      imports = [
        nixosModules."pubsubBuilder"
      ];

      nix.settings.experimental-features = [
        "nix-command"
        "flakes"
      ];

      system.extraDependencies = [ flake ];

      services.pubsub.builder = {
        enable = true;
        logLevel = "pubsub_=trace";

        settings = {
          submitter = "http://submitter:10000";
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
