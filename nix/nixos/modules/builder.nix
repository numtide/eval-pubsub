{ pubsubRecipe }:
{
  lib,
  config,
  pkgs,
  ...
}:
let
  cfg = config.services.pubsub.builder;
  settingsFormat = pkgs.formats.json { };

in
{
  options.services.pubsub.builder = {
    enable = lib.mkEnableOption "Enable the pubsub builder service.";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage pubsubRecipe { };
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
    };

    settings = lib.mkOption {
      type = lib.types.submodule {
        freeformType = settingsFormat.type;

        options = {
          submitter = lib.mkOption {
            type = lib.types.str;
            default = "ssh-ng://127.0.0.1:10000";
          };

          nix-eval-jobs = lib.mkOption {
            type = lib.types.package;
            default = pkgs.nix-eval-jobs;
          };

          storeUri = lib.mkOption {
            type = lib.types.str;
            default = "http://submitter:10000";
          };
        };
      };
      description = ''
        Settings for the pubsub builder service.
      '';
    };

    configFile = lib.mkOption {
      type = lib.types.path;
      readOnly = true;
      internal = true;
      default = settingsFormat.generate "builder-config.json" cfg.settings;
    };
  };

  config = lib.mkIf cfg.enable {
    users.users.pubsub-builder = {
      isSystemUser = true;
      group = "pubsub-builder";
    };
    users.groups.pubsub-builder = { };

    systemd.services.pubsub-builder = {
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      environment = {
        HOME = "/var/lib/pubsub-builder";
        RUST_LOG = cfg.logLevel;
      };
      serviceConfig = {
        User = "pubsub-builder";
        Group = "pubsub-builder";
        StateDirectory = "pubsub-builder";
        ExecStart = pkgs.lib.escapeShellArgs [
          (lib.getExe' cfg.package "pubsub-builder")
          "--submitter"
          cfg.settings.submitter
          "--state-dir"
          "/var/lib/pubsub-builder"
          "--store-uri"
          cfg.settings.storeUri
          "--nix-eval-jobs"
          "${lib.getExe cfg.settings.nix-eval-jobs}"
        ];
      };
    };
  };
}
