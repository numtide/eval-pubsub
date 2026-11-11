{ pubsubRecipe }:
{
  lib,
  config,
  pkgs,
  ...
}:
let
  cfg = config.services.pubsub.submitter;
  settingsFormat = pkgs.formats.json { };

in
{
  options.services.pubsub.submitter = {
    enable = lib.mkEnableOption "Enable the pubsub submitter service.";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage pubsubRecipe { };
    };

    port = lib.mkOption {
      type = lib.types.port;
      description = ''
        Port number to liste on.
      '';
      default = 10000;
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
    };

    settings = lib.mkOption {
      type = lib.types.submodule {
        freeformType = settingsFormat.type;

        options = {
          listenAddress = lib.mkOption {
            type = lib.types.listOf lib.types.str;
            description = ''
              A list of addresses to listen on.
            '';
            default = [
              "0.0.0.0:${toString cfg.port}"
              "[::]:${toString cfg.port}"
            ];
          };

          databaseUrl = lib.mkOption {
            type = lib.types.str;
            description = ''
              Address of the PostgreSQL database the pubsub submitter service will use.
            '';
            default = "host=/run/postgresql user=pubsub dbname=pubsub";
          };
        };
      };
      description = ''
        Settings for the pubsub submitter service.
      '';
    };

    configFile = lib.mkOption {
      type = lib.types.path;
      readOnly = true;
      internal = true;
      default = settingsFormat.generate "submitter-config.json" cfg.settings;
    };

    openFirewall = lib.mkEnableOption "Open the firewall ports required for the pubsub submitter service.";
  };

  config = lib.mkIf cfg.enable {
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
        RUST_LOG = cfg.logLevel;
      };
      serviceConfig = {
        User = "pubsub";
        ExecStart = "${lib.getExe' cfg.package "pubsub-submitter"} --config-file ${cfg.configFile}";
      };
    };

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ cfg.port ];
  };
}
