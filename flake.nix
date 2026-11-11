{
  inputs."flake-utils" = {
    url = "github:numtide/flake-utils";
  };

  inputs."treefmt-nix" = {
    url = "github:numtide/treefmt-nix";
    inputs."nixpkgs".follows = "nixpkgs";
  };

  inputs."nixpkgs" = {
    url = "https://channels.nixos.org/nixos-26.05/nixexprs.tar.zst";
  };

  outputs =
    inputs:
    let
      inherit (inputs."nixpkgs")
        lib
        ;

      pkgs' = lib.genAttrs systems (system: inputs."nixpkgs".legacyPackages.${system});
      systems = inputs."flake-utils".lib.defaultSystems ++ [
        "riscv-linux"
        "armv7l-linux"
      ];
    in
    {
      nixosModules = {
        pubsubSubmitter = lib.modules.importApply ./nix/nixos/modules/submitter.nix {
          pubsubRecipe = ./nix/package.nix;
        };
        pubsubBuilder = lib.modules.importApply ./nix/nixos/modules/builder.nix {
          pubsubRecipe = ./nix/package.nix;
        };
      };
    }
    // inputs."flake-utils".lib.eachSystem systems (system: {
      devShells =
        let
          pkgs = pkgs'.${system};
          treefmtEval = inputs."treefmt-nix".lib.evalModule pkgs ./treefmt.nix;
        in
        {
          default = pkgs.mkShell {
            shellHook = ''
              export FLAKE_ROOT=$PWD
              export DATABASE_URL="postgres:///pubsub?host=$(realpath $FLAKE_ROOT/postgres/)"

              . ${./shell-hook.sh}
            '';

            packages = with pkgs; [
              cargo
              cargo-expand
              rustc
              rustfmt
              nixfmt
              treefmtEval.config.build.wrapper
              protobuf
              rust-analyzer
              plantuml
              diesel-cli
              podman
              libpq
              nix-eval-jobs
              postgresql_18
            ];
          };
        };

      packages.default = pkgs'.${system}.callPackage ./nix/package.nix { };

      checks = lib.optionalAttrs (lib.elem system lib.platforms.linux) {
        e2e = import ./nix/e2e {
          pkgs = pkgs'.${system};
          pubsub = inputs.self.packages.${system}.default;
          inherit (inputs."self") nixosModules;
        };
      };
    });
}
