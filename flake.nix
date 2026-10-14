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
    inputs."flake-utils".lib.eachSystem systems (system: {
      devShells =
        let
          pkgs = pkgs'.${system};
          treefmtEval = inputs."treefmt-nix".lib.evalModule pkgs ./treefmt.nix;
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              rustc
              treefmtEval.config.build.wrapper
              protobuf
              rust-analyzer
              plantuml
            ];
          };
        };
    });
}
