{
  lib,
  rustPlatform,
  protobuf,
  libpq,
}:

rustPlatform.buildRustPackage {
  pname = "pubsub-eval";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../pubsub-builder
      ../pubsub-client
      ../pubsub-common
      ../pubsub-proto
      ../pubsub-submitter
    ];
  };

  cargoLock.lockFile = ../Cargo.lock;

  nativeBuildInputs = [ protobuf ];
  buildInputs = [ libpq ];

  doCheck = false;
}
