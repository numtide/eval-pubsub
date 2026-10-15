fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .compile_protos(
            &["proto/pubsub/server.proto", "proto/pubsub/client.proto"],
            &["proto/pubsub"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));

    Ok(())
}
