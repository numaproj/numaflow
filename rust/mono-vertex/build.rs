fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "proto/source.proto",
                "proto/sourcetransform.proto",
                "proto/sink.proto",
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("failed to compile the proto, {:?}", e))
}
