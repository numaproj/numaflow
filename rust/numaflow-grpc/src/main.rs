fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/clients")
        .compile_protos(
            &[
                "proto/source/v1/source.proto",
                "proto/sourcetransform/v1/sourcetransform.proto",
                "proto/sink/v1/sink.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile protos");
}
