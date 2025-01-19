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
                "proto/map/v1/map.proto",
                "proto/mapstream/v1/mapstream.proto",
                "proto/reduce/v1/reduce.proto",
                "proto/sessionreduce/v1/sessionreduce.proto",
                "proto/sideinput/v1/sideinput.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile protos");
}
