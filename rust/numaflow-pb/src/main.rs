fn main() {
    // Common protobuf objects
    build_common();

    // gRPC clients for UDF
    build_client();

    // protobuf objects for serde
    build_objects();
}

fn build_common() {
    prost_build::Config::new()
        .out_dir("src/common")
        .compile_protos(&["proto/metadata.proto"], &["proto"])
        .expect("failed to compile common protos");
}

fn build_client() {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/clients")
        .extern_path(".metadata", "crate::common::metadata")
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
                "proto/serving/v1/store.proto",
                "proto/accumulator/v1/accumulator.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile protos");
}

fn build_objects() {
    prost_build::Config::new()
        .out_dir("src/objects")
        .extern_path(".metadata", "crate::common::metadata")
        .compile_protos(
            &[
                "proto/isb/message.proto",
                "proto/reduce/wal/wal.proto",
                "proto/watermark/watermark.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile protos");
}
