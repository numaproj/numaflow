fn main() {
    // Common protobuf objects
    build_common();

    // gRPC clients for UDF
    build_client();

    // gRPC servers for the daemon server
    build_server();

    // protobuf objects for serde
    build_objects();
}

fn build_common() {
    let mut config = prost_build::Config::new();
    config
        .out_dir("src/common")
        // Use bytes::Bytes instead of Vec<u8> for bytes fields to avoid copying
        .bytes(&[".metadata"]);
    config
        .compile_protos(&["proto/metadata.proto"], &["proto"])
        .expect("failed to compile common protos");
}

fn build_client() {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/clients")
        .extern_path(".metadata", "crate::common::metadata")
        // Use bytes::Bytes instead of Vec<u8> for bytes fields to avoid copying
        .bytes(".sink.v1")
        .bytes(".source.v1")
        .bytes(".sourcetransformer.v1")
        .bytes(".map.v1")
        .bytes(".mapstream.v1")
        .bytes(".reduce.v1")
        .bytes(".sessionreduce.v1")
        .bytes(".sideinput.v1")
        .bytes(".serving.v1")
        .bytes(".accumulator.v1")
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

fn build_server() {
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(true)
        .out_dir("src/servers")
        .compile_protos(
            &[
                "proto/daemon/daemon.proto",
                "proto/mvtxdaemon/mvtxdaemon.proto",
                "proto/google/api/annotations.proto",
                "proto/google/api/http.proto",
            ],
            &["proto"],
        )
        .expect("failed to compile protos");
}

fn build_objects() {
    let mut config = prost_build::Config::new();
    config
        .out_dir("src/objects")
        .extern_path(".metadata", "crate::common::metadata")
        // Use bytes::Bytes instead of Vec<u8> for bytes fields to avoid copying
        .bytes(&[".isb", ".wal"]);
    config
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
