//! etcd's v3 API: the gRPC code for its protos (`proto/etcd`, at the
//! workspace root).

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../proto/etcd");
    // A protoc of our own: nothing to install.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    // SAFETY: build scripts are single threaded.
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &["../../proto/etcd/api/etcdserverpb/rpc.proto"],
            &["../../proto"],
        )
        .expect("compiling the etcd protos");
}
