//! The Container Storage Interface's gRPC code (`proto/csi`, in this crate):
//! servers only, kubelet and the sidecars are the clients.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/csi");
    // A protoc of our own, and its well-known types: nothing to install.
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    // SAFETY: build scripts are single threaded.
    unsafe { std::env::set_var("PROTOC", protoc) };
    let wkt = protoc_bin_vendored::include_path().expect("vendored protobuf includes");
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile_protos(
            &["proto/csi/csi.proto"],
            &[std::path::PathBuf::from("proto"), wkt],
        )
        .expect("compiling the CSI proto");
}
