use std::env;
use std::path::PathBuf;

fn main() {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let lib = manifest.join("../../zig-out/lib").canonicalize().unwrap();
    println!("cargo:rustc-link-search=native={}", lib.display());
    println!("cargo:rustc-link-lib=glacier");
    println!("cargo:rustc-link-arg=-Wl,-rpath={}", lib.display());
    println!("cargo:rerun-if-changed={}", lib.join("libglacier.so").display());
    println!(
        "cargo:rerun-if-changed={}",
        manifest.join("../../include/glacier.h").display()
    );
}
