#[path = "../../scripts/rust_build_provenance.rs"]
mod build_provenance;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    napi_build::setup();
    if matches!(std::env::var("CARGO_CFG_TARGET_OS").as_deref(), Ok("macos")) {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-headerpad_max_install_names");
    }
    build_provenance::emit()
}
