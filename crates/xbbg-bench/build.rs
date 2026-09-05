#[path = "../../scripts/rust_build_provenance.rs"]
mod build_provenance;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    build_provenance::emit()
}
