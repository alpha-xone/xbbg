# CI Container Images

This directory contains the Rust CI container image:

- `docker/ci/Dockerfile`: Rust CI image with toolchain + `libclang`

The manylinux wheel image lives next to the Python distribution at
`py-xbbg/docker/manylinux/Dockerfile`.

Bloomberg SDK files are intentionally **not** baked into container images.
CI downloads the SDK at runtime to avoid redistributing the SDK in a public image registry.

## Local usage with Podman

The Dockerfile is OCI-compatible, so you can build and run it with Podman.

### Build the image

```bash
podman build -f docker/ci/Dockerfile -t xbbg-ci:local .
```

### Generate `blpapi-sys` bindings artifact locally

```bash
mkdir -p target/ci-bindings

podman run --rm \
  -v "$PWD:/work" \
  -w /work \
  -e BLPAPI_BINDINGS_EXPORT_PATH=/work/target/ci-bindings/bindings.rs \
  xbbg-ci:local \
  bash -lc '
    BLPAPI_VERSION=${BLPAPI_VERSION:-3.26.2.1}
    bash ./scripts/sdktool.sh --version "$BLPAPI_VERSION" --no-set-active
    export BLPAPI_ROOT=/work/vendor/blpapi-sdk/$BLPAPI_VERSION
    export LD_LIBRARY_PATH=/work/vendor/blpapi-sdk/$BLPAPI_VERSION/Linux:$LD_LIBRARY_PATH
    cargo build -p blpapi-sys
  '
```

### Validate clippy in the CI image

```bash
podman run --rm \
  -v "$PWD:/work" \
  -w /work \
  xbbg-ci:local \
  bash -lc '
    BLPAPI_VERSION=${BLPAPI_VERSION:-3.26.2.1}
    bash ./scripts/sdktool.sh --version "$BLPAPI_VERSION" --no-set-active
    export BLPAPI_ROOT=/work/vendor/blpapi-sdk/$BLPAPI_VERSION
    export LD_LIBRARY_PATH=/work/vendor/blpapi-sdk/$BLPAPI_VERSION/Linux:$LD_LIBRARY_PATH
    cargo clippy --workspace --all-targets -- -D warnings
  '
```

## Notes

- CI publishes images to `ghcr.io/<owner>/xbbg-ci` and `ghcr.io/<owner>/xbbg-manylinux`.
- The workflow in `.github/workflows/ci-rust.yml` consumes `ghcr.io/<owner>/xbbg-ci:latest` and `ghcr.io/<owner>/xbbg-manylinux:latest`.
- Bloomberg SDK is downloaded in CI job steps (runtime), not stored in container layers.
- Windows jobs still run on native `windows-latest` runners and consume the generated bindings artifact.
