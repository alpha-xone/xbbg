# Bloomberg BLPAPI C++ SDK — Local Development

This directory is a local-development location only. The repository does not vendor Bloomberg SDK archives, headers, libraries, credentials, entitlements, or data rights. Use the helper scripts only if you are authorized to download and use the Bloomberg SDK under your Bloomberg agreements.

## Quick Start

```bash
# Add the latest SDK version (auto-detected)
bash ./scripts/sdktool.sh

# Add a specific version
bash ./scripts/sdktool.sh --version <version>

# List installed versions
bash ./scripts/sdktool.sh --list

# Remove a version
bash ./scripts/sdktool.sh --remove <version>

# Re-download and re-extract
bash ./scripts/sdktool.sh --version <version> --force

# Free disk space by clearing cached archives
bash ./scripts/sdktool.sh --clean-cache
```

```powershell
# Windows PowerShell equivalents
.\scripts\sdktool.ps1
.\scripts\sdktool.ps1 -Version <version>
.\scripts\sdktool.ps1 -List
.\scripts\sdktool.ps1 -Remove <version>
.\scripts\sdktool.ps1 -Version <version> -Force
.\scripts\sdktool.ps1 -CleanCache
```

## Layout

```
vendor/
└── blpapi-sdk/
    ├── README.md            # This file (tracked in git)
    ├── .cache/              # Downloaded archives (reusable)
    ├── 3.26.2.1/            # Extracted SDK
    │   ├── include/         # C/C++ headers (blpapi_*.h)
    │   ├── Darwin/          # macOS runtime libraries
    │   ├── Linux/           # Linux runtime libraries
    │   ├── lib/             # Windows import/runtime libraries
    │   ├── bin/             # Windows runtime DLLs + tools
    │   └── ...
    └── <another-version>/   # Another version (optional)
```

The build system (`crates/blpapi-sys/build.rs`) scans versioned subdirs under `BLPAPI_ROOT`
and picks the latest installed version automatically.

**Pixi users**: `pixi.toml` sets `BLPAPI_ROOT` via activation — no manual env setup needed.

## Notes

- Everything under `vendor/blpapi-sdk/` except this README is git-ignored.
- The official Python `blpapi` wheels already bundle the C++ runtime for supported platforms. Use the SDK here only for building from source or running C++ tooling, and only from an authorized source under your Bloomberg agreements.
- The helper scripts auto-detect the host OS and choose the Bloomberg archive format for that platform; they do not grant Bloomberg SDK redistribution rights or service access.
