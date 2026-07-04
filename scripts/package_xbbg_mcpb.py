#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

UNSUPPORTED_PLATFORM_MESSAGE = (
    "MCPB supports macOS arm64, Linux amd64, and Windows amd64; use GitHub release assets "
    "or build from source for this platform"
)
MISSING_REAL_BINARY_MESSAGE = (
    "could not locate the real xbbg-mcp binary. Reinstall it or set XBBG_MCP_REAL_BIN."
)
MISSING_RUNTIME_MESSAGE = (
    "could not locate the Bloomberg runtime library. Set XBBG_MCP_LIB_DIR, BLPAPI_LIB_DIR, "
    "or BLPAPI_ROOT, or install Bloomberg's blpapi package."
)

README_TEMPLATE = """# xbbg MCP

Local stdio MCP server for authorized Bloomberg/xbbg users.

Version: {version}

This MCPB bundles the xbbg MCP launchers and prebuilt xbbg MCP binaries for macOS arm64, Linux amd64, and Windows amd64. It does not bundle Bloomberg SDK files, Bloomberg runtime libraries, Bloomberg credentials, or market data. You must provide Bloomberg runtime access locally through your own Bloomberg agreements and entitlements.

The macOS/Linux launcher searches for Bloomberg runtime libraries through `XBBG_MCP_LIB_DIR`, `BLPAPI_LIB_DIR`, `BLPAPI_ROOT`, a vendored authorized SDK layout, or the official Python `blpapi` package. The Windows launcher uses the same precedence and prepends the resolved directory to `PATH` before launching `xbbg-mcp.exe`.

Documentation: https://github.com/xbbg-org/xbbg/tree/main/apps/xbbg-mcp
Privacy policy: https://github.com/xbbg-org/xbbg/tree/main/apps/xbbg-mcp#privacy-policy
"""

POSIX_FIND_REAL_BINARY = f'''find_real_binary() {{
    home=$(script_dir)
    os_name=$(uname -s 2>/dev/null || printf 'unknown')
    arch_name=$(uname -m 2>/dev/null || printf 'unknown')

    case "$os_name:$arch_name" in
        Darwin:arm64|Darwin:aarch64)
            real_bin="$home/bin/darwin-arm64/xbbg-mcp-real"
            ;;
        Linux:x86_64|Linux:amd64)
            real_bin="$home/bin/linux-amd64/xbbg-mcp-real"
            ;;
        *)
            die "{UNSUPPORTED_PLATFORM_MESSAGE}"
            ;;
    esac

    if [ ! -x "$real_bin" ]; then
        die "{MISSING_REAL_BINARY_MESSAGE}"
    fi

    export XBBG_MCP_REAL_BIN="$real_bin"
    printf '%s\\n' "$XBBG_MCP_REAL_BIN"
    return 0
}}'''

WINDOWS_LAUNCHER = rf'''$ErrorActionPreference = "Stop"

function Warn($message) {{
    [Console]::Error.WriteLine("xbbg-mcp: $message")
}}

function Die($message) {{
    Warn $message
    exit 1
}}

function Contains-BlpapiLib($dir) {{
    if (-not $dir) {{
        return $false
    }}
    if (-not (Test-Path -LiteralPath $dir -PathType Container)) {{
        return $false
    }}

    foreach ($name in @("blpapi3_64.dll", "blpapi3_32.dll")) {{
        if (Test-Path -LiteralPath (Join-Path $dir $name) -PathType Leaf) {{
            return $true
        }}
    }}

    return $false
}}

function Resolve-SdkRootLayout($root) {{
    if (-not $root) {{
        return $null
    }}
    if (-not (Test-Path -LiteralPath $root -PathType Container)) {{
        return $null
    }}

    foreach ($candidate in @(
        $root,
        (Join-Path $root "bin"),
        (Join-Path $root "lib"),
        (Join-Path $root "lib64")
    )) {{
        if (Contains-BlpapiLib $candidate) {{
            return $candidate
        }}
    }}

    $versionChildren = Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue |
        Where-Object {{ $_.Name -match '^\d+\.\d+\.\d+(\.\d+)?$' }} |
        Sort-Object -Descending -Property @{{ Expression = {{
            (($_.Name -split '\\.') | ForEach-Object {{ "{{0:D8}}" -f [int]$_ }}) -join '.'
        }} }}

    foreach ($child in $versionChildren) {{
        foreach ($candidate in @(
            $child.FullName,
            (Join-Path $child.FullName "bin"),
            (Join-Path $child.FullName "lib"),
            (Join-Path $child.FullName "lib64")
        )) {{
            if (Contains-BlpapiLib $candidate) {{
                return $candidate
            }}
        }}
    }}

    return $null
}}

function Find-PythonBlpapiDir() {{
    $pythonScript = @'
from pathlib import Path

try:
    import blpapi
except Exception as exc:
    raise SystemExit(1) from exc

module_path = getattr(blpapi, "__file__", None)
if not module_path:
    raise SystemExit(1)

path = Path(module_path).resolve().parent
for candidate in (path, path / "lib"):
    if any((candidate / name).is_file() for name in ("blpapi3_64.dll", "blpapi3_32.dll")):
        print(candidate)
        raise SystemExit(0)

raise SystemExit(1)
'@

    foreach ($python in @("py", "python", "python3")) {{
        if (-not (Get-Command $python -ErrorAction SilentlyContinue)) {{
            continue
        }}

        $output = & $python -c $pythonScript 2>$null
        if ($LASTEXITCODE -eq 0 -and $output) {{
            return ($output | Select-Object -First 1)
        }}
    }}

    return $null
}}

function Find-VendoredSdkDir($baseDir) {{
    $vendorRoot = Join-Path $baseDir "vendor\blpapi-sdk"
    return Resolve-SdkRootLayout $vendorRoot
}}

function Find-RuntimeLibDir() {{
    if ($env:XBBG_MCP_LIB_DIR -and (Contains-BlpapiLib $env:XBBG_MCP_LIB_DIR)) {{
        return $env:XBBG_MCP_LIB_DIR
    }}

    if ($env:BLPAPI_LIB_DIR -and (Contains-BlpapiLib $env:BLPAPI_LIB_DIR)) {{
        return $env:BLPAPI_LIB_DIR
    }}

    if ($env:BLPAPI_ROOT) {{
        $libDir = Resolve-SdkRootLayout $env:BLPAPI_ROOT
        if ($libDir) {{
            return $libDir
        }}
    }}

    $mcpbRoot = Join-Path $PSScriptRoot ".."
    $libDir = Find-VendoredSdkDir $mcpbRoot
    if ($libDir) {{
        return $libDir
    }}

    $libDir = Find-PythonBlpapiDir
    if ($libDir) {{
        return $libDir
    }}

    return $null
}}

$architectures = @($env:PROCESSOR_ARCHITECTURE, $env:PROCESSOR_ARCHITEW6432) | Where-Object {{ $_ }}
if (-not ($architectures -contains "AMD64")) {{
    Die "{UNSUPPORTED_PLATFORM_MESSAGE}"
}}

$realBin = Join-Path $PSScriptRoot "bin\windows-amd64\xbbg-mcp.exe"
if (-not (Test-Path -LiteralPath $realBin -PathType Leaf)) {{
    Die "{MISSING_REAL_BINARY_MESSAGE}"
}}

$libDir = Find-RuntimeLibDir
if (-not $libDir) {{
    Die "{MISSING_RUNTIME_MESSAGE}"
}}

$env:PATH = "$libDir;$env:PATH"
& $realBin @args
exit $LASTEXITCODE
'''


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage a cross-platform xbbg MCPB directory.")
    parser.add_argument("--version", required=True)
    parser.add_argument("--darwin-arm64-bin", required=True, type=Path)
    parser.add_argument("--linux-amd64-bin", required=True, type=Path)
    parser.add_argument("--windows-amd64-bin", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def require_executable(path: Path) -> Path:
    if not path.is_file() or not os.access(path, os.X_OK):
        raise SystemExit(f"missing executable: {path}")
    return path


def require_file(path: Path) -> Path:
    if not path.is_file():
        raise SystemExit(f"missing file: {path}")
    return path


def chmod_755(path: Path) -> None:
    path.chmod(0o755)


def build_manifest(version: str) -> dict[str, object]:
    return {
        "manifest_version": "0.3",
        "name": "xbbg-mcp",
        "display_name": "xbbg MCP",
        "version": version,
        "description": "Local Bloomberg request/response tools for authorized xbbg users.",
        "long_description": (
            "Run xbbg Bloomberg request/response tools as a local stdio MCP server. This extension "
            "requires the user's own authorized Bloomberg runtime and does not bundle Bloomberg SDK "
            "files, credentials, or market data."
        ),
        "author": {
            "name": "xbbg-org",
            "url": "https://github.com/xbbg-org",
        },
        "repository": {
            "type": "git",
            "url": "https://github.com/xbbg-org/xbbg.git",
        },
        "homepage": "https://github.com/xbbg-org/xbbg",
        "documentation": "https://github.com/xbbg-org/xbbg/tree/main/apps/xbbg-mcp",
        "support": "https://github.com/xbbg-org/xbbg/issues",
        "server": {
            "type": "binary",
            "entry_point": "server/xbbg-mcp",
            "mcp_config": {
                "command": "${__dirname}/server/xbbg-mcp",
                "args": [],
                "env": {
                    "XBBG_MCP_LIB_DIR": "${user_config.blpapi_lib_dir}",
                    "XBBG_MCP_HOST": "${user_config.host}",
                    "XBBG_MCP_PORT": "${user_config.port}",
                    "XBBG_MCP_AUTH_METHOD": "${user_config.auth_method}",
                    "XBBG_MCP_MAX_ROWS": "${user_config.max_rows}",
                    "XBBG_MCP_MAX_STRING_CHARS": "${user_config.max_string_chars}",
                },
                "platform_overrides": {
                    "win32": {
                        "command": "powershell.exe",
                        "args": [
                            "-NoProfile",
                            "-ExecutionPolicy",
                            "Bypass",
                            "-File",
                            "${__dirname}/server/xbbg-mcp.ps1",
                        ],
                    },
                },
            },
        },
        "tools": [
            {"name": "bdp", "description": "Bloomberg reference data request."},
            {"name": "bdh", "description": "Bloomberg historical data request."},
            {"name": "bds", "description": "Bloomberg bulk data request."},
            {"name": "bdib", "description": "Bloomberg intraday bar request."},
            {"name": "bql", "description": "Bloomberg Query Language request."},
            {"name": "bsrch", "description": "Bloomberg search request."},
            {"name": "bflds", "description": "Bloomberg field metadata lookup."},
            {"name": "request", "description": "Generic Bloomberg request."},
        ],
        "tools_generated": True,
        "keywords": ["mcp", "model-context-protocol", "bloomberg", "market-data", "finance", "xbbg"],
        "license": "Apache-2.0",
        "privacy_policies": ["https://github.com/xbbg-org/xbbg/tree/main/apps/xbbg-mcp#privacy-policy"],
        "compatibility": {
            "claude_desktop": ">=1.0.0",
            "platforms": ["darwin", "linux", "win32"],
        },
        "user_config": {
            "blpapi_lib_dir": {
                "type": "directory",
                "title": "Bloomberg runtime library directory",
                "description": (
                    "Optional directory containing libblpapi3.dylib, libblpapi3.so, libblpapi3_64.so, "
                    "blpapi3_64.dll, or blpapi3_32.dll. Leave empty to let the launcher try BLPAPI_ROOT, "
                    "a vendored authorized SDK layout, or Python blpapi."
                ),
                "required": False,
            },
            "host": {
                "type": "string",
                "title": "Bloomberg host",
                "description": "Bloomberg API host for DAPI/BPIPE.",
                "default": "localhost",
                "required": False,
            },
            "port": {
                "type": "number",
                "title": "Bloomberg port",
                "description": "Bloomberg API port.",
                "default": 8194,
                "min": 1,
                "max": 65535,
                "required": False,
            },
            "auth_method": {
                "type": "string",
                "title": "Authentication method",
                "description": (
                    "Bloomberg auth method. Use none for local Desktop API/DAPI unless your environment requires "
                    "SAPI/BPIPE auth."
                ),
                "default": "none",
                "required": False,
            },
            "max_rows": {
                "type": "number",
                "title": "Maximum returned rows",
                "description": "Maximum rows returned to the MCP client per response.",
                "default": 500,
                "min": 1,
                "required": False,
            },
            "max_string_chars": {
                "type": "number",
                "title": "Maximum string characters",
                "description": "Maximum characters per string value returned to the MCP client.",
                "default": 2048,
                "min": 16,
                "required": False,
            },
        },
    }


def render_posix_launcher(repo_root: Path) -> str:
    source_path = repo_root / "scripts" / "xbbg-mcp"
    source = source_path.read_text(encoding="utf-8")
    marker = "find_real_binary() {\n"
    start = source.find(marker)
    if start == -1:
        raise SystemExit(f"{source_path} does not contain find_real_binary()")

    next_function = "\n\nappend_loader_path() {"
    end = source.find(next_function, start)
    if end == -1:
        raise SystemExit(f"{source_path} does not contain append_loader_path() after find_real_binary()")

    return source[:start] + POSIX_FIND_REAL_BINARY + source[end:]


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as output:
        json.dump(payload, output, indent=2)
        output.write("\n")
    tmp_path.replace(path)


def stage_bundle(args: argparse.Namespace) -> Path:
    darwin_bin = require_executable(args.darwin_arm64_bin)
    linux_bin = require_executable(args.linux_amd64_bin)
    windows_bin = require_file(args.windows_amd64_bin)

    repo_root = Path(__file__).resolve().parents[1]
    license_path = repo_root / "LICENSE"
    if not license_path.is_file():
        raise SystemExit("missing LICENSE")

    output_dir = args.output_dir.resolve()
    staging_dir = output_dir / "xbbg-mcp-mcpb"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)

    server_dir = staging_dir / "server"
    darwin_dir = server_dir / "bin" / "darwin-arm64"
    linux_dir = server_dir / "bin" / "linux-amd64"
    windows_dir = server_dir / "bin" / "windows-amd64"
    for directory in (darwin_dir, linux_dir, windows_dir):
        directory.mkdir(parents=True, exist_ok=True)

    posix_launcher = server_dir / "xbbg-mcp"
    posix_launcher.write_text(render_posix_launcher(repo_root), encoding="utf-8")
    powershell_launcher = server_dir / "xbbg-mcp.ps1"
    powershell_launcher.write_text(WINDOWS_LAUNCHER, encoding="utf-8")

    shutil.copyfile(darwin_bin, darwin_dir / "xbbg-mcp-real")
    shutil.copyfile(linux_bin, linux_dir / "xbbg-mcp-real")
    shutil.copyfile(windows_bin, windows_dir / "xbbg-mcp.exe")
    shutil.copyfile(license_path, staging_dir / "LICENSE")

    (staging_dir / "README.md").write_text(README_TEMPLATE.format(version=args.version), encoding="utf-8")
    write_json_atomic(staging_dir / "manifest.json", build_manifest(args.version))

    for path in (
        posix_launcher,
        powershell_launcher,
        darwin_dir / "xbbg-mcp-real",
        linux_dir / "xbbg-mcp-real",
        windows_dir / "xbbg-mcp.exe",
    ):
        chmod_755(path)

    return staging_dir


def main() -> int:
    staging_dir = stage_bundle(parse_args())
    print(staging_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
