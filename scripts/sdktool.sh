#!/usr/bin/env sh
set -eu

usage() {
    printf '%s\n' \
        "Usage: ./scripts/sdktool.sh [VERSION] [--no-set-active] [--force]" \
        "       ./scripts/sdktool.sh --remove VERSION" \
        "       ./scripts/sdktool.sh --list" \
        "       ./scripts/sdktool.sh --clean-cache"
}

die() {
    printf 'error: %s\n' "$1" >&2
    exit 1
}

note() {
    printf '%s\n' "$1"
}

resolve_python() {
    if command -v python3 >/dev/null 2>&1; then
        command -v python3
        return
    fi

    if command -v python >/dev/null 2>&1; then
        command -v python
        return
    fi

    die "python3 or python is required"
}

PYTHON_BIN=$(resolve_python)
REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VENDOR_BASE="$REPO_ROOT/vendor/blpapi-sdk"
CACHE_DIR="$VENDOR_BASE/.cache"
ENV_FILE="$REPO_ROOT/.env"
INDEX_URL="https://blpapi.bloomberg.com/repository/releases/python/simple/blpapi/"

MODE="add"
VERSION=""
SET_ACTIVE=1
FORCE=0

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --version)
            [ $# -ge 2 ] || die "--version requires a value"
            VERSION=$2
            shift 2
            ;;
        --no-set-active)
            SET_ACTIVE=0
            shift
            ;;
        --set-active)
            SET_ACTIVE=1
            shift
            ;;
        --force)
            FORCE=1
            shift
            ;;
        --remove)
            MODE="remove"
            shift
            ;;
        --list)
            MODE="list"
            shift
            ;;
        --clean-cache)
            MODE="clean-cache"
            shift
            ;;
        --*)
            die "unknown option: $1"
            ;;
        *)
            if [ -z "$VERSION" ]; then
                VERSION=$1
                shift
            else
                die "unexpected argument: $1"
            fi
            ;;
    esac
done

validate_version() {
    [ -n "$1" ] || die "version is required"
    if "$PYTHON_BIN" -c "import re,sys; sys.exit(0 if re.fullmatch(r'\\d+\\.\\d+\\.\\d+(?:\\.\\d+)?', sys.argv[1]) else 1)" "$1"; then
        return
    else
        die "invalid version format: $1"
    fi
}

download_text() {
    if command -v curl >/dev/null 2>&1; then
        curl --fail --silent --show-error --location "$1"
        return
    fi

    if command -v wget >/dev/null 2>&1; then
        wget -qO- "$1"
        return
    fi

    die "curl or wget is required"
}

download_file() {
    if command -v curl >/dev/null 2>&1; then
        curl --fail --silent --show-error --location --output "$2" "$1"
        return
    fi

    if command -v wget >/dev/null 2>&1; then
        wget -qO "$2" "$1"
        return
    fi

    die "curl or wget is required"
}

resolve_latest_version() {
    index_content=$(download_text "$INDEX_URL")
    INDEX_CONTENT="$index_content" "$PYTHON_BIN" -c "import os,re; versions=sorted({m.group(1) for m in re.finditer(r'blpapi-(\\d+\\.\\d+\\.\\d+(?:\\.\\d+)?)\\.tar\\.gz', os.environ.get('INDEX_CONTENT', ''))}, key=lambda s: tuple(int(part) for part in (s.split('.') + ['0'])[:4])); print(versions[-1] if versions else '')"
}

get_active_sdk_version() {
    [ -f "$ENV_FILE" ] || return 0
    "$PYTHON_BIN" -c "from pathlib import Path; import re,sys; path=Path(sys.argv[1]); match=re.search(r'^\\s*BLPAPI_ROOT\\s*=\\s*.*?/([0-9]+\\.[0-9]+\\.[0-9]+(?:\\.[0-9]+)?)\\s*$', path.read_text(), re.MULTILINE); print(match.group(1) if match else '')" "$ENV_FILE"
}

set_active_sdk_version() {
    "$PYTHON_BIN" -c "from pathlib import Path; import sys; env_file=Path(sys.argv[1]); version=sys.argv[2]; env_line=f'BLPAPI_ROOT=vendor/blpapi-sdk/{version}'; content=env_file.read_text() if env_file.exists() else ''; lines=[line for line in content.splitlines() if not line.lstrip().startswith('BLPAPI_ROOT=')]; lines.append(env_line); env_file.write_text('\\n'.join(lines) + '\\n')" "$ENV_FILE" "$VERSION"
    note "[OK] .env updated: BLPAPI_ROOT=vendor/blpapi-sdk/$VERSION"
}

clear_active_sdk_version() {
    [ -f "$ENV_FILE" ] || return 0
    "$PYTHON_BIN" -c "from pathlib import Path; import sys; env_file=Path(sys.argv[1]); version=sys.argv[2]; target=f'vendor/blpapi-sdk/{version}'; lines=env_file.read_text().splitlines(); kept=[line for line in lines if not (line.lstrip().startswith('BLPAPI_ROOT=') and target in line)]; env_file.write_text('\\n'.join(kept) + '\\n') if kept else env_file.unlink(missing_ok=True)" "$ENV_FILE" "$VERSION"
}

platform_info() {
    os_name=$(uname -s 2>/dev/null || printf 'unknown')
    arch_name=$(uname -m 2>/dev/null || printf 'unknown')

    case "$os_name" in
        Darwin)
            case "$arch_name" in
                arm64|aarch64)
                    PLATFORM_LABEL="macOS arm64"
                    ARCHIVE_FILE_NAME="blpapi_cpp_${VERSION}-macos-arm64.tar.gz"
                    EXTRACTOR="tar.gz"
                    ;;
                *)
                    die "unsupported macOS architecture: $arch_name"
                    ;;
            esac
            ;;
        Linux)
            PLATFORM_LABEL="Linux"
            ARCHIVE_FILE_NAME="blpapi_cpp_${VERSION}-linux.tar.gz"
            EXTRACTOR="tar.gz"
            ;;
        MINGW*|MSYS*|CYGWIN*)
            PLATFORM_LABEL="Windows"
            ARCHIVE_FILE_NAME="blpapi_cpp_${VERSION}-windows.zip"
            EXTRACTOR="zip"
            ;;
        *)
            die "unsupported operating system: $os_name"
            ;;
    esac

    ARCHIVE_URL="https://blpapi.bloomberg.com/download/releases/raw/files/$ARCHIVE_FILE_NAME"
}

extract_archive() {
    archive_path=$1
    staging_dir=$2

    case "$EXTRACTOR" in
        zip)
            command -v unzip >/dev/null 2>&1 || die "unzip is required for $ARCHIVE_FILE_NAME"
            unzip -q "$archive_path" -d "$staging_dir"
            ;;
        tar.gz)
            tar -xzf "$archive_path" -C "$staging_dir"
            ;;
        *)
            die "unsupported extractor: $EXTRACTOR"
            ;;
    esac
}

list_versions() {
    [ -d "$VENDOR_BASE" ] || return 0
    "$PYTHON_BIN" -c "from pathlib import Path; import re,sys; root=Path(sys.argv[1]); versions=[p.name for p in root.iterdir() if p.is_dir() and re.fullmatch(r'\\d+\\.\\d+\\.\\d+(?:\\.\\d+)?', p.name)]; versions.sort(key=lambda s: tuple(int(part) for part in (s.split('.') + ['0'])[:4])); print('\\n'.join(versions))" "$VENDOR_BASE"
}

case "$MODE" in
    clean-cache)
        if [ ! -d "$CACHE_DIR" ]; then
            note "No cache directory found. Nothing to clean."
            exit 0
        fi
        set +e
        found_cache=0
        for path in "$CACHE_DIR"/*; do
            if [ -f "$path" ]; then
                found_cache=1
                rm -f "$path"
                note "Removed: ${path##*/}"
            fi
        done
        set -e
        if [ "$found_cache" -eq 0 ]; then
            note "Cache is already empty."
        fi
        exit 0
        ;;
    list)
        active_version=$(get_active_sdk_version || true)
        listed=0
        for version in $(list_versions); do
            listed=1
            if [ "$version" = "$active_version" ]; then
                note "$version (active)"
            else
                note "$version"
            fi
        done
        if [ "$listed" -eq 0 ]; then
            note "No SDK versions installed."
        fi
        exit 0
        ;;
    remove)
        validate_version "$VERSION"
        VERSION_DIR="$VENDOR_BASE/$VERSION"
        [ -d "$VERSION_DIR" ] || die "version $VERSION is not installed"
        rm -rf "$VERSION_DIR"
        note "Removed: $VERSION_DIR"
        set +e
        for path in "$CACHE_DIR"/blpapi_cpp_"$VERSION"-*; do
            [ -e "$path" ] || continue
            rm -f "$path"
            note "Removed: ${path##*/}"
        done
        set -e
        if [ "$(get_active_sdk_version || true)" = "$VERSION" ]; then
            clear_active_sdk_version
            note "Cleared BLPAPI_ROOT from .env"
        fi
        exit 0
        ;;
esac

if [ -z "$VERSION" ]; then
    VERSION=$(resolve_latest_version)
    [ -n "$VERSION" ] || die "failed to resolve latest SDK version"
else
    validate_version "$VERSION"
fi

platform_info

VERSION_DIR="$VENDOR_BASE/$VERSION"
ARCHIVE_PATH="$CACHE_DIR/$ARCHIVE_FILE_NAME"

note "Bloomberg C++ SDK"
note "  Version : $VERSION"
note "  Platform: $PLATFORM_LABEL"
note "  Target  : $VERSION_DIR"

if [ -d "$VERSION_DIR" ] && [ "$FORCE" -ne 1 ]; then
    note "[OK] Version $VERSION is already present."
    if [ "$SET_ACTIVE" -eq 1 ]; then
        set_active_sdk_version
    fi
    exit 0
fi

if [ "$FORCE" -eq 1 ] && [ -d "$VERSION_DIR" ]; then
    rm -rf "$VERSION_DIR"
fi

mkdir -p "$VENDOR_BASE" "$CACHE_DIR"

if [ -f "$ARCHIVE_PATH" ] && [ "$FORCE" -ne 1 ]; then
    note "[OK] Using cached download: $ARCHIVE_FILE_NAME"
else
    rm -f "$ARCHIVE_PATH"
    note "[..] Downloading $ARCHIVE_FILE_NAME"
    download_file "$ARCHIVE_URL" "$ARCHIVE_PATH"
fi

TMP_EXTRACT=$(mktemp -d "${TMPDIR:-/tmp}/xbbg-sdk.XXXXXX")
trap 'rm -rf "$TMP_EXTRACT"' EXIT INT TERM HUP

extract_archive "$ARCHIVE_PATH" "$TMP_EXTRACT"

set -- "$TMP_EXTRACT"/*
if [ "$1" = "$TMP_EXTRACT/*" ]; then
    mkdir -p "$VERSION_DIR"
elif [ $# -eq 1 ] && [ -d "$1" ]; then
    mv "$1" "$VERSION_DIR"
else
    mkdir -p "$VERSION_DIR"
    for entry in "$TMP_EXTRACT"/* "$TMP_EXTRACT"/.[!.]* "$TMP_EXTRACT"/..?*; do
        [ -e "$entry" ] || continue
        mv "$entry" "$VERSION_DIR"/
    done
fi

if [ "$SET_ACTIVE" -eq 1 ]; then
    set_active_sdk_version
fi

note "[OK] Extracted to $VERSION_DIR"

for rel in include lib Linux Darwin bin; do
    if [ -e "$VERSION_DIR/$rel" ]; then
        note "  $rel/ : $VERSION_DIR/$rel"
    fi
done
