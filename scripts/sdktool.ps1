<#
.SYNOPSIS
    Manages the Bloomberg C++ SDK for local development (add, remove, list, cache).

.DESCRIPTION
    Downloads the Bloomberg C++ SDK zip from Bloomberg's release URL, extracts it
    into vendor/blpapi-sdk/<version>/, and optionally updates the .env file with
    BLPAPI_ROOT so the build system (blpapi-sys/build.rs) can locate the SDK.

    Supports multiple SDK versions side-by-side. Downloaded zips are cached in
    vendor/blpapi-sdk/.cache/ to avoid redundant downloads.

    When -Version is omitted, the script automatically resolves the latest available
    version by querying Bloomberg's Python Simple Index (PEP 503).

.PARAMETER Version
    SDK version (e.g., "3.25.12.1"). When omitted during add, automatically
    resolves the latest available version from Bloomberg's servers.
    Required when using -Remove.

.PARAMETER SetActive
    When true (default), updates the .env file at the repo root with
    BLPAPI_ROOT pointing to the added version.

.PARAMETER Force
    Re-download and re-extract even if the version directory already exists.

.PARAMETER Remove
    Remove an SDK version. Deletes the version directory, its cached zip,
    and clears .env if the removed version was the active one.

.PARAMETER List
    Show all SDK versions and which one is currently active, then exit.

.PARAMETER CleanCache
    Remove all cached SDK zip files to free disk space.

.EXAMPLE
    .\scripts\sdktool.ps1
    # Resolves and adds the latest SDK version, sets it active.

.EXAMPLE
    .\scripts\sdktool.ps1 -Version 3.25.12.1
    # Adds a specific version and sets it active.

.EXAMPLE
    .\scripts\sdktool.ps1 -Version 3.24.0.1 -SetActive:$false
    # Adds 3.24.0.1 without changing the active SDK.

.EXAMPLE
    .\scripts\sdktool.ps1 -Remove 3.25.12.1
    # Removes version 3.25.12.1 and its cached zip.

.EXAMPLE
    .\scripts\sdktool.ps1 -List
    # Lists SDK versions and highlights the active one.

.EXAMPLE
    .\scripts\sdktool.ps1 -CleanCache
    # Removes all cached zip files.

.EXAMPLE
    .\scripts\sdktool.ps1 -Force
    # Re-downloads and re-extracts the latest version.
#>
[CmdletBinding(DefaultParameterSetName = 'Add')]
param(
    [Parameter(ParameterSetName = 'Add', Position = 0)]
    [Parameter(ParameterSetName = 'Remove', Position = 0, Mandatory)]
    [string]$Version,

    [Parameter(ParameterSetName = 'Add')]
    [bool]$SetActive = $true,

    [Parameter(ParameterSetName = 'Add')]
    [switch]$Force,

    [Parameter(ParameterSetName = 'Remove', Mandatory)]
    [switch]$Remove,

    [Parameter(ParameterSetName = 'List', Mandatory)]
    [switch]$List,

    [Parameter(ParameterSetName = 'CleanCache', Mandatory)]
    [switch]$CleanCache
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Resolve paths relative to this script's location (scripts/ -> repo root)
# ---------------------------------------------------------------------------
$RepoRoot   = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$VendorBase = Join-Path (Join-Path $RepoRoot 'vendor') 'blpapi-sdk'
$CacheDir   = Join-Path $VendorBase '.cache'
$EnvFile    = Join-Path $RepoRoot '.env'

function Get-PlatformInfo {
    param([string]$Version)

    $windows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)
    $linux   = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)
    $macos   = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::OSX)
    $arch    = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()

    if ($windows) {
        return @{
            Label           = 'Windows'
            ArchiveFileName = "blpapi_cpp_${Version}-windows.zip"
            Extractor       = 'zip'
        }
    }

    if ($linux) {
        return @{
            Label           = 'Linux'
            ArchiveFileName = "blpapi_cpp_${Version}-linux.tar.gz"
            Extractor       = 'tar.gz'
        }
    }

    if ($macos) {
        if ($arch -eq 'arm64') {
            return @{
                Label           = 'macOS arm64'
                ArchiveFileName = "blpapi_cpp_${Version}-macos-arm64.tar.gz"
                Extractor       = 'tar.gz'
            }
        }

        throw "Unsupported macOS architecture '$arch'. Add a Bloomberg archive mapping for this architecture before using sdktool.ps1."
    }

    throw 'Unsupported operating system for sdktool.ps1.'
}

# ---------------------------------------------------------------------------
# Helper: resolve the latest SDK version from Bloomberg's Python Simple Index
# ---------------------------------------------------------------------------
function Resolve-LatestVersion {
    $indexUrl = 'https://blpapi.bloomberg.com/repository/releases/python/simple/blpapi/'

    Write-Host '[..] Resolving latest SDK version ...' -ForegroundColor Yellow
    Write-Host "     Index: $indexUrl" -ForegroundColor DarkGray

    try {
        $ProgressPreference = 'SilentlyContinue'
        $response = Invoke-WebRequest -Uri $indexUrl -UseBasicParsing
        $ProgressPreference = 'Continue'
    }
    catch {
        Write-Host ('[FAIL] Could not fetch version index: {0}' -f $_) -ForegroundColor Red
        throw 'Failed to resolve latest SDK version. Specify -Version explicitly.'
    }

    # Extract version numbers from .tar.gz filenames (one per version, no dupes)
    # Matches both 3-part (3.25.3) and 4-part (3.25.12.1) versions
    $versions = @()
    $matches_ = [regex]::Matches($response.Content, 'blpapi-(\d+\.\d+\.\d+(?:\.\d+)?)\.tar\.gz')

    foreach ($m in $matches_) {
        $versions += $m.Groups[1].Value
    }

    if ($versions.Count -eq 0) {
        throw 'No versions found in Bloomberg index. The page format may have changed.'
    }

    # Deduplicate, then sort descending by [System.Version] for correct numeric ordering
    # System.Version handles both 3-part (Major.Minor.Build) and 4-part (Major.Minor.Build.Revision)
    $unique = $versions | Select-Object -Unique
    $sorted = $unique | Sort-Object {
        # Normalize to 4-part so [System.Version] parses consistently
        $v = $_
        if (($v.Split('.')).Count -eq 3) { $v = "$v.0" }
        [System.Version]$v
    } -Descending

    $latest = $sorted[0]

    Write-Host ('[OK] Latest version: {0}' -f $latest) -ForegroundColor Green
    return $latest
}

# ---------------------------------------------------------------------------
# Helper: read active SDK root from .env
# ---------------------------------------------------------------------------
function Get-ActiveSdkVersion {
    if (-not (Test-Path $EnvFile)) { return $null }

    $line = Get-Content $EnvFile -ErrorAction SilentlyContinue |
            Where-Object { $_ -match '^\s*BLPAPI_ROOT\s*=' }

    if ($line) {
        # Extract version from path like vendor/blpapi-sdk/3.25.12.1
        if ($line -match 'vendor/blpapi-sdk/([0-9]+\.[0-9]+\.[0-9]+(?:\.[0-9]+)?)') {
            return $Matches[1]
        }
    }
    return $null
}

# ---------------------------------------------------------------------------
# Helper: write/update BLPAPI_ROOT in .env
# ---------------------------------------------------------------------------
function Set-ActiveSdkVersion {
    $relativePath = "vendor/blpapi-sdk/$Version"
    $envLine      = "BLPAPI_ROOT=$relativePath"

    if (Test-Path $EnvFile) {
        $content = Get-Content $EnvFile -Raw -ErrorAction SilentlyContinue
        if ($null -eq $content) { $content = '' }

        if ($content -match '(?m)^\s*BLPAPI_ROOT\s*=.*$') {
            # Replace existing line
            $newContent = $content -replace '(?m)^\s*BLPAPI_ROOT\s*=.*$', $envLine
        }
        else {
            # Append — ensure trailing newline before appending
            $newContent = $content.TrimEnd("`r", "`n") + [Environment]::NewLine + $envLine + [Environment]::NewLine
        }
    }
    else {
        $newContent = $envLine + [Environment]::NewLine
    }

    Set-Content -Path $EnvFile -Value $newContent.TrimEnd("`r", "`n") -NoNewline
    # Append a final newline for POSIX compatibility
    Add-Content -Path $EnvFile -Value ''

    Write-Host ('[OK] .env updated: {0}' -f $envLine) -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# -CleanCache: purge cached zip files and exit
# ---------------------------------------------------------------------------
if ($PSCmdlet.ParameterSetName -eq 'CleanCache') {
    if (-not (Test-Path $CacheDir)) {
        Write-Host 'No cache directory found. Nothing to clean.' -ForegroundColor Yellow
        return
    }

    $cachedFiles = @(Get-ChildItem -Path $CacheDir -File -ErrorAction SilentlyContinue)

    if ($cachedFiles.Count -eq 0) {
        Write-Host 'Cache is already empty.' -ForegroundColor Yellow
        return
    }

    $totalBytes = 0
    foreach ($f in $cachedFiles) { $totalBytes += $f.Length }
    $totalMB = [math]::Round($totalBytes / 1MB, 1)

    Write-Host ''
    Write-Host ('Removing {0} cached file(s) ({1} MB) ...' -f $cachedFiles.Count, $totalMB) -ForegroundColor Yellow

    foreach ($f in $cachedFiles) {
        Remove-Item -Path $f.FullName -Force
        Write-Host ('  Removed: {0}' -f $f.Name) -ForegroundColor DarkGray
    }

    Write-Host ('[OK] Cache cleaned. Freed {0} MB.' -f $totalMB) -ForegroundColor Green
    Write-Host ''
    return
}

# ---------------------------------------------------------------------------
# -Remove: remove an SDK version and exit
# ---------------------------------------------------------------------------
if ($PSCmdlet.ParameterSetName -eq 'Remove') {
    if ($Version -notmatch '^\d+\.\d+\.\d+(\.\d+)?$') {
        throw "Invalid version format: '$Version'. Expected format: X.Y.Z or X.Y.Z.W (e.g., 3.25.12.1)"
    }

    $VersionDir = Join-Path $VendorBase $Version

    if (-not (Test-Path $VersionDir)) {
        Write-Host ('Version {0} is not installed. Nothing to remove.' -f $Version) -ForegroundColor Yellow
        return
    }

    Write-Host ''
    Write-Host ('Removing Bloomberg C++ SDK v{0} ...' -f $Version) -ForegroundColor Yellow

    # Remove the version directory
    Remove-Item -Path $VersionDir -Recurse -Force
    Write-Host ('  Removed: {0}' -f $VersionDir) -ForegroundColor DarkGray

    $CachedArchives = @(Get-ChildItem -Path $CacheDir -File -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like "blpapi_cpp_${Version}-*" })
    foreach ($archive in $CachedArchives) {
        Remove-Item -Path $archive.FullName -Force
        Write-Host ('  Removed: {0}' -f $archive.Name) -ForegroundColor DarkGray
    }

    # Clear .env if this was the active version
    $activeVersion = Get-ActiveSdkVersion
    if ($activeVersion -eq $Version) {
        if (Test-Path $EnvFile) {
            $content = Get-Content $EnvFile -Raw -ErrorAction SilentlyContinue
            if ($content) {
                $newContent = $content -replace '(?m)^\s*BLPAPI_ROOT\s*=.*\r?\n?', ''
                $newContent = $newContent.TrimEnd("`r", "`n")
                if ($newContent) {
                    Set-Content -Path $EnvFile -Value $newContent -NoNewline
                    Add-Content -Path $EnvFile -Value ''
                }
                else {
                    Remove-Item -Path $EnvFile -Force
                }
            }
        }
        Write-Host '  Cleared BLPAPI_ROOT from .env (was active)' -ForegroundColor DarkGray
    }

    Write-Host ('[OK] Version {0} removed.' -f $Version) -ForegroundColor Green
    Write-Host ''
    return
}

# ---------------------------------------------------------------------------
# -List: show installed versions and exit
# ---------------------------------------------------------------------------
if ($PSCmdlet.ParameterSetName -eq 'List') {
    if (-not (Test-Path $VendorBase)) {
        Write-Host 'No SDK versions installed.' -ForegroundColor Yellow
        Write-Host "  Run: .\scripts\sdktool.ps1 [-Version <ver>]"
        return
    }

    $activeVersion = Get-ActiveSdkVersion
    $installed = Get-ChildItem -Path $VendorBase -Directory -ErrorAction SilentlyContinue |
                 Where-Object { $_.Name -match '^\d+\.\d+\.\d+(\.\d+)?$' } |
                  Sort-Object Name

    if (-not $installed) {
        Write-Host 'No SDK versions installed.' -ForegroundColor Yellow
        Write-Host "  Run: .\scripts\sdktool.ps1 [-Version <ver>]"
        return
    }

    Write-Host ''
    Write-Host 'Installed Bloomberg C++ SDK versions:' -ForegroundColor Cyan
    Write-Host ''

    foreach ($dir in $installed) {
        if ($dir.Name -eq $activeVersion) {
            Write-Host ('  {0} (active)' -f $dir.Name) -ForegroundColor Green
        } else {
            Write-Host ('  {0}' -f $dir.Name) -ForegroundColor White
        }
    }

    Write-Host ''
    Write-Host "  SDK root: $VendorBase" -ForegroundColor DarkGray
    Write-Host ''
    return
}

# ---------------------------------------------------------------------------
# Add flow (default)
# ---------------------------------------------------------------------------

# Resolve version: auto-detect latest if not specified
if (-not $Version) {
    $Version = Resolve-LatestVersion
}
elseif ($Version -notmatch '^\d+\.\d+\.\d+(\.\d+)?$') {
    throw "Invalid version format: '$Version'. Expected format: X.Y.Z or X.Y.Z.W (e.g., 3.25.12.1)"
}

$VersionDir      = Join-Path $VendorBase $Version
$Platform        = Get-PlatformInfo -Version $Version
$ArchiveFileName = $Platform.ArchiveFileName
$ArchivePath     = Join-Path $CacheDir $ArchiveFileName
$DownloadUrl     = "https://blpapi.bloomberg.com/download/releases/raw/files/$ArchiveFileName"

Write-Host ''
Write-Host "Bloomberg C++ SDK" -ForegroundColor Cyan
Write-Host "  Version : $Version"
Write-Host "  Platform: $($Platform.Label)"
Write-Host "  Target  : $VersionDir"
Write-Host ''

# --- Check if already present (idempotent) ----------------------------------
if ((Test-Path $VersionDir) -and -not $Force) {
    Write-Host ('[OK] Version {0} is already present.' -f $Version) -ForegroundColor Green

    if ($SetActive) {
        # Still update .env in case it drifted
        Set-ActiveSdkVersion
    }

    Write-Host ''
    return
}

if ($Force -and (Test-Path $VersionDir)) {
    Write-Host '[..] -Force specified - removing existing installation...' -ForegroundColor Yellow
    Remove-Item -Path $VersionDir -Recurse -Force
}

# --- Ensure directories exist -----------------------------------------------
foreach ($dir in @($VendorBase, $CacheDir)) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

# --- Download (skip if cached) ---------------------------------------------
if ((Test-Path $ArchivePath) -and -not $Force) {
    Write-Host ('[OK] Using cached download: {0}' -f $ArchiveFileName) -ForegroundColor Green
} else {
    Write-Host ('[..] Downloading {0} ...' -f $ArchiveFileName) -ForegroundColor Yellow
    Write-Host "     URL: $DownloadUrl" -ForegroundColor DarkGray

    try {
        $ProgressPreference = 'SilentlyContinue'   # drastically speeds up Invoke-WebRequest
        Invoke-WebRequest -Uri $DownloadUrl -OutFile $ArchivePath -UseBasicParsing
        $ProgressPreference = 'Continue'
    }
    catch {
        # Clean up partial download
        if (Test-Path $ArchivePath) { Remove-Item $ArchivePath -Force }
        Write-Host ('[FAIL] Download failed: {0}' -f $_) -ForegroundColor Red
        throw "Failed to download Bloomberg C++ SDK v${Version}. Verify the version number and your network connection."
    }

    $sizeKB = [math]::Round((Get-Item $ArchivePath).Length / 1KB)
    Write-Host ('[OK] Downloaded ({0} KB)' -f $sizeKB) -ForegroundColor Green
}

# --- Extract ----------------------------------------------------------------
Write-Host '[..] Extracting SDK ...' -ForegroundColor Yellow

$TempExtract = Join-Path $VendorBase ".tmp-extract-$Version"

try {
    # Clean up any leftover temp directory from a prior failed run
    if (Test-Path $TempExtract) { Remove-Item $TempExtract -Recurse -Force }
    New-Item -ItemType Directory -Path $TempExtract -Force | Out-Null

    if ($Platform.Extractor -eq 'zip') {
        Expand-Archive -Path $ArchivePath -DestinationPath $TempExtract -Force
    }
    else {
        & tar -xzf $ArchivePath -C $TempExtract
        if ($LASTEXITCODE -ne 0) {
            throw 'tar extraction failed.'
        }
    }

    $innerEntries = @(Get-ChildItem -Path $TempExtract -Force)

    if ($innerEntries.Count -eq 1 -and $innerEntries[0].PSIsContainer) {
        Move-Item -Path $innerEntries[0].FullName -Destination $VersionDir -Force
    }
    elseif ($innerEntries.Count -eq 0) {
        Move-Item -Path $TempExtract -Destination $VersionDir -Force
    }
    else {
        New-Item -ItemType Directory -Path $VersionDir -Force | Out-Null
        foreach ($entry in $innerEntries) {
            Move-Item -Path $entry.FullName -Destination $VersionDir -Force
        }
    }
}
catch {
    # Clean up on failure
    if (Test-Path $TempExtract) { Remove-Item $TempExtract -Recurse -Force -ErrorAction SilentlyContinue }
    if (Test-Path $VersionDir)  { Remove-Item $VersionDir  -Recurse -Force -ErrorAction SilentlyContinue }
    Write-Host ('[FAIL] Extraction failed: {0}' -f $_) -ForegroundColor Red
    throw "Failed to extract Bloomberg C++ SDK v${Version}."
}
finally {
    # Always clean temp directory if it still exists
    if (Test-Path $TempExtract) { Remove-Item $TempExtract -Recurse -Force -ErrorAction SilentlyContinue }
}

Write-Host ('[OK] Extracted to {0}' -f $VersionDir) -ForegroundColor Green

# --- Update .env (set active) -----------------------------------------------
if ($SetActive) {
    Set-ActiveSdkVersion
}
else {
    Write-Host '[--] Skipping .env update (-SetActive is false)' -ForegroundColor DarkGray
}

# --- Summary ----------------------------------------------------------------
Write-Host ''
Write-Host "Bloomberg C++ SDK v$Version added and ready." -ForegroundColor Cyan

$includeDir = Join-Path $VersionDir 'include'
$libDir     = Join-Path $VersionDir 'lib'
$linuxDir   = Join-Path $VersionDir 'Linux'
$darwinDir  = Join-Path $VersionDir 'Darwin'
$binDir     = Join-Path $VersionDir 'bin'
if (Test-Path $includeDir) { Write-Host "  include/ : $includeDir" -ForegroundColor DarkGray }
if (Test-Path $libDir)     { Write-Host "  lib/     : $libDir" -ForegroundColor DarkGray }
if (Test-Path $linuxDir)   { Write-Host "  Linux/   : $linuxDir" -ForegroundColor DarkGray }
if (Test-Path $darwinDir)  { Write-Host "  Darwin/  : $darwinDir" -ForegroundColor DarkGray }
if (Test-Path $binDir)     { Write-Host "  bin/     : $binDir" -ForegroundColor DarkGray }
Write-Host ''
