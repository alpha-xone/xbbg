# xbbg Release Process

This document explains the release process for xbbg, intended for AI agents and maintainers.

## Overview

xbbg uses **semantic versioning** (SemVer) with Python package versions **automatically derived from git tags** via `setuptools_scm`. The JS package families use the same version numbers, stamped during release workflows: `vX.Y.Z` for the npm publish flow and `js-vX.Y.Z` for the GitHub-only JS asset flow. The build system is `setuptools` + `setuptools-rust` + `setuptools_scm` for Python and npm package stamping for JS.

### Version Format

```
{major}.{minor}.{patch}[-{pre-release}]

Examples:
- 0.12.1        # Stable release
- 0.12.1b1      # Beta pre-release
- 0.12.1a1      # Alpha pre-release
- 0.12.1rc1     # Release candidate
```

Dev builds (untagged commits) automatically get versions like `0.12.1.dev268+g84acdcf.d20260219`.

### Build System

| Component | Package | Purpose |
|-----------|---------|---------|
| Build backend | `setuptools` | Python packaging |
| Rust extension | `setuptools-rust` | Compiles PyO3 extension (`xbbg._core`) |
| Version | `setuptools_scm` | Derives Python package versions from git tags |
| JS package version | `js-xbbg/scripts/stamp-version.ts` | Stamps `@xbbg/core` wrapper/platform package versions for JS release workflows |
| Build tool | `uv` | Fast package manager and build frontend |

## Release Workflow

### Step 1: Update CHANGELOG.md

Ensure all changes are documented under the `[Unreleased]` section:

```markdown
## [Unreleased]

### Added
- New feature description

### Changed
- Modified behavior description

### Fixed
- Bug fix description
```

**Categories** (use only what applies):
- `Added` - New features
- `Changed` - Changes in existing functionality
- `Deprecated` - Soon-to-be removed features
- `Removed` - Removed features
- `Fixed` - Bug fixes
- `Security` - Vulnerability fixes

### Step 2: Commit CHANGELOG Updates

```bash
git add CHANGELOG.md
git commit -m "docs(CHANGELOG): prepare for vX.Y.Z release"
git push
```

### Step 3: Trigger Release Workflow

Go to **GitHub Actions** > **Bump Version and Create Release** > **Run workflow**

**Parameters:**
| Parameter | Description | Options |
|-----------|-------------|---------|
| `bump_type` | Version increment | `major`, `minor`, `patch` |
| `pre_release` | Pre-release type | `none`, `alpha`, `beta`, `rc` |
| `pre_number` | Pre-release number | `1`, `2`, `3`, etc. |
| `create_release` | Create GitHub release | `true`, `false` |

**Examples:**
- `0.12.1` → `0.13.0`: bump_type=`minor`, pre_release=`none`
- `0.12.1` → `0.12.2`: bump_type=`patch`, pre_release=`none`
- `0.12.1` → `0.12.2b1`: bump_type=`patch`, pre_release=`beta`, pre_number=`1`

### Step 4: What Happens Automatically

1. **Version Calculation**: Computes new version from current tags
2. **Changelog Update**: Renames `[Unreleased]` to `[version] - date`
3. **README Release Sync**: Updates the `README.md` latest-release marker block to the new version/tag
4. **Git Tag**: Creates `vX.Y.Z` tag and pushes it
5. **GitHub Release**: Creates release with notes from CHANGELOG
6. **PyPI Publish**: Tag push triggers `pypi_upload.yml` — builds wheels and publishes via OIDC trusted publishing
7. **npm Publish**: Tag push triggers `npm-publish.yml` — builds platform-native `@xbbg/core-*` packages, stamps JS versions from the git tag, and publishes `@xbbg/core` to npm via trusted publishing
8. **Release Assets**: Wheels and sdist are attached to the GitHub release

## CI/CD Workflows

### On Every Push/PR

| Workflow | File | Purpose |
|----------|------|---------|
| CI | `ci-rust.yml` | Rust lint, clippy, build, test (Linux + Windows) |
| Docker | `ci-docker.yml` | Build CI Docker image |

### On Release (tag push `v*`)

| Workflow | File | Purpose |
|----------|------|---------|
| Release | `pypi_upload.yml` | Build wheels (manylinux_2_28 Linux + Windows + macOS × Python 3.10–3.14), sdist, publish to PyPI, attach to GitHub release |
| Release | `npm-publish.yml` | Build and publish stable `@xbbg/core` prebuilt native packages for supported platforms, then publish the `@xbbg/core` wrapper and `@xbbg/langgraph` package via npm trusted publishing |

### npm trusted publishing setup

`npm-publish.yml` is tokenless: the publish job uses GitHub OIDC (`id-token: write`) from GitHub-hosted runners and npm CLI `>=11.10.0`. Configure this once on npmjs.com for each published package before relying on tag-push publishing:

| npm package | Publisher | GitHub org/user | Repository | Workflow filename | Environment |
|-------------|-----------|-----------------|------------|-------------------|-------------|
| `@xbbg/core` | GitHub Actions | `alpha-xone` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/core-linux-x64` | GitHub Actions | `alpha-xone` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/core-win32-x64` | GitHub Actions | `alpha-xone` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/core-darwin-arm64` | GitHub Actions | `alpha-xone` | `xbbg` | `npm-publish.yml` | leave blank |
| `@xbbg/langgraph` | GitHub Actions | `alpha-xone` | `xbbg` | `npm-publish.yml` | leave blank |

GitHub environment `npm` is intentionally not required because current repository credentials cannot create it. Add an environment only if an admin wants reviewer-based release approvals; if you do, update both the workflow `environment:` and all npm trusted publisher entries to the exact same environment name.

After a successful OIDC publish, set each package's npm **Publishing access** to require 2FA and disallow tokens, then revoke any temporary publish tokens.

`npm-publish.yml` intentionally publishes only stable npm versions (`vX.Y.Z`). Python-style pre-release tags such as `vX.Y.Zb1` still trigger the workflow glob but are skipped because they are not valid npm semver versions for this package family.

### Manual Trigger

| Workflow | File | Purpose |
|----------|------|---------|
| Bump Version | `semantic_version.yml` | Calculate version, update CHANGELOG and README release marker, create tag + GitHub release |
| JS GitHub Release | `js_github_release.yml` | Build, validate, and attach GitHub-only JS tarballs for `@xbbg/core` on `js-vX.Y.Z` |
| npm Publish Retry | `npm-publish.yml` | Manual retry of trusted npm publishing for a stable `vX.Y.Z` version |

### JS GitHub-only package release

Use this workflow when you want GitHub release assets for the JS packages without npm publishing.

Go to **GitHub Actions** > **JS GitHub Release** > **Run workflow**

**Parameters:**
| Parameter | Description |
|-----------|-------------|
| `version` | Package version to stamp into the JS tarballs; the workflow creates or reuses the `js-vX.Y.Z` tag |
| `notes` | Optional maintainer notes appended to the GitHub release body |
| `draft` | Create the GitHub release as a draft |

**What happens automatically:**

1. Validates the requested version and targets the current workflow commit
2. Creates or reuses the `js-vX.Y.Z` tag without touching the global `vX.Y.Z` release flow
3. Builds native assets for the supported JS targets
4. Stamps both JS package families with the selected version
5. Packs and validates the GitHub release tarballs
6. Attaches the tarballs to a GitHub release on `js-vX.Y.Z`


**Attached artifacts (currently supported):**

- `@xbbg/core` wrapper + `darwin-arm64`, `linux-x64` (glibc 2.28+), `win32-x64` platform tarballs

Docker images are not part of this release. CI images stay in GHCR and do not bundle Bloomberg SDK files.

### Manual npm trusted publishing retry

Use this workflow only when a stable npm release needs to be retried after the canonical `vX.Y.Z` tag flow. Do not use it for GitHub-only JS assets; use `js_github_release.yml` and a `js-vX.Y.Z` tag for that case.

Go to **GitHub Actions** > **Publish JS Packages** > **Run workflow**.

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `version` | Stable npm version/tag to publish, such as `v1.2.3`; pre-release forms are rejected for npm publishing |

**What happens automatically:**

1. Validates the stable semver version and skips non-npm pre-release tags from the `v*` trigger
2. Builds the supported native platform packages (`linux-x64`, `win32-x64`, `darwin-arm64`)
3. Installs JS package dependencies before stamping package versions so `package-lock.json` stays consistent
4. Runs a packed-install smoke test before publishing
5. Publishes missing packages in dependency order: platform packages first, then `@xbbg/core`
6. Uses npm trusted publishing/OIDC with provenance from GitHub Actions; no npm token is required for normal releases

The npm Trusted Publisher configuration must match the workflow filename exactly: `npm-publish.yml`, repository `xbbg-org/xbbg`, and blank environment unless a matching GitHub environment is intentionally added.

## Local Development

### Build Locally

```bash
# Install the SDK into vendor/blpapi-sdk/ and let the build discover it
bash ./scripts/sdktool.sh
# Windows PowerShell: .\scripts\sdktool.ps1

# Build wheel (includes Rust extension)
uv build

# Build sdist only (no Rust compilation)
uv build --sdist
```

### Check Current Version

```bash
# Latest release tags
git tag --sort=-version:refname | head -5

# Local dev version (from setuptools_scm)
python -c "from setuptools_scm import get_version; print(get_version())"

# Installed package version
python -c "import xbbg; print(xbbg.__version__)"
```

### Check What's on PyPI

```bash
pip index versions xbbg
```

## Branch Strategy

| Branch | Purpose |
|--------|---------|
| `main` | v1.x development (Rust-backed stable line) |
| `release/0.x` | v0.x maintenance releases (pure-Python stable line) |
| `feat/*` | New features (PRs to main) |
| `fix/*` | Bug fixes (PRs to main or release/0.x) |
| `chore/*` | Maintenance tasks |

> **Note:** When releasing from `release/0.x`, the downstream `update-readme` and `update-index` workflows will target `main` by default. Review and revert any unintended changes to `main` after a `release/0.x` release.

### After Merging PRs

1. Delete merged branches
2. Update CHANGELOG.md on main
3. Trigger release workflow when ready

## Troubleshooting

### Release Workflow Failed

1. Check workflow logs in GitHub Actions
2. Common issues:
   - Empty CHANGELOG `[Unreleased]` section (blocked by validation)
   - Version already exists on PyPI
   - Bloomberg SDK download URL changed
   - Rust compilation error

### Version Already on PyPI

PyPI rejects duplicate versions. To fix:
1. Increment pre-release number (e.g., `b3` → `b4`)
2. Or fix issues and bump patch version

### Local Build Fails

1. Ensure `BLPAPI_ROOT` points to the Bloomberg SDK directory (must contain `include/` and `lib/`)
2. Ensure Rust toolchain is installed (`rustup show`)
3. For bindgen issues, set `LIBCLANG_PATH` (see `.cargo/config.toml` comments)
4. CI uses pregenerated bindings (`BLPAPI_PREGENERATED_BINDINGS`) to skip bindgen entirely

## For AI Agents

When asked to create a release:

1. **Review pending changes**: Read `CHANGELOG.md` `[Unreleased]` section
2. **Check for uncommitted changes**: Run `git status`
3. **Determine version bump**:
   - Breaking changes → `major`
   - New features → `minor`
   - Bug fixes only → `patch`
   - Pre-release → add `alpha`/`beta`/`rc`
4. **Guide user to GitHub Actions** to trigger the `semantic_version.yml` workflow for the canonical `vX.Y.Z` release
5. **For GitHub-only JS package assets**, guide the user to `js_github_release.yml` with an explicit version; it builds the supported `@xbbg/core` JS tarballs and tags `js-vX.Y.Z`

**Do NOT manually:**
- Edit version numbers in code for Python releases (managed by `setuptools_scm` from git tags)
- Create `vX.Y.Z` git tags directly (the canonical release workflow handles this)
- Reuse `vX.Y.Z` tags for JS-only GitHub assets; use `js-vX.Y.Z` instead so the PyPI/npm publish workflows do not trigger
- Upload to PyPI manually (OIDC trusted publishing only)
- Upload npm packages manually except for emergency recovery or first-time package seeding; normal npm releases must go through `npm-publish.yml` trusted publishing on a stable `vX.Y.Z` tag

## CHANGELOG.md Format

```markdown
## [Unreleased]

### Added
- New feature description (#PR_NUMBER)

### Changed
- Modified behavior description (#PR_NUMBER)

### Deprecated
- Feature that will be removed in future versions

### Removed
- Feature removed in this release

### Fixed
- Bug fix description (#ISSUE_NUMBER)

### Security
- Security fix description (CVE if applicable)
```

## Writing Good Release Notes

### DO:
- Write from the user's perspective ("Users can now..." not "We added...")
- Be specific about what changed and why it matters
- Link to relevant issues/PRs with `(#123)` format
- Group related changes together
- Mention breaking changes prominently
- Include migration instructions for breaking changes

### DON'T:
- Leave the `[Unreleased]` section empty
- Use vague descriptions ("Various improvements")
- Include internal implementation details users don't need
- Forget to categorize changes
- Leave placeholder text

### Example: Good Release Notes

```markdown
## [Unreleased]

### Added
- Native Arrow carrier with explicit `backend="native"`, `backend="pyarrow"` for real PyArrow tables, and a Narwhals default that preserves legacy PyArrow-backed behavior when PyArrow is installed (#173)
- Output format control with `Format` enum (long, semi_long, long_typed, long_metadata)
- `bta()` function for Bloomberg Technical Analysis (#175)
- `get_sdk_info()` as replacement for deprecated `getBlpapiVersion()`

### Changed
- All API functions now accept `backend` and `format` parameters
- Internal pipeline uses xbbg native Arrow tables with explicit optional conversion backends
- **BREAKING**: Deprecated `wide` output removed; use `semi_long` or pivot `long` results explicitly

### Deprecated
- `connect()` / `disconnect()` - engine auto-initializes in v1.0
- `getBlpapiVersion()` - use `get_sdk_info()` instead

### Fixed
- Empty DataFrame handling in helper functions with LONG format output (#180)
- Memory leak in streaming subscriptions (#182)
```

### Example: Bad Release Notes

```markdown
## [Unreleased]

- Various bug fixes
- Performance improvements
- TODO: add more details
- Updated some stuff
```

## Pre-release Types

| Type | When to Use |
|------|-------------|
| **alpha** | Early testing, API may change significantly |
| **beta** | Feature complete, testing for bugs |
| **rc** | Release candidate, final testing before stable |

## Validation

The release workflow validates:

1. **Non-empty**: `[Unreleased]` must have content (workflow fails if empty)
2. **No placeholders**: Warns if TODO/FIXME/WIP/TBD detected
3. **Format check**: Warns if standard categories not found

## Pre-Release Checklist

Before triggering a release, ensure:

- [ ] `CHANGELOG.md` `[Unreleased]` section is populated with all changes
- [ ] Changes are categorized correctly (Added, Changed, Deprecated, Removed, Fixed, Security)
- [ ] No placeholder text (TODO, FIXME, WIP, TBD) remains
- [ ] Issue/PR numbers are referenced where applicable
- [ ] Breaking changes are clearly marked
