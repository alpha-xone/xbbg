"""Regression tests for the cross-platform MCPB packager."""

from __future__ import annotations

import argparse

import pytest

from scripts import package_xbbg_mcpb


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("3.20.0", "3.20.0"),
        ("3.26.4.2", "3.26.4.2"),
        ("03.020.000.0002", "3.20.0.2"),
    ],
)
def test_parse_blpapi_version_canonicalizes_numeric_versions(value: str, expected: str) -> None:
    assert package_xbbg_mcpb.parse_blpapi_version(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "3.20",
        "3.20.0.1.2",
        "3.20.-1",
        "3.20.65536",
        '3.20.0"; Write-Output injected; #',
    ],
)
def test_parse_blpapi_version_rejects_invalid_or_unsafe_values(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        package_xbbg_mcpb.parse_blpapi_version(value)


def test_minimum_blpapi_version_uses_newest_imported_symbol(tmp_path) -> None:
    binary = tmp_path / "xbbg-mcp-real"
    binary.write_bytes(b"prefix\0BLPAPI_3.6.4\0BLPAPI_3.20.0\0BLPAPI_3.15.0\0suffix")

    assert package_xbbg_mcpb.minimum_blpapi_version(binary) == "3.20.0"


def test_minimum_blpapi_version_fails_closed_without_symbols(tmp_path) -> None:
    binary = tmp_path / "xbbg-mcp-real"
    binary.write_bytes(b"no Bloomberg symbol versions")

    with pytest.raises(SystemExit, match="pass --min-blpapi-version explicitly"):
        package_xbbg_mcpb.minimum_blpapi_version(binary)


def test_windows_launcher_controls_dll_precedence() -> None:
    launcher = package_xbbg_mcpb.render_windows_launcher("03.020.000")

    assert '$requiredBlpapiVersion = [Version]"3.20.0"' in launcher
    assert "Assert-ValidatedDllWins $realBin $libDir" in launcher
    assert "$startInfo.WorkingDirectory = $libDir" in launcher
    assert '$startInfo.EnvironmentVariables["PATH"] = "$libDir;$env:PATH"' in launcher
    assert '$env:PATH = "$libDir;$env:PATH"' not in launcher


def test_windows_launcher_revalidates_direct_call_input() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        package_xbbg_mcpb.render_windows_launcher('3.20.0"; Write-Output injected; #')
