from __future__ import annotations

import importlib
import logging
from pathlib import Path
import sys
from unittest import TestCase
import warnings

_CASE = TestCase()


def test_import_xbbg_is_warning_free():
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        _CASE.assertIsNotNone(importlib.import_module("xbbg"))

    _CASE.assertEqual(recorded, [])


def _write_fake_blpapi(root: Path) -> Path:
    """Create a stand-in blpapi wheel carrying Bloomberg's invalid escape sequence."""
    package = root / "blpapi"
    package.mkdir()
    # Mirrors the docstrings in Bloomberg's resolutionlist.py / topiclist.py,
    # which Python 3.12+ reports as SyntaxWarning when first compiled.
    package.joinpath("__init__.py").write_text(
        '"""Stand-in for Bloomberg\'s wheel.\n'
        "\n"
        "InvalidArgumentException: If adding :class:`Message`\\s received\n"
        '"""\n'
        "\n"
        '__version__ = "0.0.0-fake"\n',
        encoding="utf-8",
    )
    return package


def test_blpapi_discovery_import_suppresses_upstream_syntax_warning(tmp_path, monkeypatch):
    """xbbg imports blpapi only to locate the SDK, so the wheel's own warnings must not leak.

    Bloomberg ships invalid escape sequences in its Python package. Users who never
    touch blpapi themselves would otherwise see SyntaxWarnings from `import xbbg`.
    """
    from xbbg import _sdk

    _write_fake_blpapi(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "blpapi", raising=False)

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        module = _sdk._import_blpapi_module()

    _CASE.assertIsNotNone(module)
    _CASE.assertEqual(getattr(module, "__version__", None), "0.0.0-fake")
    _CASE.assertEqual([entry for entry in recorded if issubclass(entry.category, SyntaxWarning)], [])


def test_get_lib_version_failure_is_debug_observable(monkeypatch, caplog):
    import xbbg
    from xbbg import _sdk

    def raise_sdk_version():
        raise RuntimeError("boom")

    monkeypatch.setattr(xbbg, "_core", type("FakeCore", (), {"sdk_version": raise_sdk_version})(), raising=False)

    with caplog.at_level(logging.DEBUG, logger="xbbg._sdk"):
        _CASE.assertIsNone(_sdk._get_lib_version(Path("C:/missing/blpapi3_64.dll")))

    _CASE.assertTrue(
        any("Could not determine Bloomberg SDK runtime version" in record.message for record in caplog.records)
    )
    _CASE.assertTrue(any(record.exc_info for record in caplog.records))


def test_get_sdk_info_runtime_failure_is_debug_observable(monkeypatch, caplog):
    import xbbg
    from xbbg import _sdk

    def raise_sdk_version():
        raise RuntimeError("boom")

    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.setattr(xbbg, "_core", type("FakeCore", (), {"sdk_version": raise_sdk_version})(), raising=False)

    with caplog.at_level(logging.DEBUG, logger="xbbg._sdk"):
        info = _sdk.get_sdk_info()

    _CASE.assertIn("runtime_version", info)
    _CASE.assertTrue(
        any("Could not determine Bloomberg SDK runtime version" in record.message for record in caplog.records)
    )
    _CASE.assertTrue(any(record.exc_info for record in caplog.records))


def test_prepare_sdk_failure_is_debug_observable(monkeypatch, caplog):
    from xbbg import _sdk

    def raise_prepare():
        raise RuntimeError("boom")

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(_sdk, "_add_sdk_to_dll_search_path", raise_prepare)

    with caplog.at_level(logging.DEBUG, logger="xbbg._sdk"):
        _sdk._prepare_sdk_for_core_import()

    _CASE.assertTrue(any("Failed to prepare Bloomberg SDK" in record.message for record in caplog.records))
    _CASE.assertTrue(any(record.exc_info for record in caplog.records))


def test_set_sdk_path_prepares_manual_sdk(monkeypatch, tmp_path):
    from xbbg import _sdk

    sdk_dir = tmp_path / "sdk"
    sdk_dir.mkdir()
    lib_path = sdk_dir / "blpapi3_64.dll"
    lib_path.write_text("placeholder")
    calls = []

    monkeypatch.setattr(_sdk, "_find_sdk_lib", lambda path: lib_path if path == sdk_dir else None)
    monkeypatch.setattr(_sdk, "_prepare_sdk_for_core_import", lambda: calls.append("prepare"))
    monkeypatch.setattr(_sdk, "_manual_sdk_path", None)
    monkeypatch.setattr(_sdk, "_sdk_info", {"cached": True})

    _sdk.set_sdk_path(sdk_dir)

    _CASE.assertEqual(_sdk._manual_sdk_path, sdk_dir)
    _CASE.assertIsNone(_sdk._sdk_info)
    _CASE.assertEqual(calls, ["prepare"])


def test_find_sdk_lib_checks_windows_bin_dir(monkeypatch, tmp_path):
    from xbbg import _sdk

    sdk_dir = tmp_path / "sdk"
    bin_dir = sdk_dir / "bin"
    bin_dir.mkdir(parents=True)
    lib_path = bin_dir / "blpapi3_64.dll"
    lib_path.write_text("placeholder")

    monkeypatch.setattr(sys, "platform", "win32")

    _CASE.assertEqual(_sdk._find_sdk_lib(sdk_dir), lib_path)


def test_dapi_candidate_paths_include_windows_terminal_roots(monkeypatch):
    from xbbg import _sdk

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("SYSTEMDRIVE", "C:")
    monkeypatch.setenv("PROGRAMFILES", r"C:\Program Files")
    monkeypatch.setenv("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
    monkeypatch.setenv("LOCALAPPDATA", r"C:\Users\analyst\AppData\Local")

    paths = _sdk._dapi_candidate_paths()

    _CASE.assertIn(Path(r"C:\blp\DAPI"), paths)
    _CASE.assertIn(Path(r"C:\Program Files (x86)") / "Bloomberg" / "Blp" / "DAPI", paths)
    _CASE.assertIn(Path(r"C:\Program Files") / "Bloomberg" / "Blp" / "DAPI", paths)


def test_get_sdk_info_prefers_dapi_path_with_runtime(monkeypatch, tmp_path):
    from xbbg import _sdk

    empty_dapi = tmp_path / "empty-dapi"
    empty_dapi.mkdir()
    program_files_dapi = tmp_path / "Program Files (x86)" / "Bloomberg" / "Blp" / "DAPI"
    program_files_dapi.mkdir(parents=True)
    lib_path = program_files_dapi / "blpapi3_64.dll"
    lib_path.write_text("placeholder")

    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.setattr(_sdk, "_manual_sdk_path", None)
    monkeypatch.setattr(_sdk, "_dapi_candidate_paths", lambda: [empty_dapi, program_files_dapi])
    monkeypatch.setattr(_sdk, "_find_sdk_lib", lambda path: lib_path if path == program_files_dapi else None)
    monkeypatch.setattr(_sdk, "_get_lib_version", lambda _lib_path: "3.0.0.0")
    monkeypatch.delenv("BLPAPI_ROOT", raising=False)

    info = _sdk.get_sdk_info()
    dapi_sources = [source for source in info["sources"] if source["name"] == "dapi"]

    _CASE.assertEqual(len(dapi_sources), 1)
    _CASE.assertEqual(dapi_sources[0]["path"], program_files_dapi)
    _CASE.assertEqual(dapi_sources[0]["version"], "3.0.0.0")


def test_windows_dll_directory_handles_are_retained(monkeypatch, tmp_path):
    from xbbg import _sdk

    sdk_dir = tmp_path / "sdk"
    sdk_dir.mkdir()
    lib_path = sdk_dir / "blpapi3_64.dll"
    lib_path.write_text("placeholder")
    import os

    handles = []

    monkeypatch.setattr(_sdk, "_collect_sdk_candidate_dirs", lambda: [sdk_dir])
    monkeypatch.setattr(_sdk, "_find_sdk_lib", lambda path: lib_path if path == sdk_dir else None)
    monkeypatch.setattr(_sdk, "_dll_directory_handles", [])
    monkeypatch.setattr(os, "add_dll_directory", lambda path: handles.append(path) or f"handle:{path}", raising=False)

    _sdk._add_sdk_to_dll_search_path()

    _CASE.assertEqual(handles, [str(sdk_dir)])
    _CASE.assertEqual(_sdk._dll_directory_handles, [f"handle:{sdk_dir}"])


def test_package_prepare_failure_is_debug_observable(monkeypatch, caplog):
    import xbbg
    from xbbg import _sdk

    def raise_prepare():
        raise RuntimeError("boom")

    monkeypatch.setattr(_sdk, "_prepare_sdk_for_core_import", raise_prepare)

    with caplog.at_level(logging.DEBUG, logger="xbbg"):
        importlib.reload(xbbg)

    _CASE.assertTrue(
        any("Failed to prepare Bloomberg SDK for package import" in record.message for record in caplog.records)
    )
    _CASE.assertTrue(any(record.exc_info for record in caplog.records))
