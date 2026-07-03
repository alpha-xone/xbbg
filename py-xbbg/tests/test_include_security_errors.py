"""Tests for request flag plumbing."""

from __future__ import annotations

from collections.abc import Sized

import pytest

from xbbg._core import ArrowRecordBatch, ArrowTable
from xbbg.services import Operation, RequestParams, Service


def _sample_batch() -> ArrowRecordBatch:
    return ArrowTable.from_pylist(
        [
            {"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"},
        ]
    ).to_batches()[0]


def test_request_params_to_dict_omits_include_security_errors_when_false():
    """RequestParams.to_dict() should omit the flag when False."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    result = params.to_dict()

    assert "include_security_errors" not in result


def test_request_params_to_dict_includes_include_security_errors_when_true():
    """RequestParams.to_dict() should include the flag when True."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        include_security_errors=True,
    )

    result = params.to_dict()

    assert result["include_security_errors"] is True


def test_request_params_to_dict_omits_return_eids_when_false():
    """RequestParams.to_dict() should omit return_eids when False."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    result = params.to_dict()

    assert "return_eids" not in result


def test_request_params_to_dict_includes_return_eids_when_true():
    """RequestParams.to_dict() should include return_eids when True."""
    params = RequestParams(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        return_eids=True,
    )

    result = params.to_dict()

    assert result["return_eids"] is True


@pytest.mark.asyncio
async def test_arequest_passes_include_security_errors_to_engine(monkeypatch):
    """arequest() should pass include_security_errors=True to engine.request()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        include_security_errors=True,
    )

    assert captured["include_security_errors"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_omits_include_security_errors_when_false(monkeypatch):
    """arequest() should not include include_security_errors when False."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    assert "include_security_errors" not in captured
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_passes_return_eids_to_engine(monkeypatch):
    """arequest() should pass return_eids=True to engine.request()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
        return_eids=True,
    )

    assert captured["return_eids"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_arequest_omits_return_eids_when_false(monkeypatch):
    """arequest() should not include return_eids when False."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def request(self, params_dict):
            captured.update(params_dict)
            return _sample_batch()

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())

    result = await blp.arequest(
        service=Service.REFDATA,
        operation=Operation.REFERENCE_DATA,
        securities=["IBM US Equity"],
        fields=["PX_LAST"],
    )

    assert "return_eids" not in captured
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_abdp_forwards_include_security_errors(monkeypatch):
    """abdp() should forward include_security_errors to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(blp, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abdp("IBM US Equity", "PX_LAST", include_security_errors=True)

    assert captured["include_security_errors"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_abdp_forwards_return_eids(monkeypatch):
    """abdp() should forward return_eids to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(blp, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abdp("IBM US Equity", "PX_LAST", return_eids=True)

    assert captured["return_eids"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


@pytest.mark.asyncio
async def test_abdh_forwards_return_eids(monkeypatch):
    """abdh() should forward return_eids to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "date": "2024-01-01", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(blp, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    result = await blp.abdh("IBM US Equity", "PX_LAST", start_date="2024-01-01", return_eids=True)

    assert captured["return_eids"] is True
    assert isinstance(result, Sized)
    assert len(result) == 1


def test_bdp_forwards_include_security_errors(monkeypatch):
    """bdp() sync wrapper should forward include_security_errors to arequest()."""
    from xbbg import blp

    captured: dict[str, object] = {}

    class FakeEngine:
        async def resolve_field_types(self, field_list, field_types, default_type):
            return field_types or dict.fromkeys(field_list, default_type)

    async def fake_route_kwargs(_service, _operation, _kwargs):
        return [], []

    async def fake_arequest(*_args, **kwargs):
        captured.update(kwargs)
        return [{"ticker": "IBM US Equity", "field": "PX_LAST", "value": "123.45"}]

    monkeypatch.setattr(blp, "_get_engine", lambda: FakeEngine())
    monkeypatch.setattr(blp, "_aroute_kwargs", fake_route_kwargs)
    monkeypatch.setattr(blp, "arequest", fake_arequest)
    monkeypatch.setattr(blp, "convert_backend_frame", lambda df, _backend: df)

    assert callable(blp.bdp)
    result = blp.bdp("IBM US Equity", "PX_LAST", include_security_errors=True)

    assert captured["include_security_errors"] is True
    assert len(result) == 1
