"""Tests for streaming API enhancements (Tasks 3-12).

These tests verify:
1. New parameter signatures on asubscribe, astream, stream, subscribe
2. Validation logic for config params (flush_threshold, stream_capacity, overflow_policy)
3. Warning behavior for tick_mode + flush_threshold conflicts
4. Subscription.stats property exists
5. Backward compatibility (all new params have defaults)

All tests run offline — no Bloomberg connection required.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest


class TestAsubscribeSignature:
    """Verify asubscribe() has all new streaming params with correct defaults."""

    def test_asubscribe_signature(self):
        """All new params exist with correct defaults."""
        from xbbg.blp import asubscribe

        sig = inspect.signature(asubscribe)
        params = sig.parameters

        assert "service" in params
        assert params["service"].default is None

        assert "options" in params
        assert params["options"].default is None

        assert "conflate" in params
        assert params["conflate"].default is False

        assert "tick_mode" in params
        assert params["tick_mode"].default is False

        assert "flush_threshold" in params
        assert params["flush_threshold"].default is None

        assert "stream_capacity" in params
        assert params["stream_capacity"].default is None

        assert "overflow_policy" in params
        assert params["overflow_policy"].default is None

        assert "all_fields" in params
        assert params["all_fields"].default is False


class TestAstreamSignature:
    """Verify astream() has callback and config params."""

    def test_astream_signature(self):
        """callback param exists with default None; config params present."""
        from xbbg.blp import astream

        sig = inspect.signature(astream)
        params = sig.parameters

        assert "callback" in params
        assert params["callback"].default is None

        assert "flush_threshold" in params
        assert params["flush_threshold"].default is None

        assert "stream_capacity" in params
        assert params["stream_capacity"].default is None

        assert "overflow_policy" in params
        assert params["overflow_policy"].default is None

        assert "all_fields" in params
        assert params["all_fields"].default is False

        assert "conflate" in params
        assert params["conflate"].default is False


class TestStreamSignature:
    """Verify stream() also has the new params."""

    def test_stream_signature(self):
        """stream() has callback, flush_threshold, stream_capacity, overflow_policy."""
        from xbbg.blp import stream

        sig = inspect.signature(stream)
        params = sig.parameters

        assert "callback" in params
        assert params["callback"].default is None

        assert "flush_threshold" in params
        assert params["flush_threshold"].default is None

        assert "stream_capacity" in params
        assert params["stream_capacity"].default is None

        assert "overflow_policy" in params
        assert params["overflow_policy"].default is None

        assert "all_fields" in params
        assert params["all_fields"].default is False

        assert "conflate" in params
        assert params["conflate"].default is False


class TestStreamingServiceHelpersSignature:
    """avwap / amktbar / adepth / achains forward all_fields."""

    def test_all_fields_kwarg_defaults(self):
        from xbbg.blp import achains, adepth, amktbar, avwap

        for fn in (adepth, achains):
            sig = inspect.signature(fn)
            assert "all_fields" in sig.parameters
            assert sig.parameters["all_fields"].default is False

        for fn in (avwap, amktbar):
            sig = inspect.signature(fn)
            assert "all_fields" in sig.parameters
            assert sig.parameters["all_fields"].default is True


class TestVwapContract:
    """Verify Market VWAP helpers use Bloomberg's required subscription shape."""

    def _install_fake_engine(self, monkeypatch, captured: dict[str, object]):
        import xbbg.blp as blp_module

        class FakePySubscription:
            tickers = ["//blp/mktvwap/ticker/IBM US Equity"]
            failed_tickers = []
            failures = []
            topic_states = [("//blp/mktvwap/ticker/IBM US Equity", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["VWAP"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

            def __init__(self):
                self.added: list[list[str]] = []

            async def add(self, tickers):
                self.added.append(tickers)

        fake_sub = FakePySubscription()

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return fake_sub

        monkeypatch.setattr(blp_module, "_get_engine", lambda: FakeEngine())
        return blp_module, fake_sub

    def test_avwap_signature_uses_vwap_only_contract(self):
        from xbbg.blp import avwap

        sig = inspect.signature(avwap)
        assert "fields" not in sig.parameters
        assert sig.parameters["all_fields"].default is True

    def test_avwap_builds_explicit_market_vwap_subscription(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.avwap(
                ["IBM US Equity", "//blp/mktvwap/ticker/MSFT US Equity"],
                start_time="10:00",
                end_time="16:00",
            )
        )

        assert captured["service"] == Service.MKTVWAP.value
        assert captured["tickers"] == [
            "//blp/mktvwap/ticker/IBM US Equity",
            "//blp/mktvwap/ticker/MSFT US Equity",
        ]
        assert captured["fields"] == ["VWAP"]
        assert captured["options"] == ["VWAP_START_TIME=10:00", "VWAP_END_TIME=16:00"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("AAPL US Equity"))
        assert fake_sub.added == [["//blp/mktvwap/ticker/AAPL US Equity"]]

    def test_asubscribe_mktvwap_normalizes_topics_and_validates_contract(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.asubscribe(
                "IBM US Equity",
                "VWAP",
                service=Service.MKTVWAP,
                options=["VWAP_START_TIME=10:00", "VWAP_END_TIME=16:00"],
                all_fields=True,
            )
        )

        assert captured["service"] == Service.MKTVWAP.value
        assert captured["tickers"] == ["//blp/mktvwap/ticker/IBM US Equity"]
        assert captured["fields"] == ["VWAP"]
        assert captured["options"] == ["VWAP_START_TIME=10:00", "VWAP_END_TIME=16:00"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("MSFT US Equity"))
        assert fake_sub.added == [["//blp/mktvwap/ticker/MSFT US Equity"]]

    def test_mktvwap_rejects_invalid_contract_inputs(self):
        from xbbg.blp import asubscribe, avwap
        from xbbg.services import Service

        with pytest.raises(ValueError, match="market-VWAP topic"):
            asyncio.run(avwap("//blp/mktdata/ticker/IBM US Equity"))

        with pytest.raises(ValueError, match="VWAP"):
            asyncio.run(
                asubscribe(
                    "IBM US Equity",
                    ["RT_PX_VWAP"],
                    service=Service.MKTVWAP,
                )
            )


class TestMktbarContract:
    """Verify market-bar helpers use Bloomberg's required subscription shape."""

    def _install_fake_engine(self, monkeypatch, captured: dict[str, object]):
        import xbbg.blp as blp_module

        class FakePySubscription:
            tickers = ["//blp/mktbar/ticker/ES1 Index"]
            failed_tickers = []
            failures = []
            topic_states = [("//blp/mktbar/ticker/ES1 Index", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["LAST_PRICE"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

            def __init__(self):
                self.added: list[list[str]] = []
                self.removed: list[list[str]] = []

            async def add(self, tickers):
                self.added.append(tickers)

            async def remove(self, tickers):
                self.removed.append(tickers)

        fake_sub = FakePySubscription()

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return fake_sub

        monkeypatch.setattr(blp_module, "_get_engine", lambda: FakeEngine())
        return blp_module, fake_sub

    def test_amktbar_signature_uses_bar_size(self):
        from xbbg.blp import amktbar

        sig = inspect.signature(amktbar)
        assert "bar_size" in sig.parameters
        assert sig.parameters["bar_size"].default == 1
        assert "interval" not in sig.parameters

    def test_amktbar_builds_explicit_market_bar_subscription(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.amktbar(
                ["ES1 Index", "/figi/BBG000JB5HR2", "isin/GB00B16GWD56 LN"],
                bar_size=5,
                start_time="13:30",
                end_time="20:00",
            )
        )

        assert captured["service"] == Service.MKTBAR.value
        assert captured["tickers"] == [
            "//blp/mktbar/ticker/ES1 Index",
            "//blp/mktbar/figi/BBG000JB5HR2",
            "//blp/mktbar/isin/GB00B16GWD56 LN",
        ]
        assert captured["fields"] == ["LAST_PRICE"]
        assert captured["options"] == ["bar_size=5", "start_time=13:30", "end_time=20:00"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("EURUSD Curncy"))
        asyncio.run(sub.remove("/figi/BBG000JB5HR2"))
        assert fake_sub.added == [["//blp/mktbar/ticker/EURUSD Curncy"]]
        assert fake_sub.removed == [["//blp/mktbar/figi/BBG000JB5HR2"]]

    def test_asubscribe_mktbar_normalizes_topics_and_validates_contract(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module, fake_sub = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.asubscribe(
                "ES1 Index",
                "LAST_PRICE",
                service=Service.MKTBAR,
                options=["bar_size=1"],
                all_fields=True,
            )
        )

        assert captured["service"] == Service.MKTBAR.value
        assert captured["tickers"] == ["//blp/mktbar/ticker/ES1 Index"]
        assert captured["fields"] == ["LAST_PRICE"]
        assert captured["options"] == ["bar_size=1"]
        assert captured["all_fields"] is True

        asyncio.run(sub.add("ticker/EURUSD Curncy"))
        assert fake_sub.added == [["//blp/mktbar/ticker/EURUSD Curncy"]]

    def test_mktbar_rejects_invalid_contract_inputs(self):
        from xbbg.blp import amktbar, asubscribe
        from xbbg.services import Service

        with pytest.raises(ValueError, match="bar_size"):
            asyncio.run(amktbar("ES1 Index", bar_size=0))

        with pytest.raises(ValueError, match="market-bar topic"):
            asyncio.run(amktbar("//blp/mktdata/ticker/ES1 Index"))

        with pytest.raises(ValueError, match="LAST_PRICE"):
            asyncio.run(
                asubscribe(
                    "ES1 Index",
                    ["OPEN"],
                    service=Service.MKTBAR,
                    options=["bar_size=1"],
                )
            )

        with pytest.raises(ValueError, match="bar_size"):
            asyncio.run(asubscribe("ES1 Index", "LAST_PRICE", service=Service.MKTBAR))

        for invalid_option in (["bar_size"], ["bar_size="], ["bar_size=0"], ["bar_size=1441"], ["bar_size=abc"]):
            with pytest.raises(ValueError, match="bar_size"):
                asyncio.run(
                    asubscribe(
                        "ES1 Index",
                        "LAST_PRICE",
                        service=Service.MKTBAR,
                        options=invalid_option,
                    )
                )


class TestConflatedMarketDataContract:
    """Verify mktdata conflation is exposed as a typed subscription option."""

    def _install_fake_engine(self, monkeypatch, captured: dict[str, object]):
        import xbbg.blp as blp_module

        class FakePySubscription:
            tickers = ["ES1 Index"]
            failed_tickers = []
            failures = []
            topic_states = [("ES1 Index", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["BID", "ASK"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return FakePySubscription()

        monkeypatch.setattr(blp_module, "_get_engine", lambda: FakeEngine())
        return blp_module

    def test_asubscribe_conflate_adds_mktdata_option(self, monkeypatch):
        from xbbg.services import Service

        captured: dict[str, object] = {}
        blp_module = self._install_fake_engine(monkeypatch, captured)

        sub = asyncio.run(
            blp_module.asubscribe(
                "ES1 Index",
                ["BID", "ASK"],
                conflate=True,
                all_fields=True,
            )
        )

        assert captured["service"] == Service.MKTDATA.value
        assert captured["tickers"] == ["ES1 Index"]
        assert captured["fields"] == ["BID", "ASK"]
        assert captured["options"] == ["conflate"]
        assert captured["all_fields"] is True
        assert sub.tickers == ["ES1 Index"]

    def test_conflate_normalizes_ampersand_and_avoids_duplicates(self, monkeypatch):
        captured: dict[str, object] = {}
        blp_module = self._install_fake_engine(monkeypatch, captured)

        asyncio.run(
            blp_module.asubscribe(
                "ES1 Index",
                ["BID", "ASK"],
                options=["&conflate", "delayed"],
                conflate=True,
            )
        )

        assert captured["options"] == ["conflate", "delayed"]

    def test_conflate_rejects_non_mktdata_service(self):
        from xbbg.blp import asubscribe
        from xbbg.services import Service

        with pytest.raises(ValueError, match="//blp/mktdata"):
            asyncio.run(asubscribe("IBM US Equity", "VWAP", service=Service.MKTVWAP, conflate=True))

    def test_conflate_rejects_interval_option(self):
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="interval"):
            asyncio.run(asubscribe("ES1 Index", ["BID", "ASK"], options=["interval=5"], conflate=True))


class TestConfigValidation:
    """Verify ValueError raised for invalid config params.

    Validation happens BEFORE the engine call, so these work offline.
    """

    def test_config_validation_flush_threshold(self):
        """flush_threshold=0 raises ValueError."""
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="flush_threshold"):
            asyncio.run(asubscribe(["AAPL US Equity"], ["LAST_PRICE"], flush_threshold=0))

    def test_config_validation_stream_capacity(self):
        """stream_capacity=0 raises ValueError."""
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="stream_capacity"):
            asyncio.run(asubscribe(["AAPL US Equity"], ["LAST_PRICE"], stream_capacity=0))

    def test_config_validation_overflow_policy(self):
        """Invalid overflow_policy raises ValueError."""
        from xbbg.blp import asubscribe

        with pytest.raises(ValueError, match="overflow_policy"):
            asyncio.run(
                asubscribe(
                    ["AAPL US Equity"],
                    ["LAST_PRICE"],
                    overflow_policy="invalid_policy",
                )
            )


class TestTickModeWarning:
    """Verify warning when tick_mode=True conflicts with flush_threshold."""

    def test_tick_mode_flush_threshold_warning(self):
        """tick_mode=True with flush_threshold>1 emits UserWarning before engine call."""
        from xbbg.blp import asubscribe

        captured: dict[str, object] = {}

        class FakePySubscription:
            tickers = ["AAPL US Equity"]
            failed_tickers = []
            failures = []
            topic_states = [("AAPL US Equity", "pending", 1)]
            session_status = {
                "state": "up",
                "last_change_us": 1,
                "disconnect_count": 0,
                "reconnect_count": 0,
            }
            admin_status = {
                "slow_consumer_warning_active": False,
                "slow_consumer_warning_count": 0,
                "slow_consumer_cleared_count": 0,
                "data_loss_count": 0,
                "last_warning_us": None,
                "last_cleared_us": None,
                "last_data_loss_us": None,
            }
            service_status = []
            events = []
            fields = ["LAST_PRICE"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 0,
                "last_message_us": 0,
                "last_data_loss_us": 0,
                "effective_overflow_policy": "drop_newest",
            }

        class FakeEngine:
            async def subscribe_with_options(self, service, tickers, fields, options, **kwargs):
                captured.update(
                    {
                        "service": service,
                        "tickers": tickers,
                        "fields": fields,
                        "options": options,
                        **kwargs,
                    }
                )
                return FakePySubscription()

        import xbbg.blp as blp_module

        original_get_engine = blp_module._get_engine
        blp_module._get_engine = lambda: FakeEngine()

        try:
            with pytest.warns(UserWarning, match="tick_mode"):
                sub = asyncio.run(
                    asubscribe(
                        ["AAPL US Equity"],
                        ["LAST_PRICE"],
                        tick_mode=True,
                        flush_threshold=50,
                    )
                )
        finally:
            blp_module._get_engine = original_get_engine

        assert sub._tick_mode is True
        assert captured["flush_threshold"] == 1


class TestSubscriptionConversion:
    """Subscription iteration converts batches through the constructor-bound path."""

    def test_backend_none_yields_native_table_from_batch(self, monkeypatch):
        from xbbg import blp as blp_module
        from xbbg._core import ArrowTable
        from xbbg.blp import Subscription

        table = ArrowTable.from_pylist([{"ticker": "IBM US Equity", "LAST_PRICE": 123.45}])

        class FakeBatch:
            def __init__(self):
                self.to_table_calls = 0

            def to_table(self):
                self.to_table_calls += 1
                return table

        class FakePySubscription:
            async def __anext__(self):
                return batch

        def fail_facade_conversion(_frame, _backend):
            raise AssertionError("Subscription.__anext__ must not call facade backend conversion")

        batch = FakeBatch()
        monkeypatch.setattr(blp_module, "_convert_result_backend", fail_facade_conversion)
        sub = Subscription(FakePySubscription(), raw=False, backend=None)

        result = asyncio.run(sub.__anext__())

        assert result is table
        assert batch.to_table_calls == 1

    def test_explicit_backend_uses_bound_converter_without_facade_conversion(self, monkeypatch):
        from xbbg import blp as blp_module
        from xbbg._core import ArrowTable
        from xbbg.blp import Backend, Subscription

        table = ArrowTable.from_pylist([{"ticker": "IBM US Equity", "LAST_PRICE": 123.45}])
        converted = object()
        calls = []

        class FakeBatch:
            def to_table(self):
                calls.append("to_table")
                return table

        class FakePySubscription:
            async def __anext__(self):
                return FakeBatch()

        def bound_converter(frame, backend):
            calls.append((frame, backend))
            return converted

        def fail_facade_conversion(_frame, _backend):
            raise AssertionError("Subscription.__anext__ must not call facade backend conversion")

        monkeypatch.setattr(blp_module, "convert_backend_frame", bound_converter)
        monkeypatch.setattr(blp_module, "_convert_result_backend", fail_facade_conversion)
        sub = Subscription(FakePySubscription(), raw=False, backend=Backend.NATIVE)

        result = asyncio.run(sub.__anext__())

        assert result is converted
        assert calls == ["to_table", (table, Backend.NATIVE)]


class TestSubscriptionStats:
    """Verify Subscription class has a stats property."""

    def test_subscription_stats_property_exists(self):
        """Subscription.stats is a property descriptor."""
        from xbbg.blp import Subscription

        assert hasattr(Subscription, "stats")
        assert isinstance(inspect.getattr_static(Subscription, "stats"), property)


class TestSubscriptionFailureMetadata:
    """Verify Subscription exposes non-fatal failure metadata."""

    def test_failure_properties_exist(self):
        from xbbg.blp import Subscription

        assert isinstance(inspect.getattr_static(Subscription, "failed_tickers"), property)
        assert isinstance(inspect.getattr_static(Subscription, "failures"), property)
        assert isinstance(inspect.getattr_static(Subscription, "status"), property)
        assert isinstance(inspect.getattr_static(Subscription, "events"), property)
        assert isinstance(inspect.getattr_static(Subscription, "topic_states"), property)

    def test_failure_properties_proxy_underlying_subscription(self):
        from xbbg.blp import Subscription

        class FakePySubscription:
            tickers = ["SPY US Equity"]
            failed_tickers = ["/isin/BMG8192H1557"]
            failures = [
                (
                    "/isin/BMG8192H1557",
                    "Security is not valid for subscription [EX336]",
                    "failure",
                )
            ]
            topic_states = [
                ("SPY US Equity", "streaming", 123),
                ("/isin/BMG8192H1557", "failed", 456),
            ]
            session_status = {
                "state": "up",
                "last_change_us": 789,
                "disconnect_count": 1,
                "reconnect_count": 1,
            }
            admin_status = {
                "slow_consumer_warning_active": True,
                "slow_consumer_warning_count": 2,
                "slow_consumer_cleared_count": 1,
                "data_loss_count": 3,
                "last_warning_us": 10,
                "last_cleared_us": 11,
                "last_data_loss_us": 12,
            }
            service_status = [("//blp/mktdata", True, 99)]
            events = [
                (1, "session", "info", "SessionConnectionUp", None, "worker=0 active_subscriptions=1"),
                (
                    2,
                    "subscription",
                    "warning",
                    "SubscriptionFailure",
                    "/isin/BMG8192H1557",
                    "Security is not valid for subscription [EX336]",
                ),
            ]
            fields = ["LAST_PRICE"]
            is_active = True
            all_failed = False
            stats = {
                "messages_received": 0,
                "dropped_batches": 0,
                "batches_sent": 0,
                "slow_consumer": False,
                "data_loss_events": 3,
                "last_message_us": 100,
                "last_data_loss_us": 12,
                "effective_overflow_policy": "drop_newest",
            }

        sub = Subscription(FakePySubscription(), raw=True, backend=None)

        assert sub.tickers == ["SPY US Equity"]
        assert sub.failed_tickers == ["/isin/BMG8192H1557"]
        assert sub.failures == [
            {
                "ticker": "/isin/BMG8192H1557",
                "reason": "Security is not valid for subscription [EX336]",
                "kind": "failure",
            }
        ]
        assert sub.topic_states["SPY US Equity"]["state"] == "streaming"
        assert sub.session_status["state"] == "up"
        assert sub.admin_status["data_loss_count"] == 3
        assert sub.service_status["//blp/mktdata"]["up"] is True
        assert sub.events[1]["message_type"] == "SubscriptionFailure"
        assert sub.status["session"]["reconnect_count"] == 1


class TestBackwardCompatibility:
    """Verify all new params are optional (backward compat)."""

    def test_backward_compat_signature(self):
        """asubscribe can be called with just tickers and fields — all new params have defaults."""
        from xbbg.blp import asubscribe

        sig = inspect.signature(asubscribe)
        params = sig.parameters

        assert "tickers" in params
        assert "fields" in params

        # Every new param must have a default (i.e. is optional)
        new_params = [
            "service",
            "options",
            "conflate",
            "tick_mode",
            "flush_threshold",
            "stream_capacity",
            "overflow_policy",
        ]
        for param_name in new_params:
            assert param_name in params, f"{param_name} missing from signature"
            assert params[param_name].default is not inspect.Parameter.empty, f"{param_name} should have a default"
