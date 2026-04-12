"""Tests for SessionMgr — market session logic."""

import pytest
from bot import SessionMgr, ScalpConfig, Session
from tests.conftest import make_candle


class TestSessionGet:
    """Session determination by UTC hour."""

    def test_asia_session(self, session_mgr):
        assert session_mgr.get(0) == Session.ASIA
        assert session_mgr.get(3) == Session.ASIA
        assert session_mgr.get(6) == Session.ASIA

    def test_london_session(self, session_mgr):
        assert session_mgr.get(7) == Session.LONDON
        assert session_mgr.get(9) == Session.LONDON
        assert session_mgr.get(11) == Session.LONDON

    def test_ny_overlap(self, session_mgr):
        # Overlap takes priority over London (12-15)
        assert session_mgr.get(12) == Session.NY_OVERLAP
        assert session_mgr.get(13) == Session.NY_OVERLAP
        assert session_mgr.get(14) == Session.NY_OVERLAP

    def test_new_york_session(self, session_mgr):
        assert session_mgr.get(15) == Session.NEW_YORK
        assert session_mgr.get(16) == Session.NEW_YORK

    def test_off_session(self, session_mgr):
        assert session_mgr.get(17) == Session.OFF
        assert session_mgr.get(20) == Session.OFF
        assert session_mgr.get(23) == Session.OFF

    def test_boundary_hours(self, session_mgr):
        """Edge cases at session boundaries."""
        assert session_mgr.get(7) == Session.LONDON   # London starts
        assert session_mgr.get(12) == Session.NY_OVERLAP  # Overlap starts (takes priority)
        assert session_mgr.get(15) == Session.NEW_YORK    # NY after overlap
        assert session_mgr.get(17) == Session.OFF          # Markets close


class TestIsTradeable:
    """Tradeable session check."""

    def test_asia_not_tradeable(self, session_mgr):
        assert session_mgr.is_tradeable(3) is False

    def test_london_tradeable(self, session_mgr):
        assert session_mgr.is_tradeable(8) is True

    def test_overlap_tradeable(self, session_mgr):
        assert session_mgr.is_tradeable(13) is True

    def test_new_york_tradeable(self, session_mgr):
        assert session_mgr.is_tradeable(15) is True

    def test_off_not_tradeable(self, session_mgr):
        assert session_mgr.is_tradeable(20) is False


class TestGetRR:
    """Risk/reward ratio per session."""

    def test_overlap_rr(self, session_mgr, cfg):
        rr = session_mgr.get_rr(Session.NY_OVERLAP, cfg)
        assert rr == cfg.OVERLAP_RR_RATIO

    def test_ny_rr(self, session_mgr, cfg):
        rr = session_mgr.get_rr(Session.NEW_YORK, cfg)
        assert rr == cfg.NY_RR_RATIO

    def test_london_rr(self, session_mgr, cfg):
        rr = session_mgr.get_rr(Session.LONDON, cfg)
        assert rr == cfg.LONDON_RR_RATIO

    def test_other_session_gets_default_rr(self, session_mgr, cfg):
        rr = session_mgr.get_rr(Session.ASIA, cfg)
        assert rr == cfg.DEFAULT_RR_RATIO
        rr = session_mgr.get_rr(Session.OFF, cfg)
        assert rr == cfg.DEFAULT_RR_RATIO


class TestCalcAsiaRange:
    """Asia session high/low extraction from 5M candles."""

    def test_empty_candles(self, session_mgr):
        high, low = session_mgr.calc_asia_range([])
        assert high == 0.0
        assert low == 999999.0

    def test_no_asia_candles(self, session_mgr):
        # All candles during London
        candles = [
            make_candle(2050, 2055, 2045, 2052, time_str="2025-01-15T08:00:00Z"),
            make_candle(2052, 2058, 2048, 2055, time_str="2025-01-15T09:00:00Z"),
        ]
        high, low = session_mgr.calc_asia_range(candles)
        assert high == 0.0
        assert low == 999999.0

    def test_extracts_asia_range(self, session_mgr):
        candles = [
            make_candle(2050, 2060, 2040, 2055, time_str="2025-01-15T01:00:00Z"),
            make_candle(2055, 2070, 2045, 2065, time_str="2025-01-15T03:00:00Z"),
            make_candle(2065, 2080, 2060, 2075, time_str="2025-01-15T05:00:00Z"),
            # London candle (should be excluded)
            make_candle(2075, 2100, 2030, 2090, time_str="2025-01-15T08:00:00Z"),
        ]
        high, low = session_mgr.calc_asia_range(candles)
        assert high == 2080  # max high from Asia candles
        assert low == 2040   # min low from Asia candles

    def test_handles_invalid_timestamps(self, session_mgr):
        candles = [
            {"open": 100, "high": 110, "low": 90, "close": 105, "time": "invalid"},
        ]
        high, low = session_mgr.calc_asia_range(candles)
        assert high == 0.0
        assert low == 999999.0


class TestParseTime:
    """Timestamp parsing."""

    def test_iso_format_with_z(self, session_mgr):
        candle = {"time": "2025-01-15T10:30:00Z"}
        dt = session_mgr._parse_time(candle)
        assert dt is not None
        assert dt.hour == 10
        assert dt.minute == 30

    def test_iso_format_with_offset(self, session_mgr):
        candle = {"time": "2025-01-15T10:30:00+00:00"}
        dt = session_mgr._parse_time(candle)
        assert dt is not None

    def test_invalid_time_returns_none(self, session_mgr):
        candle = {"time": "not-a-date"}
        assert session_mgr._parse_time(candle) is None

    def test_missing_time_returns_none(self, session_mgr):
        assert session_mgr._parse_time({}) is None
