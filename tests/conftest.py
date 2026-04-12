"""Shared fixtures for the Gold Scalper test suite."""

import pytest
import time
from bot import (
    ScalpConfig, BotState, ScalpAnalyzer, SessionMgr,
    ScalpSignal, Database, Direction, Session, TradePhase,
    ScalpTrade, SwingPoint, OrderBlock, FairValueGap,
)


@pytest.fixture
def cfg():
    """Default ScalpConfig with safe test defaults."""
    return ScalpConfig()


@pytest.fixture
def state(cfg):
    """BotState initialized with default config."""
    s = BotState(cfg)
    s.balance = 10_000.0
    s.start_balance = 10_000.0
    s.equity = 10_000.0
    return s


@pytest.fixture
def analyzer(cfg):
    """ScalpAnalyzer instance."""
    return ScalpAnalyzer(cfg)


@pytest.fixture
def session_mgr(cfg):
    """SessionMgr instance."""
    return SessionMgr(cfg)


@pytest.fixture
def db(tmp_path):
    """In-memory-like Database using a temp file."""
    d = Database(str(tmp_path / "test.db"))
    yield d
    d.close()


def make_candle(open_: float, high: float, low: float, close: float,
                volume: int = 100, time_str: str = "2025-01-15T10:00:00Z"):
    """Helper to build a candle dict."""
    return {
        "open": open_, "high": high, "low": low, "close": close,
        "tickVolume": volume, "time": time_str,
    }


def make_candles_uptrend(n: int = 30, start: float = 2000.0, step: float = 1.0):
    """Generate n candles in an uptrend."""
    candles = []
    for i in range(n):
        o = start + i * step
        c = o + step * 0.8
        h = c + step * 0.1
        l = o - step * 0.1
        candles.append(make_candle(o, h, l, c))
    return candles


def make_candles_downtrend(n: int = 30, start: float = 2100.0, step: float = 1.0):
    """Generate n candles in a downtrend."""
    candles = []
    for i in range(n):
        o = start - i * step
        c = o - step * 0.8
        l = c - step * 0.1
        h = o + step * 0.1
        candles.append(make_candle(o, h, l, c))
    return candles


def make_candles_flat(n: int = 30, price: float = 2050.0, noise: float = 0.5):
    """Generate n sideways candles."""
    candles = []
    for i in range(n):
        o = price + (i % 2) * noise
        c = price - (i % 2) * noise
        h = max(o, c) + noise * 0.5
        l = min(o, c) - noise * 0.5
        candles.append(make_candle(o, h, l, c))
    return candles
