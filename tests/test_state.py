"""Tests for BotState and ScalpConfig — initialization and defaults."""

import pytest
from bot import ScalpConfig, BotState, Session, Direction


class TestScalpConfig:
    """Configuration defaults and constraints."""

    def test_default_risk_percent(self):
        cfg = ScalpConfig()
        assert cfg.RISK_PERCENT == 1.0

    def test_default_max_concurrent_trades(self):
        cfg = ScalpConfig()
        assert cfg.MAX_CONCURRENT_TRADES == 3

    def test_default_max_daily_trades(self):
        cfg = ScalpConfig()
        assert cfg.MAX_DAILY_TRADES == 30

    def test_sl_constraints(self):
        cfg = ScalpConfig()
        assert cfg.MIN_SL_POINTS < cfg.MAX_SL_POINTS
        assert cfg.MIN_SL_POINTS > 0

    def test_rr_ratios_positive(self):
        cfg = ScalpConfig()
        assert cfg.DEFAULT_RR_RATIO > 0
        assert cfg.LONDON_RR_RATIO > 0
        assert cfg.NY_RR_RATIO > 0
        assert cfg.OVERLAP_RR_RATIO > 0

    def test_tp1_rr_less_than_full_rr(self):
        cfg = ScalpConfig()
        assert cfg.TP1_RR_RATIO < cfg.DEFAULT_RR_RATIO

    def test_partial_percent_valid(self):
        cfg = ScalpConfig()
        assert 0 < cfg.PARTIAL_PERCENT < 1

    def test_session_hours_non_overlapping_logic(self):
        cfg = ScalpConfig()
        # Asia ends before London starts
        assert cfg.ASIA_END <= cfg.LONDON_START
        # London ends at or before NY starts
        assert cfg.LONDON_END <= cfg.NY_END
        # Overlap is within London-NY window
        assert cfg.OVERLAP_START >= cfg.LONDON_START
        assert cfg.OVERLAP_END <= cfg.NY_END

    def test_bb_std_dev_positive(self):
        cfg = ScalpConfig()
        assert cfg.BB_STD_DEV > 0

    def test_rsi_thresholds_ordered(self):
        cfg = ScalpConfig()
        assert cfg.RSI_EXTREME_OVERSOLD < cfg.RSI_OVERSOLD
        assert cfg.RSI_OVERBOUGHT < cfg.RSI_EXTREME_OVERBOUGHT
        assert cfg.RSI_OVERSOLD < cfg.RSI_OVERBOUGHT


class TestBotState:
    """BotState initialization."""

    def test_initial_state(self, state):
        assert state.running is True
        assert state.session == Session.OFF
        assert state.daily_trades == 0
        assert state.daily_pnl == 0.0
        assert state.daily_wins == 0
        assert state.daily_losses == 0
        assert state.consecutive_losses == 0
        assert len(state.active_trades) == 0
        assert len(state.candles_1m) == 0
        assert len(state.candles_5m) == 0

    def test_asia_range_defaults(self, state):
        assert state.asia_high == 0.0
        assert state.asia_low == 999999.0

    def test_session_level_defaults(self, state):
        assert state.prev_session_high == 0.0
        assert state.prev_session_low == 999999.0
        assert state.current_session_high == 0.0
        assert state.current_session_low == 999999.0

    def test_trend_default_none(self, state):
        assert state.trend_5m is None

    def test_ema_defaults(self, state):
        assert state.ema_fast == 0.0
        assert state.ema_slow == 0.0

    def test_vwap_default(self, state):
        assert state.vwap == 0.0

    def test_telegram_timing_defaults(self, state):
        assert state.last_tg_time == 0.0
        assert state.last_heartbeat_time == 0.0
