"""Tests for PositionMgr — lot size calculation and trade lifecycle."""

import pytest
from bot import ScalpConfig, BotState, Direction


class TestCalcLots:
    """Position sizing based on risk percentage and SL distance."""

    def _make_pos_mgr(self, balance=10_000.0):
        """Create a minimal PositionMgr for lot calculation tests."""
        cfg = ScalpConfig()
        state = BotState(cfg)
        state.balance = balance

        # PositionMgr needs conn, db, tg — but calc_lots is pure math
        # so we import and construct with None stubs
        from bot import PositionMgr
        return PositionMgr(state, conn=None, db=None, tg=None)

    def test_basic_lot_calculation(self):
        pm = self._make_pos_mgr(balance=10_000.0)
        # Risk 1% of 10k = $100
        # SL dist = 5.0 points, per_lot = $100/point
        # lots = 100 / (5.0 * 100) = 0.2
        lots = pm.calc_lots(5.0)
        assert lots == 0.2

    def test_minimum_lot_size(self):
        pm = self._make_pos_mgr(balance=100.0)
        # Very small account: risk = $1, lots would be tiny
        lots = pm.calc_lots(5.0)
        assert lots >= 0.01  # minimum lot size

    def test_maximum_lot_cap(self):
        pm = self._make_pos_mgr(balance=1_000_000.0)
        # Huge account would want large lots
        lots = pm.calc_lots(1.0)
        assert lots <= 0.5  # capped at 0.5

    def test_zero_sl_distance(self):
        pm = self._make_pos_mgr(balance=10_000.0)
        lots = pm.calc_lots(0.0)
        assert lots == 0.01  # minimum when SL distance is 0

    def test_negative_sl_distance(self):
        pm = self._make_pos_mgr(balance=10_000.0)
        lots = pm.calc_lots(-5.0)
        assert lots == 0.01  # minimum when SL distance is negative

    def test_lot_rounding(self):
        pm = self._make_pos_mgr(balance=10_000.0)
        lots = pm.calc_lots(3.33)
        # Should be rounded to 2 decimal places
        assert lots == round(lots, 2)

    def test_scales_with_balance(self):
        pm_small = self._make_pos_mgr(balance=5_000.0)
        pm_large = self._make_pos_mgr(balance=20_000.0)
        lots_small = pm_small.calc_lots(5.0)
        lots_large = pm_large.calc_lots(5.0)
        assert lots_large > lots_small

    def test_scales_with_sl_distance(self):
        pm = self._make_pos_mgr(balance=10_000.0)
        lots_tight = pm.calc_lots(2.0)
        lots_wide = pm.calc_lots(8.0)
        assert lots_tight > lots_wide  # tighter SL = more lots

    def test_risk_percent_applied(self):
        pm = self._make_pos_mgr(balance=10_000.0)
        # Default risk is 1%
        # Risk = 10000 * 0.01 = 100
        # lots = 100 / (10 * 100) = 0.1
        lots = pm.calc_lots(10.0)
        assert lots == 0.1
