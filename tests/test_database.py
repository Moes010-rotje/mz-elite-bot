"""Tests for Database — trade and daily stats persistence."""

import pytest
import sqlite3
from bot import Database, ScalpTrade, Direction, TradePhase


class TestDatabaseInit:
    """Database initialization and schema creation."""

    def test_creates_tables(self, db):
        cursor = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        assert "scalp_trades" in tables
        assert "daily_stats" in tables

    def test_wal_mode_enabled(self, db):
        result = db.conn.execute("PRAGMA journal_mode").fetchone()
        assert result[0] == "wal"


class TestSaveTrade:
    """Saving and updating trades."""

    def test_save_new_trade(self, db):
        trade = ScalpTrade(
            id="test_1", direction=Direction.LONG,
            entry=2050.0, sl=2045.0, tp=2060.0, tp1=2052.0,
            lots=0.1
        )
        db.save_trade(trade, session="london")

        row = db.conn.execute(
            "SELECT * FROM scalp_trades WHERE id = ?", ("test_1",)
        ).fetchone()
        assert row is not None
        assert row[1] == "buy"       # direction
        assert row[2] == 2050.0      # entry
        assert row[3] == 2045.0      # sl
        assert row[4] == 2060.0      # tp
        assert row[5] == 0.1         # lots

    def test_update_existing_trade(self, db):
        trade = ScalpTrade(
            id="test_2", direction=Direction.SHORT,
            entry=2050.0, sl=2055.0, tp=2040.0, tp1=2047.0,
            lots=0.2
        )
        db.save_trade(trade)

        # Update phase and PnL
        trade.phase = TradePhase.TP1_HIT
        trade.pnl = 50.0
        db.save_trade(trade)

        row = db.conn.execute(
            "SELECT phase, pnl FROM scalp_trades WHERE id = ?", ("test_2",)
        ).fetchone()
        assert row[0] == "tp1_hit"
        assert row[1] == 50.0

    def test_save_trade_with_session(self, db):
        trade = ScalpTrade(
            id="test_3", direction=Direction.LONG,
            entry=2050.0, sl=2045.0, tp=2060.0, tp1=2052.0,
            lots=0.1
        )
        db.save_trade(trade, session="ny_overlap")

        row = db.conn.execute(
            "SELECT session FROM scalp_trades WHERE id = ?", ("test_3",)
        ).fetchone()
        assert row[0] == "ny_overlap"

    def test_multiple_trades(self, db):
        for i in range(5):
            trade = ScalpTrade(
                id=f"multi_{i}", direction=Direction.LONG,
                entry=2050.0 + i, sl=2045.0, tp=2060.0, tp1=2052.0,
                lots=0.1
            )
            db.save_trade(trade)

        count = db.conn.execute("SELECT COUNT(*) FROM scalp_trades").fetchone()[0]
        assert count == 5


class TestSaveDaily:
    """Daily statistics persistence."""

    def test_save_daily_stats(self, db):
        db.save_daily("2025-01-15", trades=10, wins=7, losses=3, pnl=250.0, dd=1.5)

        row = db.conn.execute(
            "SELECT * FROM daily_stats WHERE date = ?", ("2025-01-15",)
        ).fetchone()
        assert row is not None
        assert row[1] == 10    # trades
        assert row[2] == 7     # wins
        assert row[3] == 3     # losses
        assert row[4] == 250.0 # pnl
        assert row[5] == 1.5   # max_drawdown

    def test_update_daily_stats(self, db):
        db.save_daily("2025-01-15", trades=5, wins=3, losses=2, pnl=100.0, dd=0.5)
        db.save_daily("2025-01-15", trades=10, wins=7, losses=3, pnl=250.0, dd=1.5)

        count = db.conn.execute(
            "SELECT COUNT(*) FROM daily_stats WHERE date = ?", ("2025-01-15",)
        ).fetchone()[0]
        assert count == 1  # upserted, not duplicated

        row = db.conn.execute(
            "SELECT trades FROM daily_stats WHERE date = ?", ("2025-01-15",)
        ).fetchone()
        assert row[0] == 10

    def test_multiple_days(self, db):
        for i in range(3):
            db.save_daily(f"2025-01-{15+i}", trades=i*5, wins=i*3,
                         losses=i*2, pnl=i*100.0, dd=i*0.5)

        count = db.conn.execute("SELECT COUNT(*) FROM daily_stats").fetchone()[0]
        assert count == 3


class TestDatabaseClose:
    """Database connection cleanup."""

    def test_close_does_not_raise(self, tmp_path):
        db = Database(str(tmp_path / "close_test.db"))
        db.close()
        # Verify connection is closed by trying to use it
        with pytest.raises(Exception):
            db.conn.execute("SELECT 1")
