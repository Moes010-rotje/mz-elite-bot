"""Tests for ScalpAnalyzer — technical indicator calculations."""

import pytest
from bot import ScalpAnalyzer, ScalpConfig, Direction, SwingPoint
from tests.conftest import make_candle, make_candles_uptrend, make_candles_downtrend, make_candles_flat


class TestATR:
    """Average True Range calculation."""

    def test_atr_returns_default_when_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 5
        assert analyzer.atr(candles, period=10) == 5.0

    def test_atr_with_exact_period_data(self, analyzer):
        candles = make_candles_uptrend(12, start=2000, step=2.0)
        result = analyzer.atr(candles, period=10)
        assert result > 0
        assert isinstance(result, float)

    def test_atr_increases_with_volatility(self, analyzer):
        # Low volatility candles
        low_vol = [make_candle(100, 100.5, 99.5, 100)] * 15
        atr_low = analyzer.atr(low_vol, period=10)

        # High volatility candles
        high_vol = [make_candle(100, 110, 90, 100)] * 15
        atr_high = analyzer.atr(high_vol, period=10)

        assert atr_high > atr_low

    def test_atr_uses_true_range_including_gaps(self, analyzer):
        # Candle that gaps up from previous close
        candles = [make_candle(100, 102, 98, 100)] * 5
        candles.append(make_candle(110, 115, 109, 112))  # gap up
        candles += [make_candle(112, 114, 110, 113)] * 10
        result = analyzer.atr(candles, period=10)
        assert result > 0


class TestEMA:
    """Exponential Moving Average calculation."""

    def test_ema_returns_zero_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 3
        assert analyzer.ema(candles, period=9) == 0.0

    def test_ema_with_exact_period_equals_sma(self, analyzer):
        candles = [make_candle(100, 101, 99, float(100 + i)) for i in range(9)]
        result = analyzer.ema(candles, period=9)
        expected_sma = sum(100 + i for i in range(9)) / 9
        assert result == pytest.approx(expected_sma, abs=0.01)

    def test_ema_fast_above_slow_in_uptrend(self, analyzer):
        candles = make_candles_uptrend(30, start=2000, step=2.0)
        ema_fast = analyzer.ema(candles, period=9)
        ema_slow = analyzer.ema(candles, period=21)
        assert ema_fast > ema_slow

    def test_ema_fast_below_slow_in_downtrend(self, analyzer):
        candles = make_candles_downtrend(30, start=2100, step=2.0)
        ema_fast = analyzer.ema(candles, period=9)
        ema_slow = analyzer.ema(candles, period=21)
        assert ema_fast < ema_slow


class TestSwings:
    """Swing high/low detection."""

    def test_no_swings_with_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 3
        highs, lows = analyzer.swings(candles)
        assert highs == []
        assert lows == []

    def test_detects_swing_high(self, analyzer):
        # Create a clear peak in the middle
        candles = []
        for i in range(10):
            if i == 5:
                candles.append(make_candle(100, 120, 99, 100))  # peak
            else:
                candles.append(make_candle(100, 105, 95, 100))
        highs, lows = analyzer.swings(candles)
        assert len(highs) >= 1
        assert highs[0].price == 120
        assert highs[0].is_high is True

    def test_detects_swing_low(self, analyzer):
        # Create a clear trough in the middle
        candles = []
        for i in range(10):
            if i == 5:
                candles.append(make_candle(100, 101, 80, 100))  # trough
            else:
                candles.append(make_candle(100, 105, 95, 100))
        highs, lows = analyzer.swings(candles)
        assert len(lows) >= 1
        assert lows[0].price == 80
        assert lows[0].is_high is False


class TestOrderBlocks:
    """Order block identification."""

    def test_no_obs_with_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)]
        obs = analyzer.order_blocks(candles)
        assert obs == []

    def test_detects_bullish_ob(self, analyzer):
        # Bearish candle followed by engulfing bullish candle
        candles = [make_candle(100, 101, 99, 100)] * 18  # padding
        candles.append(make_candle(100, 101, 98, 99))     # bearish: open 100, close 99
        candles.append(make_candle(99, 103, 98, 102))     # bullish engulfing: close > prev high
        obs = analyzer.order_blocks(candles)
        bullish = [ob for ob in obs if ob.direction == Direction.LONG]
        assert len(bullish) >= 1

    def test_detects_bearish_ob(self, analyzer):
        # Bullish candle followed by engulfing bearish candle
        candles = [make_candle(100, 101, 99, 100)] * 18
        candles.append(make_candle(99, 102, 98, 101))     # bullish: open 99, close 101
        candles.append(make_candle(101, 103, 96, 97))     # bearish engulfing: close < prev low
        obs = analyzer.order_blocks(candles)
        bearish = [ob for ob in obs if ob.direction == Direction.SHORT]
        assert len(bearish) >= 1

    def test_old_obs_are_pruned(self, analyzer):
        # OBs older than OB_MAX_AGE_CANDLES should be removed
        candles = [make_candle(100, 101, 99, 100)] * 5
        # Create OB early
        candles.append(make_candle(100, 101, 98, 99))
        candles.append(make_candle(99, 103, 98, 102))
        # Add many more candles to push OB out of range
        candles += [make_candle(102, 103, 101, 102)] * 25
        obs = analyzer.order_blocks(candles)
        # The early OB should have been pruned
        for ob in obs:
            assert ob.candle_index >= len(candles) - analyzer.cfg.OB_MAX_AGE_CANDLES


class TestFVGs:
    """Fair Value Gap detection."""

    def test_no_fvgs_with_flat_candles(self, analyzer):
        candles = make_candles_flat(20, price=2050, noise=0.1)
        atr = analyzer.atr(candles)
        fvgs = analyzer.fvgs(candles, atr)
        assert len(fvgs) == 0

    def test_detects_bullish_fvg(self, analyzer):
        # Bullish FVG: candle 3 low > candle 1 high (gap up)
        candles = [make_candle(100, 101, 99, 100)] * 5
        candles.append(make_candle(100, 102, 99, 101))   # candle 1: high = 102
        candles.append(make_candle(103, 105, 102, 104))   # candle 2
        candles.append(make_candle(106, 108, 105, 107))   # candle 3: low = 105 > 102
        atr = 1.0  # use small ATR so FVG qualifies
        fvgs = analyzer.fvgs(candles, atr)
        bullish = [f for f in fvgs if f.direction == Direction.LONG]
        assert len(bullish) >= 1

    def test_detects_bearish_fvg(self, analyzer):
        # Bearish FVG: candle 1 low > candle 3 high (gap down)
        candles = [make_candle(100, 101, 99, 100)] * 5
        candles.append(make_candle(100, 102, 98, 101))    # candle 1: low = 98
        candles.append(make_candle(97, 98, 95, 96))       # candle 2
        candles.append(make_candle(94, 95, 93, 94))       # candle 3: high = 95 < 98
        atr = 1.0
        fvgs = analyzer.fvgs(candles, atr)
        bearish = [f for f in fvgs if f.direction == Direction.SHORT]
        assert len(bearish) >= 1

    def test_fvgs_capped_at_10(self, analyzer):
        # Generate many FVGs and verify only last 10 are kept
        candles = []
        price = 2000
        for i in range(50):
            candles.append(make_candle(price, price + 1, price - 1, price))
            price += 5  # big gaps
        atr = 0.1
        fvgs = analyzer.fvgs(candles, atr)
        assert len(fvgs) <= 10


class TestLiquiditySweep:
    """Liquidity sweep detection."""

    def test_no_sweep_without_data(self, analyzer):
        assert analyzer.liquidity_sweep([], [], []) is None
        assert analyzer.liquidity_sweep([make_candle(100, 101, 99, 100)], [], []) is None

    def test_detects_bullish_sweep(self, analyzer):
        # Last candle wicks below a swing low but closes above it
        swing_lows = [SwingPoint(5, 2000.0, False)]
        candles = [
            make_candle(2005, 2006, 2004, 2005),
            make_candle(2003, 2004, 1998, 2002),  # low < 2000, close > 2000
        ]
        result = analyzer.liquidity_sweep(candles, [], swing_lows)
        assert result == Direction.LONG

    def test_detects_bearish_sweep(self, analyzer):
        # Last candle wicks above a swing high but closes below it
        swing_highs = [SwingPoint(5, 2050.0, True)]
        candles = [
            make_candle(2045, 2046, 2044, 2045),
            make_candle(2048, 2052, 2047, 2048),  # high > 2050, close < 2050
        ]
        result = analyzer.liquidity_sweep(candles, swing_highs, [])
        assert result == Direction.SHORT


class TestMomentumCandle:
    """Momentum candle identification."""

    def test_no_momentum_on_doji(self, analyzer):
        # Doji: open == close, no body
        doji = make_candle(100, 105, 95, 100)
        result = analyzer.is_momentum_candle(doji, atr_val=5.0)
        assert result is None

    def test_bullish_momentum_candle(self, analyzer):
        # Big bullish body, small wicks
        candle = make_candle(100, 110.1, 99.9, 110)  # body=10, total=10.2
        result = analyzer.is_momentum_candle(candle, atr_val=5.0)
        assert result == Direction.LONG

    def test_bearish_momentum_candle(self, analyzer):
        candle = make_candle(110, 110.1, 99.9, 100)  # bearish body=10
        result = analyzer.is_momentum_candle(candle, atr_val=5.0)
        assert result == Direction.SHORT


class TestExhaustionCandle:
    """Exhaustion candle detection."""

    def test_no_exhaustion_on_normal_candle(self, analyzer):
        candle = make_candle(100, 102, 98, 101)
        assert analyzer.is_exhaustion_candle(candle) is None

    def test_bearish_exhaustion_long_upper_wick(self, analyzer):
        # Long upper wick = bearish exhaustion
        candle = make_candle(100, 110, 99.5, 100.5)  # upper wick = 9.5 of 10.5 total
        result = analyzer.is_exhaustion_candle(candle)
        assert result == Direction.SHORT

    def test_bullish_exhaustion_long_lower_wick(self, analyzer):
        # Long lower wick = bullish exhaustion
        candle = make_candle(100, 100.5, 90, 99.5)  # lower wick = 9.5 of 10.5 total
        result = analyzer.is_exhaustion_candle(candle)
        assert result == Direction.LONG

    def test_zero_range_candle_returns_none(self, analyzer):
        candle = make_candle(100, 100, 100, 100)
        assert analyzer.is_exhaustion_candle(candle) is None


class TestRoundNumbers:
    """Round number zone detection."""

    def test_at_round_number(self, analyzer):
        assert analyzer.near_round_number(2000.0) is True
        assert analyzer.near_round_number(2050.0) is True
        assert analyzer.near_round_number(2100.0) is True

    def test_near_round_number_within_zone(self, analyzer):
        # Zone is +-3.0 by default, interval is 50
        assert analyzer.near_round_number(2002.0) is True
        assert analyzer.near_round_number(2048.0) is True

    def test_far_from_round_number(self, analyzer):
        assert analyzer.near_round_number(2025.0) is False
        assert analyzer.near_round_number(2015.0) is False


class TestAsiaSweep:
    """Asia range sweep detection."""

    def test_no_sweep_when_no_asia_range(self, analyzer):
        candle = make_candle(2050, 2052, 2048, 2050)
        assert analyzer.asia_sweep(2050, 0, 999999, candle) is None

    def test_bullish_asia_sweep(self, analyzer):
        # Price sweeps below Asia low then closes above
        candle = make_candle(2048, 2050, 2038, 2046)
        result = analyzer.asia_sweep(2046, 2060, 2040, candle)
        assert result == Direction.LONG

    def test_bearish_asia_sweep(self, analyzer):
        # Price sweeps above Asia high then closes below
        candle = make_candle(2058, 2065, 2055, 2057)
        result = analyzer.asia_sweep(2057, 2060, 2040, candle)
        assert result == Direction.SHORT


class TestBollingerBands:
    """Bollinger Band calculations."""

    def test_returns_zeros_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 5
        upper, middle, lower = analyzer.bollinger_bands(candles, period=20)
        assert upper == 0.0 and middle == 0.0 and lower == 0.0

    def test_bands_symmetry(self, analyzer):
        candles = make_candles_flat(25, price=2050, noise=2.0)
        upper, middle, lower = analyzer.bollinger_bands(candles, period=20, std_dev=2.0)
        assert upper > middle > lower
        # Bands should be roughly symmetric around middle
        assert abs((upper - middle) - (middle - lower)) < 1.0

    def test_wider_bands_with_more_volatility(self, analyzer):
        # Need varying close values to produce non-zero std dev
        low_vol = [make_candle(100, 100.5, 99.5, 100 + (i % 3) * 0.1) for i in range(25)]
        high_vol = [make_candle(100, 110, 90, 100 + (i % 3) * 5.0) for i in range(25)]
        u1, m1, l1 = analyzer.bollinger_bands(low_vol, period=20)
        u2, m2, l2 = analyzer.bollinger_bands(high_vol, period=20)
        assert (u2 - l2) > (u1 - l1)

    def test_bb_percent_b_at_lower_band(self, analyzer):
        assert analyzer.bb_percent_b(100, 120, 100) == pytest.approx(0.0)

    def test_bb_percent_b_at_upper_band(self, analyzer):
        assert analyzer.bb_percent_b(120, 120, 100) == pytest.approx(1.0)

    def test_bb_percent_b_at_middle(self, analyzer):
        assert analyzer.bb_percent_b(110, 120, 100) == pytest.approx(0.5)

    def test_bb_width_zero_middle(self, analyzer):
        assert analyzer.bb_width(10, 5, 0) == 0.0


class TestRSI:
    """RSI calculation."""

    def test_rsi_default_on_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 3
        assert analyzer.rsi(candles, period=7) == 50.0

    def test_rsi_overbought_in_strong_uptrend(self, analyzer):
        candles = make_candles_uptrend(30, start=2000, step=3.0)
        rsi_val = analyzer.rsi(candles, period=7)
        assert rsi_val > 60  # should be elevated in uptrend

    def test_rsi_oversold_in_strong_downtrend(self, analyzer):
        candles = make_candles_downtrend(30, start=2100, step=3.0)
        rsi_val = analyzer.rsi(candles, period=7)
        assert rsi_val < 40  # should be depressed in downtrend

    def test_rsi_100_when_no_losses(self, analyzer):
        # Only gains
        candles = [make_candle(100 + i, 101 + i, 99 + i, 101 + i) for i in range(15)]
        rsi_val = analyzer.rsi(candles, period=7)
        assert rsi_val > 90  # near 100

    def test_rsi_range_0_to_100(self, analyzer):
        for candles in [make_candles_uptrend(30), make_candles_downtrend(30), make_candles_flat(30)]:
            rsi_val = analyzer.rsi(candles, period=7)
            assert 0 <= rsi_val <= 100


class TestStochRSI:
    """Stochastic RSI calculation."""

    def test_stoch_rsi_default_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 5
        assert analyzer.stoch_rsi(candles) == 50.0

    def test_stoch_rsi_range(self, analyzer):
        candles = make_candles_uptrend(40, start=2000, step=1.0)
        val = analyzer.stoch_rsi(candles)
        assert 0 <= val <= 100


class TestMeanReversion:
    """Mean reversion signal combining BB + RSI + StochRSI."""

    def test_no_signal_when_disabled(self, analyzer):
        cfg = ScalpConfig()
        cfg.USE_MEAN_REVERSION = False
        candles = make_candles_downtrend(30)
        assert analyzer.mean_reversion(candles, 1900, cfg) is None

    def test_no_signal_on_flat_market(self, analyzer, cfg):
        candles = make_candles_flat(30, price=2050, noise=0.1)
        result = analyzer.mean_reversion(candles, 2050, cfg)
        assert result is None

    def test_no_signal_insufficient_data(self, analyzer, cfg):
        candles = [make_candle(100, 101, 99, 100)] * 5
        result = analyzer.mean_reversion(candles, 100, cfg)
        assert result is None


class TestVWAP:
    """Volume Weighted Average Price."""

    def test_vwap_returns_zero_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 3
        assert analyzer.calculate_vwap(candles) == 0.0

    def test_vwap_with_equal_volume(self, analyzer):
        # With equal volume, VWAP = average of typical prices
        candles = [make_candle(100, 102, 98, 100, volume=100)] * 10
        vwap = analyzer.calculate_vwap(candles)
        typical = (102 + 98 + 100) / 3
        assert vwap == pytest.approx(typical, abs=0.01)

    def test_vwap_weighted_toward_high_volume(self, analyzer):
        candles = [
            make_candle(100, 102, 98, 100, volume=10),    # typical = 100
            make_candle(100, 102, 98, 100, volume=10),
            make_candle(100, 102, 98, 100, volume=10),
            make_candle(100, 102, 98, 100, volume=10),
            make_candle(200, 202, 198, 200, volume=1000),  # typical = 200, much higher volume
        ]
        vwap = analyzer.calculate_vwap(candles)
        # VWAP should be much closer to 200 than 100
        assert vwap > 150

    def test_vwap_handles_zero_volume(self, analyzer):
        candles = [make_candle(100, 102, 98, 100, volume=0)] * 10
        vwap = analyzer.calculate_vwap(candles)
        assert vwap > 0  # should use fallback volume of 1


class TestADX:
    """ADX trend strength calculation."""

    def test_adx_default_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 5
        assert analyzer.calculate_adx(candles) == 25.0

    def test_adx_high_in_strong_trend(self, analyzer):
        candles = make_candles_uptrend(60, start=2000, step=5.0)
        adx = analyzer.calculate_adx(candles, period=14)
        assert adx > 20  # should show trending market

    def test_adx_returns_float(self, analyzer):
        candles = make_candles_flat(60, price=2050)
        adx = analyzer.calculate_adx(candles, period=14)
        assert isinstance(adx, float)


class TestVWAPSignal:
    """VWAP direction signal."""

    def test_no_signal_when_vwap_zero(self, analyzer):
        assert analyzer.vwap_signal(2050, 0) is None

    def test_long_when_price_above_vwap(self, analyzer):
        assert analyzer.vwap_signal(2055, 2050) == Direction.LONG

    def test_short_when_price_below_vwap(self, analyzer):
        assert analyzer.vwap_signal(2045, 2050) == Direction.SHORT

    def test_none_when_price_at_vwap(self, analyzer):
        # Within 0.1% of VWAP = no signal
        assert analyzer.vwap_signal(2050, 2050) is None


class TestDoublePattern:
    """Double bottom/top detection."""

    def test_no_pattern_insufficient_data(self, analyzer):
        candles = [make_candle(100, 101, 99, 100)] * 5
        assert analyzer.detect_double_pattern(candles, [], []) is None

    def test_double_bottom_detected(self, analyzer):
        candles = make_candles_flat(20, price=2050, noise=3.0)
        # ATR will be ~6, tolerance = 6 * 0.3 = 1.8
        # Two lows within tolerance
        lows = [
            SwingPoint(5, 2040.0, False),
            SwingPoint(15, 2040.5, False),  # within 1.8 tolerance
        ]
        # Price is above the double bottom
        candles[-1] = make_candle(2050, 2052, 2049, 2051)
        result = analyzer.detect_double_pattern(candles, [], lows)
        assert result == Direction.LONG

    def test_double_top_detected(self, analyzer):
        candles = make_candles_flat(20, price=2050, noise=3.0)
        highs = [
            SwingPoint(5, 2060.0, True),
            SwingPoint(15, 2060.5, True),
        ]
        # Price is below the double top
        candles[-1] = make_candle(2050, 2052, 2049, 2051)
        result = analyzer.detect_double_pattern(candles, highs, [])
        assert result == Direction.SHORT


class TestSessionLevelReaction:
    """Previous session high/low reaction."""

    def test_no_reaction_when_no_levels(self, analyzer):
        candle = make_candle(2050, 2052, 2048, 2050)
        assert analyzer.check_session_level_reaction(2050, candle, 0, 999999) is None

    def test_short_at_session_high(self, analyzer):
        session_high = 2060.0
        session_low = 2040.0
        # Candle wicks to session high but closes below
        candle = make_candle(2058, 2061, 2057, 2058)
        result = analyzer.check_session_level_reaction(2058, candle, session_high, session_low)
        assert result == Direction.SHORT

    def test_long_at_session_low(self, analyzer):
        session_high = 2060.0
        session_low = 2040.0
        # Candle wicks to session low but closes above
        candle = make_candle(2042, 2043, 2039, 2042)
        result = analyzer.check_session_level_reaction(2042, candle, session_high, session_low)
        assert result == Direction.LONG
