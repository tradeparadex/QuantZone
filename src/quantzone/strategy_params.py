from decimal import Decimal as D

from .utils.parameters_manager import Param, ParamsManager


class StrategyParameters:
    PARAM_CLOSE_ONLY_MODE = Param("close_only_mode", "False", bool)
    PARAM_ENABLED = Param("enabled", "False", bool)
    PARAM_ORDER_LEVEL_SPREAD = Param("order_level_spread", "2", D)
    PARAM_ORDER_LEVEL_SPREAD_LAMBDA = Param("order_level_spread_lambda", "0.5", D)
    PARAM_ORDER_SIZE_SPREAD_LAMBDA = Param("order_size_spread_lambda", "0.8", D)
    PARAM_ORDER_LEVEL_AMOUNT_PCT = Param("order_level_amount_pct", "20", D)
    PARAM_REEVAL_TIME_SEC = Param("reeval_time_sec", "1", float)
    PARAM_ORDER_INSERT_TIME_SEC = Param("order_insert_time_sec", "2", float)
    PARAM_BUY_LEVELS = Param("buy_levels", "4", int)
    PARAM_SELL_LEVELS = Param("sell_levels", "4", int)
    PARAM_ORDER_REFRESH_TOLERANCE_PCT = Param("order_refresh_tolerance_pct", "0.1", D)
    PARAM_PRICE_ADJUSTMENT_BPS = Param("price_adjustment_bps", "0", D)

    PARAM_BID_SPREAD = Param("bid_spread", "0.01", D)
    PARAM_ASK_SPREAD = Param("ask_spread", "0.01", D)
    PARAM_MINIMUM_SPREAD = Param("minimum_spread", "0", D)
    PARAM_ORDER_AMOUNT_USD = Param("order_amount_usd", "400", D)
    PARAM_FIXED_ORDER_SIZE = Param("fixed_order_size", "0", D)
    PARAM_POS_LEAN_BPS_PER_100K_USD = Param("pos_lean_bps_per_100k_usd", "200", D)
    PARAM_MAX_POSITION_USD = Param("max_position_usd", "2000", D)
    PARAM_TAKER_THRESHOLD_BPS = Param("taker_threshold_bps", "10", D)
    PARAM_PRICE_EMA_SEC = Param("price_ema_sec", "60", float)
    PARAM_FR_EMA_SEC = Param("fr_ema_sec", "2880", float)
    PARAM_BASIS_EMA_SEC = Param("basis_ema_sec", "2880", float)
    PARAM_MAX_LEVERAGE = Param("max_leverage", "4", D)
    PARAM_MAX_MARGIN_RATIO = Param("max_margin_ratio", "10", D)
    PARAM_GLOBAL_POS_LEAN_BPS_PER_100K_USD = Param("global_pos_lean_bps_per_100k_usd", "200", D)
    PARAM_PRICING_BASIS_FACTOR = Param("pricing_basis_factor", "0.5", D)
    PARAM_PRICING_VOLATILITY_FACTOR = Param("pricing_volatility_factor", "0.1", D)
    PARAM_VOL_WINDOW_SIZE = Param("vol_window_size", "1000", int)
    PARAM_PRICING_FUNDING_RATE_FACTOR = Param("pricing_funding_rate_factor", "0.5", D)
    PARAM_EMPTY_BOOK_PENALTY = Param("empty_book_penalty", "0.01", D)
    PARAM_MAX_MARKET_LATENCY_SEC = Param("max_market_latency_sec", "10", float)
    PARAM_MAX_DATA_DELAY_SEC = Param("max_data_delay_sec", "800", float)

    PARAM_PRICE_CEILING = Param("price_ceiling", "0", D)
    PARAM_PRICE_FLOOR = Param("price_floor", "0", D)
    PARAM_BULK_REQUESTS = Param("bulk_requests", "True", bool)
    PARAM_BASE_VOLATILITY = Param("base_volatility", "0.05", D)
    PARAM_VOLATILITY_EXPONENT = Param("volatility_exponent", "2", D)
    PARAM_VOLATILITY_CAP = Param("volatility_cap", "1", D)
    PARAM_ANCHOR_PRICE = Param("anchor_price", "0", D)
    PARAM_EXTERNAL_PRICE_MULTIPLIER = Param("external_price_multiplier", "1", D)

    PARAM_PUBLISH_ORDER_LATENCY = Param("publish_order_latency", "False", bool)
    PARAM_ORDER_SIZE_OBFUSCATION_FACTOR_PCT = Param("order_size_obfuscation_factor_pct", "0", D)
    PARAM_PREMIUM_FACTOR = Param("premium_factor", "0", D)
    PARAM_PREMIUM_WINDOW_SIZE_SEC = Param("premium_window_size_sec", "1800", float)
    PARAM_PREMIUM_ADJUSTMENT_CAP = Param("premium_adjustment_cap", "0.001", D)
    PARAM_TAKE_PROFIT_BPS = Param("take_profit_bps", "0", D)
    PARAM_TAKE_PROFIT_DECAY_FACTOR = Param("take_profit_decay_factor_sec", "60", D)
    PARAM_CANCEL_BY_EXCHANGE_ORDER_ID = Param("cancel_by_exchange_order_id", "True", bool)
    PARAM_ORDER_RATIO_TO_CANCEL_ALL = Param("order_ratio_to_cancel_all", "0.5", D)

    @classmethod
    def params(cls) -> list[Param]:
        strategy_params = [
            getattr(cls, name)
            for name in dir(cls)
            if isinstance(getattr(cls, name), Param) and name.startswith("PARAM_")
        ]
        return strategy_params


class StrategeParamsMixin:
    _params_manager: ParamsManager

    @property
    def is_enabled(self):
        return bool(self._params_manager.get_param_value(StrategyParameters.PARAM_ENABLED))

    @property
    def take_profit_bps(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_TAKE_PROFIT_BPS))

    @property
    def take_profit_decay_factor_sec(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_TAKE_PROFIT_DECAY_FACTOR))

    @property
    def publish_order_latency(self) -> bool:
        return bool(self._params_manager.get_param_value(StrategyParameters.PARAM_PUBLISH_ORDER_LATENCY))

    @property
    def premium_correction_factor(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PREMIUM_FACTOR))

    @property
    def price_adjustment(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PRICE_ADJUSTMENT_BPS) / D(10_000))

    @property
    def order_insert_time_ms(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_INSERT_TIME_SEC) * 1000)

    @property
    def reevaluation_time_sec(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_REEVAL_TIME_SEC) * 1000)

    @property
    def base_volatility(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_BASE_VOLATILITY))

    @property
    def volatility_cap(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_VOLATILITY_CAP))

    @property
    def premium_adjustment_cap(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PREMIUM_ADJUSTMENT_CAP))

    @property
    def volatility_exponent(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_VOLATILITY_EXPONENT))

    @property
    def pricing_basis_factor(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PRICING_BASIS_FACTOR))

    @property
    def pricing_volatility_factor(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PRICING_VOLATILITY_FACTOR))

    @property
    def vol_window_size(self) -> int:
        return int(self._params_manager.get_param_value(StrategyParameters.PARAM_VOL_WINDOW_SIZE))

    @property
    def pricing_funding_rate_factor(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PRICING_FUNDING_RATE_FACTOR))

    @property
    def premium_window_size_sec(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_PREMIUM_WINDOW_SIZE_SEC))

    @property
    def price_ema_sec(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_PRICE_EMA_SEC))

    @property
    def fr_ema_sec(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_FR_EMA_SEC))

    @property
    def basis_ema_sec(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_BASIS_EMA_SEC))

    @property
    def pos_lean_bps_per_100k(self) -> D:
        return (
            D(self._params_manager.get_param_value(StrategyParameters.PARAM_POS_LEAN_BPS_PER_100K_USD))
            / D(100_000)
            / D(10_000)
        )

    @property
    def pos_global_lean_bps_per_100k(self) -> D:
        return (
            D(self._params_manager.get_param_value(StrategyParameters.PARAM_GLOBAL_POS_LEAN_BPS_PER_100K_USD))
            / D(100_000)
            / D(10_000)
        )

    @property
    def empty_book_penalty(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_EMPTY_BOOK_PENALTY))

    @property
    def max_market_latency_ms(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_MAX_MARKET_LATENCY_SEC)) * 1000

    @property
    def max_data_delay_ms(self) -> float:
        return float(self._params_manager.get_param_value(StrategyParameters.PARAM_MAX_DATA_DELAY_SEC)) * 1000

    @property
    def order_level_spread(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_LEVEL_SPREAD))

    @property
    def order_level_amount_bps(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_LEVEL_AMOUNT_PCT)) / D("100")

    @property
    def buy_levels(self) -> int:
        return int(self._params_manager.get_param_value(StrategyParameters.PARAM_BUY_LEVELS))

    @property
    def sell_levels(self) -> int:
        return int(self._params_manager.get_param_value(StrategyParameters.PARAM_SELL_LEVELS))

    @property
    def bid_spread(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_BID_SPREAD)) / D("100")

    @property
    def ask_spread(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ASK_SPREAD)) / D("100")

    @property
    def order_level_spread_lambda(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_LEVEL_SPREAD_LAMBDA))

    @property
    def order_size_spread_lambda(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_SIZE_SPREAD_LAMBDA))

    @property
    def price_ceiling(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PRICE_CEILING))

    @property
    def price_floor(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_PRICE_FLOOR))

    @property
    def taker_threshold_bps(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_TAKER_THRESHOLD_BPS))

    @property
    def order_amount_usd(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_AMOUNT_USD))

    @property
    def max_leverage(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_MAX_LEVERAGE))

    @property
    def max_margin_ratio(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_MAX_MARGIN_RATIO))

    @property
    def max_position_usd(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_MAX_POSITION_USD))

    @property
    def order_refresh_tolerance(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_REFRESH_TOLERANCE_PCT)) / D("100")

    @property
    def order_size_obfuscation_factor_pct(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_SIZE_OBFUSCATION_FACTOR_PCT)) / D(
            "100"
        )

    @property
    def minimum_spread(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_MINIMUM_SPREAD)) / D("100")

    @property
    def bulk_requests(self) -> bool:
        return bool(self._params_manager.get_param_value(StrategyParameters.PARAM_BULK_REQUESTS))

    @property
    def external_price_multiplier(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_EXTERNAL_PRICE_MULTIPLIER))

    @property
    def fixed_order_size(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_FIXED_ORDER_SIZE))

    @property
    def anchor_price(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ANCHOR_PRICE))

    @property
    def cancel_by(self) -> str:
        by_exchange_order_id = self._params_manager.get_param_value(
            StrategyParameters.PARAM_CANCEL_BY_EXCHANGE_ORDER_ID
        )
        if by_exchange_order_id or by_exchange_order_id is None:
            order_identifier = "exchange_order_id"
        else:
            order_identifier = "client_order_id"
        return order_identifier

    @property
    def order_ratio_to_cancel_all(self) -> D:
        return D(self._params_manager.get_param_value(StrategyParameters.PARAM_ORDER_RATIO_TO_CANCEL_ALL))
