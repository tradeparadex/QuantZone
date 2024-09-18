import asyncio
import logging
import os
import time
from decimal import Decimal as D
from typing import List
import traceback
import numpy as np
from connectors.base_connector import _get_connector
from utils.async_utils import safe_ensure_future
from utils.data_methods import (
    ConnectorBase,
    ExponentialMovingAverage,
    Order,
    OrderType,
    PriceSize,
    PriceType,
    Proposal,
    RollingAnnualizedVolatility,
    Side,
    Ticker,
    TriggerType,
)
from utils.metrics_publisher import MetricsMessage, MetricsPublisher
from utils.parameters_manager import Param, ParamsManager
from utils.risk_manager import RiskManager


class PerpMarketMaker:

    PARAM_CLOSE_ONLY_MODE = "close_only_mode"
    PARAM_ENABLED = "enabled"
    PARAM_ORDER_LEVEL_SPREAD = "order_level_spread"
    PARAM_ORDER_LEVEL_AMOUNT_PCT = "order_level_amount_pct"
    PARAM_ORDER_INSERT_TIME_SEC = "order_insert_time_sec"
    PARAM_REEVAL_TIME_SEC = "reeval_time_sec"
    PARAM_ORDER_REFRESH_TOLERANCE_PCT = "order_refresh_tolerance_pct"
    PARAM_PRICE_ADJUSTMENT_BPS = "price_adjustment_bps"
    PARAM_BUY_LEVELS = "buy_levels"
    PARAM_SELL_LEVELS = "sell_levels"
    PARAM_BID_SPREAD = "bid_spread"
    PARAM_ASK_SPREAD = "ask_spread"
    PARAM_MINIMUM_SPREAD = "minimum_spread"
    PARAM_ORDER_AMOUNT_USD = "order_amount_usd"
    PARAM_POS_LEAN_BPS_PER_100K_USD = "pos_lean_bps_per_100k_usd"
    PARAM_MAX_POSITION_USD = "max_position_usd"
    PARAM_TAKER_THRESHOLD_BPS = "taker_threshold_bps"
    PARAM_PRICE_EMA_SEC = "price_ema_sec"
    PARAM_FR_EMA_SEC = "fr_ema_sec"
    PARAM_BASIS_EMA_SEC = "basis_ema_sec"
    PARAM_MAX_LEVERAGE = "max_leverage"
    PARAM_MAX_MARGIN_RATIO = "max_margin_ratio"
    PARAM_GLOBAL_POS_LEAN_BPS_PER_100K_USD = "global_pos_lean_bps_per_100k_usd"
    PARAM_PRICING_BASIS_FACTOR = "pricing_basis_factor"
    PARAM_PRICING_VOLATILITY_FACTOR = "pricing_volatility_factor"
    PARAM_VOL_WINDOW_SIZE = "vol_window_size"
    PARAM_PRICING_FUNDING_RATE_FACTOR = "pricing_funding_rate_factor"
    PARAM_EMPTY_BOOK_PENALTY = "empty_book_penalty"
    PARAM_MAX_MARKET_LATENCY_SEC = "max_market_latency_sec"
    PARAM_MAX_DATA_DELAY_SEC = "max_data_delay_sec"
    PARAM_ORDER_LEVEL_SPREAD_LAMBDA = "order_level_spread_lambda"
    PARAM_ORDER_SIZE_SPREAD_LAMBDA = "order_size_spread_lambda"
    PARAM_PRICE_CEILING = "price_ceiling"
    PARAM_PRICE_FLOOR = "price_floor"
    PARAM_BULK_REQUESTS = "bulk_requests"
    PARAM_BASE_VOLATILITY = "base_volatility"
    PARAM_VOLATILITY_EXPONENT = "volatility_exponent"
    PARAM_VOLATILITY_CAP = "volatility_cap"



    def __init__(self, loop: asyncio.AbstractEventLoop, 
            rm: RiskManager=RiskManager, 
            pm: ParamsManager=ParamsManager, 
            mp: MetricsPublisher=MetricsPublisher
        ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.loop = loop

        self.market_connector = _get_connector('paradex_perp', loop=self.loop)

        self.algo_name = "PARABOT_MM"
        self.market: str = os.getenv("ALGO_PARAMS_MARKET")

        self.external_markets: str = os.getenv('ALGO_PARAMS_PRICE_SOURCES')

        if self.external_markets not in [None, '']:
            self.external_market_symbol = self.external_markets.split(':')[-1]
            self.external_market_exchange = self.external_markets.split(':')[0]
            self.external_connector = _get_connector(self.external_market_exchange, loop=self.loop)
        else:
            self.external_connector = None
            self.external_market_symbol = None
            self.external_market_exchange = None

        self._smoothen_spot_price: ExponentialMovingAverage = None
        self._smoothen_basis: ExponentialMovingAverage = None
        self._smoothen_funding_rate: ExponentialMovingAverage = None
        self._rolling_vol: RollingAnnualizedVolatility = None

        self._next_order_timestamp = 0
        self._next_reeval_timestamp = 0
        self._last_system_health_ok = self.now_ms()
        
        self.processing = False

        strategy_parameters = [
            Param(self.PARAM_CLOSE_ONLY_MODE, 'False', bool),
            Param(self.PARAM_ENABLED, 'False', bool),
            Param(self.PARAM_PRICE_ADJUSTMENT_BPS, '0', D),
            Param(self.PARAM_ORDER_LEVEL_SPREAD, '2', D),
            Param(self.PARAM_ORDER_LEVEL_SPREAD_LAMBDA, '0.5', D),
            Param(self.PARAM_ORDER_SIZE_SPREAD_LAMBDA, '0.8', D),
            Param(self.PARAM_ORDER_LEVEL_AMOUNT_PCT, '20', D),
            Param(self.PARAM_REEVAL_TIME_SEC, '1', float),
            Param(self.PARAM_ORDER_INSERT_TIME_SEC, '2', float),
            Param(self.PARAM_ORDER_REFRESH_TOLERANCE_PCT, '0.1', D),
            Param(self.PARAM_BUY_LEVELS, '4', int),
            Param(self.PARAM_SELL_LEVELS, '4', int),
            Param(self.PARAM_BID_SPREAD, '0.01', D),
            Param(self.PARAM_ASK_SPREAD, '0.01', D),
            Param(self.PARAM_MINIMUM_SPREAD, '0', D),
            Param(self.PARAM_ORDER_AMOUNT_USD, '400', D),
            Param(self.PARAM_POS_LEAN_BPS_PER_100K_USD, '200', D),
            Param(self.PARAM_MAX_POSITION_USD, '2000', D),
            Param(self.PARAM_TAKER_THRESHOLD_BPS, '10', D),
            Param(self.PARAM_PRICE_EMA_SEC, '60', float),
            Param(self.PARAM_FR_EMA_SEC, '2880', float),
            Param(self.PARAM_BASIS_EMA_SEC, '2880', float),
            Param(self.PARAM_MAX_LEVERAGE, '4', D),
            Param(self.PARAM_MAX_MARGIN_RATIO, '10', D),
            Param(self.PARAM_GLOBAL_POS_LEAN_BPS_PER_100K_USD, '200', D),
            Param(self.PARAM_PRICING_BASIS_FACTOR, '0.5', D),
            Param(self.PARAM_PRICING_VOLATILITY_FACTOR, '0.1', D),
            Param(self.PARAM_PRICING_FUNDING_RATE_FACTOR, '0.5', D),
            Param(self.PARAM_VOL_WINDOW_SIZE, '1000', int),
            Param(self.PARAM_EMPTY_BOOK_PENALTY, '0.01', D),
            Param(self.PARAM_MAX_MARKET_LATENCY_SEC, '10', float),
            Param(self.PARAM_MAX_DATA_DELAY_SEC, '800', float),
            Param(self.PARAM_PRICE_CEILING, '0', D),
            Param(self.PARAM_PRICE_FLOOR, '0', D),
            Param(self.PARAM_BULK_REQUESTS, 'True', bool),
            Param(self.PARAM_BASE_VOLATILITY, '0.05', D),
            Param(self.PARAM_VOLATILITY_EXPONENT, '2', D),
            Param(self.PARAM_VOLATILITY_CAP, '1', D)
        ]

        self._metrics_pub = mp()
        self._risk_manager = rm(parent=self)
        self._params_manager = pm(parent=self, params=strategy_parameters)

        self._reeval_task = None

        self._publish_metrics = time.time()
        self._metrics_publish_interval = 30

    @property
    def is_enabled(self):
        return self._params_manager.get_param_value(self.PARAM_ENABLED)

    @property
    def price_adjustment(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_PRICE_ADJUSTMENT_BPS) / D(10_000)
    
    @property
    def order_insert_time_ms(self) -> float:
        return self._params_manager.get_param_value(self.PARAM_ORDER_INSERT_TIME_SEC) * 1000

    @property
    def reevaluation_time_sec(self) -> float:
        return self._params_manager.get_param_value(self.PARAM_REEVAL_TIME_SEC) * 1000
    
    @property
    def base_volatility(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_BASE_VOLATILITY)

    @property
    def volatility_cap(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_VOLATILITY_CAP)
    
    @property
    def volatility_exponent(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_VOLATILITY_EXPONENT)
    
    @property
    def active_orders(self) -> List[Order]:
        return list(self.market_connector.active_orders.values())

    @property
    def pricing_basis_factor(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_PRICING_BASIS_FACTOR)
    
    @property
    def pricing_volatility_factor(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_PRICING_VOLATILITY_FACTOR)

    @property
    def vol_window_size(self) -> int:
        return self._params_manager.get_param_value(self.PARAM_VOL_WINDOW_SIZE)
    
    @property
    def pricing_funding_rate_factor(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_PRICING_FUNDING_RATE_FACTOR)
    
    @property
    def price_ema_sec(self) -> float:
        return self._params_manager.get_param_value(self.PARAM_PRICE_EMA_SEC)
    
    @property
    def fr_ema_sec(self) -> float:
        return self._params_manager.get_param_value(self.PARAM_FR_EMA_SEC)

    @property
    def basis_ema_sec(self) -> float:
        return self._params_manager.get_param_value(self.PARAM_BASIS_EMA_SEC)
    
    @property
    def pos_lean_bps_per_100k(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_POS_LEAN_BPS_PER_100K_USD) / D(100_000) / D(10_000)
    
    @property
    def pos_global_lean_bps_per_100k(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_GLOBAL_POS_LEAN_BPS_PER_100K_USD) / D(100_000) / D(10_000)
    
    @property
    def empty_book_penalty(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_EMPTY_BOOK_PENALTY)
    
    @property
    def max_market_latency_ms(self) -> float:
        return self._params_manager.get_param_value(self.PARAM_MAX_MARKET_LATENCY_SEC) * 1000
    
    @property
    def max_data_delay_ms(self) -> float:
        return self._params_manager.get_param_value(self.PARAM_MAX_DATA_DELAY_SEC) * 1000
    
    @property
    def order_level_spread(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_ORDER_LEVEL_SPREAD)
    
    @property
    def order_level_amount_bps(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_ORDER_LEVEL_AMOUNT_PCT) / D('100')
    
    @property
    def buy_levels(self) -> int:
        return self._params_manager.get_param_value(self.PARAM_BUY_LEVELS)
    
    @property
    def sell_levels(self) -> int:
        return self._params_manager.get_param_value(self.PARAM_SELL_LEVELS)
    
    @property
    def bid_spread(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_BID_SPREAD) / D('100')
    
    @property
    def ask_spread(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_ASK_SPREAD) / D('100')
    
    @property
    def order_level_spread_lambda(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_ORDER_LEVEL_SPREAD_LAMBDA)

    @property
    def order_size_spread_lambda(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_ORDER_SIZE_SPREAD_LAMBDA)

    @property
    def price_ceiling(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_PRICE_CEILING)

    @property
    def price_floor(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_PRICE_FLOOR)
    
    @property
    def taker_threshold_bps(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_TAKER_THRESHOLD_BPS)
    
    @property
    def order_amount_usd(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_ORDER_AMOUNT_USD)
    
    @property
    def max_leverage(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_MAX_LEVERAGE)
    
    @property
    def max_margin_ratio(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_MAX_MARGIN_RATIO)
    
    @property
    def max_position_usd(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_MAX_POSITION_USD)
    
    @property
    def order_refresh_tolerance(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_ORDER_REFRESH_TOLERANCE_PCT) / D('100')

    @property
    def minimum_spread(self) -> D:
        return self._params_manager.get_param_value(self.PARAM_MINIMUM_SPREAD) / D('100')
    
    @property
    def bulk_requests(self) -> bool:
        return self._params_manager.get_param_value(self.PARAM_BULK_REQUESTS)

    @property
    async def is_close_only_mode(self):
        if self._params_manager.get_param_value(self.PARAM_CLOSE_ONLY_MODE):
            return True
        return not (await self._risk_manager.can_quote_to_open())

    @property
    def min_order_amount(self) -> D:
        return self.market_connector.trading_rules[self.market].min_notional_size / self.get_price_by_type(PriceType.Mid)

    @property
    def order_amount(self) -> D:
        return self.order_amount_usd / self.get_price_by_type(PriceType.Mid)


    def now_ns(self) -> int:
        return int(time.time_ns())

    def now_ms(self) -> int:
        return int(self.now_ns() / 1e6)

    def _publish_strat_metric(self, tag, val):
        msg = MetricsMessage(
            timestamp=int(time.time() * 1000),
            process_name=self.algo_name,
            tag_name=tag,
            market=self.market,
            value=val,
            account=self.market_connector.get_account_str()
        )

        if isinstance(val, D):
            if not val.is_finite():
                self.logger.warning(f"Value for {tag} is not finite. Skipping publish.")
                return

        if isinstance(val, float):
            if not np.isfinite(val):
                self.logger.warning(f"Value for {tag} is not finite. Skipping publish.")
                return

        self._metrics_pub.stream_metrics(msg)

    def create_base_proposal(self):
        market: ConnectorBase = self.market_connector
        buys = []
        sells = []

        _num_ticks_increment = self.order_level_spread * market.trading_rules[self.market].min_price_increment
        _order_increment = self.order_level_amount_bps / D(10_000) * self.order_amount
        for level in range(0, self.buy_levels):
            price = self.get_fair_price(side=Side.BUY) - ((np.exp(self.order_level_spread_lambda * level) - 1) * _num_ticks_increment)
            size = self.order_amount + (_order_increment * (np.exp(self.order_size_spread_lambda * level) - 1))

            if size > 0:
                buys.append(PriceSize(price, size))                   
        for level in range(0, self.sell_levels):
            price = self.get_fair_price(side=Side.SELL) + ((np.exp(self.order_level_spread_lambda * level) - 1) * _num_ticks_increment)
            size = self.order_amount + (_order_increment * (np.exp(self.order_size_spread_lambda * level) - 1))

            if size > 0:
                sells.append(PriceSize(price, size))

        return Proposal(buys, sells)

    def quantize_values(self, proposal: Proposal):
        market: ConnectorBase = self.market_connector
        for buy in proposal.buys:
            buy.price = market.quantize_order_price(self.market, buy.price)
            buy.size = market.quantize_order_amount(self.market, buy.size)
        for sell in proposal.sells:
            sell.price = market.quantize_order_price(self.market, sell.price)
            sell.size = market.quantize_order_amount(self.market, sell.size)

        # filter if size is less than min_order_amount
        proposal.buys = [buy for buy in proposal.buys if buy.size > 0]
        proposal.sells = [sell for sell in proposal.sells if sell.size > 0]
    
    def outside_tolerance(self, current_prices: List[D], proposal_prices: List[D]) -> List[int]:
        
        within_tolerance = []
        deviated = []

        tolerances = [self.order_refresh_tolerance * D(1 + 5 * np.sqrt(i)) for i in range(len(current_prices))]
        for idx, px in enumerate(proposal_prices):
            if idx >= len(current_prices):
                break
            if abs(px - current_prices[idx]) / current_prices[idx] > tolerances[idx]:
                deviated.append(idx)
            else:
                within_tolerance.append(idx)

        if len(proposal_prices) < len(current_prices):
            deviated.extend(range(len(proposal_prices), len(current_prices)))

        return deviated, within_tolerance

    def cancel_orders_below_min_spread(self):
        if self.minimum_spread <= 0:
            return

        price = self.get_price_by_type(PriceType.Mid)

        if not price.is_finite():
            price = self._smoothen_spot_price.value
        
        for order in self.active_orders:
            negation = -1 if order.side == Side.BUY else 1
            if (negation * (order.price - price) / price) < self.minimum_spread / 2:
                self.logger.info(f"Order is below minimum spread ({self.minimum_spread})."
                                   f" Canceling Order: {order}")
                self.cancel_order(self.market, order.client_order_id)
                self.logger.info(f"Canceling order {order.client_order_id} below min spread.")
    
    def cancel_active_orders(self, proposal: Proposal):

        _active_orders = [o for o in self.active_orders if o.status not in ['CANCELLING']]
        if len(_active_orders) == 0:
            return

        buys_to_cancel = []
        sells_to_cancel = []

        buys_to_keep = []
        sells_to_keep = []

        active_buy_orders = [o for o in _active_orders if o.side == Side.BUY]
        active_sell_orders = [o for o in _active_orders if o.side == Side.SELL]

        active_buy_orders.sort(key=lambda x: x.price, reverse=True)
        active_sell_orders.sort(key=lambda x: x.price)

        active_buy_prices = [D(str(o.price)) for o in active_buy_orders]
        active_sell_prices = [D(str(o.price)) for o in active_sell_orders]

        active_buy_ids = [o.client_order_id for o in active_buy_orders]
        active_sell_ids = [o.client_order_id for o in active_sell_orders]

        if proposal is not None and self.order_refresh_tolerance >= 0:
            proposal_buys = [buy.price for buy in proposal.buys]
            proposal_sells = [sell.price for sell in proposal.sells]

            self.logger.info(f"active_buy_prices: {active_buy_prices}")
            self.logger.info(f"proposal_buys: {proposal_buys}")
            
            buys_to_cancel, buys_to_keep = self.outside_tolerance(active_buy_prices, proposal_buys)
            sells_to_cancel, sells_to_keep = self.outside_tolerance(active_sell_prices, proposal_sells)

            self.logger.info(f"buys_to_cancel: {buys_to_cancel}")
            self.logger.info(f"buys_to_keep: {buys_to_keep}")

        else:
            buys_to_cancel = range(len(_active_orders))
            sells_to_cancel = range(len(_active_orders))

        proposal.buys = [item for idx, item in enumerate(proposal.buys) if idx not in buys_to_keep]
        proposal.sells = [item for idx, item in enumerate(proposal.sells) if idx not in sells_to_keep]
        
        if len(buys_to_cancel) > 0 or len(sells_to_cancel) > 0:

            buy_ids_to_cancel = [active_buy_ids[idx] for idx in buys_to_cancel]
            sell_ids_to_cancel = [active_sell_ids[idx] for idx in sells_to_cancel]
            
            for id in buy_ids_to_cancel + sell_ids_to_cancel:
                self.cancel_order(self.market, id)
                self.logger.info(f"(z) Canceling order {id} outside of tolerance.")
        else:
            self.logger.info(f"Not canceling active orders since difference between new order prices "
                               f"and current order prices is within "
                               f"{self.order_refresh_tolerance:.2%} order_refresh_tolerance_pct")

    async def apply_budget_constraint(self, proposal: Proposal):
        if await self.is_close_only_mode:
            tot_pos_usd = self.get_active_position(self.market)
            if tot_pos_usd >= 0:
                proposal.buys = []
            elif tot_pos_usd <= 0:
                proposal.sells = []
            else:
                proposal.buys = []
                proposal.sells = []

    def filter_out_takers(self, proposal: Proposal):
        market: ConnectorBase = self.market_connector

        top_ask =self.get_price_by_type(PriceType.BestAsk)
        if not top_ask.is_finite():
            top_ask = self.get_fair_price(side=Side.SELL) * (1 + self.empty_book_penalty)
        
        top_bid = self.get_price_by_type(PriceType.BestBid)
        if not top_bid.is_finite():
            top_bid = self.get_fair_price(side=Side.BUY) * (1 - self.empty_book_penalty)

        price_tick = market.trading_rules[self.market].min_price_increment

        if not top_ask.is_nan() and len(proposal.buys) > 0:

            for idx, buy in enumerate(proposal.buys):
                self.logger.debug(f"buy price: {buy.price}, top_ask: {top_ask}, price_tick: {price_tick}, thresh: {self.taker_threshold_bps}")
                if idx == 0 and (buy.price / (top_ask+price_tick) - 1) * D(10_000) > self.taker_threshold_bps:

                    new_size = market.quantize_order_amount(self.market, max(min(buy.size, self.order_amount), self.min_order_amount))
                    proposal.buys[idx] = PriceSize(top_ask+price_tick, new_size, OrderType.LIMIT)
                elif buy.price >= top_ask:
                    proposal.buys[idx].price = top_bid - ((np.exp(self.order_level_spread_lambda * idx) - 1) * price_tick)
        
        if not top_bid.is_nan() and len(proposal.sells) > 0:

            for idx, sell in enumerate(proposal.sells):
                self.logger.debug(f"sell price: {sell.price}, top_bid: {top_bid}, price_tick: {price_tick}, thresh: {self.taker_threshold_bps}")
                if idx == 0 and ((top_bid-price_tick) / sell.price - 1) * D(10_000) > self.taker_threshold_bps:

                    new_size = market.quantize_order_amount(self.market, max(min(sell.size, self.order_amount), self.min_order_amount))
                    proposal.sells[idx] = PriceSize(top_bid-price_tick, new_size, OrderType.LIMIT)
                elif sell.price <= top_bid:
                    proposal.sells[idx].price = top_ask + ((np.exp(self.order_level_spread_lambda * idx) - 1) * price_tick)

    def apply_order_levels_modifiers(self, proposal: Proposal):
        self.apply_price_band(proposal)

    def apply_price_band(self, proposal: Proposal):
        if self.price_ceiling > 0 and self.get_price_by_type(PriceType.BestAsk) >= self.price_ceiling:
            proposal.buys = []
        if self.price_floor > 0 and self.get_price_by_type(PriceType.BestBid) <= self.price_floor:
            proposal.sells = []

    def execute_orders_proposal(self, proposal: Proposal):
        if self.now_ms() < self._next_order_timestamp:
            return
        else:
            self._next_order_timestamp = self.now_ms() + self.order_insert_time_ms

        all_orders = []
        for oreq in proposal.buys:
            all_orders.append(Order(self.market, Side.BUY, oreq.price, oreq.size, oreq.type))
        for oreq in proposal.sells:
            all_orders.append(Order(self.market, Side.SELL, oreq.price, oreq.size, oreq.type))

        if self.bulk_requests:
            self.market_connector.bulk_insert_orders(all_orders)
        else:
            for order in all_orders:
                self.market_connector.insert_order(order)
        
        self._publish_strat_metric("order_insert", len(all_orders))

    def market_data_ready(self) -> bool:
        if self.market not in self.market_connector.orderbooks:
            self.logger.warning(f"Market data not ready for {self.market}.")
            return False
        
        if self._smoothen_basis is None or self._smoothen_basis.value is None:
            self.logger.warning("Funding rate not ready.")
            return False

        if self._smoothen_spot_price is None or self._smoothen_spot_price.value is None:
            self.logger.warning("Spot price not ready.")
            return False
        
        if self._rolling_vol is None:
            self.logger.warning("Volatility not ready.")
            return False
        
        if self.get_price_by_type(PriceType.Mid) is None:
            self.logger.warning("Mid price not set.")
            return False
        
        return True


    async def reeval(self, trigger: TriggerType):
        if self.processing:
            self.logger.warning("Already processing. Skipping reeval.")
            return
        
        self.processing = True

        try:

            if not self.is_enabled:
                self.logger.info("Strategy is disabled. Skipping reeval.")
                self.cancel_all_orders()
                self.processing = False
                return

            if not self._risk_manager.is_system_health_ok():
                self.logger.warning("System health deteriorated. Market making will be halted.")
                self.cancel_all_orders()
                self.processing = False

                if self.now_ms() - self._last_system_health_ok > 20 * 60 * 1000:
                    # re-subscribe to data channels
                    await self._subscribe_to_data()
                return
            else:
                self._last_system_health_ok = self.now_ms()

            if self.now_ms() < self._next_reeval_timestamp:
                self.processing = False
                return
            else:
                self._next_reeval_timestamp = self.now_ms() + self.reevaluation_time_sec

            if not self.market_data_ready():
                self.processing = False
                return

            proposal = self.create_base_proposal()
            self.logger.debug(f"Initial proposals: {proposal}")

            # 2. Apply functions that limit numbers of buys and sells proposal
            self.apply_order_levels_modifiers(proposal)
            self.logger.debug(f"Proposals after order level modifier: {proposal}")
            
            # 4. Apply taker threshold, i.e. don't take more than a certain percentage of the market.
            self.filter_out_takers(proposal)
            self.logger.debug(f"Proposals after takers filter: {proposal}")

            await self.apply_budget_constraint(proposal)
            self.logger.debug(f"Proposals after budget constraint: {proposal}")

            self.quantize_values(proposal)
            self.logger.debug(f"Proposals after quantization: {proposal}")

            self.cancel_active_orders(proposal)
            self.logger.debug(f"Filtered proposal: {proposal}")

            self.cancel_orders_below_min_spread()
            self.logger.info(f"final proposal: {proposal}")

            self.execute_orders_proposal(proposal)
        except Exception as e:
            self.logger.error(f"Error in reeval: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.processing = False


    def cancel_order(self, mkt: str, order_id: str):
        self.market_connector.cancel_order(order_id)

    def cancel_all_orders(self):
        if self.bulk_requests:
            self.market_connector.bulk_cancel_orders([o.client_order_id for o in self.active_orders if o.status not in ['CANCELLING']])
        else:
            for order in self.active_orders:
                self.cancel_order(self.market, order.client_order_id)

    def get_price_by_type(self, price_type: PriceType = None) -> D:
        if price_type is None:
            price_type = PriceType.Mid

        if price_type == PriceType.BestBid:
            _val = self.market_connector.orderbooks[self.market].get_best_bid()
        elif price_type == PriceType.BestAsk:
            _val = self.market_connector.orderbooks[self.market].get_best_ask()
        else:
            _val = self.market_connector.orderbooks[self.market].get_mid()

        return D(_val) if _val is not None else None


    def get_external_connector_price(self, mkt, price_type: PriceType = None) -> D:
        if price_type is None:
            price_type = PriceType.Mid

        if price_type == PriceType.BestBid:
            _val = self.external_connector.orderbooks[mkt].get_best_bid()
        elif price_type == PriceType.BestAsk:
            _val = self.external_connector.orderbooks[mkt].get_best_ask()
        else:
            _val = self.external_connector.orderbooks[mkt].get_mid()

        return D(_val)

    def is_ready_to_trade(self) -> bool:
        if self.market not in self.market_connector.orderbooks:
            return False

        if not self.market_connector.account_info:
            return False

        if self.market not in self.market_connector.latest_fundings:
            return False
        
        return True
    
    def get_active_position(self, mkt: str) -> D:
        pos_obj = self.market_connector.positions.get(mkt)
        if pos_obj is None:
            return D(0)
        return D(pos_obj['size'])
    
    def get_global_position_usd(self) -> D:
        return D(np.sum(
            [np.sum([D(p['cost_usd']), D(p['unrealized_pnl']), D(p['unrealized_funding_pnl'])]) 
             for p in self.market_connector.positions.values()
            ]))

    def update_emas(self, timestamp: float):
        inst_rate = self.get_inst_rate()
        inst_basis = self.get_inst_basis()

        self._smoothen_funding_rate.update(inst_rate, timestamp)
        self._smoothen_basis.update(inst_basis, timestamp)

        _price = self.get_base_price(PriceType.Mid)

        self._smoothen_spot_price.update(_price, timestamp)
        self._rolling_vol.update(_price, timestamp)

    def get_base_price(self, price_type) -> float:
        if self.external_connector is None:
            raw_spot = self.get_price_by_type(price_type)
        else:
            raw_spot = self.get_external_connector_price(self.external_market_symbol, price_type)
        return raw_spot

    def get_vol_adjustment(self, publish=False) -> float:
        volatility = self._rolling_vol.get_value()
        
        # Calculate the ratio of current volatility to base volatility
        vol_ratio = volatility / self.base_volatility

        # Apply a power function to exaggerate changes
        exponent = self.volatility_exponent
        vol_nonlinear = min(self.volatility_cap, np.power(vol_ratio, exponent))
        vol_adj = vol_nonlinear * self.pricing_volatility_factor / 2

        if publish:
            self._publish_strat_metric('volatility', volatility)
            self._publish_strat_metric('volatility_nonlinear', vol_nonlinear)

        return vol_adj

    def get_fair_price(self, side: Side = None, publish: bool = False) -> float:

        price_type = None
        if side == Side.BUY:
            price_type = PriceType.BestBid
        elif side == Side.SELL:
            price_type = PriceType.BestAsk
        
        raw_spot = self.get_base_price(price_type)
        raw_spot_ema = self._smoothen_spot_price.value
        self.logger.debug(f"raw_spot: {raw_spot:.6f}, raw_spot_ema: {raw_spot_ema:.6f}")

        raw_basis_ema = self._smoothen_basis.value
        raw_fr = self.get_inst_rate()
        raw_fr_ema = self._smoothen_funding_rate.value

        self.logger.debug(f"raw_basis: {raw_basis_ema:.6f}, raw_fr_ema: {raw_fr_ema:.6f}")
        _base_price = raw_spot_ema
        factored_basis = raw_basis_ema * self.pricing_basis_factor

        base_ask = max(raw_spot, raw_spot_ema)
        base_bid = min(raw_spot, raw_spot_ema)
        self.logger.debug(f"factored_basis: {factored_basis:.6f}")

        factored_fr = raw_fr_ema * self.pricing_funding_rate_factor
        self.logger.debug(f"factored_fr: {factored_fr:.6f}")
    
        cur_pos = self.get_active_position(self.market)
        cur_pos_account_usd = self.get_global_position_usd()
        
        cur_pos_usd = cur_pos * raw_spot
        self.logger.debug(f"cur_pos: {cur_pos:.4g}, cur_pos_usd: {cur_pos_usd:.4g}, cur_pos_account_usd: {cur_pos_account_usd:.4g}")

        pos_lean = -1 * self.pos_lean_bps_per_100k * cur_pos_usd 
        global_pos_lean = -1 * self.pos_global_lean_bps_per_100k * cur_pos_account_usd

        self.logger.debug(f"pos_lean: {pos_lean:.4g}, global_pos_lean: {global_pos_lean:.4g}")

        pos_adj = pos_lean + global_pos_lean

        vol_adj = self.get_vol_adjustment(publish=publish)

        market_bid = self.get_price_by_type(PriceType.BestBid)
        market_ask = self.get_price_by_type(PriceType.BestAsk)

        raw_adj = self.price_adjustment

        fair_ask = base_ask + _base_price * (factored_basis - factored_fr + pos_adj + vol_adj + raw_adj + self.ask_spread)
        fair_bid = base_bid + _base_price * (factored_basis - factored_fr + pos_adj - vol_adj + raw_adj - self.bid_spread)

        if not market_ask.is_finite():
            self.logger.warning("Market ask is not finite. Widen more.")
            final_ask = fair_ask + _base_price * self.empty_book_penalty
        else: 
            final_ask = fair_ask

        if not market_bid.is_finite():
            self.logger.warning("Market bid is not finite. Widen more.")
            final_bid = fair_bid - _base_price * self.empty_book_penalty
        else:
            final_bid = fair_bid

        if publish:
            self._publish_strat_metric('spot', raw_spot)
            self._publish_strat_metric('spot_ema', raw_spot_ema)
            self._publish_strat_metric('basis_ema', raw_basis_ema)
            self._publish_strat_metric('fr_ema', raw_fr_ema)
            self._publish_strat_metric('fr', raw_fr)
            self._publish_strat_metric('volatility_adj', vol_adj)
            self._publish_strat_metric('price_adjustment', raw_adj)
            self._publish_strat_metric('spread_adj', self.ask_spread + self.bid_spread)
            self._publish_strat_metric('basis_adj', factored_basis)
            self._publish_strat_metric('fr_adj', -factored_fr)
            self._publish_strat_metric('pos_adj_loc', pos_lean)
            self._publish_strat_metric('pos_adj_global', global_pos_lean)
            self._publish_strat_metric('quote_spread', final_ask - final_bid)
            self._publish_strat_metric('market_spread', market_ask - market_bid)
            self._publish_strat_metric('final_ask', final_ask)
            self._publish_strat_metric('final_bid', final_bid)
            self._publish_strat_metric('market_bid', market_bid)
            self._publish_strat_metric('market_ask', market_ask)
            self._publish_strat_metric('ratio_quoted_market_bid', final_bid / market_bid - 1)
            self._publish_strat_metric('ratio_quoted_market_ask', final_ask / market_ask - 1)
            self._publish_strat_metric('algo_pos_usd', cur_pos_usd)
    
        if side == Side.BUY:
            return final_bid
        elif side == Side.SELL:
            return final_ask

        return (final_bid + final_ask) / D(2)

    def start(self):
        self.logger.info("Starting...")
        self._params_manager.start()

        self._params_manager.publish(Param("algo_up", 1))
        self._params_manager.publish(Param("algo_pair", self.market))
        self._params_manager.publish(Param("algo_name", self.algo_name))
        self._params_manager.publish(Param("algo_env", os.getenv("PARADEX_ENVIRONMENT", "unknown")))
        self._params_manager.publish_state()


        self._smoothen_spot_price = ExponentialMovingAverage(half_life_ms=self.price_ema_sec*1000, init_val=None)
        self._smoothen_basis = ExponentialMovingAverage(half_life_ms=self.fr_ema_sec*1000)
        self._smoothen_funding_rate = ExponentialMovingAverage(half_life_ms=self.basis_ema_sec*1000)
        self._rolling_vol = RollingAnnualizedVolatility(window_size=self.vol_window_size)

        self.logger.info("Started.")

    def stop(self):
        self.logger.info("Stopping...")
        self._params_manager.publish(Param("algo_down", 1))

        self.cancel_all_orders()
        self.logger.info("Stopped.")

        self._params_manager.stop()


    def graceful_shutdown(self, signum, frame):
        self.stop()
        self.logger.info("Shutting down...")

    def get_inst_rate(self) -> D:
        if self.market not in self.market_connector.latest_fundings:
            return D(0)
        
        eigth_hr_rate = self.market_connector.latest_fundings[self.market]['funding_rate']
        
        # Annualized rate to per-second rate
        payment_freq_seconds = 5
        periods_per_year = 365.25 * 24 / 8  # Number of 8-hour periods in a year
        inst_rate = D(eigth_hr_rate) * D(periods_per_year) / D(payment_freq_seconds)
        return inst_rate

    def get_inst_basis(self) -> D:
        if self.market not in self.market_connector.latest_fundings:
            return D(0)
        mark_price = D(self.market_connector.latest_fundings[self.market]['mark_price'])
        spot_price = D(self.market_connector.latest_fundings[self.market]['oracle_price'])
        basis = (mark_price - spot_price) / spot_price
        return basis


    async def on_market_data(self, update_type: str, ticker: Ticker, data: dict):
        if not self.is_ready_to_trade():
            self.cancel_all_orders()
            return

        self.update_emas(self.now_ms())
        
        if self._reeval_task is None or self._reeval_task.done():
            self._reeval_task = safe_ensure_future(self.reeval(TriggerType.MARKET_DATA))

    async def on_trade(self, data: dict):
        self.logger.info(f"on_trade: {data}")

    async def _subscribe_to_data(self):
        await self.market_connector.subscribe_to_data_channels(self.market, self.on_market_data)
        await self.market_connector.subscribe_to_trade_channels(self.market, self.on_trade)
        
        if self.external_connector is not None:
            await self.external_connector.subscribe_to_data_channels(self.external_markets.split(':')[-1], self.on_market_data)

    async def run(self):

        self.start()

        await self.market_connector.initialize()
        await self.external_connector.initialize()

        await self.market_connector.start()
        await self.market_connector.setup_trading_rules(self.market)

        await self.external_connector.start()

        await self._subscribe_to_data()
        

        while True:
            self.logger.info("Cycle evaluation...")

            if self.market_data_ready():
                self.get_fair_price(publish=True)
            
            self.market_connector.sync_open_orders(self.market)
            await self.reeval(TriggerType.PERIODIC)
            await asyncio.sleep(30)

            # heartbeat
            self._params_manager.publish(Param("algo_up", 1))
