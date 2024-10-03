"""
This module contains utility classes and methods for handling trading data.

It includes various enums, data structures, and helper classes used throughout
the trading system for representing orders, prices, and other trading-related concepts.
"""

import asyncio
import datetime as dt
import time
from abc import ABC, abstractmethod
from collections import deque, namedtuple
from decimal import Decimal as D
from enum import Enum
from typing import Dict, List

import numpy as np
import structlog
from sortedcontainers import SortedDict


class OrderType(Enum):
    MARKET = 1
    LIMIT = 2
    LIMIT_MAKER = 3

    def is_limit_type(self):
        return self in (OrderType.LIMIT, OrderType.LIMIT_MAKER)
    
class TriggerType(Enum):
    MARKET_DATA = 1
    PERIODIC = 2

class Side(Enum):
    BUY = 'buy'
    SELL = 'sell'

class PriceType(Enum):
    BestBid = 'best_bid'
    BestAsk = 'best_ask'
    Mid = 'mid'


class PriceSize:
    """
    A class representing a price and size pair for trading orders.
    """
    def __init__(self, price: D, size: D, type: OrderType = OrderType.LIMIT_MAKER):
        self.price: D = price
        self.size: D = size
        self.type: OrderType = type

    def __repr__(self):
        return f"[ p: {self.price} s: {self.size} ]"


class Order:
    """
    A class representing an order in the trading system.
    """
    def __init__(self, symbol: str, side: Side, price: D, amount: D, order_type: OrderType):
        self.symbol = symbol
        self.side = side
        self.price = price
        self.amount = amount
        self.order_type = order_type
        self.client_order_id = None
        self.exhange_order_id = None
        self.status = 'CREATED'

    def __repr__(self):
        return f"[{self.symbol}|{self.order_type}] {self.side} {self.amount}@{self.price} ({self.status})"

TradingRules = namedtuple('TradeTick', ['min_price_increment', 'min_notional_size', 'min_amount_increment'])

class ConnectorBase(ABC):
    """
    An abstract base class for connectors in the trading system.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.logger = structlog.get_logger(self.__class__.__name__)
        self.trading_rules: Dict[str, TradingRules] = {}
        self.orderbooks: Dict[str, Depth] = {}
        self.bbos = {}
        self.latest_fundings = {}
        self.account_info = {}
        self.positions = {}
    
    @abstractmethod
    async def initialize(self):
        pass

    def quantize_order_price(self, symbol: str, price: D):
        tick_size = self.trading_rules[symbol].min_price_increment
        return price.quantize(tick_size)
    
    def quantize_order_amount(self, symbol: str, amount: D):
        min_amount_increment = self.trading_rules[symbol].min_amount_increment
        return amount.quantize(min_amount_increment)


class Proposal:
    """
    A class representing a proposal for buying or selling a financial instrument.
    """
    def __init__(self, buys: List[PriceSize], sells: List[PriceSize]):
        self.buys: List[PriceSize] = buys
        self.sells: List[PriceSize] = sells

    def __repr__(self):
        return f"{len(self.buys)} buys: {', '.join([str(o) for o in self.buys])} " \
               f"{len(self.sells)} sells: {', '.join([str(o) for o in self.sells])}"

class Ticker:
    """
    A class representing a financial instrument with its symbol and exchange.
    """
    def __init__(self, symbol: str, exchange: str):
        self.symbol = symbol
        self.exchange = exchange

    def __str__(self):
        return f"{self.symbol}@{self.exchange}"

class UpdateType(Enum):
    ORDERBOOK = 1
    TRADETICK = 2
    BBO = 3
    FUNDING = 4

class RollingAnnualizedVolatility:
    """
    A class for calculating rolling annualized volatility.

    This class maintains a fixed-size window of price data and their corresponding
    timestamps. It provides methods to update the data and calculate the
    annualized volatility based on the stored price history.

    Attributes:
        prices (deque): A fixed-size deque storing historical prices.
        timestamps (deque): A fixed-size deque storing timestamps corresponding to the prices.
        logger (Logger): A logger instance for this class.
    """
    def __init__(self, window_size: int):
        self.logger = structlog.get_logger(self.__class__.__name__)
        self.prices = deque(maxlen=window_size)
        self.timestamps = deque(maxlen=window_size)

    def update(self, new_price: D, new_timestamp: float):
        """Update with a new price and its timestamp."""
        if len(self.prices) > 0 and new_price == self.prices[-1]:
            return
        self.prices.append(new_price)
        self.timestamps.append(D(new_timestamp))

    def get_value(self):
        """Calculate and return the annualized volatility based on stored prices."""
        if len(self.prices) < 4:
            return D(0)  # Not enough data to calculate volatility

        prices_array = np.array(self.prices, dtype=np.float64)
        log_returns_array = np.diff(np.log(prices_array))

        # Get timestamps and calculate time differences
        timestamps_array = np.array(self.timestamps, dtype=np.float64)
        time_diffs = np.diff(timestamps_array)

        # Create a mask for non-zero time differences
        non_zero_mask = time_diffs != 0

        # Filter log returns and time diffs to only include non-zero time diffs
        filtered_log_returns = log_returns_array[non_zero_mask]
        filtered_time_diffs = time_diffs[non_zero_mask]

        # If all time diffs were zero, return 0 (or handle this case as appropriate)
        if len(filtered_time_diffs) == 0:
            return D(0)

        # Normalize log returns by the square root of the time differences
        normalized_log_returns = filtered_log_returns / np.sqrt(filtered_time_diffs)

        # Calculate sample variance of normalized log returns
        variance = np.var(normalized_log_returns, ddof=1)  # ddof=1 for sample variance

        # Calculate daily volatility
        normalized_volatility = np.sqrt(variance)
        
        # Annualize the volatility
        avg_interval_ms = np.mean(filtered_time_diffs)
        ms_per_year = 365 * 24 * 60 * 60 * 1000  # milliseconds
        annualized_volatility = normalized_volatility * np.sqrt(ms_per_year / avg_interval_ms)

        return D(annualized_volatility)


class ExponentialMovingAverage:
    """
    A class for calculating an exponential moving average (EMA).

    This class maintains a single value and updates it using an exponential decay
    based on a half-life specified in milliseconds. It provides methods to update
    the EMA value and retrieve the current EMA value, applying decay based on the
    current timestamp.

    Attributes:
        value (Decimal): The current EMA value.
        timestamp (float): The timestamp when the EMA value was last updated.
        half_life (Decimal): The half-life in milliseconds.
        lambda_ (Decimal): The decay constant based on the half-life.
        decay_on_read (bool): Whether to decay the EMA value on read.
    """
    def __init__(self, half_life_ms: D, decay_on_read: bool = False, init_val: D=D(0)):
        self.value = init_val
        self.timestamp = .0
        self.half_life = D(half_life_ms) # half-life in ms
        self.lambda_ = D(np.log(2)) / self.half_life  # decay constant based on half-life
        self.decay_on_read = decay_on_read

    def decay(self, current_timestamp: float):
        """Update the EMA value based on the time decay."""
        time_difference = D(current_timestamp - self.timestamp)
        decay_factor = D(np.exp(-self.lambda_ * time_difference))
        self.value *= decay_factor
        self.timestamp = current_timestamp

    def update(self, new_value: float, new_timestamp: float):
        """Update the EMA with a new value at a new timestamp."""
        if self.value is None:
            self.value = D(new_value)
        time_difference = D(new_timestamp - self.timestamp)
        decay_factor = D(np.exp(-self.lambda_ * time_difference))
        self.value = (1 - decay_factor) * D(new_value) + decay_factor * self.value
        self.timestamp = new_timestamp

    def get_value(self, current_timestamp: float):
        """Return the current EMA value, applying decay based on the current timestamp."""
        if self.decay_on_read:
            self.decay(current_timestamp)
        return self.value


class Level:
    """
    A class representing a price level in an order book.
    """
    def __init__(self, px: float, qty: float, offset: int=0) -> None:
        self.px = float(px) if px not in [None, ''] else None
        self.qty = float(qty) if qty not in [None, ''] else None
        self.offset = int(offset)

    def __str__(self):
        return f"{self.px}@{self.qty}"

class Depth:
    """
    A class representing an order book depth.
    """
    def __init__(self, iid: str) -> None:
        self.logger = structlog.get_logger(self.__class__.__name__)

        self.bids = SortedDict()
        self.asks = SortedDict()
        self.iid = iid

        self.exchange_ts: int = 0
        self.received_ts: int = 0

        self.last_bid_zero_offset = {}
        self.last_ask_zero_offset = {}


    def update_order_book(self, depth_diff: Dict, reset: bool = False):
        """Update the order book with a depth difference."""
        if reset:
            self.bids.clear()
            self.asks.clear()
        self.update_order_book_side(depth_diff['bids'], self.bids, self.last_bid_zero_offset, 'B')
        self.update_order_book_side(depth_diff['asks'], self.asks, self.last_ask_zero_offset, 'A')

    def update_order_book_side(self, book_side: List[Dict], order_book: SortedDict, last_offsets: Dict, side: str):
        """Update the order book side with a depth difference."""
        for level in book_side:
            price = D(level['price'])
            offset = int(level.get('offset', 0))
            if price in order_book:
                # Only update the quantity if the new offset is bigger than the stored one
                self.logger.debug(f"({side}:0) level: {level}, {offset} > {order_book[price].offset}, {offset > order_book[price].offset}")
                if offset > order_book[price].offset:
                    if level['size'] == '0':
                        last_offsets[price] = offset
                        # Delete the price level
                        del order_book[price]
                    else:
                        order_book[price].qty = float(level['size'])
                        order_book[price].offset = offset
            else:
                self.logger.debug(f"({side}:1) level: {level}, {offset} > {last_offsets.get(price, 0)}, {offset > last_offsets.get(price, 0)}")
                if offset >= last_offsets.get(price, 0):
                    # Add a new price level with the quantity and offset
                    if level['size'] != '0':
                        order_book[price] = Level(price, level['size'], level.get('offset', 0))
                    else:
                        last_offsets[price] = offset

    def get_best_bid(self):
        """Return the best bid price."""
        return self.bids.peekitem(index=-1)[1].px if len(self.bids) > 0 else None
    
    def get_best_ask(self):
        """Return the best ask price."""
        return self.asks.peekitem(index=0)[1].px if len(self.asks) > 0 else None

    def get_mid(self):
        """Return the mid price."""
        if len(self.bids) == 0 or len(self.asks) == 0:
            return None
        return (self.bids.peekitem(index=-1)[1].px + self.asks.peekitem(index=0)[1].px) / 2
    
    def get_spread(self):
        """Return the spread."""
        return self.asks.peekitem(index=0)[1].px - self.bids.peekitem(index=-1)[1].px
    
    def is_stale(self, max_age_s: int = 10):
        """Check if the order book is stale."""
        return (time.time_ns() - self.received_ts) > max_age_s * 1e9
    
    def is_crossed(self):
        """Check if the order book is crossed."""
        return self.bids.peekitem(index=-1)[1].px > self.asks.peekitem(index=0)[1].px
    
    def uncross_book(self):
        """Uncross the order book."""
        while self.is_crossed():
            best_bid = self.bids.peekitem(index=-1)[1]
            best_ask = self.asks.peekitem(index=0)[1]
            if best_bid.offset > best_ask.offset:
                self.asks.popitem(index=0)
            else:
                self.bids.popitem(index=-1)

    def __str__(self):
        return f"Depth<{self.iid}@{dt.datetime.fromtimestamp(self.received_ts/1e9)}>(BID={self.bids.peekitem(index=-1)[1]};ASK={self.asks.peekitem(index=0)[1]})"


