import asyncio
from abc import ABC, abstractmethod
from collections.abc import Callable
from decimal import Decimal as D

import structlog

from ..utils.data_methods import Depth, Level, Order, TradingRules


class ExternalConnector(ABC):
    """
    An abstract base class for connectors in the trading system.
    """

    trading_rules: dict[str, TradingRules]
    orderbooks: dict[str, Depth]
    bbos: dict[str, dict[str, Level]]
    latest_fundings: dict[str, dict]
    account_info: dict
    positions: dict[str, dict]
    _data_callbacks: dict[str, Callable]
    _trade_callbacks: dict[str, Callable]

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.logger = structlog.get_logger(self.__class__.__name__)
        self.trading_rules = {}
        self.orderbooks = {}
        self.bbos = {}
        self.latest_fundings = {}
        self.account_info = {}
        self.positions = {}
        self._data_callbacks = {}
        self._trade_callbacks = {}

    @abstractmethod
    async def initialize(self): ...

    @abstractmethod
    async def start(self): ...

    @abstractmethod
    async def subscribe_to_data_channels(self, market, callback): ...

    def quantize_order_price(self, symbol: str, price: D):
        tick_size = self.trading_rules[symbol].min_price_increment
        return price.quantize(tick_size)

    def quantize_order_amount(self, symbol: str, amount: D):
        min_amount_increment = self.trading_rules[symbol].min_amount_increment
        return amount.quantize(min_amount_increment)


class Connector(ExternalConnector):
    active_orders: dict[str, Order]

    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__(loop)
        self.active_orders = {}

    @abstractmethod
    async def setup_trading_rules(self, symbol): ...

    @abstractmethod
    def sync_open_orders(self, mkt): ...

    @abstractmethod
    async def subscribe_to_trade_channels(self, market, callback): ...

    @abstractmethod
    def get_position_size(self, mkt: str) -> D: ...

    @abstractmethod
    def cancel_all_orders(self, mkt): ...
