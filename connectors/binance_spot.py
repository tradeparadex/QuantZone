"""
This module implements a connector for the Binance spot exchange.

It provides functionality to interact with the Binance API, including
market data subscriptions and order book management.

Classes:
    BinanceSpotConnector: Main connector class for Binance spot exchange.
"""

import asyncio
import structlog
import time
from typing import Dict
from binance import AsyncClient, BinanceSocketManager

from utils.data_methods import (
    Ticker,
    Level,
    Depth,
    UpdateType
)

class BinanceSpotConnector:
    """
    A connector class for interacting with the Binance spot exchange.

    This class provides methods for market data subscriptions and order book management.

    Attributes:
        logger (logging.Logger): Logger for this class.
        loop (asyncio.AbstractEventLoop): Event loop for asynchronous operations.
        exchange (str): The name of the exchange ("binance_perp").
        binance (AsyncClient): Binance AsyncClient instance.
        _socket_manager (BinanceSocketManager): Binance WebSocket manager.
        orderbooks (Dict[str, Depth]): Dictionary of order books for different markets.
        bbos (Dict[str, Dict[str, Level]]): Dictionary of best bid and offer data.
        latest_fundings (Dict[str, dict]): Dictionary of latest funding data.
        _data_callbacks (Dict[str, callable]): Callbacks for market data updates.
        _trade_callbacks (Dict[str, callable]): Callbacks for trade updates.
        positions (Dict[str, dict]): Dictionary of user's positions.
        _symbol_map (Dict[str, str]): Mapping between internal and exchange symbols.
    """

    def __init__(self, loop):
        """
        Initialize the BinanceSpotConnector.

        Args:
            loop (asyncio.AbstractEventLoop): Event loop for asynchronous operations.
        """
        self.logger = structlog.get_logger(self.__class__.__name__)
        self.loop = loop
        self.exchange = "binance_perp"
        self.binance: AsyncClient = None
        self._socket_manager: BinanceSocketManager = None

        self.orderbooks: Dict[str, Depth] = {}
        self.bbos: Dict[str, Dict[str, Level]] = {}
        self.latest_fundings: Dict[str, dict] = {}

        self._data_callbacks: Dict[str, callable] = {}
        self._trade_callbacks: Dict[str, callable] = {}

        self.positions: Dict[str, dict] = {}

        self._symbol_map: Dict[str, str] = {}

    async def initialize(self):
        """Initialize the Binance client and socket manager."""
        self.binance = AsyncClient()
        self._socket_manager = BinanceSocketManager(self.binance)

    async def _connect_to_ws(self):
        """Connect to WebSocket (placeholder method)."""
        pass

    async def _on_bbo(self, channel, msg):
        """
        Handle best bid and offer updates.

        Args:
            channel: The channel the message was received on.
            msg: The message containing BBO data.
        """
        _data = msg['params']['data']
        _ticker = _data['market']
        self.logger.debug(f"bbo: {_data}")
        _latest_bbo = {
            'ask': Level(px=_data['ask'], qty=_data['ask_size'], offset=_data['seq_no']),
            'bid': Level(px=_data['bid'], qty=_data['bid_size'], offset=_data['seq_no'])
        }
        self.bbos[_data['market']] = _latest_bbo
        await self._data_callbacks[_data['market']](UpdateType.BBO, _ticker, _latest_bbo)

    async def _on_funding_data(self, channel, msg):
        """Handle funding data updates (placeholder method)."""
        pass

    async def _on_fills(self, channel, msg):
        """
        Handle fill updates.

        Args:
            channel: The channel the message was received on.
            msg: The message containing fill data.
        """
        _data = msg['params']['data']
        self.logger.info(f"fills: {_data}")

    async def _on_order_update(self, channel, msg):
        """Handle order updates (placeholder method)."""
        pass

    async def _on_orderbook(self, channel, msg):
        """
        Handle order book updates.

        Args:
            channel: The channel the message was received on.
            msg: The message containing order book data.
        """
        _data = msg['params']['data']
        self.logger.debug(f"orderbook: {_data}")
        _ticker = _data['market']

        _update_type = _data['update_type']
        _offset = _data['seq_no']
        
        if _ticker not in self.orderbooks:
            self.orderbooks[_ticker] = Depth(_ticker)

        ob = self.orderbooks[_ticker]
        if _update_type == 's':
            _up_dict = {
                'bids': [
                    {'price': x['price'], 'size': x['size'], 'offset': _offset} 
                    for x in [y for y in _data['inserts'] if y['side'] == 'BUY']
                ],
                'asks': [
                    {'price': x['price'], 'size': x['size'], 'offset': _offset} 
                    for x in [y for y in _data['inserts'] if y['side'] == 'SELL']
                ]
            }
            ob.update_order_book(_up_dict, reset=True)
        elif _update_type == 'd':
            _up_dict = {
                'bids': [
                    {'price': x['price'], 'size': '0', 'offset': _offset} 
                    for x in [y for y in _data['deletes'] if y['side'] == 'BUY']
                ],
                'asks': [
                    {'price': x['price'], 'size': '0', 'offset': _offset} 
                    for x in [y for y in _data['deletes'] if y['side'] == 'SELL']
                ]
            }
            ob.update_order_book(_up_dict)
        else:
            self.logger.info(f"orderbook: {_data}")

        ob.received_ts = time.time_ns()
        ob.exchange_ts = int(_data['last_updated_at'] * 1e6)

        await self._data_callbacks[_ticker](UpdateType.ORDERBOOK, ob)

    async def _on_positions(self, channel, msg):
        """Handle position updates (placeholder method)."""
        pass

    async def _on_trades(self, channel, msg):
        """Handle trade updates (placeholder method)."""
        pass

    async def _on_account_update(self, channel, msg):
        """Handle account updates (placeholder method)."""
        pass
    
    def _translate_symbol(self, market):
        """
        Translate internal market symbol to Binance symbol format.

        Args:
            market (str): Internal market symbol.

        Returns:
            str: Binance symbol format.
        """
        return market.replace('-', '').upper()

    async def _listen_to_data(self, ts, symbol):
        """
        Listen to market data updates for a specific symbol.

        Args:
            ts: WebSocket connection for the symbol.
            symbol (str): The market symbol to listen for.
        """
        _ticker = Ticker(symbol, self.exchange)

        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = Depth(symbol)
        
        async with ts as tscm:
            while True:
                res = await tscm.recv()

                _up_dict = {
                    'bids': [
                        {'price': x[0], 'size': x[1], 'offset': res['lastUpdateId']} 
                        for x in res['bids']
                    ],
                    'asks': [
                        {'price': x[0], 'size': x[1], 'offset': res['lastUpdateId']} 
                        for x in res['asks']
                    ]
                }

                ob = self.orderbooks[symbol]
                ob.update_order_book(_up_dict, reset=True)

                ob.received_ts = time.time_ns()
                ob.exchange_ts = time.time_ns()

                await self._data_callbacks[symbol](UpdateType.ORDERBOOK, _ticker, ob)
    
    async def subscribe_to_data_channels(self, market, callback):
        """
        Subscribe to market data channels for a specific market.

        Args:
            market (str): The market symbol to subscribe to.
            callback (callable): The callback function for data updates.
        """
        self._data_callbacks[market] = callback
        og_symbol = self._translate_symbol(market)
        self._symbol_map[og_symbol] = market

        ts = self._socket_manager.depth_socket(og_symbol, depth=10, interval=100)
        asyncio.ensure_future(self._listen_to_data(ts, market), loop=self.loop)

    async def subscribe_to_trade_channels(self, market, callback):
        """Subscribe to trade channels (placeholder method)."""
        pass

    async def subscribe_to_account_channels(self):
        """Subscribe to account channels (placeholder method)."""
        pass

    async def start(self):
        """Start the connector by connecting to WebSocket and subscribing to account channels."""
        await self._connect_to_ws()
        await self.subscribe_to_account_channels()