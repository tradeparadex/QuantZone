"""
This module implements a connector for the Bybit UTA exchange.

It provides functionality to interact with the Bybit UTA API, including
market data subscriptions and order book management.

Classes:
    BybitUTAConnector: Main connector class for Bybit UTA exchange.
"""

import asyncio
import os
import time
from decimal import Decimal as D
from typing import Dict

import numpy as np
import structlog
from pybit.unified_trading import HTTP, WebSocket
from utils.data_methods import (
    ConnectorBase,
    Depth,
    Level,
    Order,
    OrderType,
    Side,
    Ticker,
    TradingRules,
    UpdateType,
    Position,
    AccountInfo
)
from utils.async_utils import safe_ensure_future


class BybitUTAConnector(ConnectorBase):
    """
    A connector class for interacting with the Bybit UTA exchange.

    This class provides methods for market data subscriptions and order book management.
    """

    def __init__(self, loop, key=None, secret=None, channel='linear'):
        """
        Initialize the BybitUTAConnector.
        """
        super().__init__(loop)
        self.logger = structlog.get_logger(self.__class__.__name__)
        self.exchange = "bybit_uta"
        if channel == 'spot':
            self.exchange = "bybit_spot"
        self.key = key or os.getenv("BYBIT_UTA_API_KEY")
        self.secret = secret or os.getenv("BYBIT_UTA_API_SECRET")

        self.channel_type = channel

        self._http_client: HTTP = None
        self._socket_manager: WebSocket = None
        self._private_socket_manager: WebSocket = None

        self.orderbooks: Dict[str, Depth] = {}
        self.bbos: Dict[str, Dict[str, Level]] = {}
        self.latest_fundings: Dict[str, dict] = {}

        self._data_callbacks: Dict[str, callable] = {}
        self._trade_callbacks: Dict[str, callable] = {}

        self.positions: Dict[str, dict] = {}

        self._symbol_map: Dict[str, str] = {}

    async def initialize(self):
        """Initialize the Bybit UTA client and socket manager."""
        testnet = os.getenv("BYBIT_UTA_TESTNET", "false").lower() == "true"

        self._http_client = HTTP(
            testnet=testnet,
            demo=testnet,
            api_key=self.key, 
            api_secret=self.secret
        )

        if self.key and self.secret:
            self._private_socket_manager = WebSocket(
                testnet=testnet,
                demo=testnet,
                channel_type="private",
                api_key=self.key,
                api_secret=self.secret,
                trace_logging=False,
                retries=8888,
                restart_on_error=True,
            )
        
        self._ws_client = WebSocket(
            testnet=testnet,
            channel_type=self.channel_type,
            trace_logging=False,
            retries=8888,
            restart_on_error=True,
        )

    async def _connect_to_ws(self):
        """Connect to WebSocket (placeholder method)."""
        pass

    def get_positions(self) -> Dict[str, Position]:
        acct_positions = self._http_client.get_positions(category=self.channel_type, settleCoin='USDC')
        positions = {}
        for a in acct_positions['result']['list']:
            _symbol = self._translate_symbol_back(a['symbol'])
            positions[_symbol] = Position(
                _symbol, 
                notional=D(a['positionValue']) * (-1 if a['side'] == 'Sell' else 1), 
                size=D(a['size']) * (-1 if a['side'] == 'Sell' else 1)
            )
        return positions

    async def setup_trading_rules(self, market):
        """
        Set up trading rules for a market.

        Args:
            market (str): The market symbol.
        """

        _all_markets = self._http_client.get_instruments_info(category=self.channel_type, inst_type='PERP')

        for market in _all_markets['result']['list']:
            if market['symbol'].endswith('USDT'):
                continue

            _symbol = self._translate_symbol_back(market['symbol'])
            self.trading_rules[_symbol] = TradingRules(
                min_amount_increment=D(market['lotSizeFilter']['qtyStep']),
                min_price_increment=D(market['priceFilter']['tickSize']),
                min_notional_size=D(market['lotSizeFilter']['minOrderQty']),
            )

    def insert_order(self, order: Order):
        """
        Insert a single order.

        Args:
            order (Order): The Order object to insert.
        """

        assert order.order_type in [OrderType.IOC], "Only IOC orders are supported"

        if order.order_type == OrderType.LIMIT_MAKER:
            _order_type = 'Limit'
            _order_instruction = "PostOnly"
        elif order.order_type == OrderType.LIMIT:
            _order_type = 'Limit'
            _order_instruction = "GTC"
        elif order.order_type == OrderType.MARKET:
            _order_type = 'Market'
            _order_instruction = "IOC"
        elif order.order_type == OrderType.IOC:
            _order_type = 'Limit'
            _order_instruction = "IOC"

        _order_side = 'Buy' if order.side == Side.BUY else 'Sell'

        _symbol = self._translate_symbol(order.symbol)
        self.logger.info(f"inserting order: {order.client_order_id}")

        try:
            resp = self._http_client.place_order(
                category=self.channel_type,
                side=_order_side,
                price=order.price,
                symbol=_symbol,
                qty=order.amount,
                orderType=_order_type,
                timeInForce=_order_instruction
            )
        except Exception as e:
            self.logger.error(f"order failed: {e}")
            return
            

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
        Translate internal market symbol to BYBIT symbol format.

        Args:
            market (str): Internal market symbol.

        Returns:
            str: BYBIT symbol format.
        """
        token = market.split('-')[0]
        base = market.split('-')[1]

        if token.startswith('k'):
            token = token.replace('k', '1000')

        if self.channel_type == 'spot':
            return token + base

        if base == 'USDT':
            return token + 'USDT'

        return token + 'PERP'
    
    def _translate_symbol_back(self, market):
        """
        Translate BYBIT symbol format to internal market symbol.

        Args:
            market (str): BYBIT symbol format.

        Returns:
            str: Internal market symbol.
        """
        token = market.split('PERP')[0]

        if token.startswith('1000'):
            token = token.replace('1000', 'k')

        if token in ['']:
            pass

        return token + '-USD-PERP'

    def _listen_to_data(self, message):
        """
        Listen to market data updates for a specific symbol.
        """

        data = message['data']
        symbol = data['s']
        _ticker = Ticker(self._symbol_map[symbol], self.exchange)
        update_type = message['type']

        if _ticker.symbol not in self.orderbooks:
            self.orderbooks[_ticker.symbol] = Depth(_ticker.symbol)

        _up_dict = {
            'bids': [
                {'price': x[0], 'size': x[1], 'offset': data['seq']} 
                for x in data['b']
            ],
            'asks': [
                {'price': x[0], 'size': x[1], 'offset': data['seq']} 
                for x in data['a']
            ]
        }

        ob = self.orderbooks[_ticker.symbol]

        if update_type == 'snapshot':
            ob.update_order_book(_up_dict, reset=True)
        else:
            ob.update_order_book(_up_dict)

        ob.received_ts = time.time_ns()
        ob.exchange_ts = time.time_ns()

        safe_ensure_future(self._data_callbacks[_ticker.symbol](UpdateType.ORDERBOOK, _ticker, ob), loop=self.loop)

    def get_account_info(self) -> AccountInfo:
        _wallet = self._http_client.get_wallet_balance(accountType='UNIFIED')['result']['list'][0]['coin']
        usdc_wallet = [a for a in _wallet if a['coin'] == 'USDC'][0]
        return AccountInfo(
            free_collateral=D(usdc_wallet['availableToWithdraw']),
            account_value=D(usdc_wallet['equity'])
        )

    def fetch_bbo(self, symbol):
        """
        Fetch the best bid and offer for a specific symbol.

        Args:
            symbol (str): The market symbol to fetch the BBO for.

        Returns:
            dict: The best bid and offer for the symbol.
        """
        _symbol = self._translate_symbol(symbol)
        _bbo = self._http_client.get_orderbook(symbol=_symbol, category='linear')

        _data = _bbo['result']
        _latest_bbo = {
            'ask': Level(px=_data['a'][0][0], qty=_data['a'][0][1], offset=_data['seq']),
            'bid': Level(px=_data['b'][0][0], qty=_data['b'][0][1], offset=_data['seq'])
        }
        self.bbos[_symbol] = _latest_bbo
        return _latest_bbo
    
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

        self._ws_client.orderbook_stream(50, og_symbol, self._listen_to_data)
        

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