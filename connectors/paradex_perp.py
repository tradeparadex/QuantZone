"""
This module implements a connector for the Paradex perpetual exchange.

It provides functionality to interact with the Paradex API, including
order management, market data subscriptions, and account information.

Classes:
    ParadexPerpConnector: Main connector class for Paradex perpetual exchange.
"""

import asyncio
import logging
import os
import time
from decimal import Decimal as D
from typing import Dict, Callable, Any
import hashlib
from utils.data_methods import (
    Order,
    ConnectorBase, 
    Depth, 
    Level, 
    Ticker, 
    TradingRules, 
    OrderType,
    Side,
    UpdateType
)
from utils.api_utils import SyncRateLimiter
from marshmallow.exceptions import ValidationError
from paradex_py import Paradex
from paradex_py.api.ws_client import ParadexWebsocketChannel
from paradex_py.environment import PROD, TESTNET
from paradex_py.common.order import Order as ParadexOrder
from paradex_py.common.order import OrderType as ParadexOrderType, OrderSide as ParadexOrderSide

class ParadexPerpConnector(ConnectorBase):
    """
    A connector class for interacting with the Paradex perpetual exchange.

    This class provides methods for order management, market data subscriptions,
    and account information retrieval.

    Attributes:
        paradex (Paradex): The Paradex client instance.
        exchange (str): The name of the exchange ("paradex_perp").
        orderbooks (Dict[str, Depth]): A dictionary of order books for different markets.
        bbos (Dict[str, Dict[str, Level]]): A dictionary of best bid and offer data.
        latest_fundings (Dict[str, dict]): A dictionary of latest funding data.
        account_info (dict): Information about the user's account.
        positions (Dict[str, dict]): A dictionary of user's positions.
        active_orders (Dict[str, Order]): A dictionary of active orders.

    Methods:
        get_account_str(): Get the account address as a string.
        initialize(): Initialize the Paradex client.
        generate_incremental_unique_id(mkt): Generate a unique order ID.
        cancel_order(client_order_id): Cancel an order by its client ID.
        sync_open_orders(mkt): Synchronize open orders for a market.
        bulk_cancel_orders(orders): Cancel multiple orders.
        bulk_insert_orders(orders): Insert multiple orders.
        insert_order(order): Insert a single order.
        _connect_to_ws(): Connect to the WebSocket.
        subscribe_to_data_channels(market, callback): Subscribe to market data channels.
        subscribe_to_trade_channels(market, callback): Subscribe to trade channels.
        subscribe_to_account_channels(): Subscribe to account-related channels.
        setup_trading_rules(market): Set up trading rules for a market.
        snapshots(): Take snapshots of current positions.
        start(): Start the connector.
    """

    def __init__(self, loop):
        """
        Initialize the ParadexPerpConnector.

        Args:
            loop: The event loop to use for asynchronous operations.
        """
        super().__init__(loop)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.paradex: Paradex = None
        self.exchange = "paradex_perp"
        self.orderbooks: Dict[str, Depth] = {}
        self.bbos: Dict[str, Dict[str, Level]] = {}
        self.latest_fundings: Dict[str, dict] = {}
        self.account_info = {}

        self._data_callbacks: Dict[str, callable] = {}
        self._trade_callbacks: Dict[str, callable] = {}

        self.positions: Dict[str, dict] = {}

        self.active_orders: Dict[str, Order] = {}

        self._order_counter = 0

        self.rate_limiter = SyncRateLimiter(3) # X requests per second, adjust as needed

    def rate_limited_request(self, method: Callable, *args, **kwargs) -> Any:
        """
        Rate-limit the request to the Paradex API.
        """
        self.rate_limiter.acquire()
        return method(*args, **kwargs)

    def get_account_str(self):
        """Get the account address as a string."""
        return str(self.paradex.account.l2_address)

    async def initialize(self):
        """Initialize the Paradex client with environment-specific settings."""
        _para_env = os.getenv("PARADEX_ENVIRONMENT")
        _paradex_perpetual_chain = PROD if _para_env == "mainnet" else (TESTNET if _para_env == "testnet" else _para_env)

        self.paradex = Paradex(
            env=_paradex_perpetual_chain, 
            l1_address=os.getenv("PARADEX_L1_ADDRESS"), 
            l2_private_key=os.getenv("PARADEX_PRIVATE_KEY"),
            logger=self.logger
        )

    def generate_incremental_unique_id(self, mkt):
        """
        Generate a unique order ID.

        Args:
            mkt (str): The market symbol.

        Returns:
            str: A unique hexadecimal ID.
        """
        current_time = str(time.time_ns()) + str(self._order_counter) + str(mkt)
        self._order_counter += 1
        hash_object = hashlib.sha256(current_time.encode())
        return hash_object.hexdigest()

    def cancel_order(self, client_order_id):
        """
        Cancel an order by its client ID.

        Args:
            client_order_id (str): The client order ID to cancel.
        """
        try:
            resp = self.rate_limited_request(self.paradex.api_client.cancel_order_by_client_id, client_order_id)
            self.active_orders[client_order_id].status = 'CANCELLING'
        except ValidationError as ve:
            if 'Missing data for required field' in str(ve):
                # This might indicate that the order doesn't exist
                del self.active_orders[client_order_id]
            else:
                self.logger.error(f"Unexpected validation error for order {client_order_id}: {ve}")
        except Exception as e:
            if e.args[0].error == 'CLIENT_ORDER_ID_NOT_FOUND':
                del self.active_orders[client_order_id]
            else:
                self.logger.error(f"cancel order failed: {e}")

    def sync_open_orders(self, mkt):
        """
        Synchronize open orders for a market.

        Args:
            mkt (str): The market symbol.
        """
        for ao in set(self.active_orders.keys()):
            try:
                po = self.rate_limited_request(self.paradex.api_client.fetch_order_by_client_id, ao)
                self.process_order_update(po)
            except ValidationError as ve:
                if 'Missing data for required field' in str(ve):
                    # This might indicate that the order doesn't exist
                    del self.active_orders[ao]
                else:
                    self.logger.error(f"Unexpected validation error for order {ao}: {ve}")
            except Exception as e:
                if e.args[0].error == 'CLIENT_ORDER_ID_NOT_FOUND':
                    del self.active_orders[ao]
                else:
                    self.logger.error(f"fetch order failed: {e}")

        existing_orders = []
        try:
            p_orders = self.rate_limited_request(self.paradex.api_client.fetch_orders, {'market': mkt})
            existing_orders = p_orders['results']
        except Exception as e:
            self.logger.error(f"fetch orders failed: {e}")
    

        for po in existing_orders:

            if po['client_id'] not in self.active_orders:
                self.active_orders[po['client_id']] = Order(
                    symbol=po['market'],
                    side=Side.BUY if po['side'] == 'BUY' else Side.SELL,
                    price=D(po['price']),
                    amount=D(po['size']),
                    order_type=OrderType.LIMIT if po['type'] == 'LIMIT' else OrderType.MARKET
                )
                self.active_orders[po['client_id']].client_order_id = po['client_id']

            self.process_order_update(po)


    def bulk_cancel_orders(self, orders):
        """
        Cancel multiple orders.

        Args:
            orders (list): A list of order IDs to cancel.
        """
        for order in orders:
            self.cancel_order(order)

    def bulk_insert_orders(self, orders):
        """
        Insert multiple orders.

        Args:
            orders (list): A list of Order objects to insert.
        """
        for order in orders:
            self.insert_order(order)
    
    def insert_order(self, order: Order):
        """
        Insert a single order.

        Args:
            order (Order): The Order object to insert.
        """
        if order.order_type == OrderType.LIMIT_MAKER:
            _order_type = ParadexOrderType.Limit
            _order_instruction = "POST_ONLY"
        elif order.order_type == OrderType.LIMIT:
            _order_type = ParadexOrderType.Limit
            _order_instruction = "GTC"
        elif order.order_type == OrderType.MARKET:
            _order_type = ParadexOrderType.Market
            _order_instruction = "IOC"

        _order_side = ParadexOrderSide.Buy if order.side == Side.BUY else ParadexOrderSide.Sell

        order.client_order_id = self.generate_incremental_unique_id(order.symbol)
        self.logger.info(f"inserting order: {order.client_order_id}")

        _po = ParadexOrder(
            market=order.symbol,
            order_type=_order_type,
            order_side=_order_side,
            size=order.amount,
            limit_price=order.price,
            client_id=order.client_order_id,
            signature_timestamp=None,
            instruction=_order_instruction, # Order Instruction, GTC, IOC or POST_ONLY if empty GTC
            reduce_only=False,
            recv_window=None,
            stp="EXPIRE_MAKER",
            trigger_price=None,
        )
        try:
            resp = self.rate_limited_request(self.paradex.api_client.submit_order, _po)
        except Exception as e:
            self.logger.error(f"order failed: {e}")
            return
        
        self.active_orders[_po.client_id] = order
        self.process_order_update(resp)

    async def _connect_to_ws(self):
        """Connect to the WebSocket with retry logic."""
        is_connected = False
        while not is_connected:
            is_connected = await self.paradex.ws_client.connect()
            if not is_connected:
                self.logger.info("connection failed, retrying in 1 second")
                await asyncio.sleep(1)
        self.logger.info("connected to ws")

    async def _on_bbo(self, channel, msg):
        """Handle best bid and offer updates."""
        _data = msg['params']['data']
        _symbol = _data['market']
        _ticker = Ticker(_data['market'], self.exchange)
        self.logger.debug(f"bbo: {_data}")
        _latest_bbo = {
            'ask': Level(px=_data['ask'], qty=_data['ask_size'], offset=_data['seq_no']),
            'bid': Level(px=_data['bid'], qty=_data['bid_size'], offset=_data['seq_no'])
        }
        self.bbos[_symbol] = _latest_bbo
        await self._data_callbacks[_symbol](UpdateType.BBO, _ticker, _latest_bbo)

    async def _on_funding_data(self, channel, msg):
        """Handle funding data updates."""
        _data = msg['params']['data']
        _symbol = _data['market']
        _ticker = Ticker(_data['market'], self.exchange)
        self.logger.debug(f"funding_data: {_data}")
        
        funding_premium = D(_data['funding_premium'])
        funding_rate = D(_data['funding_rate'])
        oracle_price = funding_premium / funding_rate
        mark_price = funding_premium + oracle_price

        _data['oracle_price'] = oracle_price
        _data['mark_price'] = mark_price

        self.latest_fundings[_symbol] = _data
        await self._data_callbacks[_symbol](UpdateType.FUNDING, _ticker, _data)

    async def _on_fills(self, channel, msg):
        """Handle fill updates."""
        _data = msg['params']['data']
        self.logger.info(f"fills: {_data}")
        # Trigger pos sync

    async def _on_order_update(self, channel, msg):
        """Handle order updates."""
        _data = msg['params']['data']
        self.logger.info(f"order_update: {_data}")
        self.process_order_update(_data)

    def process_order_update(self, _data):
        """Process order updates and maintain active orders."""
        if _data['client_id'] not in self.active_orders:
            self.logger.warning(f"order not found: {_data['client_id']}")
            return
        if _data['status'] == 'NEW':
            self.active_orders[_data['client_id']].status = 'NEW'
        elif _data['status'] == 'OPEN':
            self.active_orders[_data['client_id']].status = 'OPEN'
        elif _data['status'] == 'CLOSED':
            self.logger.info(f"order closed: {_data['cancel_reason']}")
            del self.active_orders[_data['client_id']]
        else:
            self.active_orders[_data['client_id']] = _data

    async def _on_orderbook(self, channel, msg):
        """Handle orderbook updates."""
        _data = msg['params']['data']
        self.logger.debug(f"orderbook: {_data}")
        _symbol = _data['market']
        _ticker = Ticker(_data['market'], self.exchange)

        _update_type = _data['update_type']
        _offset = _data['seq_no']
        
        if _symbol not in self.orderbooks:
            self.orderbooks[_symbol] = Depth(_ticker)

        ob = self.orderbooks[_symbol]
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

        await self._data_callbacks[_symbol](UpdateType.ORDERBOOK, _ticker, ob)

    async def _on_positions(self, channel, msg):
        """Handle position updates."""
        _data = msg['params']['data']
        self.logger.debug(f"positions: {_data}")
        self.positions[_data['market']] = _data

    async def _on_trades(self, channel, msg):
        """Handle trade updates."""
        _data = msg['params']['data']
        self.logger.info(f"trades: {_data}")

    async def _on_trade_bust(self, channel, msg):
        """Handle trade bust updates."""
        _data = msg['params']['data']
        self.logger.info(f"trade_bust: {_data}")

    async def _on_account_update(self, channel, msg):
        """Handle account updates."""
        _data = msg['params']['data']
        self.logger.debug(f"account_update: {_data}")
        self.account_info = _data

    async def subscribe_to_data_channels(self, market, callback):
        """
        Subscribe to market data channels.

        Args:
            market (str): The market symbol.
            callback (callable): The callback function for data updates.
        """
        self._data_callbacks[market] = callback

        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.BBO,
            callback=self._on_bbo,
            params={"market": market},
        )

        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.FILLS,
            callback=self._on_fills,
            params={"market": market},
        )

        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.ORDER_BOOK,
            callback=self._on_orderbook,
            params={"market": market},
        )

        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.ORDER_BOOK_DELTAS,
            callback=self._on_orderbook,
            params={"market": market},
        )

        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.FUNDING_DATA,
            callback=self._on_funding_data,
            params={"market": market},
        )

    async def subscribe_to_trade_channels(self, market, callback):
        """
        Subscribe to trade channels.

        Args:
            market (str): The market symbol.
            callback (callable): The callback function for trade updates.
        """
        self._trade_callbacks[market] = callback

        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.ORDERS,
            callback=self._on_order_update,
            params={"market": market},
        )

        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.TRADES,
            callback=self._on_trades,
            params={"market": market},
        )

        self.sync_open_orders(market)


    async def subscribe_to_account_channels(self):
        """Subscribe to account-related channels."""
        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.ACCOUNT,
            self._on_account_update,
        )
        
        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.POSITIONS, 
            callback=self._on_positions
        )
        
        await self.paradex.ws_client.subscribe(
            ParadexWebsocketChannel.TRADEBUSTS, 
            callback=self._on_trade_bust
        )

    async def setup_trading_rules(self, market):
        """
        Set up trading rules for a market.

        Args:
            market (str): The market symbol.
        """
        _para_markets = self.paradex.api_client.fetch_markets()

        for market in _para_markets['results']:
            self.trading_rules[market['symbol']] = TradingRules(
                min_amount_increment=D(market['order_size_increment']),
                min_price_increment=D(market['price_tick_size']),
                min_notional_size=D(market['min_notional']),
            )


    async def snapshots(self):
        """Take snapshots of current positions."""
        positions = self.paradex.api_client.fetch_positions()
        for pos in positions['results']:
            self.positions[pos['market']] = pos

    async def start(self):
        """Start the connector by initializing snapshots and connections."""
        await self.snapshots()
        await self._connect_to_ws()
        await self.subscribe_to_account_channels()
