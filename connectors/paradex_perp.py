import asyncio
import datetime as dt
import logging
import os
import time
from collections import defaultdict
from decimal import Decimal as D
from enum import Enum
from typing import Dict
import hashlib

import numpy as np
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

from marshmallow.exceptions import ValidationError
from paradex_py import Paradex
from paradex_py.api.ws_client import ParadexWebsocketChannel
from paradex_py.environment import PROD, TESTNET
from paradex_py.common.order import Order as ParadexOrder
from paradex_py.common.order import OrderType as ParadexOrderType, OrderSide as ParadexOrderSide
from paradex_py.api.models import ApiError

class ParadexPerpConnector(ConnectorBase):
    def __init__(self, loop):
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

    def get_account_str(self):
        return str(self.paradex.account.l2_address)

    async def initialize(self):

        _para_env = os.getenv("PARADEX_ENVIRONMENT")
        _paradex_perpetual_chain = PROD if _para_env == "mainnet" else (TESTNET if _para_env == "testnet" else _para_env)

        self.paradex = Paradex(
            env=_paradex_perpetual_chain, 
            l1_address=os.getenv("PARADEX_L1_ADDRESS"), 
            l2_private_key=os.getenv("PARADEX_PRIVATE_KEY"),
            logger=self.logger
        )

    def generate_incremental_unique_id(self, mkt):
        current_time = str(time.time_ns()) + str(self._order_counter) + str(mkt)
        self._order_counter += 1
        hash_object = hashlib.sha256(current_time.encode())
        return hash_object.hexdigest()

    def cancel_order(self, client_order_id):
        try:
            resp = self.paradex.api_client.cancel_order_by_client_id(client_order_id)
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
        for ao in set(self.active_orders.keys()):
            try:
                po = self.paradex.api_client.fetch_order_by_client_id(ao)
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
            p_orders = self.paradex.api_client.fetch_orders({'market': mkt})
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
        for order in orders:
            self.cancel_order(order)

    def bulk_insert_orders(self, orders):
        for order in orders:
            self.insert_order(order)
    
    def insert_order(self, order: Order):
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
            resp = self.paradex.api_client.submit_order(_po)
        except Exception as e:
            self.logger.error(f"order failed: {e}")
            return
        
        self.active_orders[_po.client_id] = order
        self.process_order_update(resp)

    async def _connect_to_ws(self):
        is_connected = False
        while not is_connected:
            is_connected = await self.paradex.ws_client.connect()
            if not is_connected:
                self.logger.info("connection failed, retrying in 1 second")
                await asyncio.sleep(1)
        self.logger.info("connected to ws")

    async def _on_bbo(self, channel, msg):
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
        _data = msg['params']['data']
        self.logger.info(f"fills: {_data}")
        # Trigger pos sync

    async def _on_order_update(self, channel, msg):
        _data = msg['params']['data']
        self.logger.info(f"order_update: {_data}")
        self.process_order_update(_data)

    def process_order_update(self, _data):
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
        _data = msg['params']['data']
        self.logger.info(f"positions: {_data}")
        self.positions[_data['market']] = _data

    async def _on_trades(self, channel, msg):
        _data = msg['params']['data']
        self.logger.info(f"trades: {_data}")

    async def _on_trade_bust(self, channel, msg):
        _data = msg['params']['data']
        self.logger.info(f"trade_bust: {_data}")

    async def _on_account_update(self, channel, msg):
        _data = msg['params']['data']
        self.logger.debug(f"account_update: {_data}")
        self.account_info = _data

    async def subscribe_to_data_channels(self, market, callback):

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
        _para_markets = self.paradex.api_client.fetch_markets()

        for market in _para_markets['results']:
            self.trading_rules[market['symbol']] = TradingRules(
                min_amount_increment=D(market['order_size_increment']),
                min_price_increment=D(market['price_tick_size']),
                min_notional_size=D(market['min_notional']),
            )


    async def snapshots(self):
        positions = self.paradex.api_client.fetch_positions()
        for pos in positions['results']:
            self.positions[pos['market']] = pos

    async def start(self):
        await self.snapshots()
        await self._connect_to_ws()
        await self.subscribe_to_account_channels()
