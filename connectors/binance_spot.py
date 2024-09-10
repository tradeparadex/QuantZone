import asyncio
import logging

import os
import time

import datetime as dt
import numpy as np

from enum import Enum
from typing import Dict
from decimal import Decimal as D
from collections import defaultdict

from binance import AsyncClient, BinanceSocketManager

from utils.data_methods import (
    Ticker,
    Level,
    Depth,
    UpdateType
)

class BinanceSpotConnector:
    def __init__(self, loop):
        self.logger = logging.getLogger(self.__class__.__name__)
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
        self.binance = AsyncClient()
        self._socket_manager = BinanceSocketManager(self.binance)

    async def _connect_to_ws(self):
        pass

    async def _on_bbo(self, channel, msg):
        _data = msg['params']['data']
        self.logger.info(f"bbo: {_data}")
        _latest_bbo = {
            'ask': Level(px=_data['ask'], qty=_data['ask_size'], offset=_data['seq_no']),
            'bid': Level(px=_data['bid'], qty=_data['bid_size'], offset=_data['seq_no'])
        }
        self.bbos[_data['market']] = _latest_bbo
        await self._data_callbacks[_data['market']](UpdateType.BBO, _ticker, _latest_bbo)

    async def _on_funding_data(self, channel, msg):
        pass

    async def _on_fills(self, channel, msg):
        _data = msg['params']['data']
        self.logger.info(f"fills: {_data}")

    async def _on_order_update(self, channel, msg):
        pass

    async def _on_orderbook(self, channel, msg):
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
        pass

    async def _on_trades(self, channel, msg):
        pass

    async def _on_account_update(self, channel, msg):
        pass
    
    def _translate_symbol(self, market):
        return market.replace('-', '').upper()


    async def _listen_to_data(self, ts, symbol):
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
        self._data_callbacks[market] = callback
        og_symbol = self._translate_symbol(market)
        self._symbol_map[og_symbol] = market

        ts = self._socket_manager.depth_socket(og_symbol, depth=10, interval=100)
        asyncio.ensure_future(self._listen_to_data(ts, market), loop=self.loop)

    async def subscribe_to_trade_channels(self, market, callback):
        pass

    async def subscribe_to_account_channels(self):
        pass

    async def start(self):
        await self._connect_to_ws()
        await self.subscribe_to_account_channels()
