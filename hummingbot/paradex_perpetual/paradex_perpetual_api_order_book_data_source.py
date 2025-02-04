import asyncio
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_derivative import (
        ParadexPerpetualDerivative,
    )


class ParadexPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    _bpobds_logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'ParadexPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._snapshot_messages_queue_key = "order_book_snapshot"
        self._diff_messages_queue_key = "order_book_diff"

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        response: List = await self._request_complete_funding_info(trading_pair)

        funding_event = response['results'][0]

        funding_premium = Decimal(funding_event['funding_premium'])
        funding_rate = Decimal(funding_event['funding_rate'])
        oracle_price = funding_premium / funding_rate
        mark_price = funding_premium + oracle_price

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=oracle_price,
            mark_price=mark_price,
            next_funding_utc_timestamp=self._next_funding_time(),
            rate=funding_rate,
        )
        return funding_info

    async def listen_for_funding_info(self, output: asyncio.Queue):
        """
        Reads the funding info events queue and updates the local funding info information.
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    funding_info = await self.get_funding_info(trading_pair)
                    funding_info_update = FundingInfoUpdate(
                        trading_pair=trading_pair,
                        index_price=funding_info.index_price,
                        mark_price=funding_info.mark_price,
                        next_funding_utc_timestamp=funding_info.next_funding_utc_timestamp,
                        rate=funding_info.rate,
                    )
                    output.put_nowait(funding_info_update)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public funding info updates from exchange")
            finally:
                await asyncio.sleep(CONSTANTS.FUNDING_RATE_UPDATE_INTERNAL_SECOND)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        ex_trading_pair = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        data = await self._connector._api_get(
            path_url=f"{CONSTANTS.SNAPSHOT_REST_URL}/{ex_trading_pair}",
            data={})
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_response.update({"trading_pair": trading_pair})
        snapshot_msg: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": snapshot_response["trading_pair"],
            "update_id": int(snapshot_response['seq_no']),
            "bids": [[float(i[0]), float(i[1])] for i in snapshot_response['bids']],
            "asks": [[float(i[0]), float(i[1])] for i in snapshot_response['asks']],
        }, timestamp=int(snapshot_response['last_updated_at'] / 1e3))
        return snapshot_msg

    async def _connected_websocket_assistant(self) -> WSAssistant:
        url = f"{web_utils.wss_url(self._domain)}"
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trades_payload = {
                    "id": int(time.time() * 1_000_000),
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "params": {"channel": CONSTANTS.TRADES_ENDPOINT_NAME.format(market=symbol)},
                }

                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

                order_book_payload = {
                    "id": int(time.time() * 1_000_000),
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "params": {"channel": CONSTANTS.DEPTH_ENDPOINT_NAME.format(market=symbol)},
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)

                self.logger().info(f"Subscribing to {trades_payload}")
                await ws.send(subscribe_trade_request)
                self.logger().info(f"Subscribing to {order_book_payload}")
                await ws.send(subscribe_orderbook_request)

                self.logger().info("Subscribed to public order book, trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book data streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "result" not in event_message:
            stream_name = event_message['params'].get("channel")
            if stream_name.startswith("order_book.") and stream_name.endswith(".deltas"):
                channel = self._diff_messages_queue_key
            elif stream_name.startswith("order_book."):
                channel = self._snapshot_messages_queue_key
            elif stream_name.startswith("trades."):
                channel = self._trade_messages_queue_key
        return channel

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        timestamp: float = raw_message["data"]["time"] * 1e-3
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
            raw_message["data"]["coin"])
        data = raw_message["data"]
        order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": trading_pair,
            "update_id": data["time"],
            "bids": [[float(i['px']), float(i['sz'])] for i in data["levels"][0]],
            "asks": [[float(i['px']), float(i['sz'])] for i in data["levels"][1]],
        }, timestamp=timestamp)
        message_queue.put_nowait(order_book_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        _data = raw_message["params"]["data"]
        timestamp: float = _data["last_updated_at"] * 1e-3
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
            _data["market"])

        if _data["update_type"] not in ["s"]:
            self.logger().warning(f"Unknown order book update type {_data['update_type']}")
            return

        order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair,
            "update_id": _data["seq_no"],
            "bids": [[float(i['price']), float(i['size'])] for i in _data["inserts"] if i['side'] == "BUY"],
            "asks": [[float(i['price']), float(i['size'])] for i in _data["inserts"] if i['side'] == "SELL"],
        }, timestamp=timestamp)
        message_queue.put_nowait(order_book_message)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_data = raw_message["params"]["data"]

        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(
            trade_data["market"])

        if trade_data["trade_type"] == "FILL":
            trade_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.TRADE, {
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.SELL.value) if trade_data["side"] == "SELL" else float(
                    TradeType.BUY.value),
                "trade_id": trade_data["id"],
                "price": float(trade_data["price"]),
                "amount": float(trade_data["size"])
            }, timestamp=trade_data["created_at"] * 1e-3)

            message_queue.put_nowait(trade_message)
        else:
            self.logger().warning(f"Unknown trade type {trade_data['trade_type']}")

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        return

    async def _request_complete_funding_info(self, trading_pair: str):
        ex_trading_pair = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        data = await self._connector._api_get(path_url=CONSTANTS.GET_LAST_FUNDING_RATE_PATH_URL,
                                               params={"market": ex_trading_pair})
        return data

    def _next_funding_time(self) -> int:
        """
        Funding premium is updated every 5 seconds as mentioned in https://docs.paradex.trade/risk-system/funding-mechanism
        """
        return time.time() + 5
