import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_web_utils as web_utils
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_derivative import (
        ParadexPerpetualDerivative,
    )


class ParadexPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # Recommended to Ping/Update listen key to keep connection alive
    HEARTBEAT_TIME_INTERVAL = 30.0
    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: AuthBase,
            trading_pairs: List[str],
            connector: 'ParadexPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN,
    ):

        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._ws_assistants: List[WSAssistant] = []
        self._connector = connector
        self._current_listen_key = None
        self._listen_for_user_stream_task = None
        self._last_listen_key_ping_ts = None
        self._trading_pairs: List[str] = trading_pairs

        self.token = None

    @property
    def last_recv_time(self) -> float:
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._get_ws_assistant()
        url = f"{web_utils.wss_url(self._domain)}"
        extra_headers = {
            "Authorization": f"Bearer {self._auth.paradex_account.jwt_token}"
        }
        await ws.connect(ws_url=url, ping_timeout=self.HEARTBEAT_TIME_INTERVAL, ws_headers=extra_headers)
        
        auth_payload = {
            "id": int(time.time() * 1_000_000),
            "jsonrpc": "2.0",
            "method": "auth",
            "params": {"bearer": self._auth.paradex_account.jwt_token}
        }

        auth_id_req = WSJSONRequest(payload=auth_payload)
        self.logger().info(f"Subscribing to {auth_payload}")
        await ws.send(auth_id_req)

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to order events.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                orders_change_payload = {
                    "id": int(time.time() * 1_000_000),
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "params": {"channel": CONSTANTS.USER_ORDERS_ENDPOINT_NAME.format(market=symbol)},
                }

                subscribe_order_change_request: WSJSONRequest = WSJSONRequest(
                    payload=orders_change_payload,
                    is_auth_required=True)
                
                self.logger().info(f"Subscribing to {orders_change_payload}")
                await websocket_assistant.send(subscribe_order_change_request)

            trades_payload = {
                "id": int(time.time() * 1_000_000),
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {"channel": CONSTANTS.USER_TRADES_ENDPOINT_NAME.format(market=symbol)},
            }

            subscribe_trades_request: WSJSONRequest = WSJSONRequest(
                payload=trades_payload,
                is_auth_required=True)
            
            self.logger().info(f"Subscribing to {trades_payload}")
            await websocket_assistant.send(subscribe_trades_request)

            fills_payload = {
                "id": int(time.time() * 1_000_000),
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {"channel": CONSTANTS.USER_FILLS_ENDPOINT_NAME.format(market=symbol)},
            }

            subscribe_fills_request: WSJSONRequest = WSJSONRequest(
                payload=fills_payload,
                is_auth_required=True)
            
            self.logger().info(f"Subscribing to {fills_payload}")
            await websocket_assistant.send(subscribe_fills_request)

            positions_payload = {
                "id": int(time.time() * 1_000_000),
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {"channel": CONSTANTS.USER_POSITION_ENDPOINT_NAME},
            }

            subscribe_positions_request: WSJSONRequest = WSJSONRequest(
                payload=positions_payload,
                is_auth_required=True)
            
            self.logger().info(f"Subscribing to {positions_payload}")
            await websocket_assistant.send(subscribe_positions_request)

            self.logger().info("Subscribed to private order and trades changes channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to user streams...")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        # self.logger().info(f"Received event message {event_message}")        
        if event_message.get("error") is not None:
            err_msg = event_message.get("error", {}).get("message", event_message.get("error"))
            raise IOError({
                "label": "WSS_ERROR",
                "message": f"Error received via websocket - {err_msg}."
            })
        else:
            _data = event_message.get('result', event_message.get('params', {}))
            _channel = _data.get("channel")
            
            if _channel is None:
                self.logger().warning(f"None channel {event_message}")
                return
            
            if _channel.startswith(CONSTANTS.USER_ORDERS_ENDPOINT_NAME.split('.')[0] + '.'):
                queue.put_nowait(event_message)
            elif _channel.startswith(CONSTANTS.USER_TRADES_ENDPOINT_NAME.split('.')[0] + '.'):
                queue.put_nowait(event_message)
            elif _channel.startswith(CONSTANTS.USER_FILLS_ENDPOINT_NAME.split('.')[0] + '.'):
                queue.put_nowait(event_message)
            elif _channel == CONSTANTS.USER_POSITION_ENDPOINT_NAME:
                queue.put_nowait(event_message)
            else:
                self.logger().warning(f"Unknown channel {_channel}")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant,
                    queue=queue)
            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload={"method": "ping"})
                await websocket_assistant.send(ping_request)
