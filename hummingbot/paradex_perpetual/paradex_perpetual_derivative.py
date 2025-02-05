import asyncio
import hashlib
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, AsyncIterable, Dict, List, Optional, Tuple

from bidict import bidict
from paradex_py.common.order import (
    Order as ParadexOrderObject,
    OrderSide as ParadexOrderSide,
    OrderType as ParadexOrderType,
)

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.paradex_perpetual import (
    paradex_perpetual_constants as CONSTANTS,
    paradex_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_api_order_book_data_source import (
    ParadexPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_auth import ParadexPerpetualAuth
from hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_user_stream_data_source import (
    ParadexPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import AccountEvent, PositionModeChangeEvent
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

bpm_logger = None


POS_SIDE_MAP = {
    "LONG": PositionSide.LONG,
    "SHORT": PositionSide.SHORT
}

class ParadexPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 12.0

    def __init__(
            self,
            client_config_map: "ClientConfigAdapter",
            paradex_perpetual_l1_address: str,
            paradex_perpetual_is_testnet: bool,
            paradex_perpetual_l1_private_key: Optional[str] = None,
            paradex_perpetual_l2_private_key: Optional[str] = None,
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
            domain: str = CONSTANTS.DOMAIN,
    ):
        self.paradex_perpetual_l1_address = paradex_perpetual_l1_address
        self.paradex_perpetual_is_testnet = paradex_perpetual_is_testnet
        self.paradex_perpetual_l1_private_key = paradex_perpetual_l1_private_key
        self.paradex_perpetual_l2_private_key = paradex_perpetual_l2_private_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._position_mode = None
        self._last_trade_history_timestamp = None
        self.coin_to_asset: Dict[str, int] = {}
        super().__init__(client_config_map)

    SHORT_POLL_INTERVAL = 5.0

    LONG_POLL_INTERVAL = 12.0

    @property
    def name(self) -> str:
        # Note: domain here refers to the entire exchange name. i.e. paradex_perpetual or paradex_perpetual_testnet
        return self._domain

    @property
    def authenticator(self) -> ParadexPerpetualAuth:
        return ParadexPerpetualAuth(
                self.paradex_perpetual_l1_address,
                self.paradex_perpetual_is_testnet,
                self.paradex_perpetual_l1_private_key,
                self.paradex_perpetual_l2_private_key)

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return web_utils.build_rate_limits(self.trading_pairs)

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.AVAILABLE_MARKETS_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.AVAILABLE_MARKETS_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    def start(self, clock: Clock, timestamp: float):
        super().start(clock, timestamp)
        self.set_position_mode(PositionMode.ONEWAY)

    async def _make_network_check_request(self):
        await self._api_get(path_url=self.check_network_request_path, data={})

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self):
        """
        This method needs to be overridden to provide the accurate information depending on the exchange.
        """
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    async def _update_trading_rules(self):
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path,
                                             data={})
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    async def _initialize_trading_pair_symbol_map(self):
        try:
            self.logger().info("Retrieving trading pairs from Paradex Perpetuals...")
            exchange_info = await self._api_get(path_url=self.trading_pairs_request_path,
                                                 data={})

            self.logger().info("got trading pairs from Paradex Perpetuals...")
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ParadexPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return ParadexPerpetualUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_trade_history(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )
        await self._sleep(60 * 5)

    async def _update_order_status(self):
        await self._update_orders()

    async def _update_lost_orders_status(self):
        await self._update_lost_orders()

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = is_maker or False
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):

        cancel_result = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_URL.format(client_id=order_id),
            limit_id=CONSTANTS.CANCEL_ORDER_URL,
            is_auth_required=True)

        if "success" == cancel_result.get("status"):
            return True
    

        self.logger().debug(f"The order {order_id} does not exist on Paradex Perpetuals. "
                            f"No cancelation needed.")
        await self._order_tracker.process_order_not_found(order_id)
        raise IOError(f'{cancel_result["response"]["data"]["statuses"][0]["error"]}')

        return False

    # === Orders placing ===

    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type=OrderType.LIMIT,
            price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        """
        Creates a promise to create a buy order using the parameters

        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price

        :return: the id assigned by the connector to the order (the client id)
        """
        order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length
        )
        md5 = hashlib.md5()
        md5.update(order_id.encode('utf-8'))
        hex_order_id = f"0x{md5.hexdigest()}"
        if order_type is OrderType.MARKET:
            mid_price = self.get_mid_price(trading_pair)
            slippage = CONSTANTS.MARKET_ORDER_SLIPPAGE
            market_price = mid_price * Decimal(1 + slippage)
            price = self.quantize_order_price(trading_pair, market_price)

        safe_ensure_future(self._create_order(
            trade_type=TradeType.BUY,
            order_id=hex_order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            **kwargs))
        return hex_order_id

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.LIMIT,
             price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        """
        Creates a promise to create a sell order using the parameters.
        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price
        :return: the id assigned by the connector to the order (the client id)
        """
        order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length
        )
        md5 = hashlib.md5()
        md5.update(order_id.encode('utf-8'))
        hex_order_id = f"0x{md5.hexdigest()}"
        if order_type is OrderType.MARKET:
            mid_price = self.get_mid_price(trading_pair)
            slippage = CONSTANTS.MARKET_ORDER_SLIPPAGE
            market_price = mid_price * Decimal(1 - slippage)
            price = self.quantize_order_price(trading_pair, market_price)

        safe_ensure_future(self._create_order(
            trade_type=TradeType.SELL,
            order_id=hex_order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            **kwargs))
        return hex_order_id

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            position_action: PositionAction = PositionAction.NIL,
            **kwargs,
    ) -> Tuple[str, float]:

        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        param_order_type = ParadexOrderType.Limit
        param_order_tif = "GTC"
        if order_type == OrderType.LIMIT_MAKER:
            param_order_tif = "POST_ONLY"
        elif order_type == OrderType.MARKET:
            param_order_type = ParadexOrderType.Market

        side_map = {
            TradeType.BUY: ParadexOrderSide.Buy,
            TradeType.SELL: ParadexOrderSide.Sell
        }

        self.logger().info(f"Creating order tif: {param_order_tif}")
        order_object = ParadexOrderObject(
            stp="EXPIRE_BOTH",
            client_id=order_id,
            market=symbol,
            order_side=side_map[trade_type],
            limit_price=price,
            size=amount,
            order_type=param_order_type,
            instruction=param_order_tif
        )
        order_object.signature = self._auth.paradex_account.sign_order(order_object)

        api_params = order_object.dump_to_dict()

        order_result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_URL,
            data=api_params,
            is_auth_required=True)
        
        if order_result.get("error") is not None:
            raise IOError(f"Error submitting order {order_id}: {order_result['message']}")
        
        o_id = str(order_result["id"])
        return (o_id, self.current_timestamp)

    async def _update_trade_history(self):
        orders = list(self._order_tracker.all_fillable_orders.values())
        all_fillable_orders = self._order_tracker.all_fillable_orders_by_exchange_order_id
        all_fills_response = []
        if len(orders) > 0:

            trading_pairs = {o.trading_pair for o in orders}
            symbols = await asyncio.gather(
                *[self.exchange_symbol_associated_to_pair(trading_pair=t) for t in trading_pairs]
            )
            try:
                for symbol in symbols:
                    _response = await self._api_get(
                        path_url=CONSTANTS.ACCOUNT_TRADE_LIST_URL,
                        is_auth_required=True,
                        params={"market": symbol})
                    all_fills_response.extend(_response["results"])
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates. Error: {request_error}",
                    exc_info=request_error,
                )
            for trade_fill in all_fills_response:
                self._process_trade_rs_event_message(order_fill=trade_fill, all_fillable_order=all_fillable_orders)

    def _process_trade_rs_event_message(self, order_fill: Dict[str, Any], all_fillable_order):
        exchange_order_id = str(order_fill["id"])
        fillable_order = all_fillable_order.get(exchange_order_id)
        
        if fillable_order is not None:
            fee_asset = fillable_order.quote_asset

            position_action = PositionAction.OPEN if order_fill["dir"].split(" ")[0] == "Open" else PositionAction.CLOSE
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=position_action,
                percent_token=fee_asset,
                flat_fees=[TokenAmount(amount=Decimal(order_fill["fee"]), token=fee_asset)]
            )

            trade_update = TradeUpdate(
                trade_id=str(order_fill["tid"]),
                client_order_id=fillable_order.client_order_id,
                exchange_order_id=str(order_fill["oid"]),
                trading_pair=fillable_order.trading_pair,
                fee=fee,
                fill_base_amount=Decimal(order_fill["sz"]),
                fill_quote_amount=Decimal(order_fill["px"]) * Decimal(order_fill["sz"]),
                fill_price=Decimal(order_fill["px"]),
                fill_timestamp=order_fill["time"] * 1e-3,
            )

            self._order_tracker.process_trade_update(trade_update)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # Use _update_trade_history instead
        pass

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        try:
            raise error
        except (asyncio.TimeoutError, KeyError):
            self.logger().debug(
                f"Tracked order {order.client_order_id} does not have an exchange id. "
                f"Attempting fetch in next polling interval."
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Error fetching status update for the active order {order.client_order_id}: {request_error}.",
            )
            self.logger().debug(
                f"Order {order.client_order_id} not found counter: {self._order_tracker._order_not_found_records.get(order.client_order_id, 0)}")
            await self._order_tracker.process_order_not_found(order.client_order_id)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        client_order_id = tracked_order.client_order_id
        order_update = await self._api_get(
            path_url=CONSTANTS.ORDER_URL.format(client_id=client_order_id),
            limit_id=CONSTANTS.ORDER_URL,
            is_auth_required=True)

        return self._get_order_update_object(order_update, trading_pair=tracked_order.trading_pair, client_order_id=client_order_id)

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Paradex. Check API key and network connection.",
                )
                await self._sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to messages from _user_stream_tracker.user_stream queue.
        Traders, Orders, and Balance updates from the WS.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                # self.logger().info(f"Received event message {event_message}")    
                if isinstance(event_message, dict):
                    if 'params' in event_message:
                        _data = event_message["params"].get("data",{})
                        channel: str = event_message["params"].get("channel", None)
                    else:
                        _data = event_message.get("result", {})
                        channel: str = _data.get("channel", None)
                elif event_message is asyncio.CancelledError:
                    raise asyncio.CancelledError
                else:
                    raise Exception(event_message)

                if channel.startswith(CONSTANTS.USER_ORDERS_ENDPOINT_NAME.split('.')[0] + '.'):
                    if 'status' in _data:
                        self._process_order_message(_data)
                elif channel == CONSTANTS.USER_POSITION_ENDPOINT_NAME:
                    if len(_data.keys()) > 1:
                        await self._process_position_message(_data)
                elif channel.startswith(CONSTANTS.USER_TRADES_ENDPOINT_NAME.split('.')[0] + '.'):
                    if len(_data.keys()) > 1:
                        await self._process_trade_message(_data)
                else:
                    self.logger().error(
                        f"Unexpected message in user stream: {event_message}.")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(2.0)

    async def _process_position_message(self, position: Dict[str, Any]):

        hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(position['market'])

        position_side = POS_SIDE_MAP[position["side"]]

        unrealized_pnl = Decimal(position.get("unrealized_pnl"))
        entry_price = Decimal(position.get("average_entry_price"))
        amount = Decimal(position.get("size", 0))

        # TODO: fetch leverage
        leverage = Decimal(10)
        pos_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)
        
        if amount != 0:
            _position = Position(
                trading_pair=hb_trading_pair,
                position_side=position_side,
                unrealized_pnl=unrealized_pnl,
                entry_price=entry_price,
                amount=amount,
                leverage=leverage
            )
            self._perpetual_trading.set_position(pos_key, _position)
        else:
            self._perpetual_trading.remove_position(pos_key)


    async def _process_trade_message(self, trade: Dict[str, Any], client_order_id: Optional[str] = None):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        Example Trade:
        """
        exchange_order_id = str(trade["order_id"])
        tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)

        if tracked_order is None:
            all_orders = self._order_tracker.all_fillable_orders
            for k, v in all_orders.items():
                await v.get_exchange_order_id()
            _cli_tracked_orders = [o for o in all_orders.values() if exchange_order_id == o.exchange_order_id]
            if not _cli_tracked_orders:
                self.logger().debug(f"Ignoring trade message with id {exchange_order_id}: not in in_flight_orders.")
                return
            tracked_order = _cli_tracked_orders[0]

        if trade['fill_type'] in ['FILL', 'PARTIAL_FILL']:

            fee_asset = trade['fee_currency']
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=PositionAction.NIL,
                percent_token=fee_asset,
                flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=fee_asset)]
            )
            trade_update: TradeUpdate = TradeUpdate(
                trade_id=str(trade["id"]),
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                fill_timestamp=trade["created_at"] * 1e-3,
                fill_price=Decimal(trade["price"]),
                fill_base_amount=Decimal(trade["size"]),
                fill_quote_amount=Decimal(trade["price"]) * Decimal(trade["size"]),
                fee=fee,
            )
            self._order_tracker.process_trade_update(trade_update)
        else:
            self.logger().warning(f"Ignoring trade message with id {exchange_order_id}: not a fill trade.")

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancelation or failure event if needed.

        :param order_msg: The order response from either REST or web socket API (they are of the same format)

        Example Order:
        """
        client_order_id = order_msg["client_id"]
        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if not tracked_order:
            self.logger().debug(f"Ignoring order message with id {client_order_id}: not in in_flight_orders.")
            return

        order_update = self._get_order_update_object(order_msg, trading_pair=tracked_order.trading_pair, client_order_id=client_order_id)
        self._order_tracker.process_order_update(order_update=order_update)
    
    def _get_order_update_object(self, order_msg: Dict[str, Any], trading_pair: str, client_order_id: str) -> OrderUpdate:
        
        current_state = order_msg["status"]
        if order_msg["status"] not in CONSTANTS.ORDER_STATE:
            self.logger().error(f"Unknown order status {order_msg['status']} for order {client_order_id}.")
            return

        _state = CONSTANTS.ORDER_STATE[current_state]
        if _state == OrderState.CANCELED:
            if order_msg["cancel_reason"] is not None and order_msg["cancel_reason"] not in ["USER_CANCELED", "EXPIRED", ""]:
                _state = OrderState.FAILED
        elif _state == OrderState.OPEN:
            if order_msg['remaining_size'] != order_msg['size']:
                _state = OrderState.PARTIALLY_FILLED
        elif _state == OrderState.FILLED:
            if order_msg['remaining_size'] != '0':
                _state = OrderState.PARTIALLY_FILLED
 
        order_update: OrderUpdate = OrderUpdate(
            trading_pair=trading_pair,
            update_timestamp=order_msg["timestamp"] * 1e-3,
            new_state=_state,
            client_order_id=order_msg["client_id"] or client_order_id,
            exchange_order_id=str(order_msg["id"]),
        )
        
        return order_update
    
    async def _format_trading_rules(self, exchange_info_dict: List) -> List[TradingRule]:
        """
        Queries the necessary API endpoint and initialize the TradingRule object for each trading pair being traded.

        Parameters
        ----------
        exchange_info_dict:
            Trading rules dictionary response from the exchange
        """
        # rules: list = exchange_info_dict[0]
        self.coin_to_asset = {asset["symbol"]: asset for asset in
                              exchange_info_dict['results']}

        return_val: list = []
        for asset_info in exchange_info_dict['results']:
            try:
                ex_symbol = asset_info["symbol"]
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=ex_symbol)
                step_size = Decimal(asset_info.get("order_size_increment"))
                price_size = Decimal(asset_info.get("price_tick_size"))
                
                # TODO: Creates problems because of lack of price
                # collateral_token = asset_info.get("settlement_currency")
                collateral_token = asset_info.get("quote_currency")
                min_notional = Decimal(asset_info.get("min_notional"))
                return_val.append(
                    TradingRule(
                        trading_pair,
                        min_base_amount_increment=step_size,
                        min_price_increment=price_size,
                        min_notional_size=min_notional,
                        buy_order_collateral_token=collateral_token,
                        sell_order_collateral_token=collateral_token,
                    )
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {exchange_info_dict}. Skipping.",
                                    exc_info=True)
        return return_val

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List):
        mapping = bidict()
        for symbol_data in filter(web_utils.is_exchange_information_valid, exchange_info.get("results", [])):
            exchange_symbol = symbol_data["symbol"]
            base = symbol_data["base_currency"]
            quote = symbol_data["quote_currency"]
            trading_pair = combine_to_hb_trading_pair(base, quote)
            if trading_pair in mapping.inverse:
                self._resolve_trading_pair_symbols_duplicate(mapping, exchange_symbol, base, quote)
            else:
                mapping[exchange_symbol] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        response = await self._api_get(path_url=CONSTANTS.TICKER_PRICE_CHANGE_URL,
                                        data={"market": exchange_symbol})
        price = 0
        for index, i in enumerate(response[0]['universe']):
            if i['name'] == 'coin':
                price = float(response[1][index]['markPx'])
        return price

    def _resolve_trading_pair_symbols_duplicate(self, mapping: bidict, new_exchange_symbol: str, base: str, quote: str):
        """Resolves name conflicts provoked by futures contracts.

        If the expected BASEQUOTE combination matches one of the exchange symbols, it is the one taken, otherwise,
        the trading pair is removed from the map and an error is logged.
        """
        expected_exchange_symbol = f"{base}{quote}"
        trading_pair = combine_to_hb_trading_pair(base, quote)
        current_exchange_symbol = mapping.inverse[trading_pair]
        if current_exchange_symbol == expected_exchange_symbol:
            pass
        elif new_exchange_symbol == expected_exchange_symbol:
            mapping.pop(current_exchange_symbol)
            mapping[new_exchange_symbol] = trading_pair
        else:
            self.logger().error(
                f"Could not resolve the exchange symbols {new_exchange_symbol} and {current_exchange_symbol}")
            mapping.pop(current_exchange_symbol)

    async def _update_balances(self):
        """
        Calls the REST API to update total and available balances.
        """
        balance_info = await self._api_get(
            path_url=CONSTANTS.BALANCES_INFO_URL, 
            is_auth_required=True)

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_INFO_URL, 
            is_auth_required=True)
        
        # TODO: see the collateral ccy issue
        coll_quote = CONSTANTS.CURRENCY
        algo_quote = "USD"
        _balances_map = {x['token']: x['size'] for x in balance_info['results']}
        self._account_balances[algo_quote] = Decimal(_balances_map[coll_quote])
        self._account_available_balances[algo_quote] = Decimal(account_info['free_collateral'])

    async def _update_positions(self):

        positions = await self._api_get(path_url=CONSTANTS.POSITION_INFORMATION_URL,
                                        data={}, is_auth_required=True)
        for position in positions["results"]:
            await self._process_position_message(position)
        
        if len(positions["results"]) < 1:
            keys = list(self._perpetual_trading.account_positions.keys())
            for key in keys:
                self._perpetual_trading.remove_position(key)

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        msg = ""
        success = True
        initial_mode = await self._get_position_mode()
        if initial_mode != mode:
            msg = "Paradex only supports the ONEWAY position mode."
            success = False
        else:
            self._position_mode = mode
            super().set_position_mode(mode)
            self.trigger_event(
                AccountEvent.PositionModeChangeSucceeded,
                PositionModeChangeEvent(self.current_timestamp, trading_pair, mode),
            )
            self.logger().debug(f"Paradex switching position mode to " f"{mode} for {trading_pair} succeeded.")
        return success, msg

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        # TODO
        msg = "Leverage setting is not supported by Paradex."
        success = True
        return success, msg

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

        funding_info_response = await self._api_get(
            path_url=CONSTANTS.GET_LAST_FUNDING_PAYMENT_PATH_URL,
            is_auth_required=True,
            params={"market": exchange_symbol})
        
        sorted_payment_response = funding_info_response["results"]
        if len(sorted_payment_response) < 1:
            timestamp, funding_rate, payment = 0, Decimal("-1"), Decimal("-1")
            return timestamp, funding_rate, payment
        elif len(sorted_payment_response) > 1:
            self.logger().warning("Multiple funding payments found. Using the first one.")

        funding_payment = sorted_payment_response[0]
        payment = Decimal(funding_payment["payment"])
        timestamp = funding_payment["created_at"] * 1e-3

        if payment == Decimal("0"):
            return 0, Decimal("-1"), Decimal("-1")

        data = await self._api_get(path_url=CONSTANTS.GET_LAST_FUNDING_RATE_PATH_URL,
                                    params={"market": exchange_symbol, "start_at": int(funding_payment["created_at"])})

        funding_rate = Decimal(data['results'][-1]['funding_rate'])
        return timestamp, funding_rate, payment

    def _last_funding_time(self) -> int:
        """
        Funding settlement occurs every trade? Using the 5 seconds refresh rate here
        """
        return int(time.time() - 5)
