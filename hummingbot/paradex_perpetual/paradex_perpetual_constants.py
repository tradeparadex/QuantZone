from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "paradex_perpetual"
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = None

MARKET_ORDER_SLIPPAGE = 0.05

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "paradex_perpetual_testnet"

PROD_BASE_URL = "https://api.prod.paradex.trade/v1"
TESTNET_BASE_URL = "https://api.testnet.paradex.trade/v1"

PROD_WS_URL = "wss://ws.api.prod.paradex.trade/v1"
TESTNET_WS_URL = "wss://ws.api.testnet.paradex.trade/v1"

FUNDING_RATE_UPDATE_INTERNAL_SECOND = 5
CURRENCY = "USDC"

AVAILABLE_MARKETS_URL = "/markets"
MARKETS_SUMMARY_URL = "/markets/summary"
ACCOUNT_INFO_URL = "/account"
BALANCES_INFO_URL = "/balance"
PING_URL = "/system/state"
GET_LAST_FUNDING_RATE_PATH_URL = "/funding/data"
GET_LAST_FUNDING_PAYMENT_PATH_URL = "/funding/payments"
POSITION_INFORMATION_URL = "/positions"
SNAPSHOT_REST_URL = "/orderbook"
TICKER_PRICE_CHANGE_URL = "/markets/summary"
ACCOUNT_TRADE_LIST_URL = "/fills"
CREATE_ORDER_URL = "/orders"
CANCEL_ORDER_URL = "/orders/by_client_id/{client_id}"
ORDER_URL = "/orders/by_client_id/{client_id}"

TRADES_ENDPOINT_NAME = "trades.{market}"
DEPTH_ENDPOINT_NAME = "order_book.{market}.snapshot@15@100ms"

USER_ORDERS_ENDPOINT_NAME = "orders.{market}"
USER_FILLS_ENDPOINT_NAME = "fills.{market}"
USER_TRADES_ENDPOINT_NAME = "fills.{market}"
USER_POSITION_ENDPOINT_NAME = "positions"

# Order Statuses
ORDER_STATE = {
    "OPEN": OrderState.OPEN,
    "UNTRIGGERED": OrderState.OPEN,
    "NEW": OrderState.OPEN,
    "CLOSED": OrderState.CANCELED
}

HEARTBEAT_TIME_INTERVAL = 60.0

ORDERS_MAX_REQUEST = 250
ORDERS_LIMIT_TIME_INTERVAL = 1
ALL_ORDERS_LIMIT = "All Orders"

ALL_MAX_REQUEST = 10
ALL_LIMIT_TIME_INTERVAL = 1
ALL_ENDPOINTS_LIMIT = "All"

RATE_LIMITS = [
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL),
    RateLimit(limit_id=ALL_ORDERS_LIMIT, limit=ORDERS_MAX_REQUEST, time_interval=ORDERS_LIMIT_TIME_INTERVAL),

    # Weight Limits for individual endpoints
    RateLimit(limit_id=CREATE_ORDER_URL, limit=ORDERS_MAX_REQUEST, time_interval=ORDERS_LIMIT_TIME_INTERVAL,
            linked_limits=[LinkedLimitWeightPair(ALL_ORDERS_LIMIT)]),
    RateLimit(limit_id=CANCEL_ORDER_URL, limit=ORDERS_MAX_REQUEST, time_interval=ORDERS_LIMIT_TIME_INTERVAL,
            linked_limits=[LinkedLimitWeightPair(ALL_ORDERS_LIMIT)]),

    RateLimit(limit_id=MARKETS_SUMMARY_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
            linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=AVAILABLE_MARKETS_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
            linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=BALANCES_INFO_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
            linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=SNAPSHOT_REST_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=PING_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ORDER_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ACCOUNT_TRADE_LIST_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ACCOUNT_INFO_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=POSITION_INFORMATION_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=GET_LAST_FUNDING_RATE_PATH_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=GET_LAST_FUNDING_PAYMENT_PATH_URL, limit=ALL_MAX_REQUEST, time_interval=ALL_LIMIT_TIME_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),

]
ORDER_NOT_EXIST_MESSAGE = "CLIENT_ORDER_ID_NOT_FOUND"
UNKNOWN_ORDER_MESSAGE = "CLIENT_ORDER_ID_NOT_FOUND"
