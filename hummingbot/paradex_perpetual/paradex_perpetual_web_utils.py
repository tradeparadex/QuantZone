import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import hummingbot.connector.derivative.paradex_perpetual.paradex_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, RESTResponse
from hummingbot.core.web_assistant.rest_post_processors import RESTPostProcessorBase
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class DeleteRESTResponse(RESTResponse):
    def __init__(self, rr: RESTResponse):
        super().__init__(rr._aiohttp_response)
    
    async def json(self) -> Any:
        return {'status': 'success'}

class ParadexPerpetualRESTPostProcessor(RESTPostProcessorBase):
    
    async def post_process(self, response: RESTResponse) -> RESTResponse:
        if response.method == RESTMethod.DELETE:
            if response.status == 204:
                return DeleteRESTResponse(response)
        return response


class ParadexPerpetualRESTPreProcessor(RESTPreProcessorBase):

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        if request.headers is None:
            request.headers = {}
        request.headers["Content-Type"] = (
            "application/json"
        )
        request.headers["Accept"] = (
            "application/json"
        )
        return request


def private_rest_url(*args, **kwargs) -> str:
    return rest_url(*args, **kwargs)


def public_rest_url(*args, **kwargs) -> str:
    return rest_url(*args, **kwargs)


def rest_url(path_url: str, domain: str = "paradex_perpetual"):
    base_url = CONSTANTS.PROD_BASE_URL if domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_BASE_URL
    return base_url + path_url


def wss_url(domain: str = "paradex_perpetual"):
    base_ws_url = CONSTANTS.PROD_WS_URL if domain ==CONSTANTS.DOMAIN else CONSTANTS.TESTNET_WS_URL
    return base_ws_url


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        auth: Optional[AuthBase] = None) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[ParadexPerpetualRESTPreProcessor()],
        rest_post_processors=[ParadexPerpetualRESTPostProcessor()],
        auth=auth)
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[ParadexPerpetualRESTPreProcessor()],
        rest_post_processors=[ParadexPerpetualRESTPostProcessor()])
    return api_factory

def _build_private_rate_limits(trading_pairs: List[str]) -> List[RateLimit]:
    rate_limits = []

    for trading_pair in trading_pairs:
        rate_limits.append(
            RateLimit(
                limit_id=f"{CONSTANTS.SNAPSHOT_REST_URL}/{trading_pair}-PERP", 
                limit=CONSTANTS.ALL_MAX_REQUEST, 
                time_interval=60,
                linked_limits=[LinkedLimitWeightPair(CONSTANTS.ALL_ENDPOINTS_LIMIT)]),
        )

    return rate_limits

def build_rate_limits(trading_pairs: Optional[List[str]] = None) -> List[RateLimit]:

    trading_pairs = trading_pairs or []
    rate_limits = []

    rate_limits.extend(CONSTANTS.RATE_LIMITS)
    rate_limits.extend(_build_private_rate_limits(trading_pairs))

    return rate_limits

def create_throttler(trading_pairs: List[str] = None) -> AsyncThrottler:
    return AsyncThrottler(build_rate_limits(trading_pairs))


async def get_current_server_time(
        throttler,
        domain
) -> float:
    return time.time()


def is_exchange_information_valid(rule: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information

    :param exchange_info: the exchange information for a trading pair

    :return: True if the trading pair is enabled, False otherwise
    """
    return True
