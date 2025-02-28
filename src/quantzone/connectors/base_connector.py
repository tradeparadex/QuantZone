"""
Import specific connector classes from their respective modules.

This section imports the necessary connector classes that will be used
to interact with different trading platforms. Each connector class is
designed to handle the specific API and functionality of its respective
trading platform.
"""

from .binance_spot import BinanceSpotConnector
from .bybit_uta import BybitUTAConnector
from .connector_base import ConnectorBase
from .paradex_perp import ParadexPerpConnector


def get_connector(name: str, **kwargs) -> ConnectorBase:
    """
    Get the connector instance based on the provided name.
    """
    match name:
        case "paradex_perp":
            return ParadexPerpConnector(**kwargs)
        case "binance_spot":
            return BinanceSpotConnector(**kwargs)
        case "bybit_uta":
            return BybitUTAConnector(**kwargs)
        case "bybit_spot":
            return BybitUTAConnector(**kwargs, channel="spot")
        case _:
            raise ValueError(f"Connector {name} not found")
