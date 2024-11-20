"""
Import specific connector classes from their respective modules.

This section imports the necessary connector classes that will be used
to interact with different trading platforms. Each connector class is
designed to handle the specific API and functionality of its respective
trading platform.
"""
from typing import TYPE_CHECKING

from connectors.binance_spot import BinanceSpotConnector
from connectors.bybit_uta import BybitUTAConnector
from connectors.paradex_perp import ParadexPerpConnector

if TYPE_CHECKING:
    from utils.data_methods import ConnectorBase


def _get_connector(name: str, **kwargs) -> 'ConnectorBase':
    """
    Get the connector instance based on the provided name.
    """
    if name == 'paradex_perp':
        return ParadexPerpConnector(**kwargs)
    elif name == 'binance_spot':
        return BinanceSpotConnector(**kwargs)
    elif name == 'bybit_uta':
        return BybitUTAConnector(**kwargs)
    elif name == 'bybit_spot':
        return BybitUTAConnector(**kwargs, channel='spot')
    else:
        raise ValueError(f"Connector {name} not found")