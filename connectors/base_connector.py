# Importing specific connector classes from their respective modules
from connectors.binance_spot import BinanceSpotConnector
from connectors.paradex_perp import ParadexPerpConnector


def _get_connector(name, **kwargs):
    if name == 'paradex_perp':
        return ParadexPerpConnector(**kwargs)
    elif name == 'binance_spot':
        return BinanceSpotConnector(**kwargs)
    else:
        raise ValueError(f"Connector {name} not found")