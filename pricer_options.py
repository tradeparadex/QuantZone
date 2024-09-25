"""
This file contains the OptionPricer class, which is responsible for calculating
fair prices for options contracts. It inherits from BasePricer and uses a
hardcoded implied volatility (IV) value to determine appropriate pricing for
buy and sell orders.
"""

import logging
from decimal import Decimal as D
from math import exp, log, sqrt
from scipy.stats import norm
from strategy import BasePricer, RawFairPrice
from utils.data_methods import PriceType, Side

class OptionPricer(BasePricer):
    """
    A pricer for options contracts.

    This class extends the BasePricer to provide specialized pricing
    calculations for options. It uses a hardcoded implied volatility (IV)
    value and the Black-Scholes model to determine fair prices for buy
    and sell orders.

    Attributes:
        strategy: The parent strategy object that contains market data
                  and configuration parameters.
        logger: A logging object for debug and error messages.
        iv: The hardcoded implied volatility value.
    """

    def __init__(self, strategy):
        super().__init__(strategy)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.iv = D('0.5')  # Hardcoded IV value (50%)

    def get_raw_fair_price(self, side: Side) -> RawFairPrice:
        """
        Calculate the raw fair price for a given side (buy or sell).

        Args:
            side (Side): The side of the order (buy or sell).

        Returns:
            RawFairPrice: The raw fair price for the given side.
        """
        spot_price = self.strategy.get_base_price(PriceType.Mid)
        strike_price = self.strategy.strike_price
        time_to_expiry = self.strategy.time_to_expiry
        risk_free_rate = self.strategy.risk_free_rate
        is_call = self.strategy.option_type == 'call'

        fair_price = self.black_scholes(spot_price, strike_price, time_to_expiry, risk_free_rate, self.iv, is_call)

        return RawFairPrice(fair=fair_price, base=spot_price)

    def black_scholes(self, S: D, K: D, T: D, r: D, sigma: D, is_call: bool) -> D:
        """
        Calculate the option price using the Black-Scholes model.

        Args:
            S (Decimal): Current price of the underlying asset
            K (Decimal): Strike price of the option
            T (Decimal): Time to expiration (in years)
            r (Decimal): Risk-free interest rate
            sigma (Decimal): Implied volatility
            is_call (bool): True for call option, False for put option

        Returns:
            Decimal: The calculated option price
        """
        d1 = (log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * sqrt(T))
        d2 = d1 - sigma * sqrt(T)

        if is_call:
            option_price = S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)
        else:
            option_price = K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

        return D(str(option_price))

    def publish_metrics(self):
        """
        Publish relevant metrics for the option pricer.
        """
        self.strategy._publish_strat_metric('implied_volatility', float(self.iv))
