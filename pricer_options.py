"""
This file contains the OptionPricer class, which is responsible for calculating
fair prices for options contracts. It inherits from BasePricer and uses a
hardcoded implied volatility (IV) value to determine appropriate pricing for
buy and sell orders.
"""

from decimal import Decimal as D
import numpy as np
import structlog
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
        self.logger = structlog.get_logger(self.__class__.__name__)
        self.iv = D('0.80')  # Hardcoded IV value

    def get_raw_fair_price(self, side: Side) -> RawFairPrice:
        """
        Calculate the raw fair price for a given side (buy or sell).

        Args:
            side (Side): The side of the order (buy or sell).

        Returns:
            RawFairPrice: The raw fair price for the given side.
        """
        spot_price = self.strategy._smoothen_spot_price.value
        strike_price = self.strike_price
        time_to_expiry = self.time_to_expiry
        risk_free_rate = D(0.01)
        is_call = self.option_type == 'call'

        bs_price = self.black_scholes(spot_price, strike_price, time_to_expiry, risk_free_rate, self.iv, is_call)

        funding_period_hours = D('8')
        funding_period_years = funding_period_hours / D(24*365)
        fair_price = self.perp_bs_price(spot_price, strike_price, self.iv, funding_period_years, self.option_type)

        return RawFairPrice(fair=fair_price, base=fair_price)

    @property
    def option_type(self):
        return 'call' if self.strategy.market.split('-')[-1] == 'C' else 'put'

    @property
    def strike_price(self):
        return D(self.strategy.market.split('-')[2])

    @property
    def time_to_expiry(self):
        return D('0.01')
    

    def option_instrinsic_value(self, S: float, K: float, option_type: str):
        assert option_type in ["call", "put"]
        if option_type == "call":
            return max(0, S-K)
        else:
            return max(0, K-S)
    
    def perp_bs_price(self, S: float, K: float, sigma: float, funding_period_years: float, option_type: str):
        return self.option_instrinsic_value(S, K, option_type) + self.perp_bs_time_value(S, K, sigma, funding_period_years)

    def perp_bs_time_value(self, S: float, K: float, sigma: float, funding_period_years: float):
        assert sigma >= 0
        if sigma == 0:
            return 0
        else:
            u = np.sqrt(1 + 8 / (sigma*sigma*funding_period_years))
            if S >= K:
                return (K / u) * (S / K) ** (-(u - 1) / 2)
            else:
                return (K / u) * (S / K) ** ((u + 1) / 2)

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
        d1 = (D(np.log(S / K)) + (r + sigma ** 2 / 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        if is_call:
            option_price = S * D(norm.cdf(float(d1))) - K * np.exp(-r * T) * D(norm.cdf(float(d2)))
        else:
            option_price = K * np.exp(-r * T) * D(norm.cdf(-float(d2))) - S * D(norm.cdf(-float(d1)))

        return D(str(option_price))

    def publish_metrics(self):
        """
        Publish relevant metrics for the option pricer.
        """
        self.strategy._publish_strat_metric('implied_volatility', float(self.iv))
