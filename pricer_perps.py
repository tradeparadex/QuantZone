"""
This file contains the PerpPricer class, which is responsible for calculating
fair prices for perpetual futures contracts. It inherits from BasePricer and
uses various market data and strategy parameters to determine appropriate
pricing for buy and sell orders.
"""

import structlog
from decimal import Decimal as D
from strategy import BasePricer, RawFairPrice
from utils.data_methods import PriceType, Side

class PerpPricer(BasePricer):
    """
    A pricer for perpetual futures contracts.

    This class extends the BasePricer to provide specialized pricing
    calculations for perpetual futures. It takes into account various
    factors such as spot price, funding rates, and basis to determine
    fair prices for buy and sell orders.

    Attributes:
        strategy: The parent strategy object that contains market data
                  and configuration parameters.
        logger: A logging object for debug and error messages.

    """

    def __init__(self, strategy):
        super().__init__(strategy)
        self.logger = structlog.get_logger(self.__class__.__name__)

    def get_raw_fair_price(self, side: Side) -> RawFairPrice:
        """
        Calculate the raw fair price for a given side (buy or sell).

        Args:
            side (Side): The side of the order (buy or sell).

        Returns:
            RawFairPrice: The raw fair price for the given side.
        """
        price_type = PriceType.BestBid if side == Side.BUY else PriceType.BestAsk

        raw_spot = self.strategy.get_base_price(price_type)
        raw_spot_ema = self.strategy._smoothen_spot_price.value

        if raw_spot is None or not raw_spot.is_finite():
            if self.strategy.use_anchor_price:
                raw_spot = self.strategy.anchor_price
                raw_spot_ema = self.strategy.anchor_price
            else:
                return None

        self.logger.debug(f"raw_spot: {raw_spot}, raw_spot_ema: {raw_spot_ema}")
                
        if side == Side.SELL:
            fair = max(raw_spot, raw_spot_ema)
        elif side == Side.BUY:
            fair = min(raw_spot, raw_spot_ema)
        else:
            fair = raw_spot_ema

        capped_basis = self.cap_values(self.factored_basis, -D(0.05), D(0.05))
        capped_fr = self.cap_values(self.factored_fr, -D(0.05), D(0.05))

        fair += raw_spot_ema * (capped_basis - capped_fr)

        return RawFairPrice(fair=fair, base=raw_spot_ema)

    @property
    def factored_basis(self) -> D:
        """
        Calculate the factored basis value.

        Returns:
            Decimal: The factored basis value.
        """
        return self.strategy._smoothen_basis.value * self.strategy.pricing_basis_factor

    @property
    def factored_fr(self) -> D:
        """
        Calculate the factored funding rate value.

        Returns:
            Decimal: The factored funding rate value.
        """
        return self.strategy._smoothen_funding_rate.value * self.strategy.pricing_funding_rate_factor

    @staticmethod
    def cap_values(val: D, min_val: D, max_val: D) -> D:
        """
        Cap a value between a minimum and maximum.

        Args:
            val (Decimal): The value to cap.
            min_val (Decimal): The minimum value.
            max_val (Decimal): The maximum value.

        Returns:
            Decimal: The capped value.
        """
        return max(min(val, max_val), min_val)
    

    def publish_metrics(self):
        """
        Publish metrics for the strategy.
        """
        self.strategy._publish_strat_metric('base', self.strategy.get_base_price(PriceType.Mid))
        self.strategy._publish_strat_metric('base_ema', self.strategy._smoothen_spot_price.value)
        self.strategy._publish_strat_metric('basis_ema', self.strategy._smoothen_basis.value)
        self.strategy._publish_strat_metric('fr_ema', self.strategy._smoothen_funding_rate.value)
        self.strategy._publish_strat_metric('fr', self.strategy.get_inst_rate())
        self.strategy._publish_strat_metric('volatility_adj', self.strategy.get_vol_adjustment())
        self.strategy._publish_strat_metric('price_adjustment', self.strategy.price_adjustment)
        self.strategy._publish_strat_metric('spread_adj', self.strategy.ask_spread + self.strategy.bid_spread)
        self.strategy._publish_strat_metric('basis_adj', self.factored_basis)
        self.strategy._publish_strat_metric('fr_adj', -self.factored_fr)
