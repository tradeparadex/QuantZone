import logging
from decimal import Decimal as D
from strategy import BasePricer, RawFairPrice
from utils.data_methods import PriceType

class PerpPricer(BasePricer):

    def __init__(self, strategy):
        super().__init__(strategy)
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_raw_fair_price(self, price_type: PriceType) -> RawFairPrice:
        
        raw_spot = self.strategy.get_base_price(price_type)
        raw_spot_ema = self.strategy._smoothen_spot_price.value
        self.logger.debug(f"raw_spot: {raw_spot:.6f}, raw_spot_ema: {raw_spot_ema:.6f}")
                
        if price_type == PriceType.BestAsk:
            fair = max(raw_spot, raw_spot_ema)
        elif price_type == PriceType.BestBid:
            fair = min(raw_spot, raw_spot_ema)
        else:
            raise ValueError(f"Invalid price type: {price_type}")

        capped_basis = self.cap_values(self.factored_basis, -D(0.05), D(0.05))
        capped_fr = self.cap_values(self.factored_fr, -D(0.05), D(0.05))

        fair += raw_spot_ema * (capped_basis - capped_fr)

        return RawFairPrice(fair=fair, base=raw_spot_ema)

    @property
    def factored_basis(self):
        return self.strategy._smoothen_basis.value * self.strategy.pricing_basis_factor

    @property
    def factored_fr(self):
        return self.strategy._smoothen_funding_rate.value * self.strategy.pricing_funding_rate_factor

    @staticmethod
    def cap_values(val: D, min_val: D, max_val: D) -> D:
        return max(min(val, max_val), min_val)
    

    def publish_metrics(self):
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
