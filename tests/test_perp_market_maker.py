import pytest
from decimal import Decimal as D
from strategy import PerpMarketMaker, Proposal, PriceSize, Side, PriceType, RawFairPrice

class MockPricer:
    def __init__(self, strategy):
        self.strategy = strategy
        self.fair_price = D('100')

    def get_raw_fair_price(self, side):
        return RawFairPrice(self.fair_price, self.fair_price)

    def publish_metrics(self):
        pass

class MockRollingVol:
    def get_value(self):
        return D('0.01')


@pytest.fixture
def strategy():
    strategy = PerpMarketMaker(loop=None, PricerClass=MockPricer)
    strategy.market = "BTC-PERP-USD"
    strategy._params_manager = MockParamsManager()
    strategy.market_connector = MockConnector()
    strategy._rolling_vol = MockRollingVol()
    return strategy

class MockParamsManager:
    def get_param_value(self, param):
        params = {
            "order_level_spread": D('2'),
            "order_level_amount_pct": D('20'),
            "buy_levels": 3,
            "sell_levels": 3,
            "order_amount_usd": D('1000'),
            "minimum_spread": D('0'),
            "bulk_requests": True,
            "base_volatility": D('0.01'),
            "pos_lean_bps_per_100k_usd": D('0'),
            "global_pos_lean_bps_per_100k_usd": D('0'),
            "volatility_exponent": D('1'),
            "volatility_cap": D('1'),
            "pricing_volatility_factor": D('0.001'),
            "order_refresh_tolerance_pct": D('1'),
        }
        return params.get(param, D('0'))

class MockConnector:
    def __init__(self):
        self.orderbooks = {"BTC-PERP-USD": MockOrderbook()}
        self.trading_rules = {"BTC-PERP-USD": MockTradingRules()}
        self.positions = {"BTC-PERP-USD": {"size": D('0'), "unrealized_pnl": D('0'), "unrealized_funding_pnl": D('0'), "cost_usd": D('0')}}

    def quantize_order_price(self, market, price):
        return round(price, 1)
    
    def quantize_order_amount(self, market, size):
        return round(size, 1)

class MockOrderbook:
    def get_best_bid(self):
        return D('99.5')

    def get_best_ask(self):
        return D('100.5')

    def get_mid(self):
        return D('100')

class MockTradingRules:
    min_price_increment = D('0.1')
    min_notional_size = D('1')

def test_create_base_proposal(strategy):
    proposal = strategy.create_base_proposal()
    assert isinstance(proposal, Proposal)
    assert len(proposal.buys) == 3
    assert len(proposal.sells) == 3
    assert proposal.buys[0].price < proposal.sells[0].price

def test_filter_out_takers(strategy):
    proposal = Proposal(
        buys=[PriceSize(D('99'), D('1'))],
        sells=[PriceSize(D('99.1'), D('1'))]   # This is below the best bid
    )
    strategy.filter_out_takers(proposal)
    assert proposal.buys[0].price == D('99')
    assert proposal.sells[0].price <= D('99.5')

def test_get_fair_price(strategy):
    fair_price_buy = strategy.get_fair_price(Side.BUY)
    fair_price_sell = strategy.get_fair_price(Side.SELL)
    assert fair_price_buy < fair_price_sell

def test_outside_tolerance(strategy):
    current_prices = [D('100'), D('99'), D('98')]
    proposal_prices = [D('101'), D('100'), D('99')]

    deviated, within_tolerance = strategy.outside_tolerance(current_prices, proposal_prices)
    assert deviated == [0]
    assert within_tolerance == [1, 2]

# Add more tests as needed