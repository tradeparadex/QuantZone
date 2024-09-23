import datetime as dt
import logging
from decimal import Decimal as D
from typing import TYPE_CHECKING

import numpy as np
from utils.data_methods import PriceType

if TYPE_CHECKING:
    from start import PerpMarketMaker


class PortfolioMarginState:
    def __init__(self, position_long: D, position_short: D, unrealized_pnl: D, open_notional: D,
                 initial_margin: D, maintenance_margin: D, total_collateral: D,
                 collateral_excess_im: D, collateral_excess_mm: D, account_value: D,
                 leverage: D, margin_ratio: D):

        self.position_long = position_long
        self.position_short = position_short
        self.unrealized_pnl = unrealized_pnl
        self.open_notional = open_notional
        self.initial_margin = initial_margin
        self.maintenance_margin = maintenance_margin
        self.total_collateral = total_collateral
        self.collateral_excess_im = collateral_excess_im
        self.collateral_excess_mm = collateral_excess_mm
        self.account_value = account_value
        self.leverage = leverage
        self.margin_ratio = margin_ratio

    def to_json(self):
        return {
            "initial_margin": self.initial_margin,
            "maintenance_margin": self.maintenance_margin,
            "total_collateral": self.total_collateral,
            "collateral_excess_im": self.collateral_excess_im,
            "collateral_excess_mm": self.collateral_excess_mm,
            "account_value": self.account_value,
            "leverage": self.leverage,
            "margin_ratio": self.margin_ratio
        }

    def __str__(self) -> str:
        return f"PortfolioMarginState(initial_margin={self.initial_margin}, maintenance_margin={self.maintenance_margin}, " \
               f"total_collateral={self.total_collateral}, collateral_excess_im={self.collateral_excess_im}, " \
               f"collateral_excess_mm={self.collateral_excess_mm}, account_value={self.account_value}, " \
               f"leverage={self.leverage}, margin_ratio={self.margin_ratio})"

    def __repr__(self) -> str:
        return self.__str__()


class RiskManager:

    def __init__(self, parent: 'PerpMarketMaker') -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parent: 'PerpMarketMaker' = parent

        self.last_trade_fetched_ts = 0
        self.trades_list = []

        self.last_funding_fetched_ts = 0
        self.funding_list = []

        self.cached_portfolio_margin_state = None
        self.last_portfolio_margin_state_ts = 0

    @property
    async def portfolio_margin_state(self) -> PortfolioMarginState:
        if self.last_portfolio_margin_state_ts == 0 or self.parent.now_ms() - self.last_portfolio_margin_state_ts > 20 * 1000:
            self.cached_portfolio_margin_state = await self._get_portfolio_open_size()
            self.last_portfolio_margin_state_ts = self.parent.now_ms()
        
        return self.cached_portfolio_margin_state

    def is_system_health_ok(self) -> bool:
        if not self.parent.market_connector:
            return False
        
        if not self.parent.market_connector.orderbooks:
            return False
        
        if not self.parent.market_connector.account_info:
            return False
        
        if self.any_data_stale():
            return False

        return True
    

    def any_data_stale(self) -> bool:
        now_ms = self.parent.now_ms()

        oldest_book = np.min([ob.received_ts for ob in self.parent.market_connector.orderbooks.values()])/1e6
        last_account = self.parent.market_connector.account_info['updated_at']

        if self.parent.external_connector:
            oldest_external_book = np.min([ob.received_ts for ob in self.parent.external_connector.orderbooks.values()])/1e6
        else:
            oldest_external_book = now_ms

        if now_ms - oldest_external_book > self.parent.max_market_latency_ms:
            self.logger.info(f"External book too old: {now_ms - oldest_external_book}")
            return True

        if now_ms - oldest_book > self.parent.max_data_delay_ms:
            self.logger.info(f"Market book too old: {now_ms - oldest_book}")
            return True
        
        if now_ms - last_account > self.parent.max_data_delay_ms:
            self.logger.info(f"Account info too old: {now_ms - last_account}")
            return True
        
        market_latency = np.max([ob.received_ts-ob.exchange_ts for ob in self.parent.market_connector.orderbooks.values()])/1e6
        if self.parent.external_connector:
            external_latency = np.max([ob.received_ts-ob.exchange_ts for ob in self.parent.external_connector.orderbooks.values()])/1e6
        else:
            external_latency = 0
        
        if market_latency > self.parent.max_market_latency_ms:
            self.logger.info(f"Market latency too high: {market_latency}")
            for ob in self.parent.market_connector.orderbooks.values():
                self.logger.info(f"Market book: {ob.received_ts-ob.exchange_ts}, {dt.datetime.fromtimestamp(ob.received_ts/1e9)} {dt.datetime.fromtimestamp(ob.exchange_ts/1e9)}")
            return True
        
        if external_latency > self.parent.max_market_latency_ms:
            self.logger.info(f"External latency too high: {external_latency}")
            return True

        return False
        

    async def _get_portfolio_open_size(self) -> float:
        account_positions = self.parent.market_connector.positions

        positions = [D(p['cost_usd'])+D(p['unrealized_pnl']) for p in account_positions.values()]
        unrealized_pnl = sum([D(p['unrealized_funding_pnl'])+D(p['unrealized_pnl']) for p in account_positions.values()])
        position_long = max(D("0"), sum(positions))
        position_short = abs(min(D("0"), sum(positions)))
        open_notional = sum([abs(p) for p in positions])

        account_info = self.parent.market_connector.account_info

        initial_margin = D(account_info['initial_margin_requirement'])
        maintenance_margin = D(account_info['maintenance_margin_requirement'])
        total_collateral = D(account_info['total_collateral'])
        coll_excess_im = D(account_info['free_collateral'])
        coll_excess_mm = D(account_info['margin_cushion'])
        account_value = D(account_info['account_value'])

        leverage = open_notional / account_value
        margin_ratio = maintenance_margin / account_value

        return PortfolioMarginState(
            position_long=position_long,
            position_short=position_short,
            unrealized_pnl=unrealized_pnl,
            open_notional=open_notional,
            initial_margin=initial_margin, 
            maintenance_margin=maintenance_margin, 
            total_collateral=total_collateral, 
            collateral_excess_im=coll_excess_im, 
            collateral_excess_mm=coll_excess_mm, 
            account_value=account_value, 
            leverage=leverage, 
            margin_ratio=margin_ratio
        )

    async def can_quote_to_open(self) -> bool:
        tot_pos_usd = self.parent.get_active_position(self.parent.market) * self.parent.get_price_by_type(PriceType.Mid)
        order_size_usd = self.parent.order_amount_usd
        
        if abs(tot_pos_usd) + order_size_usd > self.parent.max_position_usd:
            self.logger.info(f"At max position: {tot_pos_usd} + {order_size_usd} > {self.parent.max_position_usd}")
            return False
        
        cur_leverage = (await self.portfolio_margin_state).leverage
        max_leverage = self.parent.max_leverage

        if cur_leverage >= max_leverage:
            self.logger.info(f"At max leverage: {cur_leverage} > {max_leverage}")
            return False

        cur_margin_ratio = (await self.portfolio_margin_state).margin_ratio
        max_margin_ratio = self.parent.max_margin_ratio

        if cur_margin_ratio >= max_margin_ratio:
            self.logger.info(f"At max margin ratio: {cur_margin_ratio} > {max_margin_ratio}")
            return False
        
        if self.any_data_stale():
            self.logger.info("Some data source stale")
            return False
        
        return True
    
    def should_pull(self) -> bool:
        return not self.is_system_health_ok()
