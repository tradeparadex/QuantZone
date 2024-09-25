"""
This module contains the RiskManager class for managing trading risks.

The RiskManager class is responsible for monitoring and controlling various
risk factors in trading operations, including position limits, volatility,
and margin requirements.
"""

import logging
import datetime as dt
import numpy as np
from decimal import Decimal as D
from typing import TYPE_CHECKING, List, Optional
from utils.data_methods import PriceType

if TYPE_CHECKING:
    from start import PerpMarketMaker


class PortfolioMarginState:
    """
    A class representing the margin state of a portfolio.
    """
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
    """
    A class for managing risk in trading operations.

    Override to implement your own risk manager.
    """
    def __init__(self, parent: 'PerpMarketMaker') -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parent: 'PerpMarketMaker' = parent

        self.cached_portfolio_margin_state: Optional[PortfolioMarginState] = None
        self.last_portfolio_margin_state_ts: int = 0

    @property
    async def portfolio_margin_state(self) -> PortfolioMarginState:
        if self.last_portfolio_margin_state_ts == 0 or self.parent.now_ms() - self.last_portfolio_margin_state_ts > 20 * 1000:
            self.cached_portfolio_margin_state = await self._get_portfolio_open_size()
            self.last_portfolio_margin_state_ts = self.parent.now_ms()
        
        return self.cached_portfolio_margin_state

    def is_system_health_ok(self) -> bool:
        # Check if the market connector is available
        if not self.parent.market_connector:
            return False
        
        # Check if the market connector has orderbooks
        if not self.parent.market_connector.orderbooks:
            return False
        
        # Check if the market connector has account info
        if not self.parent.market_connector.account_info:
            return False
        
        # Check if any data is stale or if there are latency issues
        if self.any_data_stale():
            return False

        return True
    

    def any_data_stale(self) -> bool:
        # Check if any data is stale or if there are latency issues
        # This method returns True if any data is considered stale or if latency is too high

        # Get current timestamp in milliseconds
        now_ms = self.parent.now_ms()

        # Get the oldest received timestamp from all orderbooks in the market connector
        oldest_book = np.min([ob.received_ts for ob in self.parent.market_connector.orderbooks.values()])/1e6

        # Get the last updated timestamp from the account info
        last_account = self.parent.market_connector.account_info['updated_at']

        # If there is an external connector, get the oldest received timestamp from all orderbooks in the external connector
        if self.parent.external_connector:
            oldest_external_book = np.min([ob.received_ts for ob in self.parent.external_connector.orderbooks.values()])/1e6
        else:
            oldest_external_book = now_ms

        # Check if the external book is too old
        if now_ms - oldest_external_book > self.parent.max_market_latency_ms:
            self.logger.info(f"External book too old: {now_ms - oldest_external_book}")
            return True

        # Check if the market book is too old
        if now_ms - oldest_book > self.parent.max_data_delay_ms:
            self.logger.info(f"Market book too old: {now_ms - oldest_book}")
            return True
        
        # Check if the account info is too old
        if now_ms - last_account > self.parent.max_data_delay_ms:
            self.logger.info(f"Account info too old: {now_ms - last_account}")
            return True
        
        # Calculate the maximum latency for market data
        market_latency = np.max([ob.received_ts-ob.exchange_ts for ob in self.parent.market_connector.orderbooks.values()])/1e6
        
        # If there is an external connector, calculate the maximum latency for external data
        if self.parent.external_connector:
            external_latency = np.max([ob.received_ts-ob.exchange_ts for ob in self.parent.external_connector.orderbooks.values()])/1e6
        else:
            external_latency = 0
        
        # Check if the market latency is too high
        if market_latency > self.parent.max_market_latency_ms:
            self.logger.info(f"Market latency too high: {market_latency}")
            # Log the latency for each market book
            for ob in self.parent.market_connector.orderbooks.values():
                self.logger.info(f"Market book: {ob.received_ts-ob.exchange_ts}, {dt.datetime.fromtimestamp(ob.received_ts/1e9)} {dt.datetime.fromtimestamp(ob.exchange_ts/1e9)}")
            return True
        
        # Check if the external latency is too high
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
        """
        Check if the portfolio can quote to open a position.
        """
        
        # Get the total position in USD (long or short)
        tot_pos_usd = self.parent.get_active_position(self.parent.market) * self.parent.get_price_by_type(PriceType.Mid)
        
        # Get the order size in USD
        order_size_usd = self.parent.order_amount_usd
        
        # Check if the total position plus the order size is greater than the max position
        if abs(tot_pos_usd) + order_size_usd > self.parent.max_position_usd:
            self.logger.info(f"At max position: {tot_pos_usd} + {order_size_usd} > {self.parent.max_position_usd}")
            return False
        
        # Get the current leverage
        cur_leverage = (await self.portfolio_margin_state).leverage
        
        # Get the max leverage
        max_leverage = self.parent.max_leverage

        # Check if the current leverage is greater than the max leverage
        if cur_leverage >= max_leverage:
            self.logger.info(f"At max leverage: {cur_leverage} > {max_leverage}")
            return False

        # Get the current margin ratio
        cur_margin_ratio = (await self.portfolio_margin_state).margin_ratio
        
        # Get the max margin ratio
        max_margin_ratio = self.parent.max_margin_ratio

        # Check if the current margin ratio is greater than the max margin ratio
        if cur_margin_ratio >= max_margin_ratio:
            self.logger.info(f"At max margin ratio: {cur_margin_ratio} > {max_margin_ratio}")
            return False
        
        # Check if any data is stale
        if self.any_data_stale():
            self.logger.info("Some data source stale")
            return False
        
        return True
    
    def should_pull(self) -> bool:
        """
        Check if the risk manager should pull all quotes from the market.
        """
        return not self.is_system_health_ok()
