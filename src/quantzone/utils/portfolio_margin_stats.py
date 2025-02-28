from dataclasses import dataclass
from decimal import Decimal as D

# from strategy import PerpMarketMaker


@dataclass
class PortfolioMarginState:
    """
    A class representing the margin state of a portfolio.
    """

    position_long: D
    position_short: D
    unrealized_pnl: D
    open_notional: D
    initial_margin: D
    maintenance_margin: D
    total_collateral: D
    collateral_excess_im: D
    collateral_excess_mm: D
    account_value: D
    leverage: D
    margin_ratio: D

    def to_json(self):
        return {
            "initial_margin": self.initial_margin,
            "maintenance_margin": self.maintenance_margin,
            "total_collateral": self.total_collateral,
            "collateral_excess_im": self.collateral_excess_im,
            "collateral_excess_mm": self.collateral_excess_mm,
            "account_value": self.account_value,
            "leverage": self.leverage,
            "margin_ratio": self.margin_ratio,
        }

    def __str__(self) -> str:
        return (
            f"PortfolioMarginState(initial_margin={self.initial_margin}, maintenance_margin={self.maintenance_margin}, "
            f"total_collateral={self.total_collateral}, collateral_excess_im={self.collateral_excess_im}, "
            f"collateral_excess_mm={self.collateral_excess_mm}, account_value={self.account_value}, "
            f"leverage={self.leverage}, margin_ratio={self.margin_ratio})"
        )

    def __repr__(self) -> str:
        return self.__str__()
