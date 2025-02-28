from dataclasses import dataclass
from decimal import Decimal as D


@dataclass
class RawFairPrice:
    """
    Represents a raw fair price with base value.

    This class encapsulates a fair price and its corresponding base value,
    which are used in pricing calculations for the market making strategy.

    Attributes:
        fair (Decimal): The fair price value.
        base (Decimal): The base value associated with the fair price.
    """

    fair: D
    base: D
