from abc import ABC, abstractmethod

from ..utils.data_methods import Side
from .raw_fair_price import RawFairPrice


class PricerBase(ABC):
    """
    Abstract base class for pricing logic.

    This class defines the interface for pricing logic, which must be implemented
    by concrete subclasses.

    Attributes:
        strategy: The parent strategy instance.
    """

    def __init__(self, strategy):
        self.strategy = strategy

    @abstractmethod
    def get_raw_fair_price(self, side: Side) -> RawFairPrice: ...

    @abstractmethod
    def publish_metrics(self): ...
