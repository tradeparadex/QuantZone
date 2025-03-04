from decimal import Decimal as D


class ExponentialMovingAverage:
    """
    A class for calculating an exponential moving average (EMA).

    This class maintains a single value and updates it using an exponential decay
    based on a half-life specified in milliseconds. It provides methods to update
    the EMA value and retrieve the current EMA value, applying decay based on the
    current timestamp.

    Attributes:
        value (Decimal): The current EMA value.
        timestamp (float): The timestamp when the EMA value was last updated.
        half_life (Decimal): The half-life in milliseconds.
        lambda_ (Decimal): The decay constant based on the half-life.
        decay_on_read (bool): Whether to decay the EMA value on read.
    """

    value: D
    timestamp: float
    half_life: D
    lambda_: D

    def __init__(self, half_life_ms: D, decay_on_read: bool = False, init_val: D = D(0)):
        self.value = init_val
        # TODO: think about setting .timestamp as when init_val is accured.
        self.timestamp = 0.0
        self.half_life = D(half_life_ms)  # half-life in ms
        self.lambda_ = D.ln(D("2")) / self.half_life  # decay constant based on half-life
        self.decay_on_read = decay_on_read

    def decay(self, current_timestamp: float):
        """Update the EMA value based on the time decay."""
        time_difference = D(current_timestamp - self.timestamp)
        decay_factor = D.exp(-self.lambda_ * time_difference)
        self.value *= decay_factor
        self.timestamp = current_timestamp

    def update(self, new_value: D, new_timestamp: float):
        """Update the EMA with a new value at a new timestamp."""
        if self.value is None:
            self.value = D(new_value)
        time_difference = D(new_timestamp - self.timestamp)
        decay_factor = D.exp(-self.lambda_ * time_difference)
        self.value = (1 - decay_factor) * D(new_value) + decay_factor * self.value
        self.timestamp = new_timestamp

    def get_value(self, current_timestamp: float):
        """Return the current EMA value, applying decay based on the current timestamp."""
        if self.decay_on_read:
            self.decay(current_timestamp)
        # TODO:
        # self.corrected_value = self.value / (1 - e^{ log(2) / half_life_ms * time_difference_from_initial_update})
        # return self.corrected_value
        return self.value
