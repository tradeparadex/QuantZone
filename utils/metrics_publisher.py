"""
This module provides utilities for publishing metrics in a standardized format.

It includes classes for encoding decimal values, creating metric messages,
and publishing metrics to a stream.

Override to implement your own metrics publisher.
"""

import json
import logging
import os
import socket
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return f"{o:.4f}"
        return super().default(o)

class MetricsMessage:
    """
    A class representing a metrics message with various attributes.

    Attributes:
        timestamp (int): The timestamp of the message.
        process_name (str): The name of the process generating the message.
        tag_name (str): The tag name associated with the message.
        market (str): The market identifier.
        value (float): The value of the metric.
        account (str): The account identifier (optional).
    """
    def __init__(self, timestamp: int, process_name: str, tag_name: str, market: str, value: float, account: str = None):
        self.timestamp = timestamp
        self.process_name = process_name
        self.tag_name = tag_name
        self.market = market
        self.value = value
        self.account = account

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "process_name": self.process_name,
            "tag_name": self.tag_name,
            "market": self.market,
            "value": self.value,
            "account": self.account
        }

    def to_json(self):
        return json.dumps(self.to_dict(), cls=DecimalEncoder)

    def __str__(self):
        return self.to_json()

class MetricsPublisher:
    """
    A class for publishing metrics messages to a logging stream.

    Attributes:
        _logger (logging.Logger): The logger instance for publishing metrics.
    """
    _logger = None

    @classmethod
    def logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self):
        self.pid = os.getpid()
        self.hostname = socket.gethostname()

    def stream_metrics(self, m_msg: MetricsMessage):
        self.logger().info(f"Stream metrics: {m_msg}")
