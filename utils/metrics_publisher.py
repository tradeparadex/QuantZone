import json
import logging
import os
import socket

class MetricsMessage:
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
        return json.dumps(self.to_dict())

    def __str__(self):
        return self.to_json()

class MetricsPublisher:

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
