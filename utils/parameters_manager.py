"""
This module provides utilities for managing parameters in a configuration file.

It includes classes for defining parameters, parsing their values, and managing them.
"""
import structlog
import os
from typing import Any, Dict, List, Optional

class Param:
    def __init__(self, name: str, value: Any, val_type: Any = str) -> None:
        self.tag = name
        self.val_type = val_type
        self.value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._value = self._parse_value(new_value, self.val_type)

    @staticmethod
    def _parse_value(value: str, value_type: Any):
        if value_type == bool:
            return str(value).lower() == "true"
        return value_type(value)

class ParamsManager:

    _logger = None

    @classmethod
    def logger(cls):
        if cls._logger is None:
            cls._logger = structlog.get_logger(__name__)
        return cls._logger


    def __init__(self, parent, params: List[Param], config: dict, on_param_update: Optional[callable] = None):
        self.parent = parent
        self.params = {
            param.tag: param for param in params
        }

        self.config = config
        # Casting and Overriding default from ENV
        for param in params:
            env_key = f"ALGO_PARAMS_{param.tag.upper()}"
            # Order of priority: env > config > default
            value = os.getenv(env_key, self.config.get(env_key, param.value))
            param.value = value

        self.on_param_update = on_param_update

    def get_param_value(self, tag: str):
        return self.params[tag].value

    def start(self):
        pass

    def stop(self):
        pass

    def publish(self, param: Param):
        self.logger().info(f"Param: {param.tag} with value: {param.value}")

    def publish_state(self):
        for param in self.params.values():
            self.publish(param)