"""
This module provides utility functions and classes for API such as rate limiting.
It includes a thread-safe rate limiter to control the rate of API requests.
"""

import time
from threading import Lock

class SyncRateLimiter:
    """
    A thread-safe rate limiter to control the rate of API requests.
    """
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.updated_at = time.monotonic()
        self.lock = Lock()

    def acquire(self):
        """
        Acquires a token from the rate limiter.
        """
        with self.lock:
            while self.tokens < 1:
                self.add_new_tokens()
                time.sleep(0.00001)
            self.tokens -= 1

    def add_new_tokens(self):
        """
        Adds new tokens based on the time passed since the last update.
        """
        now = time.monotonic()
        time_passed = now - self.updated_at
        new_tokens = time_passed * self.rate_limit
        if new_tokens > 1:
            self.tokens = min(self.tokens + new_tokens, self.rate_limit)
            self.updated_at = now