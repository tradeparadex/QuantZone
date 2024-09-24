import time
from threading import Lock

class SyncRateLimiter:
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.updated_at = time.monotonic()
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            while self.tokens < 1:
                self.add_new_tokens()
                time.sleep(0.1)
            self.tokens -= 1

    def add_new_tokens(self):
        now = time.monotonic()
        time_passed = now - self.updated_at
        new_tokens = time_passed * self.rate_limit
        if new_tokens > 1:
            self.tokens = min(self.tokens + new_tokens, self.rate_limit)
            self.updated_at = now