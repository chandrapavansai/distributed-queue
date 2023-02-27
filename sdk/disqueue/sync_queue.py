import threading
from datetime import datetime


class SyncQueue:
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = []
        self._last_added = datetime.now()

    def put(self, item):
        now = datetime.now()
        self._lock.acquire()
        self._queue.append(item)
        self._last_added = max(now, self._last_added)
        self._lock.release()

    def get(self):
        self._lock.acquire()
        if len(self._queue) > 0:
            item = self._queue.pop(0)
            self._lock.release()
            return item
        self._lock.release()
        return None

    @property
    def last_added(self) -> datetime:
        self._lock.acquire()
        ans = self._last_added
        self._lock.release()
        return ans

    @property
    def empty(self) -> bool:
        self._lock.acquire()
        ans = len(self._queue) == 0
        self._lock.release()
        return ans

    def __len__(self) -> int:
        self._lock.acquire()
        ans = len(self._queue)
        self._lock.release()
        return ans
