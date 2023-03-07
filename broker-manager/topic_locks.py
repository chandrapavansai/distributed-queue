import asyncio
from collections import defaultdict

_locks = defaultdict(asyncio.Lock)


def get_lock(topic):
    return _locks[topic]
