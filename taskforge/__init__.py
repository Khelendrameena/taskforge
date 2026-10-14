"""TaskForge: a distributed task queue built from scratch on Redis.

Producer::

    from taskforge import TaskForge
    tf = TaskForge("redis://localhost:6379/0")
    res = tf.enqueue("send_email", {"to": "a@x.com"}, idempotency_key="welcome-a@x.com")

Worker::

    from taskforge import TaskForge, Worker, Registry
    registry = Registry()

    @registry.register("send_email")
    def send_email(payload): ...

    Worker(TaskForge("redis://localhost:6379/0"), handlers=registry).run()
"""

from .client import EnqueueResult, TaskForge
from .config import Config
from .exceptions import HandlerNotFound, LeaseLost, RateLimited, TaskForgeError
from .handlers import Registry
from .worker import Worker

__version__ = "0.1.0"

__all__ = [
    "TaskForge",
    "EnqueueResult",
    "Worker",
    "Registry",
    "Config",
    "TaskForgeError",
    "HandlerNotFound",
    "LeaseLost",
    "RateLimited",
    "__version__",
]
