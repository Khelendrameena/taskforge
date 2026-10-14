"""Handler registry: maps a job's ``fn`` name to the Python callable.

Workers resolve ``job["fn"]`` through this registry, which keeps the queue
engine decoupled from application code::

    registry = Registry()

    @registry.register("send_email")
    def send_email(payload):
        ...

    worker = Worker(handlers=registry, ...)
"""

from __future__ import annotations

from typing import Callable, Dict

from .exceptions import HandlerNotFound

Handler = Callable[[dict], object]


class Registry:
    def __init__(self) -> None:
        self._handlers: Dict[str, Handler] = {}

    def register(self, name: str | None = None):
        """Decorator: ``@registry.register("my_job")`` or bare ``@registry.register``."""

        def decorator(fn: Handler) -> Handler:
            key = name or fn.__name__
            self._handlers[key] = fn
            return fn

        # Allow @registry.register without parentheses.
        if callable(name):
            fn = name  # type: ignore[assignment]
            name = None
            return decorator(fn)
        return decorator

    def get(self, name: str) -> Handler:
        try:
            return self._handlers[name]
        except KeyError:
            raise HandlerNotFound(f"no handler registered for {name!r}") from None

    def names(self) -> list[str]:
        return sorted(self._handlers)
