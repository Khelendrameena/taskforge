"""TaskForge exception hierarchy."""


class TaskForgeError(Exception):
    """Base class for all TaskForge errors."""


class HandlerNotFound(TaskForgeError):
    """No handler is registered for the job's function name."""


class LeaseLost(TaskForgeError):
    """The worker's lease on a job expired (or was taken) before ack/fail.

    This is not necessarily a bug: it means the job was requeued by the
    lease sweeper and another worker owns it now. The correct response is
    to drop our copy, not to ack it.
    """


class RateLimited(TaskForgeError):
    """A tenant's token bucket is empty; the job was deferred, not dropped."""
