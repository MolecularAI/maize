"""Computational resource management"""

import logging
from multiprocessing import get_context
import os
import subprocess
import time
from typing import TYPE_CHECKING, TypeVar, Any

from maize.core.runtime import Status
from maize.utilities.execution import DEFAULT_CONTEXT

if TYPE_CHECKING:
    from maize.core.component import Component

T = TypeVar("T")


_ctx = get_context(DEFAULT_CONTEXT)


def gpu_count() -> int:
    """Finds the number of GPUs in the current system."""
    try:
        result = subprocess.run(
            "nvidia-smi -L | wc -l",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=True,
        )
        return len(result.stdout.decode().split("\n")) - 1
    except OSError:
        return 0


# https://stackoverflow.com/a/55423170/6257823
def cpu_count() -> int:
    """
    Finds the number of CPUs in the current system, while respecting
    the affinity to make it compatible with cluster management systems.

    """
    return len(os.sched_getaffinity(0))

# Only reference to something like this that I've found is:
# https://stackoverflow.com/questions/69366850/counting-semaphore-that-supports-multiple-acquires-in-an-atomic-call-how-would
class ChunkedSemaphore:
    """
    Semaphore allowing chunked atomic resource acquisition.

    Parameters
    ----------
    max_count
        Initial value representing the maximum resources
    sleep
        Time to wait in between semaphore checks

    """

    def __init__(self, max_count: int, sleep: int = 5) -> None:
        # FIXME temporary Any type hint until this gets solved
        # https://github.com/python/typeshed/issues/8799
        self._count: Any = _ctx.Value("i", max_count)
        self._max_count = max_count
        self.sleep = sleep

    def acquire(self, value: int) -> None:
        """
        Acquire resources.

        Parameters
        ----------
        value
            Amount to acquire

        Raises
        ------
        ValueError
            If the amount requested exceeds the available resources

        """
        if value > self._max_count:
            raise ValueError(f"Acquire ({value}) exceeds available resources ({self._max_count})")

        while True:
            with self._count.get_lock():
                if (self._count.value - value) >= 0:
                    self._count.value -= value
                    break

            # Sleep outside lock to make sure others can access
            time.sleep(self.sleep)

    def release(self, value: int) -> None:
        """
        Release resources.

        Parameters
        ----------
        value
            Amount to release

        Raises
        ------
        ValueError
            If the amount released exceeds the available resources

        """
        with self._count.get_lock():
            if (self._count.value + value) > self._max_count:
                raise ValueError(
                    f"Release ({value}) exceeds original resources ({self._max_count})"
                )
            self._count.value += value


class Resources:
    """
    Acquire computational resources in the form of a context manager.

    Parameters
    ----------
    max_count
        The maximum resource count
    parent
        Parent component object for status updates

    Examples
    --------
    >>> cpus = Resources(max_count=5, parent=graph)
    ... with cpus(3):
    ...     do_something(n_jobs=3)

    """

    def __init__(self, max_count: int, parent: "Component") -> None:
        self._sem = ChunkedSemaphore(max_count=max_count)
        self._val = 0
        self.parent = parent

    def __call__(self, value: int) -> "Resources":
        """
        Acquire resources.

        Parameters
        ----------
        value
            Amount to acquire

        """
        self._val = value
        return self

    def __enter__(self) -> "Resources":
        self.parent.status = Status.WAITING_FOR_RESOURCES
        self._sem.acquire(self._val)
        self.parent.status = Status.RUNNING
        return self

    def __exit__(self, *_: Any) -> None:
        self._sem.release(self._val)
        self._val = 0
