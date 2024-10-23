"""
Run
---
Graph running infrastructure enabling parallel execution of nodes.

"""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Iterable
from dataclasses import dataclass
import datetime
from enum import auto
import functools
import itertools
import logging
from logging.handlers import QueueHandler
from multiprocessing import get_context
import os
from pathlib import Path
import signal
import sys
import time
from traceback import TracebackException
from typing import TYPE_CHECKING, Any

from maize.utilities.utilities import StrEnum
from maize.utilities.execution import DEFAULT_CONTEXT, JobHandler

if TYPE_CHECKING:
    from logging import _FormatStyle
    from multiprocessing import Process, Queue
    from multiprocessing.context import ForkProcess, ForkServerProcess, SpawnProcess
    from multiprocessing.synchronize import Event as EventClass
    from maize.core.component import Component
    from typing import TextIO


_RESET = "\x1b[0m"
_GREEN = "\x1b[1;32m"
_YELLOW = "\x1b[1;33m"
_RED = "\x1b[1;31m"
_PURPLE = "\x1b[1;35m"
_BLUE = "\x1b[1;34m"
_LIGHT_BLUE = "\x1b[1;36m"


MAIZE_ISO = r"""
              ___           ___                       ___           ___
             /\__\         /\  \          ___        /\  \         /\  \
            /::|  |       /::\  \        /\  \       \:\  \       /::\  \
           /:|:|  |      /:/\:\  \       \:\  \       \:\  \     /:/\:\  \
          /:/|:|__|__   /::\~\:\  \      /::\__\       \:\  \   /::\~\:\  \
         /:/ |::::\__\ /:/\:\ \:\__\  __/:/\/__/ _______\:\__\ /:/\:\ \:\__\
         \/__/~~/:/  / \/__\:\/:/  / /\/:/  /    \::::::::/__/ \:\~\:\ \/__/
               /:/  /       \::/  /  \::/__/      \:\~~\~~      \:\ \:\__\
              /:/  /        /:/  /    \:\__\       \:\  \        \:\ \/__/
             /:/  /        /:/  /      \/__/        \:\__\        \:\__\
             \/__/         \/__/                     \/__/         \/__/

"""


log = logging.getLogger("build")


# ================================
# Node execution utilities
# ================================


class NodeException(Exception):
    """
    Exception representing execution failure in a node.
    Should not be instantiated directly, but by providing
    a traceback and using the `from_traceback_exception` method.

    """

    @classmethod
    def from_traceback_exception(cls, tbe: TracebackException) -> "NodeException":
        """Create an exception from a `TracebackException` instance."""
        return cls("\n\nOriginal traceback:\n" + "".join(tbe.format()))


class Runnable(ABC):
    """Represents an object that can be run as a separate process."""

    name: str
    signal: "EventClass"
    n_signals: int

    @abstractmethod
    def execute(self) -> None:
        """Main execution method, run in a separate process."""

    @abstractmethod
    def cleanup(self) -> None:
        """Method run on component shutdown in the main process."""


class RunPool:
    """
    Context manager for running a pool of processes.

    Upon entering, will start one process for each item
    with the `execute` method as the target. Exiting
    will cause all processes to be joined within
    `wait_time`, and any remaining ones will be terminated.

    Parameters
    ----------
    items
        Items of type `Runnable`
    wait_time
        Time to wait when joining processes before issuing a termination signal

    """

    def __init__(self, *items: Runnable, wait_time: float = 1) -> None:
        self.items = list(items)
        self.wait_time = wait_time
        self._procs: list["Process" | "SpawnProcess" | "ForkProcess" | "ForkServerProcess"] = []

    def __enter__(self) -> "RunPool":
        ctx = get_context(DEFAULT_CONTEXT)
        current_exec = sys.executable
        for item in self.items:
            if hasattr(item, "python"):
                exec_path = item.python.value.as_posix()
                log.debug("Setting executable for '%s' to '%s'", item.name, exec_path)
                ctx.set_executable(exec_path)
            else:
                ctx.set_executable(current_exec)
            proc = ctx.Process(target=item.execute, name=item.name)  # type: ignore
            log.debug("Launching '%s'", item.name)
            proc.start()
            self._procs.append(proc)
        ctx.set_executable(current_exec)
        return self

    def __exit__(self, *_: Any) -> None:
        # Flush all loggers no matter what happened in the workflow
        for hand in log.handlers:
            hand.flush()

        end_time = time.time() + self.wait_time
        for item, proc in zip(self.items, self._procs, strict=True):
            item.cleanup()
            join_time = max(0, min(end_time - time.time(), self.wait_time))
            proc.join(join_time)
            log.debug(
                "Joined runnable '%s' (PID %s) with exitcode %s", item.name, proc.pid, proc.exitcode
            )

        while self._procs:
            proc = self._procs.pop()
            if proc.is_alive():  # pragma: no cover
                # FIXME this is drastic, not sure what side effects this might have.
                # Calling `proc.terminate()` seems to, when running a larger number
                # of processes in serial (such as in pytest), cause deadlocks. This
                # might be due to queues being accessed, although all calls to queues
                # are now non-blocking.
                proc.kill()
                log.debug(
                    "Killed '%s' (PID %s) with exitcode %s", proc.name, proc.pid, proc.exitcode
                )

    @property
    def n_processes(self) -> int:
        """Returns the total number of processes."""
        return len(self._procs)


# ================================
# Signal handling
# ================================


MAX_TERM_SIGNALS = 1


def _default_signal_handler(
    signal_object: Runnable, exception_type: type[Exception], *_: Any
) -> None:  # pragma: no cover
    signal_object.n_signals += 1
    signal_object.signal.set()

    JobHandler().cancel_all()

    if signal_object.n_signals == MAX_TERM_SIGNALS:
        raise exception_type()


def init_signal(signal_object: Runnable) -> None:
    """
    Initializes the signal handler for interrupts.

    Parameters
    ----------
    signal_object
        Object with an `n_signals` attribute

    """
    int_handler = functools.partial(_default_signal_handler, signal_object, KeyboardInterrupt)
    signal.signal(signal.SIGINT, int_handler)
    signal.signal(signal.SIGTERM, int_handler)
    signal.siginterrupt(signal.SIGINT, False)
    signal.siginterrupt(signal.SIGTERM, False)


# ================================
# Status handling
# ================================


class Status(StrEnum):
    """Component run status."""

    NOT_READY = auto()
    """Not ready / not initialized"""

    READY = auto()
    """Ready to run"""

    RUNNING = auto()
    """Currently running with no IO interaction"""

    COMPLETED = auto()
    """Successfully completed everything"""

    FAILED = auto()
    """Failed via exception"""

    STOPPED = auto()
    """Stopped because channels closed, or other components completed"""

    WAITING_FOR_INPUT = auto()
    """Waiting for task input"""

    WAITING_FOR_OUTPUT = auto()
    """Waiting for task output (backpressure due to full queue)"""

    WAITING_FOR_RESOURCES = auto()
    """Waiting for compute resources (blocked by semaphore)"""

    WAITING_FOR_COMMAND = auto()
    """Waiting for an external command to complete"""


_STATUS_COLORS = {
    Status.NOT_READY: _RED,
    Status.READY: _YELLOW,
    Status.RUNNING: _BLUE,
    Status.COMPLETED: _GREEN,
    Status.FAILED: _RED,
    Status.STOPPED: _GREEN,
    Status.WAITING_FOR_INPUT: _YELLOW,
    Status.WAITING_FOR_OUTPUT: _YELLOW,
    Status.WAITING_FOR_RESOURCES: _PURPLE,
    Status.WAITING_FOR_COMMAND: _LIGHT_BLUE,
}


class StatusHandler:
    """Descriptor logging any component status updates."""

    public_name: str
    private_name: str

    def __set_name__(self, owner: type["Component"], name: str) -> None:
        self.public_name = name
        self.private_name = "_" + name

    def __get__(self, obj: "Component", objtype: Any = None) -> Any:
        return getattr(obj, self.private_name)

    def __set__(self, obj: "Component", value: Status) -> None:
        setattr(obj, self.private_name, value)

        if hasattr(obj, "logger"):
            obj.logger.debug("Status changed to %s", value.name)

        # We start and pause the timer here depending on whether
        # the node is actually doing computation (RUNNING) or
        # just blocked (WAITING)
        if hasattr(obj, "run_timer"):
            # Completed nodes get a separate explicit update during shutdown
            if value not in (Status.COMPLETED, Status.FAILED, Status.STOPPED):
                obj.send_update()
            if value in (Status.RUNNING, Status.WAITING_FOR_COMMAND):
                obj.run_timer.start()
            elif obj.run_timer.running:
                obj.run_timer.pause()


@dataclass
class StatusUpdate:
    """
    Summarizes the node status at completion of execution.

    Attributes
    ----------
    name
        Name of the node
    parents
        Names of all parents
    status
        Node status at completion, will be one of ('FAILED', 'STOPPED', 'COMPLETED')
    run_time
        Time spent in status 'RUNNING'
    full_time
        Time spent for the full node execution, including 'WAITING' for others
    n_inbound
        Number of items waiting to be received
    n_outbound
        Number of items waiting to be sent
    note
        Additional message to be printed at completion
    exception
        Exception if status is 'FAILED'

    """

    name: str
    parents: tuple[str, ...]
    status: Status
    run_time: datetime.timedelta = datetime.timedelta(seconds=0)
    full_time: datetime.timedelta = datetime.timedelta(seconds=0)
    n_inbound: int = 0
    n_outbound: int = 0
    note: str | None = None
    exception: TracebackException | None = None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, StatusUpdate):
            return (
                self.name,
                self.parents,
                self.status,
                self.n_inbound,
                self.n_outbound,
                self.note,
                self.exception,
            ) == (
                other.name,
                other.parents,
                other.status,
                other.n_inbound,
                other.n_outbound,
                other.note,
                other.exception,
            )
        return NotImplemented


def format_summaries(summaries: list[StatusUpdate], runtime: datetime.timedelta) -> str:
    """
    Create a string containing interesting information from all execution summaries.

    Parameters
    ----------
    summaries
        List of summaries to be aggregated and formatted
    runtime
        The total runtime to format

    Returns
    -------
    str
        Formatted summaries

    """
    n_success = sum(s.status == Status.COMPLETED for s in summaries)
    n_stopped = sum(s.status == Status.STOPPED for s in summaries)
    n_failed = sum(s.status == Status.FAILED for s in summaries)
    wall_time: datetime.timedelta = sum(
        (s.full_time for s in summaries), start=datetime.timedelta(seconds=0)
    )
    blocked_time: datetime.timedelta = sum(
        (s.full_time - s.run_time for s in summaries), start=datetime.timedelta(seconds=0)
    )
    smilie = ":(" if n_failed > 0 else ":)"
    msg = f"Execution completed {smilie} total runtime: {runtime}"
    msg += f"\n\t{n_success} nodes completed successfully"
    msg += f"\n\t{n_stopped} nodes stopped due to closing ports"
    msg += f"\n\t{n_failed} nodes failed"
    msg += f"\n\t{wall_time} total walltime"
    msg += f"\n\t{blocked_time} spent waiting for resources or other nodes"

    return msg


def _item_count(items: Iterable[Any], target: Any) -> int:
    count = 0
    for item in items:
        count += target == item
    return count


def format_update(summaries: dict[tuple[str, ...], StatusUpdate], color: bool = True) -> str:
    """
    Create a string containing a summary of waiting nodes.

    Parameters
    ----------
    summaries
        Dictionary of `StatusUpdate`
    color
        Whether to add color to the update

    Returns
    -------
    str
        A formatted string with information

    """
    msg = "Workflow status"
    all_names = [path[-1] for path in summaries]
    for (*_, name), node_sum in summaries.items():
        if _item_count(all_names, name) > 1:
            name = "-".join(node_sum.parents)

        if color:
            stat_color = _STATUS_COLORS[node_sum.status]
            msg += f"\n{'':>34} | {name:>16} | {stat_color}{node_sum.status.name}{_RESET}"
        else:
            msg += f"\n{'':>34} | {name:>16} | {node_sum.status.name}"

        # Show queued item information, but only if the node is running
        # (otherwise this value will never be updated)
        if (node_sum.n_inbound > 0 or node_sum.n_outbound > 0) and node_sum.status not in (
            Status.COMPLETED,
            Status.FAILED,
            Status.STOPPED,
        ):
            msg += f" ({node_sum.n_inbound} | {node_sum.n_outbound})"
    return msg


class Spinner:
    """Provides an indication that the workflow is currently running"""
    def __init__(self, interval: int) -> None:
        self.icon = list("|/-\\")
        self.cycler = itertools.cycle(self.icon)
        self.update_time = datetime.datetime.now()
        self.interval = interval

    def __call__(self) -> None:
        if (datetime.datetime.now() - self.update_time).seconds > self.interval:
            print(next(self.cycler), end="\r")
            self.update_time = datetime.datetime.now()


# ================================
# Logging
# ================================


LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(comp_name)16s | %(message)s"
_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
VALID_LEVELS = {getattr(logging, name) for name in _LEVELS} | _LEVELS


class CustomFormat(logging.Formatter):
    """Custom formatter to show colors in log messages"""

    LOG_FORMAT = "%(asctime)s | {color}%(levelname)8s{reset} | %(comp_name)16s | %(message)s"

    formats = {
        logging.DEBUG: LOG_FORMAT.format(color=_LIGHT_BLUE, reset=_RESET),
        logging.INFO: LOG_FORMAT.format(color=_BLUE, reset=_RESET),
        logging.WARNING: LOG_FORMAT.format(color=_YELLOW, reset=_RESET),
        logging.ERROR: LOG_FORMAT.format(color=_RED, reset=_RESET),
        logging.CRITICAL: LOG_FORMAT.format(color=_RED, reset=_RESET),
    }

    formats_no_color = {k: LOG_FORMAT.format(color="", reset="") for k in formats}

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: "_FormatStyle" = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, Any] | None = None,
        color: bool = True,
    ) -> None:
        self.defaults = defaults
        self.color = color
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)

    def format(self, record: logging.LogRecord) -> str:
        if self.color:
            format_string = self.formats.get(record.levelno)
        else:
            format_string = self.formats_no_color.get(record.levelno)
        formatter = logging.Formatter(format_string, defaults=self.defaults)
        return formatter.format(record)


def setup_build_logging(
    name: str, level: str | int = logging.INFO, file: Path | None = None
) -> logging.Logger:
    """
    Sets up the build-time logging functionality, running in the main process.

    Parameters
    ----------
    name
        Name of the component being logged
    level
        Logging level
    file
        File to log to

    Returns
    -------
    logging.Logger
        Logger customized to a specific component

    """
    logger = logging.getLogger("build")

    # If we don't do this check we might get duplicate build log messages
    if logger.hasHandlers():
        logger.handlers.clear()

    # When logging to a file we don't want to add the ASCII color codes
    handler: logging.FileHandler | "logging.StreamHandler[TextIO]"
    handler = logging.FileHandler(file) if file is not None else logging.StreamHandler()
    stream_formatter = CustomFormat(defaults=dict(comp_name=name), color=file is None)
    handler.setFormatter(stream_formatter)
    logger.addHandler(handler)
    if level not in VALID_LEVELS:
        raise ValueError(f"Logging level '{level}' is not valid. Valid levels: {_LEVELS}")
    logger.setLevel(level)
    return logger


def setup_node_logging(
    name: str,
    logging_queue: "Queue[logging.LogRecord | None]",
    level: str | int = logging.INFO,
    color: bool = True,
) -> logging.Logger:
    """
    Sets up the node logging functionality, running in a child process.

    Parameters
    ----------
    name
        Name of the component being logged
    logging_queue
        Global messaging queue
    level
        Logging level
    color
        Whether to log in color

    Returns
    -------
    logging.Logger
        Logger customized to a specific component

    """
    stream_formatter = CustomFormat(defaults=dict(comp_name=name), color=color)
    logger = logging.getLogger(f"run-{os.getpid()}")
    if logger.hasHandlers():
        logger.handlers.clear()
    handler = QueueHandler(logging_queue)
    handler.setFormatter(stream_formatter)
    logger.addHandler(handler)

    if level not in VALID_LEVELS:
        raise ValueError(f"Logging level '{level}' is not valid. Valid levels: {_LEVELS}")
    logger.setLevel(level)
    return logger


class Logger(Runnable):
    """
    Logging node for all nodes in the graph.

    Parameters
    ----------
    message_queue
        Queue to send logging messages to
    name
        Name of the logger
    level
        Logging level
    file
        File to log to

    """

    name: str

    def __init__(
        self,
        message_queue: "Queue[logging.LogRecord | None]",
        name: str | None = None,
        level: int = logging.INFO,
        file: Path | None = None,
    ) -> None:
        self.name = name if name is not None else self.__class__.__name__
        self.queue = message_queue
        self.level = level
        self.file = file

    def execute(self) -> None:
        """Main logging process, receiving messages from a global queue."""
        logger = logging.getLogger("run")
        handler: logging.FileHandler | "logging.StreamHandler[TextIO]"
        handler = logging.StreamHandler() if self.file is None else logging.FileHandler(self.file)
        logger.addHandler(handler)
        logger.setLevel(self.level)

        while True:
            message = self.queue.get()

            # Sentinel to quit the logging process, means we are stopping the graph
            if message is None:
                break

            logger.handle(message)

        for hand in logger.handlers:
            hand.flush()

        # Clear all items so the process can shut down normally
        self.queue.cancel_join_thread()

    def cleanup(self) -> None:
        self.queue.put(None)
