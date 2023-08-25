"""
Channels
--------
Communication channels used for passing files and data between components.

The two main channel types of interest to users are `DataChannel` and `FileChannel`.
The former is a wrapper around `multiprocessing.Queue`, while the latter uses
channel-specific directories as a shared space to transmit files. All channels
are unidirectional - a connected node cannot send and receive to the same channel.
Either can be assigned to a `Port` using the `interface.Port.set_channel` method.

Custom channels are possible and should subclass from the `Channel` abstract base class.
This will also require modifying the `graph.Graph.connect` method to use this new channel.

"""

from abc import ABC, abstractmethod
from multiprocessing import get_context
from pathlib import Path
import queue
import shutil
from tempfile import mkdtemp
import time
from typing import TYPE_CHECKING, Literal, TypeVar, Generic, Any, cast

import dill
from maize.utilities.execution import DEFAULT_CONTEXT
from maize.utilities.io import common_parent, sendtree
from maize.utilities.utilities import has_file

if TYPE_CHECKING:
    from multiprocessing import Queue

DEFAULT_TIMEOUT = 0.5


T = TypeVar("T")


class ChannelException(Exception):
    """Raised for miscallaneous channel issues."""


class ChannelFull(ChannelException):
    """Raised for full channels, will typically be caught by the port."""


class ChannelEmpty(ChannelException):
    """Raised for empty channels, will typically be caught by the port."""


class Channel(ABC, Generic[T]):
    """Represents a communication channel that will be plugged into a `Port`."""

    @property
    @abstractmethod
    def active(self) -> bool:
        """Returns whether the channel is active."""

    @property
    @abstractmethod
    def ready(self) -> bool:
        """Returns whether the channel is ready to receive from."""

    @property
    @abstractmethod
    def size(self) -> int:
        """Returns the current approximate size of the buffer"""

    @abstractmethod
    def close(self) -> None:
        """Closes the channel."""

    @abstractmethod
    def kill(self) -> None:
        """Kills the channel, called at network shutdown and ensures no orphaned processes."""

    @abstractmethod
    def send(self, item: T, timeout: float | None = None) -> None:
        """
        Send an item.

        Will attempt to send an item of any type into a channel,
        potentially blocking if the channel is full.

        Parameters
        ----------
        item
            Item to send
        timeout
            Timeout in seconds to wait for space in the channel

        Raises
        ------
        ChannelFull
            If the channel is already full

        """

    @abstractmethod
    def receive(self, timeout: float | None = None) -> T | None:
        """
        Receive an item.

        Parameters
        ----------
        timeout
            Timeout in seconds to wait for an item

        Returns
        -------
        T | None
            The received item, or ``None`` if the channel is empty

        """

    @abstractmethod
    def flush(self, timeout: float = 0.1) -> list[T]:
        """
        Flush the contents of the channel.

        Parameters
        ----------
        timeout
            The timeout for item retrieval

        Returns
        -------
        list[T]
            List of unserialized channel contents
        """


class FileChannel(Channel[list[Path] | Path]):
    """A communication channel for data in the form of files."""

    _destination_path: Path

    def __init__(self, mode: Literal["copy", "link", "move"] = "copy") -> None:
        ctx = get_context(DEFAULT_CONTEXT)
        self._channel_dir = Path(mkdtemp())
        self._payload: "Queue[list[Path]]" = ctx.Queue(maxsize=1)
        self._file_trigger = ctx.Event()  # File available trigger
        self._shutdown_signal = ctx.Event()  # Shutdown trigger
        self.mode = mode

    @property
    def active(self) -> bool:
        return not self._shutdown_signal.is_set()

    @property
    def ready(self) -> bool:
        return self._file_trigger.is_set()

    @property
    def size(self) -> int:
        return 1 if self._file_trigger.is_set() else 0

    def setup(self, destination: Path) -> None:
        """
        Setup the file channel directories.

        Parameters
        ----------
        destination
            Path to the destination directory for the input port

        """
        destination.mkdir(exist_ok=True)
        self._destination_path = destination.absolute()
        if existing := list(self._destination_path.rglob("*")):
            self.preload(existing)

    def close(self) -> None:
        # TODO Revisit this, we need to make sure we're not deleting
        # data before downstream nodes have time to receive it
        while self._file_trigger.is_set() and has_file(self._channel_dir):
            time.sleep(DEFAULT_TIMEOUT)
        self._shutdown_signal.set()
        shutil.rmtree(self._channel_dir, ignore_errors=True)

    def kill(self) -> None:
        pass

    def preload(self, items: list[Path] | Path) -> None:
        """Load a file into the channel without explicitly sending."""
        items = items if isinstance(items, list) else [items]
        try:
            self._payload.put(items, timeout=DEFAULT_TIMEOUT)
        except queue.Full as full:
            raise ChannelFull("Attempting to preload an already filled channel") from full
        self._file_trigger.set()

    def send(self, item: list[Path] | Path, timeout: float | None = None) -> None:
        # Give a time ultimatum, for the trigger
        items = item if isinstance(item, list) else [item]
        items = [item.absolute() for item in items]
        if not all(file.exists() for file in items) or self._file_trigger.is_set():
            if timeout is not None:
                time.sleep(timeout)
            if self._file_trigger.is_set():
                raise ChannelFull("File channel already has data, are you sure it was received?")

            if not all(file.exists() for file in items):
                raise ChannelException(f"Files at {common_parent(items).as_posix()} not found")

        try:
            self._payload.put(
                sendtree(items, self._channel_dir, mode=self.mode), timeout=DEFAULT_TIMEOUT
            )
        except queue.Full as full:
            raise ChannelFull("Channel already has files as payload") from full
        self._file_trigger.set()

    def receive(self, timeout: float | None = None) -> list[Path] | Path | None:
        # Wait for the trigger and then check if we have the file
        if not self._file_trigger.wait(timeout=timeout):
            return None

        try:
            files = self._payload.get(timeout=timeout)
        except queue.Empty as empty:
            raise ChannelException("Trigger was set, but no data to receive") from empty

        dest_files = sendtree(files, self._destination_path, mode="move")
        self._file_trigger.clear()
        return dest_files if len(dest_files) > 1 else dest_files[0]

    def flush(self, timeout: float = 0.1) -> list[list[Path] | Path]:
        """
        Flush the contents of the channel.

        Parameters
        ----------
        timeout
            The timeout for item retrieval

        Returns
        -------
        list[list[Path]]
            List with a paths to a file in the destination
            directory or an empty list. This is to be consistent
            with the signature of `DataChannel`.

        """
        files = self.receive(timeout=timeout)
        if files is None:
            return []
        return [files]


class DataChannel(Channel[T]):
    """
    A communication channel for data in the form of python objects.

    Any item sent needs to be serializable using `dill`.

    Parameters
    ----------
    size
        Size of the item queue

    """

    def __init__(self, size: int) -> None:
        ctx = get_context(DEFAULT_CONTEXT)
        try:
            self._queue: "Queue[bytes]" = ctx.Queue(size)

        # This will only happen for *very* large graphs
        # (more than 2000 or so connections, it will depend on the OS)
        except OSError as err:
            msg = (
                "You have reached the maximum number of channels "
                "supported by your operating system."
            )
            raise ChannelException(msg) from err

        self._n_items: Any = ctx.Value("i", size)  # Number of items in the channel
        self._n_items.value = 0
        self._signal = ctx.Event()  # False

    @property
    def active(self) -> bool:
        return not self._signal.is_set()

    @property
    def ready(self) -> bool:
        return cast(bool, self._n_items.value > 0)

    @property
    def size(self) -> int:
        return cast(int, self._n_items.value)

    def close(self) -> None:
        self._signal.set()
        # This may not be necessary and just cause problems
        # self._queue.close()

    def kill(self) -> None:
        self._queue.close()
        self._queue.cancel_join_thread()

    def preload(self, items: list[bytes] | bytes) -> None:
        """
        Pre-load the channel with a serialized item. Used by restarts.

        Parameters
        ----------
        items
            Serialized items to pre-load the channel with

        """
        if isinstance(items, bytes):
            items = [items]
        for item in items:
            self._queue.put(item)
            with self._n_items.get_lock():
                self._n_items.value += 1

    def send(self, item: T, timeout: float | None = None) -> None:
        try:
            pickled = dill.dumps(item)
            self._queue.put(pickled, timeout=timeout)

            # Sending an item and immediately polling will falsely result in
            # a supposedly empty channel, so we wait a fraction of a second
            time.sleep(0.1)
        except queue.Full as err:
            raise ChannelFull("Channel queue is full") from err

        with self._n_items.get_lock():
            self._n_items.value += 1

    def receive(self, timeout: float | None = None) -> T | None:
        # We have no buffered data, so we try receiving now
        try:
            raw_item = self._queue.get(timeout=timeout)

            # The cast here is because `dill.loads` *could* return `Any`,
            # but because we only interact with it through the channel,
            # it actually will always return `T`
            val = cast(T, dill.loads(raw_item))
        except queue.Empty:
            val = None
        else:
            with self._n_items.get_lock():
                self._n_items.value -= 1
        return val

    def flush(self, timeout: float = 0.1) -> list[T]:
        """
        Flush the contents of the channel.

        Parameters
        ----------
        timeout
            The timeout for item retrieval

        Returns
        -------
        list[T]
            List of unserialized channel contents
        """
        items: list[T] = []
        while (item := self.receive(timeout=timeout)) is not None:
            items.append(item)
        return items
