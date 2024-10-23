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


_FilePathType = list[Path] | dict[Any, Path] | Path

_T_PATH, _T_LIST, _T_DICT = 0, 1, 2


class FileChannel(Channel[_FilePathType]):
    """
    A communication channel for data in the form of files. Files must be represented
    by `Path` objects, and can either be passed alone, as a list, or as a dictionary.

    When sending a file, it is first transferred (depending on `mode`) to an escrow
    directory specific to the channel (typically a temporary directory). Upon calling
    `receive`, this file is transferred to an input-directory for the receiving node.

    Parameters
    ----------
    mode
        Whether to ``copy`` (default), ``link``, or ``move`` files from node to node.

    See Also
    --------
    DataChannel : Channel for arbitrary serializable data

    """

    _destination_path: Path

    # FileChannel allows single Path objects, as well as lists and dictionaries
    # of paths to be sent. To make this possible, we always send a dictionary of
    # paths, but convert to and from dictionaries while sending and receiving,
    # and communicate the type of data through a shared value object.
    @staticmethod
    def _convert(items: _FilePathType) -> dict[Any, Path]:
        if isinstance(items, list):
            items = {i: item for i, item in enumerate(items)}
        elif isinstance(items, Path):
            items = {0: items}
        return items

    def __init__(self, mode: Literal["copy", "link", "move"] = "copy") -> None:
        ctx = get_context(DEFAULT_CONTEXT)
        self._channel_dir = Path(mkdtemp())
        self._payload: "Queue[dict[Any, Path]]" = ctx.Queue(maxsize=1)
        self._file_trigger = ctx.Event()  # File available trigger
        self._shutdown_signal = ctx.Event()  # Shutdown trigger
        # FIXME temporary Any type hint until this gets solved
        # https://github.com/python/typeshed/issues/8799
        self._transferred_type: Any = ctx.Value("i", _T_DICT)  # Type of data sent, see _T_*
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
        while self._file_trigger.is_set() and has_file(self._channel_dir):
            time.sleep(DEFAULT_TIMEOUT)
        self._shutdown_signal.set()
        self._payload.cancel_join_thread()
        shutil.rmtree(self._channel_dir, ignore_errors=True)

    def kill(self) -> None:
        # No need to do anything special here
        pass

    def preload(self, items: _FilePathType) -> None:
        """Load a file into the channel without explicitly sending."""
        item = FileChannel._convert(items)

        self._update_type(items)
        try:
            self._payload.put(item, timeout=DEFAULT_TIMEOUT)
        except queue.Full as full:
            raise ChannelFull("Attempting to preload an already filled channel") from full
        self._file_trigger.set()

    def send(self, item: _FilePathType, timeout: float | None = None) -> None:
        items = FileChannel._convert(item)
        items = {k: item.absolute() for k, item in items.items()}
        if not all(file.exists() for file in items.values()) or self._file_trigger.is_set():
            # Give a time ultimatum, for the trigger
            if timeout is not None:
                time.sleep(timeout)
            if self._file_trigger.is_set():
                raise ChannelFull("File channel already has data, are you sure it was received?")

            if not all(file.exists() for file in items.values()):
                raise ChannelException(
                    f"Files at {common_parent(list(items.values())).as_posix()} not found"
                )

        self._update_type(item)
        try:
            self._payload.put(
                sendtree(items, self._channel_dir, mode=self.mode), timeout=DEFAULT_TIMEOUT
            )
        except queue.Full as full:
            raise ChannelFull("Channel already has files as payload") from full
        self._file_trigger.set()

    def receive(self, timeout: float | None = None) -> _FilePathType | None:
        # Wait for the trigger and then check if we have the file
        if not self._file_trigger.wait(timeout=timeout):
            return None

        try:
            files = self._payload.get(timeout=timeout)
        except queue.Empty as empty:
            raise ChannelException("Trigger was set, but no data to receive") from empty

        dest_files = sendtree(files, self._destination_path, mode="move")
        self._file_trigger.clear()
        return self._cast_type(dest_files)

    def flush(self, timeout: float = DEFAULT_TIMEOUT) -> list[_FilePathType]:
        """
        Flush the contents of the channel.

        Parameters
        ----------
        timeout
            The timeout for item retrieval

        Returns
        -------
        list[_FilePathType]
            List with a paths to a file in the destination
            directory or an empty list. This is to be consistent
            with the signature of `DataChannel`.

        """
        files = self.receive(timeout=timeout)
        if files is None:
            return []
        return [files]

    def _update_type(self, data: _FilePathType) -> None:
        """Updates the type of data being transferred to allow the correct type to be returned."""
        with self._transferred_type.get_lock():
            if isinstance(data, dict):
                self._transferred_type.value = _T_DICT
            elif isinstance(data, list):
                self._transferred_type.value = _T_LIST
            else:
                self._transferred_type.value = _T_PATH

    def _cast_type(self, data: dict[Any, Path]) -> _FilePathType:
        """Cast received data to match the original sent type."""
        if self._transferred_type.value == _T_DICT:
            return data
        elif self._transferred_type.value == _T_LIST:
            return list(data.values())
        else:
            return data[0]


class DataChannel(Channel[T]):
    """
    A communication channel for data in the form of python objects.

    Any item sent needs to be serializable using `dill`.

    Parameters
    ----------
    size
        Size of the item queue

    See Also
    --------
    FileChannel : Channel for files

    """

    def __init__(self, size: int) -> None:
        ctx = get_context(DEFAULT_CONTEXT)
        try:
            self._queue: "Queue[bytes]" = ctx.Queue(size)

        # This will only happen for *very* large graphs
        # (more than 2000 or so connections, it will depend on the OS)
        except OSError as err:  # pragma: no cover
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
        already_signalled = self._signal.is_set()
        self._signal.set()

        # A channel can be closed from both the sending and receiving side. If the
        # sending side sends an item and immediately quits, closing the channel from
        # its side, the receiving node still needs to be able to receive the data
        # cleanly, meaning we can't call `cancel_join_thread()` prematurely, as it
        # will clear the internal queue. So we only call it if we get a second closing
        # signal, i.e. from the receiving side. This means we have already received
        # any data we want, or exited for a different reason.
        if already_signalled:
            self._queue.cancel_join_thread()

    def kill(self) -> None:
        self._queue.close()

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
            time.sleep(DEFAULT_TIMEOUT)
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

    def flush(self, timeout: float = DEFAULT_TIMEOUT) -> list[T]:
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
