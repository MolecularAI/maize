"""General purpose tasks for data input / output."""

from pathlib import Path
from multiprocessing import get_context
import shutil
from typing import TYPE_CHECKING, Generic, TypeVar, Any, cast

import dill

from maize.core.node import Node, LoopedNode
from maize.core.interface import Parameter, FileParameter, Input, Output, Flag, MultiInput
from maize.utilities.execution import DEFAULT_CONTEXT

if TYPE_CHECKING:
    from multiprocessing import Queue

T = TypeVar("T")


class Dummy(Node):
    """A dummy node to connect to unused but optional input ports."""

    out: Output[Any] = Output()
    """Dummy output with type ``Any``, nothing will be sent"""

    def run(self) -> None:
        # Do nothing
        pass


class Void(LoopedNode):
    """A node that swallows whatever input it receives."""

    inp: MultiInput[Any] = MultiInput()
    """Void input, a bit like ``/dev/null``"""

    def run(self) -> None:
        for inp in self.inp:
            if inp.ready():
                _ = inp.receive()


P = TypeVar("P", bound=Path)


class LoadFile(Node, Generic[P]):
    """Provides a file specified as a parameter on an output."""

    file: FileParameter[P] = FileParameter()
    """Path to the input file"""

    out: Output[P] = Output(mode="copy")
    """File output"""

    def run(self) -> None:
        path = self.file.filepath
        if not path.exists():
            raise FileNotFoundError(f"File at path {path.as_posix()} not found")

        self.out.send(path)


class LoadFiles(Node, Generic[P]):
    """Provides multiple files specified as a parameter on an output channel."""

    files: Parameter[list[P]] = Parameter()
    """Paths to the input files"""

    out: Output[list[P]] = Output(mode="copy")
    """File output channel"""

    def run(self) -> None:
        paths = self.files.value
        for path in paths:
            if not path.exists():
                raise FileNotFoundError(f"File at path {path.as_posix()} not found")

        self.out.send(paths)


class LoadData(Node, Generic[T]):
    """Provides data passed as a parameter to an output channel."""

    data: Parameter[T] = Parameter()
    """Data to be sent to the output verbatim"""

    out: Output[T] = Output()
    """Data output"""

    def run(self) -> None:
        data = self.data.value
        self.out.send(data)


class LogResult(Node):
    """Receives data and logs it."""

    inp: Input[Any] = Input()
    """Data input"""

    def run(self) -> None:
        data = self.inp.receive()
        self.logger.info("Received data: '%s'", data)


class Log(Node, Generic[T]):
    """Logs any received data and sends it on"""

    inp: Input[T] = Input()
    """Data input"""

    out: Output[T] = Output()
    """Data output"""

    def run(self) -> None:
        data = self.inp.receive()
        msg = f"Handling data '{data}'"
        if isinstance(data, Path):
            msg += f", {'exists' if data.exists() else 'not found'}"
        self.logger.info(msg)
        self.out.send(data)


class SaveFile(Node, Generic[P]):
    """
    Receives a file and saves it to a specified location.

    You must parameterise the node with the appropriate filetype, or just ``Path``.
    If the destination doesn't exist, a folder will be created.

    """

    inp: Input[P] = Input(mode="copy")
    """File input"""

    destination: FileParameter[P] = FileParameter(exist_required=False)
    """The destination file or folder"""

    overwrite: Flag = Flag(default=False)
    """If ``True`` will overwrite any previously existing file in the destination"""

    def run(self) -> None:
        file = self.inp.receive()
        dest = self.destination.filepath

        # Inherit the received file name if not given
        if dest.is_dir() or not dest.suffix:
            dest = dest / file.name
            self.logger.debug("Using path '%s'", dest.as_posix())

        # Create parent directory if required
        if not dest.parent.exists():
            self.logger.debug("Creating parent directory '%s'", dest.parent.as_posix())
            dest.parent.mkdir()

        if dest.exists() and not self.overwrite.value:
            self.logger.warning(
                "File already exists at destination '%s', set 'overwrite' to proceed anyway",
                dest.as_posix(),
            )
        else:
            self.logger.info("Saving file to '%s'", dest.as_posix())
            shutil.copyfile(file, dest)


class SaveFiles(Node, Generic[P]):
    """
    Receives multiple files and saves them to a specified location.

    You must parameterise the node with the appropriate filetype, or just ``Path``.

    """

    inp: Input[list[P]] = Input(mode="copy")
    """Files input"""

    destination: FileParameter[P] = FileParameter(exist_required=False)
    """The destination folder"""

    overwrite: Flag = Flag(default=False)
    """If ``True`` will overwrite any previously existing file in the destination"""

    def run(self) -> None:
        files = self.inp.receive()
        folder = self.destination.filepath

        if not folder.is_dir():
            raise ValueError(f"Destination '{folder}' must be a directory")

        for file in files:
            dest = folder / file.name
            if dest.exists() and not self.overwrite.value:
                raise FileExistsError(
                    f"File already exists at destination '{dest.as_posix()}'"
                    ", set 'overwrite' to proceed anyway"
                )
            self.logger.info("Saving file to '%s'", dest.as_posix())
            shutil.copyfile(file, dest)


class FileBuffer(Node, Generic[P]):
    """
    Dynamic file storage.

    If the file exists, sends it immediately. If it doesn't,
    waits to receive it and saves it in the specified location.

    """

    inp: Input[P] = Input()
    """File input"""

    out: Output[P] = Output()
    """File output"""

    file: FileParameter[P] = FileParameter(exist_required=False)
    """Buffered file location"""

    def run(self) -> None:
        if self.file.filepath.exists():
            self.out.send(self.file.filepath)
        file = self.inp.receive()
        self.logger.info("Saving file to '%s'", self.file.filepath.as_posix())
        shutil.copyfile(file, self.file.filepath)


class Return(Node, Generic[T]):
    """
    Return a value from the input to a specialized queue to be captured by the main process.

    Examples
    --------
    >>> save = graph.add(Return[float])
    >>> # Define your workflow as normal...
    >>> graph.execute()
    >>> save.get()
    3.14159

    Note that ``get()`` will pop the item from the internal queue,
    this means the item will be lost if not assigned to a variable.

    """

    ret_queue: "Queue[bytes]"
    ret_item: T | None = None

    inp: Input[T] = Input()

    def get(self) -> T | None:
        """Returns the passed value"""
        return self.ret_item

    def build(self) -> None:
        super().build()

        # This is our return value that will be checked in the test
        self.ret_queue = get_context(DEFAULT_CONTEXT).Queue()

    def cleanup(self) -> None:
        if not self.ret_queue.empty():
            self.ret_item = cast(T, dill.loads(self.ret_queue.get()))
        self.ret_queue.close()
        return super().cleanup()

    def run(self) -> None:
        if (val := self.inp.receive_optional()) is not None:
            self.ret_queue.put(dill.dumps(val))
