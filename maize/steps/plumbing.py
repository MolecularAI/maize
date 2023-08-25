"""General purpose tasks for data-flow control."""

from copy import deepcopy
from collections.abc import Iterator
import itertools
import time
from typing import Generic, TypeVar, Any

from numpy.typing import NDArray

from maize.core.node import Node, LoopedNode
from maize.core.interface import Parameter, Input, Output, MultiInput, MultiOutput, PortInterrupt
from maize.utilities.utilities import chunks


T = TypeVar("T")


class Multiply(Node, Generic[T]):
    """Creates a list of multiple of the same item"""

    inp: Input[T] = Input()
    """Data input"""

    out: Output[list[T]] = Output()
    """Data output"""

    n_packages: Parameter[int] = Parameter()
    """Number of times to multiply the data"""

    def run(self) -> None:
        data = self.inp.receive()
        out = [data for _ in range(self.n_packages.value)]
        self.out.send(out)


class Yes(LoopedNode, Generic[T]):
    """Sends a single received value multiple times"""

    inp: Input[T] = Input()
    """Data input"""

    out: Output[T] = Output()
    """Data output"""

    data: T | None = None

    def ports_active(self) -> bool:
        return self.out.active

    def run(self) -> None:
        if self.data is None:
            self.data = self.inp.receive()
        self.out.send(self.data)


class Barrier(LoopedNode, Generic[T]):
    """Only sends data onwards if a signal is received"""

    inp: Input[T] = Input()
    """Data input"""

    out: Output[T] = Output()
    """Data output"""

    inp_signal: Input[bool] = Input()
    """Signal data, upon receiving ``True`` will send the held data onwards"""

    def run(self) -> None:
        data = self.inp.receive()
        self.logger.info("Waiting for signal...")
        if self.inp_signal.receive():
            self.out.send(data)


class Batch(Node, Generic[T]):
    """Create batches of data from a single large input"""

    inp: Input[list[T]] = Input()
    """Input data"""

    out: Output[list[T]] = Output()
    """Stream of output data chunks"""

    n_batches: Parameter[int] = Parameter()
    """Number of chunks"""

    def run(self) -> None:
        full_data = self.inp.receive()
        for i, batch in enumerate(chunks(full_data, self.n_batches.value)):
            self.logger.info("Sending batch %s/%s", i, self.n_batches.value)
            self.out.send(list(batch))


class Combine(Node, Generic[T]):
    """Combine multiple batches of data into a single dataset"""

    inp: Input[list[T] | NDArray[Any]] = Input()
    """Input data chunks to combine"""

    out: Output[list[T]] = Output()
    """Single combined output data"""

    n_batches: Parameter[int] = Parameter()
    """Number of chunks"""

    def run(self) -> None:
        data: list[T] = []
        for i in range(self.n_batches.value):
            data.extend(self.inp.receive())
            self.logger.info("Received batch %s/%s", i + 1, self.n_batches.value)
        self.logger.debug("Sending full dataset of size %s", len(data))
        self.out.send(data)


class Merge(LoopedNode, Generic[T]):
    """
    Collect inputs from multiple channels and send them to a
    single output port on a first-in-first-out (FIFO) basis.

    """

    inp: MultiInput[T] = MultiInput(optional=True)
    """Flexible number of input channels to be merged"""
    out: Output[T] = Output()
    """Single output for all merged data"""

    def run(self) -> None:
        if not self.out.active or all(not port.active for port in self.inp):
            raise PortInterrupt("Inactive ports")

        for port in self.inp:
            if port.ready() and (item := port.receive_optional()) is not None:
                self.out.send(item)


class Copy(LoopedNode, Generic[T]):
    """Copy a single input packet to multiple output channels."""

    inp: Input[T] = Input(mode="copy")
    """Single input to broadcast"""
    out: MultiOutput[T] = MultiOutput(optional=True, mode="copy")
    """Multiple outputs to broadcast over"""

    def run(self) -> None:
        if (not self.inp.active and not self.inp.ready()) or all(
            not out.active for out in self.out
        ):
            raise PortInterrupt("Inactive ports")

        val = self.inp.receive()
        for out in self.out:
            out.send(deepcopy(val))


class RoundRobin(LoopedNode, Generic[T]):
    """
    Outputs a single input packet to a single output port at a time,
    cycling through output ports.

    """

    inp: Input[T] = Input()
    """Single input to alternatingly send on"""
    out: MultiOutput[T] = MultiOutput(optional=True)
    """Multiple outputs to distribute over"""

    _output_cycle: Iterator[Output[T]]
    _current_output: Output[T]

    def prepare(self) -> None:
        super().prepare()
        self._output_cycle = itertools.cycle(self.out)
        self._current_output = next(self._output_cycle)

    def run(self) -> None:
        if not self.inp.active or all(not out.active for out in self.out):
            raise PortInterrupt("Inactive ports")

        self._current_output.send(self.inp.receive())
        self._current_output = next(self._output_cycle)


class Accumulate(LoopedNode, Generic[T]):
    """Accumulate multiple independent packets into one large packet."""

    inp: Input[T] = Input()
    """Packets to accumulate"""
    out: Output[list[T]] = Output()
    """Output for accumulated packets"""
    n_packets: Parameter[int] = Parameter()
    """Number of packets to receive before sending one large packet"""

    def run(self) -> None:
        packet = []
        for _ in range(self.n_packets.value):
            val = self.inp.receive()
            packet.append(val)
        self.out.send(packet)


class Scatter(LoopedNode, Generic[T]):
    """Decompose one large packet into it's constituent items and send them separately."""

    inp: Input[list[T]] = Input()
    """Packets of sequences that allow unpacking"""
    out: Output[T] = Output()
    """Unpacked data"""

    def run(self) -> None:
        packet = self.inp.receive()
        if not hasattr(packet, "__len__"):
            raise ValueError(f"Packet of type {type(packet)} can not be unpacked")

        for item in packet:
            self.out.send(item)


class Delay(LoopedNode, Generic[T]):
    """Pass on a packet with a custom delay."""

    inp: Input[T] = Input()
    out: Output[T] = Output()
    delay: Parameter[float | int] = Parameter(default=1)
    """Delay in seconds"""

    def run(self) -> None:
        item = self.inp.receive()
        time.sleep(self.delay.value)
        self.out.send(item)
