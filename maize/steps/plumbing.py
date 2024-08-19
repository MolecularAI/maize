"""General purpose tasks for data-flow control."""

from copy import deepcopy
from collections.abc import Iterator
import itertools
import time
from typing import Generic, TypeVar, Any

import numpy as np
from numpy.typing import NDArray

from maize.core.node import Node, LoopedNode
from maize.core.interface import (
    Parameter,
    Flag,
    Input,
    Output,
    MultiInput,
    MultiOutput,
    PortInterrupt,
)
from maize.utilities.utilities import chunks


T = TypeVar("T")

INACTIVE = "Inactive ports"


class Multiply(Node, Generic[T]):
    """
    Creates a list of multiple of the same item

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Multiply", fillcolor = "#FFFFCC", color = "#B48000"]
         2 -> n [label = "1"]
         n -> 1 [label = "[1, 1, 1]"]
       }

    """

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
    """
    Sends a single received value multiple times

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Yes", fillcolor = "#FFFFCC", color = "#B48000"]
         2 -> n [label = "1"]
         n -> 1 [label = "1, 1, 1, ..."]
       }

    """

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
    """
    Only sends data onwards if a signal is received

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Barrier", fillcolor = "#FFFFCC", color = "#B48000"]
         n -> 1 [style = dashed]
         2 -> n
         e -> n
         {rank=same e n}
       }

    """

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
    """
    Create batches of data from a single large input

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Batch", fillcolor = "#FFFFCC", color = "#B48000"]
         2 -> n [label = "[1, 2, 3, 4]"]
         n -> 1 [label = "[1, 2], [3, 4]"]
       }

    """

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
    """
    Combine multiple batches of data into a single dataset

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Combine", fillcolor = "#FFFFCC", color = "#B48000"]
         2 -> n [label = "[1, 2], [3, 4]"]
         n -> 1 [label = "[1, 2, 3, 4]"]
       }

    """

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


class MergeLists(LoopedNode, Generic[T]):
    """
    Collect lists of data from multiple inputs and merges them into a single list.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white, style = filled,
               fontname = "Helvetica,Arial,sans-serif"]
         MergeLists [label = "MergeLists", fillcolor = "#FFFFCC", color = "#B48000"]
         1 -> MergeLists [label = "[1, 2]"]
         2 -> MergeLists [label = "[3]"]
         3 -> MergeLists [label = "[4, 5]"]
         MergeLists -> 4 [label = "[1, 2, 3, 4, 5]"]
       }

    """

    inp: MultiInput[list[T]] = MultiInput()
    """Flexible number of input lists to be merged"""

    out: Output[list[T]] = Output()
    """Single output for all merged data"""

    def run(self) -> None:
        if not self.out.active or all(not port.active for port in self.inp):
            raise PortInterrupt(INACTIVE)

        has_items = False
        items: list[T] = []
        n_interrupt = 0
        for inp in self.inp:
            try:
                if not inp.optional:
                    items.extend(inp.receive())
                    has_items = True
                elif inp.ready() and (item := inp.receive_optional()) is not None:
                    items.extend(item)
                    has_items = True

            # Normally, interrupts are caught in the main maize execution loop.
            # But when one of the inputs dies while we're trying to receive from
            # it, we would still like to receive from any subsequent ports as
            # normal. This means that this node will only shutdown once all
            # inputs have signalled a shutdown.
            except PortInterrupt:
                n_interrupt += 1
                if n_interrupt >= len(self.inp):
                    raise

        # No check for an empty list, as nodes could conceivably explicitly send an empty
        # list, in that case we want to honour this and send an empty list on too.
        if has_items:
            self.out.send(items)


class Merge(LoopedNode, Generic[T]):
    """
    Collect inputs from multiple channels and send them to a
    single output port on a first-in-first-out (FIFO) basis.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         node [shape = rectangle, label = "", color = white, style = filled,
               fontname = "Helvetica,Arial,sans-serif"]
         Merge [label = "Merge", fillcolor = "#FFFFCC", color = "#B48000"]
         1 -> Merge
         2 -> Merge
         3 -> Merge
         Merge -> 4
       }

    """

    inp: MultiInput[T] = MultiInput(optional=True)
    """Flexible number of input channels to be merged"""

    out: Output[T] = Output()
    """Single output for all merged data"""

    def run(self) -> None:
        if not self.out.active or all(not port.active for port in self.inp):
            raise PortInterrupt(INACTIVE)

        for port in self.inp:
            if port.ready() and (item := port.receive_optional()) is not None:
                self.out.send(item)


class Multiplex(LoopedNode, Generic[T]):
    """
    Receives items on multiple inputs, sends them to a single output,
    receives those items, and sends them to the same output index.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Multiplex", fillcolor = "#FFFFCC", color = "#B48000"]
         n -> 1 [penwidth = 2]
         n -> 2 [style = dashed]
         n -> 3 [style = dashed]
         4 -> n [penwidth = 2]
         5 -> n [style = dashed]
         6 -> n [style = dashed]
         e [fillcolor = "#FFFFCC", color = "#B48000", style = dashed]
         n -> e
         e -> n
         {rank=same e n}
       }

    """

    inp: MultiInput[T] = MultiInput(optional=True)
    """Multiple inputs, should be the same as the number of outputs"""

    out: MultiOutput[T] = MultiOutput(optional=True)
    """Multiple outputs, should be the same as the number of inputs"""

    out_single: Output[T] = Output()
    """Single output"""

    inp_single: Input[T] = Input()
    """Single input"""

    def ports_active(self) -> bool:
        return (
            any(inp.active for inp in self.inp)
            and any(out.active for out in self.out)
            and self.out_single.active
            and self.inp_single.active
        )

    def run(self) -> None:
        for inp, out in zip(self.inp, self.out, strict=True):
            if inp.ready():
                item = inp.receive()
                self.out_single.send(item)
                new_item = self.inp_single.receive()
                out.send(new_item)


class Copy(LoopedNode, Generic[T]):
    """
    Copy a single input packet to multiple output channels.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         Copy [label = "Copy", fillcolor = "#FFFFCC", color = "#B48000"]
         Copy -> 1
         Copy -> 2
         Copy -> 3
         4 -> Copy
       }

    """

    inp: Input[T] = Input(mode="copy")
    """Single input to broadcast"""

    out: MultiOutput[T] = MultiOutput(optional=True, mode="copy")
    """Multiple outputs to broadcast over"""

    def run(self) -> None:
        if (not self.inp.active and not self.inp.ready()) or all(
            not out.active for out in self.out
        ):
            raise PortInterrupt(INACTIVE)

        val = self.inp.receive()
        self.logger.debug("Received %s", val)
        for out in self.out:
            self.logger.debug("Sending %s", val)
            out.send(deepcopy(val))


class RoundRobin(LoopedNode, Generic[T]):
    """
    Outputs a single input packet to a single output port at a time,
    cycling through output ports.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "RoundRobin", fillcolor = "#FFFFCC", color = "#B48000"]
         n -> 1 [label = 1]
         n -> 2 [style = dashed, label = 2]
         n -> 3 [style = dashed, label = 3]
         4 -> n
       }

    """

    inp: Input[T] = Input()
    """Single input to alternatingly send on"""

    out: MultiOutput[T] = MultiOutput(optional=True)
    """Multiple outputs to distribute over"""

    _output_cycle: Iterator[Output[T]]
    _current_output: Output[T]

    def prepare(self) -> None:
        self._output_cycle = itertools.cycle(self.out)
        self._current_output = next(self._output_cycle)

    def run(self) -> None:
        if not self.inp.active or all(not out.active for out in self.out):
            raise PortInterrupt(INACTIVE)

        self._current_output.send(self.inp.receive())
        self._current_output = next(self._output_cycle)


class IntegerMap(Node):
    """
    Maps an integer to another range.

    Takes a pattern describing which integer to output based on a constantly incrementing
    input integer. For example, with the pattern ``[0, 2, -1]`` an input of ``0`` will output
    ``1`` (the index of the first element), an input of ``1`` will also output ``1`` (as we
    indicated 2 iterations for this position), and an input of ``2`` and up will output ``2``
    (for the final index). This can be especially useful in conjunction with `IndexDistribute`
    to perform different actions based on a global iteration counter.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "IntegerMap [0, 2, -1]", fillcolor = "#FFFFCC", color = "#B48000"]
         n -> 1 [label = "2"]
         2 -> n [label = "1"]
       }

    See Also
    --------
    IndexDistribute
        Allows distributing data to one of several outputs based on a separately supplied index.

    """

    inp: Input[int] = Input()
    """Input integer"""

    out: Output[int] = Output()
    """Output integer"""

    pattern: Parameter[list[int]] = Parameter()
    """Output pattern"""

    def run(self) -> None:
        x = self.inp.receive()
        pat = np.array(self.pattern.value)
        outs = np.arange(len(pat)).repeat(abs(pat))
        x = min(x, len(outs) - 1)
        self.out.send(outs[x])


class Choice(Node, Generic[T]):
    """
    Sends an item from a dynamically specified input to an output.

    Receives an index separately and indexes into the
    inputs to decide from where to receive the item.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "IndexDistribute", fillcolor = "#FFFFCC", color = "#B48000"]
         1 -> n [style = dashed]
         2 -> n [label = "foo"]
         n -> 3 [label = "foo"]
         4 -> n [label = "1"]
       }

    """

    inp: MultiInput[T] = MultiInput()
    """Item inputs"""

    inp_index: Input[int] = Input()
    """Which index of the output to send the item to"""

    out: Output[T] = Output()
    """Item output"""

    clip: Flag = Flag(default=False)
    """Whether to clip the index to the maximum number of outputs"""

    def run(self) -> None:
        idx = self.inp_index.receive()
        if self.clip.value:
            idx = min(idx, len(self.inp) - 1)
        item = self.inp[idx].receive()
        self.out.send(item)


class IndexDistribute(Node, Generic[T]):
    """
    Sends an item to an output specified by a dynamic index.

    Receives an index separately and indexes into the outputs to decide where to send the item.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "IndexDistribute", fillcolor = "#FFFFCC", color = "#B48000"]
         n -> 1 [style = dashed]
         n -> 2 [label = "foo"]
         3 -> n [label = "foo"]
         4 -> n [label = "1"]
       }

    """

    inp: Input[T] = Input()
    """Item input"""

    inp_index: Input[int] = Input()
    """Which index of the output to send the item to"""

    out: MultiOutput[T] = MultiOutput()
    """Item outputs"""

    clip: Flag = Flag(default=False)
    """Whether to clip the index to the maximum number of outputs"""

    def run(self) -> None:
        idx = self.inp_index.receive()
        if self.clip.value:
            idx = min(idx, len(self.out) - 1)
        item = self.inp.receive()
        self.out[idx].send(item)


class TimeDistribute(LoopedNode, Generic[T]):
    """
    Distributes items over multiple outputs depending on the current iteration.

    Can be seen as a generalized form of `RoundRobin`, with an additional specification
    of which output to send data to how many times. For example, the pattern ``[2, 1, 10]``
    will send 2 items to the first output, 1 item to the second, and 10 items to the third.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "TimeDistribute [1, 3]", fillcolor = "#FFFFCC", color = "#B48000"]
         n -> 1 [label = 1]
         n -> 2 [style = dashed, label = "2, 3, 4"]
         4 -> n [label = "1, 2, 3, 4"]
       }

    """

    inp: Input[T] = Input()
    """Item input"""

    out: MultiOutput[T] = MultiOutput(optional=True)
    """Multiple outputs to distribute over"""

    pattern: Parameter[list[int]] = Parameter()
    """How often to send items to each output, ``-1`` is infinite"""

    cycle: Flag = Flag(default=False)
    """Whether to loop around the pattern"""

    _outputs: Iterator[Output[T]]

    def prepare(self) -> None:
        pattern = self.pattern.value
        if len(pattern) != len(self.out):
            raise ValueError(
                "The number of entries in the pattern must "
                "be equal to the number of connected outputs"
            )

        if any(n < 0 for n in pattern[:-1]):
            raise ValueError("Cannot infinitely send to a non-final output")

        self._outputs = itertools.chain.from_iterable(
            itertools.repeat(out, n) if n >= 0 else itertools.repeat(out)
            for out, n in zip(self.out, pattern)
        )

        if self.cycle.value:
            self._outputs = itertools.cycle(self._outputs)

    def run(self) -> None:
        if not self.inp.active or all(not out.active for out in self.out):
            raise PortInterrupt(INACTIVE)

        next(self._outputs).send(self.inp.receive())


class CopyEveryNIter(LoopedNode, Generic[T]):
    """
    Copy a single input packet to multiple output channels, but only every ``n`` iterations.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         Copy [label = "Copy", fillcolor = "#FFFFCC", color = "#B48000"]
         Copy -> 1 [label = "1, 2, 3"]
         Copy -> 2 [label = "2"]
         3 -> Copy [label = "1, 2, 3"]
       }

    """

    inp: Input[T] = Input(mode="copy")
    """Single input to broadcast"""

    out: MultiOutput[T] = MultiOutput(optional=True, mode="copy")
    """Multiple outputs to broadcast over"""

    freq: Parameter[int] = Parameter(default=-1)
    """How often to send, ``-1`` is only on first iteration"""

    def prepare(self) -> None:
        self._it = 1

    def run(self) -> None:
        if (not self.inp.active and not self.inp.ready()) or all(
            not out.active for out in self.out
        ):
            raise PortInterrupt(INACTIVE)

        val = self.inp.receive()
        self.logger.debug("Received %s", val)
        outputs = iter(self.out)

        self.logger.debug("Sending %s", val)
        next(outputs).send(deepcopy(val))

        if (self.freq.value == -1 and self._it == 1) or (
            self.freq.value != -1 and self._it % self.freq.value == 0
        ):
            for out in outputs:
                self.logger.debug("Sending %s", val)
                out.send(deepcopy(val))

        self._it += 1


class Accumulate(LoopedNode, Generic[T]):
    """
    Accumulate multiple independent packets into one large packet.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Accumulate", fillcolor = "#FFFFCC", color = "#B48000"]
         2 -> n [label = "1, 2, 3"]
         n -> 1 [label = "[1, 2, 3]"]
       }

    """

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
    """
    Decompose one large packet into it's constituent items and send them separately.

    .. graphviz::

       digraph {
         graph [rankdir = "LR"]
         edge [fontname = "Helvetica,Arial,sans-serif"]
         node [shape = rectangle, label = "", color = white,
               style = filled, fontname = "Helvetica,Arial,sans-serif"]
         n [label = "Scatter", fillcolor = "#FFFFCC", color = "#B48000"]
         2 -> n [label = "[1, 2, 3]"]
         n -> 1 [label = "1, 2, 3"]
       }

    """

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
    """Data input"""

    out: Output[T] = Output()
    """Data output"""

    delay: Parameter[float | int] = Parameter(default=1)
    """Delay in seconds"""

    def run(self) -> None:
        item = self.inp.receive()
        time.sleep(self.delay.value)
        self.out.send(item)
