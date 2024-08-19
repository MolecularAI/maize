"""Testing utilities"""

import logging
import queue
from typing import Any, TypeVar, cast

from maize.core.interface import MultiInput, MultiOutput
from maize.core.node import Node
from maize.core.channels import Channel
from maize.core.workflow import Workflow
from maize.utilities.io import Config


T = TypeVar("T")


class MockChannel(Channel[T]):
    """
    Mock channel class presenting a `Channel` interface for testing.
    Can be loaded with an item to simulate a node receiving it from
    a neighbour. Can also be used to retrieve a sent item for assertions.

    Parameters
    ----------
    items
        Items to be loaded into the channel

    """

    def __init__(self, items: T | list[T] | None = None) -> None:
        self._locked = False
        self._queue: queue.Queue[T] = queue.Queue()
        if items is not None:
            if not isinstance(items, list):
                items = [items]
            for item in items:
                self._queue.put(item)

    @property
    def active(self) -> bool:
        return not self._locked

    @property
    def ready(self) -> bool:
        return not self._queue.empty()

    @property
    def size(self) -> int:
        return self._queue.qsize()

    def close(self) -> None:
        self._locked = True

    def kill(self) -> None:
        # No multiprocessing here, so no special killing needed
        pass

    def receive(self, timeout: float | None = None) -> T | None:
        if not self._queue.empty():
            item = self._queue.get(timeout=timeout)
            if self._queue.empty():
                self.close()
            return item
        return None

    def send(self, item: T, timeout: float | None = None) -> None:
        self._queue.put(item, timeout=timeout)

    def get(self) -> T | None:
        """Unconditionally get the stored item for assertions."""
        return self._queue.get()

    def flush(self, timeout: float = 0.1) -> list[T]:
        data = []
        while not self._queue.empty():
            data.append(self._queue.get(timeout=timeout))
        return data


class TestRig:
    """
    Test rig for user `Node` tasks. Can be loaded with parameters and inputs.

    Parameters
    ----------
    cls
        `Node` child class to be wrapped
    config
        Global configuration for the parent workflow

    Attributes
    ----------
    inputs
        Dictionary of input values
    parameters
        Dictionary of parameter values
    node
        `Node` instance

    See Also
    --------
    MockChannel : Class simulating the behaviour of normal channels
        without the issues associated with multiple processes

    Examples
    --------
    >>> rig = TestRig(Foo)
    ... rig.set_inputs(inputs=dict(inp="bar"))
    ... rig.set_parameters(parameters=dict(param=42))
    ... rig.setup(n_outputs=2)
    ... outputs = rig.run()

    """

    inputs: dict[str, Any]
    parameters: dict[str, Any]
    node: Node

    def __init__(self, cls: type[Node], config: Config | None = None) -> None:
        self._cls = cls
        self._mock_parent = Workflow(level=logging.DEBUG)
        self._mock_parent._message_queue = queue.Queue()  # type: ignore
        if config is not None:
            self._mock_parent.config = config

    def set_inputs(self, inputs: dict[str, Any] | None) -> None:
        """Set the task input values"""
        self.inputs = inputs if inputs is not None else {}

    def set_parameters(self, parameters: dict[str, Any] | None) -> None:
        """Set the task parameter values"""
        self.parameters = parameters if parameters is not None else {}

    def setup(
        self, n_outputs: int | dict[str, int] | None = None, **kwargs: Any
    ) -> dict[str, MockChannel[Any] | list[MockChannel[Any]]]:
        """Instantiate the node and create mock interfaces."""
        self.node = self._cls(name="test", parent=self._mock_parent, **kwargs)
        self.node.logger = self._mock_parent.logger
        for name, inp in self.node.inputs.items():
            if name not in self.inputs and (inp.default is not None or inp.optional):
                continue
            items = self.inputs[name]
            if isinstance(inp, MultiInput) and isinstance(items, list):
                for item in items:
                    channel: MockChannel[Any] = MockChannel(items=item)
                    inp.set_channel(channel)
            else:
                channel = MockChannel(items=items)
                inp.set_channel(channel)

        for name, parameter in self.node.parameters.items():
            if name in self.parameters:
                parameter.set(self.parameters[name])

        outputs: dict[str, MockChannel[Any] | list[MockChannel[Any]]] = {}
        for name, out in self.node.outputs.items():
            out_channel: list[MockChannel[Any]] | MockChannel[Any]
            if isinstance(out, MultiOutput) and n_outputs is not None:
                n_out = n_outputs if isinstance(n_outputs, int) else n_outputs[name]
                out_channel = []
                for _ in range(n_out):
                    chan = MockChannel[Any]()
                    out.set_channel(chan)
                    out_channel.append(chan)
            else:
                out_channel = MockChannel()
                out.set_channel(out_channel)
            outputs[name] = out_channel

        return outputs

    def run(self) -> None:
        """Run the node with inputs and parameters previously set."""
        self.node._prepare()
        self.node._iter_run(cleanup=False)  # pylint: disable=protected-access

    def setup_run_multi(
        self,
        inputs: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
        n_outputs: int | dict[str, int] | None = None,
        **kwargs: Any,
    ) -> dict[str, MockChannel[Any] | list[MockChannel[Any]]]:
        """
        Instantiate and run the node with a specific set of parameters and inputs.
        Note that this method will potentially return a mix of `MockChannel` and
        `list[MockChannel]`, so your receiving side needs to handle both types correctly.

        Parameters
        ----------
        inputs
            Inputs for your node
        parameters
            Parameters for your node
        n_outputs
            How many outputs to create for `MultiOutput`
        kwargs
            Any additional arguments to pass to the node constructor

        Returns
        -------
        dict[str, MockChannel[Any] | list[MockChannel[Any]]]
            The outputs of the node in the form of channels potentially containing data

        See Also
        --------
        TestRig.setup_run
            Testing for nodes with a fixed number of outputs

        """
        self.set_inputs(inputs)
        self.set_parameters(parameters)
        outputs = self.setup(n_outputs=n_outputs, **kwargs)
        self.run()
        return outputs

    def setup_run(
        self,
        inputs: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, MockChannel[Any]]:
        """
        Instantiate and run the node with a specific set of parameters and inputs.
        If you need variable outputs, use `setup_run_multi` instead.

        Parameters
        ----------
        inputs
            Inputs for your node
        parameters
            Parameters for your node
        kwargs
            Any additional arguments to pass to the node constructor

        Returns
        -------
        dict[str, MockChannel[Any]]
            The outputs of the node in the form of channels potentially containing data

        See Also
        --------
        TestRig.setup_run_multi
            Testing for nodes with a variable number of outputs

        """
        self.set_inputs(inputs)
        self.set_parameters(parameters)
        outputs = self.setup(**kwargs)
        self.run()
        return cast(dict[str, MockChannel[Any]], outputs)
