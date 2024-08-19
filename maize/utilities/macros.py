"""Workflow macros to allow certain node and subgraph modifications"""

from collections.abc import Callable, Sequence
import inspect
from typing import Any, TypeVar

from maize.core.component import Component
from maize.core.interface import Input, MultiInput, MultiParameter, Output, Parameter
from maize.core.node import Node
from maize.core.graph import Graph
from maize.core.runtime import setup_build_logging
from maize.steps.plumbing import Copy, Merge, RoundRobin, Yes
from maize.utilities.testing import MockChannel
from maize.utilities.utilities import unique_id


T = TypeVar("T")
G = TypeVar("G", bound=type[Component])


def tag(name: str) -> Callable[[G], G]:
    """
    Tag a `Node` or `Graph` with a particular attribute.

    Parameters
    ----------
    name
        The tag to use

    Returns
    -------
    Callable[[G], G]
        Tagging class decorator

    """

    def decorator(cls: G) -> G:
        cls._tags.add(name)
        return cls

    return decorator


def parallel(
    node_type: type[Component],
    n_branches: int,
    inputs: Sequence[str] | None = None,
    constant_inputs: Sequence[str] | None = None,
    outputs: Sequence[str] | None = None,
    **kwargs: Any,
) -> type[Graph]:
    """
    Workflow macro to parallelize a node. The created subgraph
    will have the exact same interface as the wrapped node.

    Parameters
    ----------
    node_type
        The node class to parallelize
    n_branches
        The number of parallel branches to create
    inputs
        The names of all inputs to parallelize, will use ``'inp'`` as default
    constant_inputs
        The names of all inputs with constant loop/batch-invariant data
    outputs
        The names of all outputs to parallelize, will use ``'out'`` as default
    kwargs
        Additional arguments to be passed to the ``add`` method

    Returns
    -------
    type[Graph]
        A subgraph containing multiple parallel branches of the node

    Examples
    --------
    >>> parallel_node = flow.add(parallel(
    ...     ExampleNode,
    ...     n_branches=3,
    ...     inputs=('inp', 'inp_other'),
    ...     constant_inputs=('inp_const',),
    ...     outputs=('out',)
    ... ))
    >>> flow.connect_all(
    ...     (input_node.out, parallel_node.inp),
    ...     (other_input_node.out, parallel_node.inp_other),
    ...     (const_input.out, parallel_node.inp_const),
    ...     (parallel_node.out, output_node.inp),
    ... )

    """
    input_names = ["inp"] if inputs is None else inputs
    output_names = ["out"] if outputs is None else outputs
    constant_input_names = constant_inputs or []

    # PEP8 convention dictates CamelCase for classes
    new_name = _snake2camel(node_type.__name__) + "Parallel"
    attrs: dict[str, Any] = {}

    def build(self: Graph) -> None:
        # Because our node can have multiple inputs and outputs
        # (which we want to all parallelize), we create one RR
        # node for each input (and one Merge node for each output)
        sowers = [self.add(RoundRobin[Any], name=f"sow-{name}") for name in input_names]

        # These are inputs that are constant and unchanging over all nodes
        constant_sowers = [
            self.add(Copy[Any], name=f"sow-const-{name}") for name in constant_input_names
        ]

        # Copy expects input for each loop, but because the content is constant,
        # we can accept it once (and then send it over and over with `Yes`)
        yes_relays = [self.add(Yes[Any], name=f"yes-{name}") for name in constant_input_names]
        nodes = [
            self.add(node_type, name=f"{node_type.__name__}-{i}", **kwargs)
            for i in range(n_branches)
        ]
        reapers = [self.add(Merge[Any], name=f"reap-{name}") for name in output_names]

        # Connect all ports to each separate RR / Merge node
        for node in nodes:
            for inp, sow in zip(input_names, sowers):
                self.connect(sow.out, node.inputs[inp])
            for const_inp, const_sow in zip(constant_input_names, constant_sowers):
                self.connect(const_sow.out, node.inputs[const_inp])
            for out, reap in zip(output_names, reapers):
                self.connect(node.outputs[out], reap.inp)

        # Connect the `Yes` relay nodes
        for const_sow, yes in zip(constant_sowers, yes_relays):
            self.connect(yes.out, const_sow.inp)

        # Expose all I/O ports
        for inp, sow in zip(input_names, sowers):
            inp_port = self.map_port(sow.inp, name=inp)
            setattr(self, inp, inp_port)
        for out, reap in zip(output_names, reapers):
            out_port = self.map_port(reap.out, name=out)
            setattr(self, out, out_port)
        for name, yes in zip(constant_input_names, yes_relays):
            inp_port = self.map_port(yes.inp, name=name)
            setattr(self, name, inp_port)

        # Expose all parameters
        for name in nodes[0].parameters:
            para: MultiParameter[Any, Any] = self.combine_parameters(
                *(node.parameters[name] for node in nodes), name=name
            )
            setattr(self, name, para)

    attrs["build"] = build
    return type(new_name, (Graph,), attrs, register=False)


def lambda_node(func: Callable[[Any], Any]) -> type[Node]:
    """
    Convert an anonymous function with single I/O into a node.

    Parameters
    ----------
    func
        Lambda function taking a single argument and producing a single output

    Returns
    -------
    type[Node]
        Custom lambda wrapper node

    Examples
    --------
    >>> lam = flow.add(lambda_node(lambda x: 2 * x))
    >>> flow.connect_all((first.out, lam.inp), (lam.out, last.inp))

    """
    new_name = f"lambda-{unique_id()}"

    def run(self: Node) -> None:
        assert hasattr(self, "inp")
        assert hasattr(self, "out")
        data = self.inp.receive()
        res = func(data)
        self.out.send(res)

    attrs = {"inp": Input(), "out": Output(), "run": run}
    return type(new_name, (Node,), attrs, register=False)


def _snake2camel(string: str) -> str:
    """Converts a string from snake_case to CamelCase"""
    return "".join(s.capitalize() for s in string.split("_"))


def function_to_node(func: Callable[..., Any]) -> type[Node]:
    """
    Dynamically creates a new node type from an existing python function.

    Parameters
    ----------
    func
        Function to convert, ``args`` will be converted to inputs, ``kwargs``
        will be converted to parameters. A single return will be converted to
        one output port.

    Returns
    -------
    type[Node]
        Node class

    """
    # PEP8 convention dictates CamelCase for classes
    new_name = _snake2camel(func.__name__)

    # Prepare all positional arguments (= inputs) and
    # keyword arguments (= parameters with default)
    sig = inspect.signature(func)
    args, kwargs = {}, {}
    for name, arg in sig.parameters.items():
        if arg.default == inspect._empty:  # pylint: disable=protected-access
            args[name] = arg.annotation
        else:
            kwargs[name] = (arg.annotation, arg.default)

    attrs: dict[str, Any] = {}

    # Prepare inputs
    for name, dtype in args.items():
        input_name = f"inp_{name}" if len(args) > 1 else "inp"
        inp: Input[Any] = Input()
        inp.datatype = dtype
        attrs[input_name] = inp

    # Prepare output
    out: Output[Any] = Output()
    out.datatype = sig.return_annotation
    attrs["out"] = out

    # Prepare parameters
    for name, (dtype, default) in kwargs.items():
        param: Parameter[Any] = Parameter(default=default)
        param.datatype = dtype
        attrs[name] = param

    def run(self: Node) -> None:
        assert hasattr(self, "out")
        # Inputs will not contain MultiInput in this context, so this is safe
        args = [inp.receive() for inp in self.inputs.values()]  # type: ignore
        kwargs = {
            name: param.value
            for name, param in self.parameters.items()
            if name not in Node.__dict__
        }
        res = func(*args, **kwargs)
        self.out.send(res)

    attrs["run"] = run
    new = type(new_name, (Node,), attrs)
    return new


def node_to_function(cls: type[Node], **constructor_args: Any) -> Callable[..., dict[str, Any]]:
    """
    Convert a node class to a function that takes
    inputs and parameters as function arguments.

    Parameters
    ----------
    cls
        The node class (not instance)

    Returns
    -------
    Callable[..., dict[str, Any]]
        A function taking inputs and parameters as keyword arguments
        and returning a dictionary with outputs

    """
    node = cls(name="test", parent=Graph(), **constructor_args)

    def inner(**kwargs: Any) -> dict[str, Any]:
        for name, inp in node.inputs.items():
            items = kwargs[name]
            if isinstance(inp, MultiInput) and isinstance(items, list):
                for item in items:
                    channel: MockChannel[Any] = MockChannel(items=item)
                    inp.set_channel(channel)
            else:
                channel: MockChannel[Any] = MockChannel(items=items)  # type: ignore
                inp.set_channel(channel)

        for name, parameter in node.parameters.items():
            if name in kwargs:
                parameter.set(kwargs[name])

        outputs: dict[str, Any] = {}
        for name, out in node.outputs.items():
            channel = MockChannel()
            out.set_channel(channel)
            outputs[name] = channel

        node.logger = setup_build_logging(name="test")
        node.run()
        return {k: chan.get() for k, chan in outputs.items()}

    return inner
