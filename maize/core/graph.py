"""
Graph
-----
`Graph` is the container for any kind of workflow, and can also act as an
individual component, for example when used as a subgraph. It will contain
multiple nodes, connected together using channels. It can also directly
expose parameters.

Building a graph can be done programatically, or by reading in a suitable
input file.

"""

import itertools
from pathlib import Path
import sys
from typing import Generic, Literal, TypeVar, Any, overload, get_origin

from maize.core.component import Component
from maize.core.channels import DataChannel, Channel, FileChannel
from maize.core.interface import (
    Input,
    Interface,
    MultiPort,
    Output,
    Port,
    Parameter,
    MultiParameter,
    MultiInput,
    MultiOutput,
)
from maize.core.runtime import Status, setup_build_logging
from maize.core.node import Node
from maize.utilities.utilities import extract_type, graph_cycles, is_path_type, matching_types
from maize.utilities.visual import HAS_GRAPHVIZ, nested_graphviz


T = TypeVar("T", covariant=True)
U = TypeVar("U", bound=Component)
ChannelKeyType = tuple[tuple[str, ...], tuple[str, ...]]


def _is_node(node: type[Component]) -> bool:
    """Returns ``True`` if argument is a node class, ``False`` otherwise"""
    if (orig := get_origin(node)) is not None:
        return issubclass(orig, Node)
    return issubclass(node, Node)


class GraphBuildException(Exception):
    """Exception raised for graph build issues."""


class Graph(Component, register=False):
    """
    Represents a graph (or subgraph) consisting of individual components.

    As a user, one will typically instantiate a `Graph` and then add
    individual nodes or subgraphs and connect them together. To construct
    custom subgraphs, create a custom subclass and overwrite the `build`
    method, and add nodes and connections there as normal.

    Attributes
    ----------
    nodes
        Dictionary of nodes or subgraphs part of the `Graph`
    channels
        Dictionary of channels part of the `Graph`
    logger
        Instance of `Logger` used for logging the graph building stage

    Raises
    ------
    GraphBuildException
        If there was an error building the subgraph, e.g. an unconnected port

    """

    _GRAPH_FIELDS = ("name", "description", "level")

    def __init__(
        self, *args: Any, strict: bool = True, default_channel_size: int = 10, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)

        # While nodes can be either a 'Node' or 'Graph'
        # (as a subgraph), we flatten this topology at execution to
        # just provide one large flat graph.
        self.nodes: dict[str, Component] = {}
        self.channels: dict[ChannelKeyType, Channel[Any]] = {}
        self.logger = setup_build_logging(name=self.name, level=self.level)
        self.default_channel_size = default_channel_size
        self.strict = strict

        self.build()
        self.check()

    @property
    def flat_components(self) -> list[Component]:
        """Flattened view of all components in the graph."""
        flat: list[Component] = []
        for node in self.nodes.values():
            flat.append(node)
            if isinstance(node, Graph):
                flat.extend(node.flat_components)
        return flat

    @property
    def flat_nodes(self) -> list[Node]:
        """Flattened view of all nodes in the graph."""
        flat: list[Node] = []
        for node in self.nodes.values():
            if isinstance(node, Graph):
                flat.extend(node.flat_nodes)
            elif isinstance(node, Node):
                flat.append(node)
        return flat

    @property
    def flat_channels(self) -> set[ChannelKeyType]:
        """Flattened view of all connections in the graph."""
        channels = set(self.channels.keys())
        for node in self.nodes.values():
            if isinstance(node, Graph):
                channels |= node.flat_channels
        return channels

    @property
    def active_nodes(self) -> list[Node]:
        """Flattened view of all active nodes in the graph."""
        return [node for node in self.flat_nodes if node.status == Status.READY]

    def setup_directories(self, parent_path: Path | None = None) -> None:
        """Create all work directories for the graph / workflow."""
        if parent_path is None:
            parent_path = self.config.scratch
            if not parent_path.exists():
                parent_path.mkdir(parents=True)
        self.work_dir = Path(parent_path / f"graph-{self.name}")

        # If our graph directory already exists, we increment until we get a fresh one
        i = 0
        while self.work_dir.exists():
            self.work_dir = Path(parent_path / f"graph-{self.name}-{i}")
            i += 1

        self.work_dir.mkdir()
        for comp in self.nodes.values():
            comp.setup_directories(self.work_dir)

        # We have to defer the file channel folder creation until now
        for (_, (*inp_node_path, inp_name)), chan in self.channels.items():
            if isinstance(chan, FileChannel):
                inp_node = self.root.get_node(*inp_node_path)

                # Channel directory structure: graph/channel
                if inp_node.parent is None:
                    raise GraphBuildException(f"Node {inp_node.name} has no parent")
                chan.setup(destination=inp_node.parent.work_dir / f"{inp_node.name}-{inp_name}")

    def get_node(self, *names: str) -> "Component":
        """
        Recursively find a node in the graph.

        Parameters
        ----------
        names
            Names of nodes leading up to the potentially nested target node

        Returns
        -------
        Component
            The target component

        Raises
        ------
        KeyError
            When the target cannot be found

        Examples
        --------
        >>> g.get_node("subgraph", "subsubgraph", "foo")
        Foo(name='foo', parent=SubSubGraph(...))

        """
        root, *children = names
        nested_node = self.nodes[root]
        if isinstance(nested_node, Graph) and children:
            return nested_node.get_node(*children)
        return nested_node

    def get_parameter(self, *names: str) -> Parameter[Any]:
        """
        Recursively find a parameter in the graph.

        Parameters
        ----------
        names
            Names of components leading up to the target parameter

        Returns
        -------
        Parameter
            The target parameter

        Raises
        ------
        KeyError
            When the parameter cannot be found

        """
        *path, name = names
        node = self.get_node(*path)
        if name not in node.parameters:
            raise KeyError(
                f"Can't find parameter '{name}' in node '{node.name}'. "
                f"Available parameters: {list(node.parameters.keys())}"
            )
        return node.parameters[name]

    def get_port(self, *names: str) -> Port[Any]:
        """
        Recursively find a port in the graph.

        Parameters
        ----------
        names
            Names of components leading up to the target port

        Returns
        -------
        Port
            The target port

        Raises
        ------
        KeyError
            When the target cannot be found

        """
        *path, name = names
        node = self.get_node(*path)
        return node.ports[name]

    def check(self) -> None:
        """
        Checks if the graph was built correctly and warns about possible deadlocks.

        A correctly built graph has no unconnected ports,
        and all channel types are matched internally.

        Raises
        ------
        GraphBuildException
            If a port is unconnected

        Examples
        --------
        >>> g = Graph(name="foo")
        ... foo = g.add(Foo)
        ... bar = g.add(Bar)
        ... g.auto_connect(foo, bar)
        ... g.check()

        """
        # Check connectivity first
        for node in self.flat_nodes:
            for name, port in node.ports.items():
                # Subgraphs can have unconnected ports at build time
                if (not port.connected) and (self.parent is None):
                    # Inputs with default values do not need to be connected
                    if isinstance(port, Input) and (port.is_set or port.optional):
                        continue
                    raise GraphBuildException(
                        f"Subgraph '{self.name}' internal port '{name}' "
                        f"of node '{node.name}' was not connected"
                    )

        # Check for deadlock potential by detecting cycles
        cycles = graph_cycles(self)
        if cycles:
            msg = "Cycles found:\n"
            for cycle in cycles:
                msg += "  " + " -> ".join("-".join(c for c in cyc) for cyc in cycle) + "\n"
            self.logger.debug(msg)

    def check_dependencies(self) -> None:
        """Check all contained node dependencies"""
        for node in self.flat_nodes:
            node.check_dependencies()

    def add(
        self,
        component: type[U],
        name: str | None = None,
        parameters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> U:
        """
        Add a component to the graph.

        Parameters
        ----------
        name
            Unique name of the component
        component
            Node class or subgraph class
        kwargs
            Additional arguments passed to the component constructor

        Returns
        -------
        Component
            The initialized component

        Raises
        ------
        GraphBuildException
            If a node with the same name already exists

        Examples
        --------
        >>> g = Graph(name="foo")
        ... foo = g.add(Foo, name="foo", parameters=dict(val=42))
        ... bar = g.add(Bar)

        """
        name = component.__name__.lower() if name is None else str(name)
        if name in self.nodes:
            raise GraphBuildException(f"Node with name {name} already exists in graph")

        # Check that generic nodes are correctly parameterized
        component.datatype = extract_type(component)
        if get_origin(component) is None and issubclass(component, Generic):  # type: ignore
            component.datatype = Any
            msg = (
                f"Node of type '{component.__name__}' is a generic and should use explicit "
                "parameterization. See the 'Generic Nodes' section in the maize user guide."
            )
            if self.strict:
                raise GraphBuildException(msg)
            self.logger.warning(msg)

        comp = component(parent=self, name=name, **kwargs)
        if parameters is not None:
            comp.update_parameters(**parameters)
        self.nodes[name] = comp
        return comp

    # Come on Guido...
    _T1 = TypeVar("_T1", bound=Component)
    _T2 = TypeVar("_T2", bound=Component)
    _T3 = TypeVar("_T3", bound=Component)
    _T4 = TypeVar("_T4", bound=Component)
    _T5 = TypeVar("_T5", bound=Component)
    _T6 = TypeVar("_T6", bound=Component)

    @overload
    def add_all(
        self, c1: type[_T1], c2: type[_T2], c3: type[_T3], c4: type[_T4], /
    ) -> tuple[_T1, _T2, _T3, _T4]:
        ...

    @overload
    def add_all(
        self, c1: type[_T1], c2: type[_T2], c3: type[_T3], c4: type[_T4], c5: type[_T5], /
    ) -> tuple[_T1, _T2, _T3, _T4, _T5]:
        ...

    @overload
    def add_all(
        self,
        c1: type[_T1],
        c2: type[_T2],
        c3: type[_T3],
        c4: type[_T4],
        c5: type[_T5],
        c6: type[_T6],
        /,
    ) -> tuple[_T1, _T2, _T3, _T4, _T5, _T6]:
        ...

    # No way to type this at the moment :(
    def add_all(self, *components: type[Component]) -> tuple[Component, ...]:
        """
        Adds all specified components to the graph.

        Parameters
        ----------
        components
            All component classes to initialize

        Returns
        -------
        tuple[U, ...]
            The initialized component instances

        """
        return tuple(self.add(comp) for comp in components)

    def auto_connect(self, sending: Component, receiving: Component, size: int = 10) -> None:
        """
        Connects component nodes together automatically, based
        on port availability and datatype.

        This should really only be used in unambiguous cases, otherwise
        this will lead to an only partially-connected graph.

        Parameters
        ----------
        sending
            Sending node
        receiving
            Receiving node
        size
            Size (in items) of the queue used for communication

        Examples
        --------
        >>> g = Graph(name="foo")
        ... foo = g.add(Foo)
        ... bar = g.add(Bar)
        ... g.auto_connect(foo, bar)

        """
        for out in sending.outputs.values():
            for inp in receiving.inputs.values():
                # We don't overwrite existing connections
                if not (out.connected or inp.connected) and matching_types(
                    out.datatype, inp.datatype
                ):
                    # The check for mismatched types in 'connect()' is
                    # redundant now, but it's easier this way
                    self.connect(sending=out, receiving=inp, size=size)
                    return

    def chain(self, *nodes: Component, size: int = 10) -> None:
        """
        Connects an arbitrary number of nodes in sequence using `auto_connect`.

        Parameters
        ----------
        nodes
            Nodes to be connected in sequence
        size
            Size of each channel connecting the nodes

        Examples
        --------
        >>> g = Graph(name="foo")
        ... foo = g.add(Foo)
        ... bar = g.add(Bar)
        ... baz = g.add(Baz)
        ... g.chain(foo, bar, baz)

        """
        for sending, receiving in itertools.pairwise(nodes):
            self.auto_connect(sending=sending, receiving=receiving, size=size)

    P = TypeVar("P")
    P1 = TypeVar("P1")
    P2 = TypeVar("P2")
    P3 = TypeVar("P3")
    P4 = TypeVar("P4")
    P5 = TypeVar("P5")

    @overload
    def connect_all(
        self,
        p1: tuple[Output[P] | MultiOutput[P], Input[P] | MultiInput[P]],
        p2: tuple[Output[P1] | MultiOutput[P1], Input[P1] | MultiInput[P1]],
        /,
    ) -> None:
        ...

    @overload
    def connect_all(
        self,
        p1: tuple[Output[P] | MultiOutput[P], Input[P] | MultiInput[P]],
        p2: tuple[Output[P1] | MultiOutput[P1], Input[P1] | MultiInput[P1]],
        p3: tuple[Output[P2] | MultiOutput[P2], Input[P2] | MultiInput[P2]],
        /,
    ) -> None:
        ...

    @overload
    def connect_all(
        self,
        p1: tuple[Output[P] | MultiOutput[P], Input[P] | MultiInput[P]],
        p2: tuple[Output[P1] | MultiOutput[P1], Input[P1] | MultiInput[P1]],
        p3: tuple[Output[P2] | MultiOutput[P2], Input[P2] | MultiInput[P2]],
        p4: tuple[Output[P3] | MultiOutput[P3], Input[P3] | MultiInput[P3]],
        /,
    ) -> None:
        ...

    @overload
    def connect_all(
        self,
        p1: tuple[Output[P] | MultiOutput[P], Input[P] | MultiInput[P]],
        p2: tuple[Output[P1] | MultiOutput[P1], Input[P1] | MultiInput[P1]],
        p3: tuple[Output[P2] | MultiOutput[P2], Input[P2] | MultiInput[P2]],
        p4: tuple[Output[P3] | MultiOutput[P3], Input[P3] | MultiInput[P3]],
        p5: tuple[Output[P4] | MultiOutput[P4], Input[P4] | MultiInput[P4]],
        /,
    ) -> None:
        ...

    @overload
    def connect_all(
        self,
        p1: tuple[Output[P] | MultiOutput[P], Input[P] | MultiInput[P]],
        p2: tuple[Output[P1] | MultiOutput[P1], Input[P1] | MultiInput[P1]],
        p3: tuple[Output[P2] | MultiOutput[P2], Input[P2] | MultiInput[P2]],
        p4: tuple[Output[P3] | MultiOutput[P3], Input[P3] | MultiInput[P3]],
        p5: tuple[Output[P4] | MultiOutput[P4], Input[P4] | MultiInput[P4]],
        p6: tuple[Output[P5] | MultiOutput[P5], Input[P5] | MultiInput[P5]],
        /,
    ) -> None:
        ...

    # Same as for `add_all`: no way to type this
    def connect_all(
        self, *ports: tuple[Output[Any] | MultiOutput[Any], Input[Any] | MultiInput[Any]]
    ) -> None:
        """
        Connect multiple pairs of ports together.

        Parameters
        ----------
        ports
            Output - Input pairs to connect

        """
        for out, inp in ports:
            self.connect(sending=out, receiving=inp)

    def connect(
        self,
        sending: Output[T] | MultiOutput[T],
        receiving: Input[T] | MultiInput[T],
        size: int | None = None,
        mode: Literal["copy", "link", "move"] | None = None,
    ) -> None:
        """
        Connects component inputs and outputs together.

        Parameters
        ----------
        sending
            Output port for sending items
        receiving
            Input port for receiving items
        size
            Size (in items) of the queue used for communication
        mode
            Whether to link, copy or move files, overrides value specified for the port

        Raises
        ------
        GraphBuildException
            If the port types don't match, or the maximum number
            of channels supported by your OS has been reached

        Examples
        --------
        >>> g = Graph(name="foo")
        ... foo = g.add(Foo)
        ... bar = g.add(Bar)
        ... g.connect(foo.out, bar.inp)

        """
        if not matching_types(sending.datatype, receiving.datatype):
            msg = (
                f"Incompatible ports, sender expected '{sending.datatype}'"
                f"got '{receiving.datatype}'"
            )
            raise GraphBuildException(msg)

        if sending.parent.root is not receiving.parent.root:
            msg = (
                "Attempting to connect nodes from separate workflows, "
                f"'{sending.parent.root.name}' sending, '{receiving.parent.root.name}' receiving"
            )
            raise GraphBuildException(msg)

        # Check for accidental duplicate assignments
        for port in (sending, receiving):
            if not isinstance(port, MultiPort) and port.connected:
                raise GraphBuildException(f"Port '{port.name}' is already connected")

        # FIXME This heuristic fails when chaining multiple `Any`-parameterised generic
        # nodes after a `Path`-based node, as the information to use a `FileChannel` will
        # be lost. This originally cropped in the `parallel` macro.
        if is_path_type(sending.datatype) or is_path_type(receiving.datatype):
            # Precedence should be copy > link > move, if one
            # port wants copies we should respect that
            if mode is None:
                if "copy" in (receiving.mode, sending.mode):
                    mode = "copy"
                elif "link" in (receiving.mode, sending.mode):
                    mode = "link"
                else:
                    mode = "move"
            channel: Channel[Any] = FileChannel(mode=mode)
        else:
            size = size if size is not None else self.default_channel_size
            channel = DataChannel(size=size)

        sending.set_channel(channel)
        receiving.set_channel(channel)
        self.logger.debug(
            "Connected '%s' -> '%s' using %s(%s)",
            "-".join(sending.path),
            "-".join(receiving.path),
            channel.__class__.__name__,
            size,
        )
        self.channels[(sending.path, receiving.path)] = channel

    _P = TypeVar("_P", bound=Port[Any])

    def map_port(self, port: _P, name: str | None = None) -> _P:
        """
        Maps a port of a component to the graph.

        This will be required when creating custom subgraphs,
        ports of individual component nodes will need to be
        mapped to the subgraph.

        Parameters
        ----------
        port
            The component port
        name
            Name for the port to be registered as

        Examples
        --------
        >>> def build(self):
        ...     self.node = self.add(Example)
        ...     self.output = self.map_port(self.node.output, "output")

        """
        if name is None:
            name = port.name
        if name in self.ports:
            raise KeyError(f"Port with name '{name}' already exists in graph '{self.name}'")

        if isinstance(port, Input):
            self.inputs[name] = port
        elif isinstance(port, Output):
            self.outputs[name] = port
        setattr(self, name, port)
        return port

    def combine_parameters(
        self, *parameters: Parameter[T], name: str | None = None, default: T | None = None
    ) -> MultiParameter[T]:
        """
        Maps multiple low-level parameters to one high-level one.

        This can be useful when a single parameter needs to be
        supplied to multiple nodes within a subgraph.

        Parameters
        ----------
        parameters
            Low-level parameters of component nodes
        name
            Name of the high-level parameter
        default
            The default parameter value

        Returns
        -------
        MultiParameter
            The combined parameter object

        Examples
        --------
        >>> def build(self):
        ...     self.foo = self.add(Foo)
        ...     self.bar = self.add(Bar)
        ...     self.param = self.map_parameters(
        ...         self.foo.param, self.bar.param, name="param", default=42)

        """
        if name is None:
            name = parameters[0].name

        multi_param: MultiParameter[T] = MultiParameter(
            parameters=parameters, default=default
        ).build(name=name, parent=self)
        self.parameters[name] = multi_param
        setattr(self, name, multi_param)
        return multi_param

    def map(self, *interfaces: Interface[Any]) -> None:
        """
        Map multiple child interfaces onto the current graph.

        Parameters
        ----------
        interfaces
            Any number of ports and parameters to map

        See also
        --------
        Graph.map_parameters
            If you want to map multiple parameters to a single high-level one
        Graph.map_port
            If you want more fine-grained control over naming

        """
        for inter in interfaces:
            if isinstance(inter, Parameter):
                self.combine_parameters(inter)
            elif isinstance(inter, Port):
                self.map_port(inter)
            else:
                raise ValueError(f"'{inter}' is not a valid interface")

    def visualize(
        self,
        max_level: int = sys.maxsize,
        coloring: Literal["nesting", "status"] = "nesting",
        labels: bool = True,
    ) -> Any:
        """
        Visualize the graph using graphviz, if installed.

        Parameters
        ----------
        max_level
            Maximum nesting level to show, shows all levels by default
        coloring
            Whether to color nodes by nesting level or status
        labels
            Whether to show datatype labels

        Returns
        -------
        dot
            Graphviz `Dot` instance, in a Jupyter notebook
            this will be displayed visually automatically

        """
        if HAS_GRAPHVIZ:
            dot = nested_graphviz(self, max_level=max_level, coloring=coloring, labels=labels)
            return dot
        return None

    def build(self) -> None:
        """
        Builds a subgraph.

        Override this method to construct a subgraph encapsulating
        multiple lower-level nodes, by using the `add` and `connect`
        methods. Additionally use the `map_port` and `map_parameters`
        methods to create a subgraph that can be used just like a node.

        """

    # This is for jupyter / ipython, see:
    # https://ipython.readthedocs.io/en/stable/config/integrating.html#MyObject._repr_mimebundle_
    def _repr_mimebundle_(self, *args: Any, **kwargs: Any) -> Any:
        if (dot := self.visualize()) is not None:
            return dot._repr_mimebundle_(*args, **kwargs)  # pylint: disable=protected-access
        return None
