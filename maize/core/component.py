"""
Component
---------
Provides a component class acting as the base for a graph or a component (node) thereof.

`Component` is used as a base class for both `Node` and `Graph` and represents a
hierarchical component with a ``parent`` (if it's not the root node). It should
not be used directly. The workflow is internally represented as a tree, with the
`Workflow` representing the root node owning all other nodes. Leaf nodes are termed
`Node` and represent atomic workflow steps. Nodes with branches are (Sub-)`Graph`s,
as they contain multiple nodes, but expose the same interface that a would:

.. code-block:: text

          Workflow
          /      \
      Subgraph   Node
      /     \
    Node   Node

"""

from collections.abc import Generator, Sequence
from functools import reduce
import inspect
import itertools
import logging
from multiprocessing import get_context
from pathlib import Path
from traceback import TracebackException
from typing import (
    TYPE_CHECKING,
    Optional,
    Any,
    ClassVar,
    TypeAlias,
    Union,
    Literal,
    cast,
    get_origin,
)
from typing_extensions import TypedDict

from maize.core.interface import (
    FileParameter,
    Input,
    Interface,
    MultiInput,
    MultiOutput,
    MultiParameter,
    Output,
    Port,
    Parameter,
    MultiPort,
)
import maize.core.interface as _in
from maize.core.runtime import Status, StatusHandler, StatusUpdate
from maize.utilities.execution import DEFAULT_CONTEXT
from maize.utilities.resources import Resources, cpu_count, gpu_count
from maize.utilities.utilities import Timer, extract_superclass_type, extract_type, unique_id
from maize.utilities.io import Config, NodeConfig, with_fields

# https://github.com/python/typeshed/issues/4266
if TYPE_CHECKING:
    from logging import LogRecord
    from multiprocessing import Queue
    from multiprocessing.synchronize import Event as EventClass
    from maize.core.graph import Graph

    MessageType: TypeAlias = StatusUpdate | None


class SerialType(TypedDict):
    name: str
    tags: set[str]
    inputs: list[dict[str, Any]]
    outputs: list[dict[str, Any]]
    parameters: list[dict[str, Any]]


def merge_serialized_summaries(a: SerialType, b: SerialType) -> SerialType:
    """Merges two serialized summaries"""
    for key in ("inputs", "outputs", "parameters"):
        # Kind-of a false positive, see:
        # https://github.com/python/mypy/commit/8290bb81db80b139185a3543bd459f904841fe44?_sm_nck=1
        a[key].extend(b[key])  # type: ignore
    return a


def serialized_summary(cls: Union[type["Component"], "Component"], name: str) -> SerialType:
    """
    Provides a serialized representation of the component type or instance.

    Returns
    -------
    dict[str, Any]
        Nested dictionary of the component type structure, including I/O and parameters.

    """

    result: SerialType = {
        "name": name,
        # Check here is just for backwards compatibility
        "tags": cls.tags if hasattr(cls, "tags") else set(),
        "inputs": [],
        "outputs": [],
        "parameters": [],
    }

    for name, attr in cls.__dict__.items():
        if not isinstance(attr, Interface):
            continue

        # Extracting type information from within a non-instantiated interface isn't
        # possible, so we have to get it from the outside (i.e. here) instead
        data = {"name": name} | attr.serialized
        data["type"] = extract_type(attr) or extract_superclass_type(cls, name)
        match attr:
            case Input() | MultiInput():
                result["inputs"].append(data)
            case Output() | MultiOutput():
                result["outputs"].append(data)
            case Parameter() | MultiParameter() | FileParameter():
                result["parameters"].append(data)

    return result


class Component:
    """
    Base class for all components. Should not be used directly.

    Parameters
    ----------
    parent
        Parent component, typically the graph in context
    name
        The name of the component
    description
        An optional additional description
    fail_ok
        If True, the failure in the component will
        not trigger the whole network to shutdown
    n_attempts
        Number of attempts at executing the `run()` method
    level
        Logging level, if not given or ``None`` will use the parent logging level
    cleanup_temp
        Whether to remove any temporary directories after completion
    resume
        Whether to resume from a previous checkpoint
    logfile
        File to output all log messages to, defaults to STDOUT
    max_cpus
        Maximum number of CPUs to use, defaults to the number of available cores in the system
    max_gpus
        Maximum number of GPUs to use, defaults to the number of available GPUs in the system
    loop
        Whether to run the `run` method in a loop, as opposed to a single time

    See Also
    --------
    node.Node
        Node class for implementing custom tasks
    graph.Graph
        Graph class to group nodes together to form a subgraph
    workflow.Workflow
        Workflow class to group nodes and graphs together to an executable workflow

    """

    # By default, we keep track of all defined nodes and subgraphs so that we can instantiate
    # them from serialized workflow definitions in YAML, JSON, or TOML format. Importing the
    # node / graph definition is enough to make them available.
    __registry: ClassVar[dict[str, type["Component"]]] = {}

    def __init_subclass__(cls, name: str | None = None, register: bool = True):
        if name is None:
            name = cls.__name__.lower()
        if register:
            Component.__registry[name] = cls
        super().__init_subclass__()

    @classmethod
    def _generate_sample_config(cls, name: str) -> None:
        """Generates a sample config to be concatenated with the docstring."""
        if cls.__doc__ is not None and cls.required_callables:
            conf = NodeConfig().generate_template_toml(name, cls.required_callables)
            conf = "\n".join(f"       {c}" for c in conf.split("\n"))
            cls.__doc__ += (
                f"\n    .. rubric:: Config example\n\n    Example configuration for {name}. "
                f"Each required callable only requires either ``scripts`` "
                f"OR ``commands``.\n\n    .. code-block:: toml\n\n{conf}\n"
            )

    @classmethod
    def serialized_summary(cls) -> SerialType:
        """
        Provides a serialized representation of the component type.

        Returns
        -------
        dict[str, Any]
            Nested dictionary of the component type structure, including I/O and parameters.

        Examples
        --------
        >>> Merge.serialized_summary()
        {"name": "Merge", "inputs": [{"name": "inp", ...}]}

        """
        current = serialized_summary(cls, name=cls.__name__)
        bases: list[SerialType] = [
            base.serialized_summary()
            for base in cls.__bases__
            if hasattr(base, "serialized_summary")
        ]
        return reduce(merge_serialized_summaries, [current, *bases])

    @classmethod
    def get_summary_line(cls) -> str:
        """Provides a one-line summary of the node."""
        if cls.__doc__ is not None:
            for line in cls.__doc__.splitlines():
                if line:
                    return line.lstrip()
        return ""

    @classmethod
    def get_interfaces(
        cls, kind: Literal["input", "output", "parameter"] | None = None
    ) -> set[str]:
        """
        Returns all interfaces available to the node.

        Parameters
        ----------
        kind
            Kind of interface to retrieve

        Returns
        -------
        set[str]
            Interface names

        """
        inter = set()
        for name, attr in (cls.__dict__ | cls.__annotations__).items():
            match get_origin(attr), kind:
                case ((_in.Input | _in.MultiInput), "input" | None):
                    inter.add(name)
                case ((_in.Output | _in.MultiOutput), "output" | None):
                    inter.add(name)
                case ((_in.Parameter | _in.MultiParameter | _in.FileParameter), "parameter" | None):
                    inter.add(name)
        return inter

    @classmethod
    def get_inputs(cls) -> set[str]:
        """Returns all inputs available to the node."""
        return cls.get_interfaces(kind="input")

    @classmethod
    def get_outputs(cls) -> set[str]:
        """Returns all outputs available to the node."""
        return cls.get_interfaces(kind="output")

    @classmethod
    def get_parameters(cls) -> set[str]:
        """Returns all parameters available to the node."""
        return cls.get_interfaces(kind="parameter")

    __checked: bool = False
    """Whether a node of a certain name has been checked for runnability with `prepare()`"""

    @classmethod
    def set_checked(cls) -> None:
        """Set a node as checked, to avoid duplicate runs of `prepare()`"""
        cls.__checked = True

    @classmethod
    def is_checked(cls) -> bool:
        """``True`` if a node has been checked, to avoid duplicate runs of `prepare()`"""
        return cls.__checked

    @staticmethod
    def get_available_nodes() -> set[type["Component"]]:
        """
        Returns all available and registered nodes.

        Returns
        -------
        set[str]
            All available node names

        """
        return set(Component.__registry.values())

    @staticmethod
    def get_node_class(name: str) -> type["Component"]:
        """
        Returns the node class corresponding to the given name.

        Parameters
        ----------
        name
            Name of the component class to retrieve

        Returns
        -------
        Type[Component]
            The retrieved component class, can be passed to `add_node`

        """
        try:
            return Component.__registry[name.lower()]
        except KeyError as err:
            raise KeyError(
                f"Node of type {name.lower()} not found in the registry. "
                "Have you imported the node class definitions?"
            ) from err

    _COMPONENT_FIELDS = {"name", "description", "fail_ok", "n_attempts", "n_inputs", "n_outputs"}

    tags: ClassVar[set[str]] = set()
    """Tags to identify the component as exposing a particular kind of interface"""

    required_callables: ClassVar[list[str]] = []
    """List of external commandline programs that are required for running the component."""

    required_packages: ClassVar[list[str]] = []
    """List of required python packages"""

    logger: logging.Logger
    """Python logger for both the build and run procedures."""

    run_timer: Timer
    """Timer for the run duration, without waiting for resources or other nodes."""

    full_timer: Timer
    """Timer for the full duration, including waiting for resources or other nodes."""

    work_dir: Path
    """Working directory for the component."""

    datatype: Any = None
    """The component datatype if it's generic."""

    status = StatusHandler()
    """Current status of the component."""

    def __init__(
        self,
        parent: Optional["Graph"] = None,
        name: str | None = None,
        description: str | None = None,
        fail_ok: bool = False,
        n_attempts: int = 1,
        level: int | str | None = None,
        cleanup_temp: bool = True,
        scratch: Path | None = None,
        resume: bool = False,
        logfile: Path | None = None,
        max_cpus: int | None = None,
        max_gpus: int | None = None,
        loop: bool | None = None,
    ) -> None:
        self.name = str(name) if name is not None else unique_id()
        ctx = get_context(DEFAULT_CONTEXT)

        if parent is None:
            # This is the queue for communication with the main process, it will
            # allow exceptions to be raised, and shutdown tokens to be raised
            # Pylint freaks out here, see: https://github.com/PyCQA/pylint/issues/3488
            self._message_queue: "Queue[MessageType]" = ctx.Queue()

            # This is the logging-only queue
            self._logging_queue: "Queue[LogRecord | None]" = ctx.Queue()

            # This will signal the whole graph to shutdown gracefully
            self.signal: "EventClass" = ctx.Event()

            # Special considerations apply when resuming, especially for channel handling
            self.resume = resume

            # Global config
            self.config = Config.from_default()

            # Cleanup all working directories
            self.cleanup_temp = cleanup_temp

            # Overwrite default location
            self.scratch = Path(scratch) if scratch is not None else self.config.scratch

            # Optional logfile
            self.logfile = logfile

            # Whether to loop the `run` method for itself (nodes) or child nodes (graph)
            self.looped = loop if loop is not None else False

            # Resource management
            self.cpus = Resources(max_count=max_cpus or cpu_count(), parent=self)
            self.gpus = Resources(max_count=max_gpus or gpu_count(), parent=self)

        else:
            self._message_queue = parent._message_queue
            self._logging_queue = parent._logging_queue
            self.signal = parent.signal
            self.resume = parent.resume
            self.config = parent.config
            self.cleanup_temp = parent.cleanup_temp
            self.scratch = parent.scratch
            self.logfile = parent.logfile
            self.cpus = parent.cpus
            self.gpus = parent.gpus
            self.looped = parent.looped if loop is None else loop

        # Logging level follows the parent graph by default, but can be overridden
        if level is None:
            level = logging.INFO if parent is None or parent.level is None else parent.level
        self.level: int | str = level.upper() if isinstance(level, str) else level

        # Temporary working directory path to allow some basic pre-execution uses
        self.work_dir = Path("./")

        # Prepared commands
        self.runnable: dict[str, str] = {}

        self.parent = parent
        self.description = description
        self.fail_ok = fail_ok
        self.n_attempts = n_attempts

        # Both atomic components and subgraphs can have ports / parameters
        self.inputs: dict[str, Input[Any] | MultiInput[Any]] = {}
        self.outputs: dict[str, Output[Any] | MultiOutput[Any]] = {}
        self.parameters: dict[str, Parameter[Any]] = {}
        self.status = Status.READY

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', parent={self.parent})"

    def __lshift__(self, other: "Component") -> Union["Component", None]:
        if self.parent is not None:
            self.parent.auto_connect(receiving=self, sending=other)
            return self
        return None  # pragma: no cover

    def __rshift__(self, other: "Component") -> Union["Component", None]:
        if self.parent is not None:
            self.parent.auto_connect(receiving=other, sending=self)
            return other
        return None  # pragma: no cover

    def _init_spec(self, cls: type["Component"]) -> set[str]:
        """Gets initializable attributes for a class"""
        spec = inspect.getfullargspec(cls)
        if spec.defaults is None:
            return set(spec.args[1:])
        serializable = set()
        for arg, default_value in zip(reversed(spec.args), reversed(spec.defaults)):
            if hasattr(self, arg) and getattr(self, arg) != default_value:
                serializable.add(arg)
        return serializable

    @property
    def _serializable_attributes(self) -> set[str]:
        """Gets all serializable attributes that have not been set to their default value"""

        def _flatten(items: Sequence[Any]) -> Sequence[Any]:
            res: list[Any] = []
            for item in items:
                if hasattr(item, "__len__"):
                    res.extend(_flatten(item))
                else:
                    res.append(item)
            return res

        serializable = set()
        for cls in _flatten(inspect.getclasstree([self.__class__])):
            serializable |= self._init_spec(cls)
        return serializable

    if not TYPE_CHECKING:

        @property
        def scratch(self) -> Path:
            """The directory to run the graph in"""
            return self.config.scratch

        @scratch.setter
        def scratch(self, path: Path) -> None:
            self.config.scratch = path

    @property
    def ports(self) -> dict[str, Port[Any]]:
        """Provides a convenience iterator for all inputs and outputs."""
        return cast(dict[str, Port[Any]], self.inputs | self.outputs)

    @property
    def flat_inputs(self) -> Generator[Input[Any], None, None]:
        """Provides a flattened view of all node inputs, recursing into MultiInput"""
        for port in self.inputs.values():
            subports: list[Input[Any]] | MultiInput[Any] = (
                port if isinstance(port, MultiInput) else [port]
            )
            yield from subports

    @property
    def flat_outputs(self) -> Generator[Output[Any], None, None]:
        """Provides a flattened view of all node inputs, recursing into MultiInput"""
        for port in self.outputs.values():
            subports: list[Output[Any]] | MultiOutput[Any] = (
                port if isinstance(port, MultiOutput) else [port]
            )
            yield from subports

    @property
    def parents(self) -> tuple["Component", ...] | None:
        """Provides all parent components."""
        if self.parent is None:
            return None
        if self.parent.parents is None:
            return (self.parent,)
        return *self.parent.parents, self.parent

    @property
    def component_path(self) -> tuple[str, ...]:
        """Provides the full path to the component as a tuple of names."""
        if self.parents is None:
            return (self.name,)
        _, *parent_names = tuple(p.name for p in self.parents)
        return *parent_names, self.name

    @property
    def root(self) -> "Graph":
        """Provides the root workflow or graph instance."""
        if self.parents is None:
            return cast("Graph", self)
        root, *_ = self.parents
        return cast("Graph", root)

    @property
    def node_config(self) -> NodeConfig:
        """Provides the configuration of the current node"""
        return self.config.nodes.get(self.__class__.__name__.lower(), NodeConfig())

    @property
    def n_outbound(self) -> int:
        """Returns the number of items waiting to be sent"""
        return sum(out.size for out in self.outputs.values())

    @property
    def n_inbound(self) -> int:
        """Returns the number of items waiting to be received"""
        return sum(inp.size for inp in self.inputs.values())

    @property
    def all_parameters(self) -> dict[str, Input[Any] | MultiInput[Any] | Parameter[Any]]:
        """Returns all settable parameters and unconnected inputs"""
        inp_params = {name: inp for name, inp in self.inputs.items() if not inp.is_connected(inp)}
        return self.parameters | inp_params

    @property
    def mapped_parameters(self) -> list[Parameter[Any] | Input[Any]]:
        """Returns all parameters that have been mapped"""
        return list(
            itertools.chain.from_iterable(
                para._parameters
                for para in self.all_parameters.values()
                if isinstance(para, MultiParameter)
            )
        )

    def setup_directories(self, parent_path: Path | None = None) -> None:
        """Sets up the required directories."""
        if parent_path is None:
            parent_path = Path("./")
        self.work_dir = Path(parent_path / f"comp-{self.name}")
        self.work_dir.mkdir()

    def as_dict(self) -> dict[str, Any]:
        """Provides a non-recursive dictionary view of the component."""
        data = with_fields(self, self._COMPONENT_FIELDS & self._serializable_attributes)
        data["parameters"] = {
            k: para.value
            for k, para in self.parameters.items()
            if para.is_set and not para.is_default
        }
        data["parameters"] |= {
            k: inp.value
            for k, inp in self.inputs.items()
            if isinstance(inp, Input) and not inp.is_connected(inp) and inp.active
        }
        data["type"] = self.__class__.__name__
        data["status"] = self.status.name

        # Remove potentially unneccessary keys
        if data["status"] == self.status.READY.name:
            data.pop("status")
        if data["parameters"] == {}:
            data.pop("parameters")
        return data

    def send_update(self, exception: TracebackException | None = None) -> None:
        """Send a status update to the main process."""
        summary = StatusUpdate(
            name=self.name,
            parents=self.component_path,
            status=self.status,
            run_time=self.run_timer.elapsed_time,
            full_time=self.full_timer.elapsed_time,
            n_inbound=self.n_inbound,
            n_outbound=self.n_outbound,
            exception=exception,
        )
        self._message_queue.put(summary)

    def update_parameters(self, **kwargs: dict[str, Any]) -> None:
        """
        Update component parameters.

        Parameters
        ----------
        **kwargs
            Name - value pairs supplied as keyword arguments

        """
        for key, value in kwargs.items():
            if key not in self.all_parameters:
                raise KeyError(
                    f"Parameter '{key}' not found in component parameters\n"
                    f"  Available parameters: '{self.all_parameters.keys()}'"
                )
            if value is not None:
                self.all_parameters[key].set(value)

    def ports_active(self) -> bool:
        """
        Check if all required ports are active.

        Can be overridden by the user to allow custom shutdown scenarios,
        for example in the case of complex inter-port dependencies. By
        default only checks if any mandatory ports are inactive.

        Returns
        -------
        bool
            ``True`` if all required ports are active, ``False`` otherwise.

        """
        for subport in itertools.chain(self.flat_inputs, self.flat_outputs):
            if (not subport.active) and (not subport.optional):
                return False
        return True
