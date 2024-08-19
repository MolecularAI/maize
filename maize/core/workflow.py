"""
Workflow
--------
The top-level graph class, i.e. the root node. Subclasses from `Graph`
to add checkpointing, file IO, and execution orchestration.

"""

import argparse
import builtins as _b
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import functools
import inspect
import itertools
import logging
from pathlib import Path
import queue
import shutil
import time
from traceback import TracebackException
import typing
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    TypeVar,
    cast,
    get_args,
    get_origin,
)

import dill
import toml

from maize.core.component import Component
from maize.core.graph import Graph, GraphBuildException
from maize.core.interface import (
    Input,
    Output,
    MultiParameter,
    MultiInput,
    MultiOutput,
    ParameterException,
)
from maize.core.runtime import (
    Logger,
    RunPool,
    NodeException,
    Spinner,
    StatusUpdate,
    Status,
    format_summaries,
    format_update,
    MAIZE_ISO,
    setup_build_logging,
)
from maize.utilities.utilities import (
    Timer,
    nested_dict_to_tuple,
    tuple_to_nested_dict,
    NestedDict,
)
from maize.utilities.io import (
    DictAction,
    NodeConfig,
    args_from_function,
    create_default_parser,
    parse_groups,
    with_fields,
    with_keys,
    read_input,
    write_input,
)

from maize.utilities.execution import (
    JobResourceConfig,
    WorkflowStatus,
    ProcessError,
    CommandRunner,
    BatchSystemType,
    job_from_id,
)

if TYPE_CHECKING:
    from multiprocessing import Queue
    from maize.core.component import MessageType


TIME_STEP_SEC = 0.1
CHECKPOINT_INTERVAL_MIN = 15
STATUS_INTERVAL_MIN = 0.5

T = TypeVar("T")


def _get_message(message_queue: "Queue[T]", timeout: float = 0.1) -> T | None:
    try:
        return message_queue.get(timeout=timeout)
    except queue.Empty:
        return None


class ParsingException(Exception):
    """Exception raised for graph definition parsing errors."""


class CheckpointException(Exception):
    """Exception raised for checkpointing issues."""


def expose(factory: Callable[..., "Workflow"]) -> Callable[[], None]:
    """
    Exposes the workflow definition and sets it up for execution.

    Parameters
    ----------
    factory
        A function that returns a fully defined workflow

    Returns
    -------
    Callable[[], None]
        Runnable workflow

    """
    name = factory.__name__.lower()
    Workflow.register(name=name, factory=factory)

    def wrapped() -> None:
        # Argument parsing - we create separate groups for
        # global settings and workflow specific options
        parser = create_default_parser(help=False)

        # We first construct the parser for the workflow factory - this allows
        # us to parse args before constructing the workflow object, thus
        # allowing us to influence the construction process
        parser = args_from_function(parser, factory)

        # Parse only the factory args, everything else will be for the workflow.
        # General maize args will show up in `known`, so they need to be filtered
        known, rem = parser.parse_known_args()
        func_args = inspect.getfullargspec(factory).annotations
        func_args.pop("return")

        # Create the workflow, using only the relevant factory args
        workflow = factory(
            **{key: vars(known)[key] for key in func_args if vars(known)[key] is not None}
        )
        workflow.description = Workflow.get_workflow_summary(name=name)
        parser.description = workflow.description

        # Now create workflow-specific settings
        flow = parser.add_argument_group(workflow.name)
        flow = workflow.add_arguments(flow)

        # Help is the last arg, this allows us to create a help message dynamically
        parser.add_argument("-h", "--help", action="help")
        groups = parse_groups(parser, extra_args=rem)

        # Finally parse global settings
        args = groups["maize"]
        workflow.update_settings_with_args(args)
        workflow.update_settings_with_args(known)
        workflow.update_parameters(**vars(groups[workflow.name]))

        # Execution
        workflow.check()
        if args.check:
            workflow.logger.info("Workflow compiled successfully")
            return

        workflow.execute()

    return wrapped


@dataclass
class FutureWorkflowResult:
    """
    Represents the result of the workflow over the duration of its execution and afterwards.

    Parameters
    ----------
    id
        The native ID of the job given by the batch system upon submission
    folder
        The folder the workflow is running in
    workflow
        The workflow definition in serialized form
    backend
        The type of batch system used for running the job

    """

    id: str
    folder: Path
    workflow: dict[str, Any]
    backend: BatchSystemType
    stdout_path: Path | None
    stderr_path: Path | None

    def __post_init__(self) -> None:
        # To be able to interact with the associated job after serializing
        # this object, we need to re-create the psij `Job` object
        self._job = job_from_id(self.id, backend=self.backend)

    def to_dict(self) -> dict[str, Any]:
        """
        Serializes the result to a dictionary.

        Returns
        -------
        dict[str, Any]
            A dictionary representation of the result object

        """
        return {
            "id": self.id,
            "folder": self.folder,
            "workflow": self.workflow,
            "backend": self.backend,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], **kwargs: Any) -> "FutureWorkflowResult":
        """
        Create a result object from a dictionary representation.

        Parameters
        ----------
        data
            A dictionary representation of the result object

        Returns
        -------
        FutureWorkflowResult
            The result object

        """
        return cls(**data, **kwargs)

    def wait(self) -> WorkflowStatus:
        """
        Wait for workflow completion.

        Returns
        -------
        WorkflowStatus
            The status of the workflow at the final state

        """
        status = self._job.wait()
        if status is None:
            return WorkflowStatus.UNKNOWN
        return WorkflowStatus.from_psij(status.state)

    def query(self) -> WorkflowStatus:
        """
        Query the workflow status from the batch system.

        Returns
        -------
        WorkflowStatus
            The status of the workflow

        """
        return WorkflowStatus.from_psij(self._job.status.state)

    def done(self) -> bool:
        """
        Checks whether the workflow has finished.

        Returns
        -------
        bool
            ``True`` if the workflow is in a completed or failed state

        """
        return self.query() in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED)

    def cancel(self) -> None:
        """Cancel the workflow execution"""
        self._job.cancel()


def wait_for_all(results: list[FutureWorkflowResult], timeout: float | None = None) -> None:
    """
    Waits for all listed workflows to complete.

    Parameters
    ----------
    results
        A list of ``FutureWorkflowResult`` objects
    timeout
        Maximum time to wait for completion

    """
    start = time.time()
    while any(not res.done() for res in results):
        time.sleep(5)
        if timeout is not None and (time.time() - start) >= timeout:
            for res in results:
                res.cancel()


class Workflow(Graph, register=False):
    """
    Represents a workflow graph consisting of individual components.

    As a user, one will typically instantiate a `Workflow` and then add
    individual nodes or subgraphs and connect them together.

    """

    __registry: ClassVar[dict[str, Callable[[], "Workflow"]]] = {}

    @classmethod
    def register(cls, name: str, factory: Callable[[], "Workflow"]) -> None:
        """
        Register a workflow for global access.

        Parameters
        ----------
        name
            Name to register the workflow under
        factory
            Function returning an initialized and built workflow

        """
        Workflow.__registry[name.lower()] = factory

    @staticmethod
    def get_workflow_summary(name: str) -> str:
        """Provides a one-line summary of the workflow."""
        factory = Workflow.__registry[name.lower()]
        if factory.__doc__ is not None:
            for line in factory.__doc__.splitlines():
                if line:
                    return line.lstrip()
        return ""

    @staticmethod
    def get_available_workflows() -> set[Callable[[], "Workflow"]]:
        """
        Returns all available and registered / exposed workflows.

        Returns
        -------
        set[Callable[[], "Workflow"]]
            All available workflow factories

        """
        return set(Workflow.__registry.values())

    @classmethod
    def from_name(cls, name: str) -> "Workflow":
        """
        Create a predefined workflow from a previously registered name.

        Parameters
        ----------
        name
            Name the workflow is registered under

        Returns
        -------
        Workflow
            The constructed workflow, with all nodes and channels

        Raises
        ------
        KeyError
            If a workflow under that name cannot be found

        """
        try:
            flow = Workflow.__registry[name.lower()]()
        except KeyError as err:
            raise KeyError(
                f"Workflow with name '{name.lower()}' not found in the registry. "
                "Have you imported the workflow definitions?"
            ) from err
        return flow

    @classmethod
    def from_dict(cls, data: dict[str, Any], **kwargs: Any) -> "Workflow":
        """
        Read a graph definition from a dictionary parsed from a suitable serialization format.

        Parameters
        ----------
        data
            Tree structure as a dictionary containing graph and node parameters,
            as well as connectivity. For format details see the `read_input` method.
        kwargs
            Additional arguments passed to the `Component` constructor

        Returns
        -------
        Graph
            The constructed graph, with all nodes and channels

        Raises
        ------
        ParsingException
            If the input dictionary doesn't conform to the expected format

        """
        # Create the graph and fill it with nodes
        graph = cls(**with_keys(data, cls._GRAPH_FIELDS), strict=False, **kwargs)
        for node_data in data["nodes"]:
            if "type" not in node_data:
                raise ParsingException("Node specification requires at least 'type'")

            node_type: type[Component] = Component.get_node_class(node_data["type"])
            init_data = with_keys(
                node_data,
                cls._COMPONENT_FIELDS
                | {
                    "parameters",
                },
            )
            if "name" not in init_data:
                init_data["name"] = node_type.__name__.lower()
            graph.add(**init_data, component=node_type)

        # Check the parameter field for potential mappings from node-specific
        # parameters to global ones, these can then be set on the commandline
        for param_data in data.get("parameters", []):
            if any(field not in param_data for field in ("name", "map")):
                raise ParsingException("Parameter specification requires at least 'name' and 'map'")

            # Each defined parameter can map to several nodes,
            # so we first collect those parameters
            parameters_to_map = []
            for entry in param_data["map"]:
                path = nested_dict_to_tuple(entry)
                param = graph.get_parameter(*path)
                if isinstance(param, MultiInput):
                    raise ParsingException("Mapping MultiInputs is not currently supported")
                parameters_to_map.append(param)

            # And then map them using `MultiParameter`
            graph.combine_parameters(
                *parameters_to_map,
                name=param_data["name"],
                optional=param_data.get("optional", None),
            )
            if "value" in param_data:
                graph.update_parameters(**{param_data["name"]: param_data["value"]})

        # Now connect all ports together based on channel specification
        for channel_data in data["channels"]:
            if any(field not in channel_data for field in ("sending", "receiving")):
                raise ParsingException(
                    "Channel specification requires at least 'sending' and 'receiving'"
                )

            input_path = nested_dict_to_tuple(channel_data["receiving"])
            output_path = nested_dict_to_tuple(channel_data["sending"])
            input_port = graph.get_port(*input_path)
            output_port = graph.get_port(*output_path)

            # Mostly for mypy :)
            if not isinstance(input_port, Input | MultiInput) or not isinstance(
                output_port, Output | MultiOutput
            ):
                raise ParsingException(
                    "Port doesn't correspond to the correct type, are you sure"
                    " 'sending' and 'receiving' are correct?"
                )
            graph.connect(sending=output_port, receiving=input_port)

        # At this point a lot of parameters will not have been set, so we only check connectivity
        # (also applies to software dependencies, as those can be set via parameters as well)
        super(cls, graph).check()
        return graph

    @classmethod
    def from_file(cls, file: Path | str) -> "Workflow":
        """
        Reads in a graph definition in JSON, YAML, or TOML format
        and creates a runnable workflow graph.

        This is an example input:

        .. code:: yaml

            name: graph
            description: An optional description for the workflow
            level: INFO
            nodes:
              - name: foo
                type: ExampleNode

                # Below options are optional
                description: An optional description
                fail_ok: false
                n_attempts: 1
                parameters:
                  val: 40

            channels:
              - sending: foo: out
                receiving: bar: input

            # Optional
            parameters:
              - name: val
                map:
                  foo: val

        Parameters
        ----------
        file
            File in JSON, YAML, or TOML format

        Returns
        -------
        Workflow
            The complete graph with all connections

        Raises
        ------
        ParsingException
            If the input file doesn't conform to the expected format

        """
        file = Path(file)
        data = read_input(file)

        return cls.from_dict(data)

    @classmethod
    def from_checkpoint(cls, file: Path | str) -> "Workflow":
        """
        Initialize a graph from a checkpoint file.

        Checkpoints include two additional sections, `_data` for any data
        stored in a channel at time of shutdown, and `_status` for node
        status information. We need the data for the full graph and thus
        use a nested implementation.

        .. code:: yaml

            _data:
              - bar: input: <binary>
            _status:
              - foo: STOPPED
              - subgraph: baz: FAILED

        Parameters
        ----------
        file
            Path to the checkpoint file

        Returns
        -------
        Graph
            The initialized graph, with statuses set and channels loaded

        Raises
        ------
        ParsingException
            If the input file doesn't conform to the expected format

        """
        file = Path(file)
        data = read_input(file)
        graph = cls.from_dict(data, resume=True)
        graph.load_checkpoint(data)
        return graph

    def _set_global_attribute(self, __name: str, __value: Any, /) -> None:
        """Set an attribute for all contained components."""
        setattr(self, __name, __value)
        for comp in self.flat_components:
            setattr(comp, __name, __value)

    def load_checkpoint(self, data: dict[str, Any]) -> None:
        """
        Load checkpoint data from a dictionary.

        Uses data as generated by `read_input` to access
        the special `_data` and `_status` fields.

        Parameters
        ----------
        data
            Dictionary data to read in as a checkpoint.
            Both `_data` and `_status` are optional.

        See also
        --------
        from_checkpoint : Load a `Graph` from a checkpoint file
        to_checkpoint : Save a `Graph` to a checkpoint file

        """
        for nested_data in data.get("_data", []):
            path: list[str]
            *path, input_name, raw_data = nested_dict_to_tuple(nested_data)
            node = self.get_node(*path)
            if len(preload := dill.loads(raw_data)) > 0:
                node.inputs[input_name].preload(preload)

        for status_data in data.get("_status", []):
            *path, status = nested_dict_to_tuple(status_data)
            node = self.get_node(*path)
            node.status = Status(status)

    def as_dict(self) -> dict[str, Any]:
        return with_fields(self, self._GRAPH_FIELDS & self._serializable_attributes)

    def to_dict(self) -> dict[str, Any]:
        """
        Create a dictionary from a graph, ready to be saved in a suitable format.

        Returns
        -------
        dict[str, Any]
            Nested dictionary equivalent to the input format

        Examples
        --------
        >>> g = Workflow(name="foo")
        ... foo = g.add(Foo)
        ... bar = g.add(Bar)
        ... g.auto_connect(foo, bar)
        ... data = g.to_dict()

        """
        data = self.as_dict()
        # For the purposes of writing checkpoints we treat subgraphs just like nodes
        data["nodes"] = [comp.as_dict() for comp in self.nodes.values()]
        data["channels"] = []
        for sender_path, receiver_path in self.channels:
            channel_data: dict[str, NestedDict[str, str]] = dict(
                sending=tuple_to_nested_dict(*sender_path),
                receiving=tuple_to_nested_dict(*receiver_path),
            )
            data["channels"].append(channel_data)

        data["parameters"] = [
            param.as_dict()
            for param in self.parameters.values()
            if isinstance(param, MultiParameter)
        ]

        return data

    def to_file(self, file: Path | str) -> None:
        """
        Save the graph to a file. The type is inferred from the suffix
        and can be one of JSON, YAML, or TOML.

        Parameters
        ----------
        file
            Path to the file to save to

        Examples
        --------
        >>> g = Workflow(name="foo")
        ... foo = g.add(Foo)
        ... bar = g.add(Bar)
        ... g.auto_connect(foo, bar)
        ... g.to_file("graph.yml")

        """
        file = Path(file)

        data = self.to_dict()
        write_input(file, data)

    def to_checkpoint(self, path: Path | str | None = None, fail_ok: bool = True) -> None:
        """
        Saves the current graph state, including channel data and node liveness to a file.

        Parameters
        ----------
        path
            Optional filename for the checkpoint
        fail_ok
            If ``True``, will only log a warning instead of raising
            an exception when encountering a writing problem.

        Raises
        ------
        CheckpointException
            Raised for checkpoint writing errors when `fail_ok` is ``False``

        """
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        if path is None:
            path = self.work_dir / f"ckp-{self.name}-{timestamp}.yaml"
        path = Path(path)

        try:
            data = self.to_dict()
            data |= self._checkpoint_data()
            write_input(path, data)
            self.logger.info("Wrote checkpoint to %s", path.as_posix())

        # Problems with writing a checkpoint shouldn't
        # cause the whole graph to necessarily crash
        except Exception as err:  # pylint: disable=broad-except
            if not fail_ok:
                raise CheckpointException("Unable to complete checkpoint") from err
            self.logger.warning("Unable to save checkpoint", exc_info=err)

    def generate_config_template(self) -> str:
        """
        Generates a global configuration template in TOML format.

        Returns
        -------
        str
            The config template

        """
        conf = {}
        for node in set(self.flat_nodes):
            if node.required_callables:
                conf[node.__class__.__name__] = NodeConfig().generate_template(
                    node.required_callables
                )
        return toml.dumps(conf)

    AddableArgType = (
        argparse.ArgumentParser | argparse._ArgumentGroup  # pylint: disable=protected-access
    )
    _A = TypeVar("_A", bound=AddableArgType)

    def add_arguments(self, parser: _A) -> _A:
        """
        Adds custom arguments to an existing parser for workflow parameters

        Parameters
        ----------
        parser
            Pre-initialized parser or group

        Returns
        -------
        argparse.ArgumentParser | argparse._ArgumentGroup
            An parser instance that can be used to read additional
            commandline arguments specific to the workflow

        See Also
        --------
        Workflow.update_with_args
            Sets up a parser for the workflow, uses `add_arguments` to update it,
            and parses all arguments with updated parameters for the workflow
        Workflow.update_settings_with_args
            Updates the workflow with non-parameter settings

        """
        for name, param in self.all_parameters.items():
            # If the `datatype` is annotated it might fail a subclass
            # check, so we unpack it using `get_args` first
            dtype = param.datatype
            if get_origin(dtype) == Annotated:
                dtype = get_args(dtype)[0]
            if get_origin(dtype) is not None:
                dargs = get_args(dtype)
                dtype = get_origin(dtype)

            self.logger.debug("Matching parameter '%s' with datatype '%s'", name, dtype)

            match dtype:
                # We want people to parameterise their generic nodes
                case TypeVar():
                    parent = param.parents[0] if isinstance(param, MultiParameter) else param.parent
                    raise GraphBuildException(
                        f"Parameter '{name}' is a generic. Did you specify the datatype "
                        f"for node '{parent.name}' ('{parent.__class__.__name__}' type)?"
                    )

                # Just a simple flag
                case _b.bool:
                    parser.add_argument(
                        f"--{name}", action=argparse.BooleanOptionalAction, help=param.doc
                    )

                # File path needs special treatment, not quite sure why
                case path_type if isinstance(dtype, type) and issubclass(dtype, Path):
                    parser.add_argument(f"--{name}", type=path_type, help=param.doc)

                # Several options
                case typing.Literal:
                    parser.add_argument(
                        f"--{name}", type=str, help=param.doc, choices=get_args(dtype)
                    )

                # Anything else should be a callable type, e.g. int, float...
                case _b.int | _b.float | _b.complex | _b.str | _b.bytes:
                    doc = f"{param.doc} [{dtype.__name__}]"
                    parser.add_argument(f"--{name}", type=dtype, help=doc)  # type: ignore

                # Multiple items
                case _b.tuple | _b.list:
                    if get_origin(dargs[0]) == Literal:  # pylint: disable=comparison-with-callable
                        parser.add_argument(
                            f"--{name}", nargs="+", help=param.doc, choices=get_args(dargs[0])
                        )
                    else:
                        doc = f"{param.doc} [{dargs[0].__name__}]"
                        parser.add_argument(f"--{name}", nargs=len(dargs), type=dargs[0], help=doc)

                case _b.dict:
                    parser.add_argument(f"--{name}", nargs="+", action=DictAction, help=param.doc)

                case _:
                    self.logger.warning(
                        "Parameter '%s' with datatype '%s' could "
                        "not be exposed as a commandline argument",
                        name,
                        dtype,
                    )

        return parser

    def update_with_args(
        self, extra_options: list[str], parser: argparse.ArgumentParser | None = None
    ) -> None:
        """
        Update the graph with additional options passed from the commandline.

        Parameters
        ----------
        extra_options
            List of option strings, i.e. the output of ``parse_args``
        parser
            Optional parser to reuse

        Raises
        ------
        ParsingException
            Raised when encountering unexpected commandline options

        """
        if parser is None:
            parser = argparse.ArgumentParser()
            parser = self.add_arguments(parser)

        try:
            param_args = parser.parse_args(extra_options)
        except SystemExit as err:
            raise ParsingException(
                "Unexpected argument type in commandline args, " f"see:\n{parser.format_help()}"
            ) from err

        self.update_parameters(**vars(param_args))

    def update_settings_with_args(self, args: argparse.Namespace) -> None:
        """
        Updates the workflow with global settings from the commandline.

        Parameters
        ----------
        args
            Namespace including the args to use. See `maize -h` for possible options.

        """
        # Update the config first, other explicitly set options should override it
        if args.config:
            if not args.config.exists():
                raise FileNotFoundError(f"Config under '{args.config.as_posix()}' not found")
            self.config.update(args.config)
        if args.quiet:
            self._set_global_attribute("level", logging.WARNING)
            self.logger.setLevel(logging.WARNING)
        if args.debug:
            self._set_global_attribute("level", logging.DEBUG)
            self.logger.setLevel(logging.DEBUG)
        if args.log:
            self._set_global_attribute("logfile", args.log)
        if args.keep or args.debug:
            self._set_global_attribute("cleanup_temp", False)
        if args.parameters:
            data = read_input(args.parameters)
            self.update_parameters(**data)
        if args.scratch:
            self._set_global_attribute("scratch", Path(args.scratch))

    def check(self) -> None:
        super().check()
        super().check_dependencies()

        # These are the original names of all mapped parameters
        multi_para_names = [
            para.original_names
            for para in self.all_parameters.values()
            if isinstance(para, MultiParameter)
        ]
        orig_names = set(itertools.chain(*multi_para_names))

        for node in self.flat_nodes:
            for name, param in node.parameters.items():
                # Parameters that have been mapped to the workflow level
                # (self.parameters) should not raise an exception if not set,
                # as we might set these after a check (e.g. on the commandline)
                if not param.is_set and not param.optional and name not in orig_names:
                    raise ParameterException(
                        f"Parameter '{name}' of node '{node.name}' needs to be set explicitly"
                    )

        # Remind the user of parameters that still have to be set
        for name, para in self.all_parameters.items():
            if not para.is_set and not para.optional:
                self.logger.warning("Parameter '%s' must be set to run the workflow", name)

    def execute(self) -> None:
        """
        Run a given graph.

        This is the top-level entry for maize execution. It creates a separate
        logging process and general message queue and then starts the `execute`
        methods of all nodes. Any node may at some point signal for the full graph
        to be shut down, for example after a failure. Normal termination of a node
        is however signalled by an `runtime.StatusUpdate` instance with finished
        status. Any exceptions raised in a node are passed through the message queue
        and re-raised as a `runtime.NodeException`.

        Raises
        ------
        NodeException
            If there was an exception in any node child process

        """
        # Import version here to avoid circular import
        from maize.maize import __version__  # pylint: disable=import-outside-toplevel

        timer = Timer()
        # We only know about the logfile now, so we can set all node
        # logging and the main graph logging to use a file, if given
        logger = Logger(message_queue=self._logging_queue, file=self.logfile)
        self.logger = setup_build_logging(name=self.name, level=self.level, file=self.logfile)
        self.logger.info(MAIZE_ISO)
        self.logger.info(
            "Starting Maize version %s (c) AstraZeneca %s", __version__, time.localtime().tm_year
        )
        self.logger.info("Running workflow '%s' with parameters:", self.name)
        for node in self.flat_nodes:
            for name, param in node.parameters.items():
                if not param.is_default:
                    self.logger.info("%s = %s (from '%s')", name, param.value, node.name)

        # Setup directories recursively
        self.setup_directories()

        # This is the control queue
        receive = cast(
            "Callable[[], MessageType]",
            functools.partial(
                _get_message, message_queue=self._message_queue, timeout=TIME_STEP_SEC
            ),
        )

        # Visual run indication
        spin = Spinner(interval=2)

        with RunPool(*self.active_nodes, logger) as pool:
            timer.start()
            update_time = datetime.now()

            # 'StatusUpdate' objects with a finished status represent our stop tokens
            summaries: list[StatusUpdate] = []
            latest_status: dict[tuple[str, ...], StatusUpdate] = {}
            while not self.signal.is_set() and len(summaries) < (pool.n_processes - 1):
                delta = datetime.now() - update_time
                spin()

                event: "MessageType" = receive()
                match event:
                    case StatusUpdate(status=Status.COMPLETED | Status.STOPPED):
                        summaries.append(event)
                        latest_status[(*event.parents, event.name)] = event

                        # Because the status is set in another process, we don't know the
                        # value, and thus need to set it again in the main process to allow
                        # saving the complete state of the graph as a checkpoint
                        comp = self.get_node(*event.parents, event.name)
                        comp.status = event.status
                        self.logger.info(
                            "Node '%s' finished (%s/%s)",
                            comp.name,
                            len(summaries),
                            pool.n_processes - 1,  # Account for logger
                        )

                    # Pickling tracebacks is impossible, so we unfortunately have
                    # to go the ugly route via 'TracebackException'
                    case StatusUpdate(exception=TracebackException()):
                        summaries.append(event)

                        # This might be a bug in mypy, the pattern match
                        # already implies that exception is not ``None``
                        if event.exception is not None:
                            raise NodeException.from_traceback_exception(event.exception)

                    case StatusUpdate():
                        key = (*event.parents, event.name)
                        changed = (
                            key not in latest_status or latest_status[key].status != event.status
                        )
                        latest_status[key] = event

                        if (delta.seconds > 1 and len(latest_status) > 0 and changed) or (
                            delta.seconds > 600 and len(latest_status) > 0
                        ):
                            self.logger.info(
                                format_update(latest_status, color=self.logfile is None)
                            )
                            update_time = datetime.now()

            self.logger.debug("All nodes finished, stopping...")
            elapsed = timer.stop()
            self._cleanup()

            # Make sure we actually print the summary at the end,
            # not while the logger is still going
            time.sleep(0.5)
            self.logger.info(format_summaries(summaries, elapsed))

    def submit(
        self, folder: Path, config: JobResourceConfig, maize_config: Path | None = None
    ) -> FutureWorkflowResult:
        """
        Submit this workflow to a batch system and exit.

        Parameters
        ----------
        folder
            The directory to execute the workflow in
        config
            The batch submission configuration
        maize_config
            Path to an optional different maize configuration

        Returns
        -------
        FutureWorkflowResult
            An object representing a future workflow result. It can be serialized,
            queried, and the underlying job can be cancelled or waited for.

        """
        folder.mkdir(exist_ok=True)
        self.to_file(folder / "flow.yml")
        runner = CommandRunner(
            name="workflow-runner", prefer_batch=True, rm_config=self.config.batch_config
        )

        command = f"maize --scratch {folder} "
        if not self.cleanup_temp:
            command += "--keep "
        if self.logfile is not None:
            command += f"--log {self.logfile} "
        if maize_config is not None:
            command += f"--config {maize_config} "
        command += f"{folder / 'flow.yml'}"

        results = runner.run_async(command=command, working_dir=folder, verbose=True, config=config)

        # do some debug statements here, none of these values should be 0
        if results.id is None:
            raise ProcessError("Workflow submission failed, no ID received")

        workflow_result = FutureWorkflowResult(
            id=results.id,
            folder=folder,
            workflow=self.to_dict(),
            backend=self.config.batch_config.system,
            stdout_path=results.stdout_path,
            stderr_path=results.stderr_path,
        )

        return workflow_result

    def _cleanup(self) -> None:
        """Cleans up the graph directory if required"""
        self.logger.debug("Attempting to remove working directory %s", self.work_dir)
        if self.cleanup_temp:
            self.logger.debug("Removing %s", self.work_dir)
            shutil.rmtree(self.work_dir)

    def _checkpoint_data(self) -> dict[str, list[NestedDict[str, T]]]:
        # We save all channel data in a flat dump for each input...
        data: dict[str, list[NestedDict[str, T]]] = {"_data": [], "_status": []}
        for node in self.flat_nodes:
            for name, inp in node.inputs.items():
                if (channel_dump := inp.dump()) is None:
                    continue
                dump: NestedDict[str, T] = tuple_to_nested_dict(
                    *node.component_path, str(name), dill.dumps(channel_dump)
                )
                data["_data"].append(dump)

        # ... and all the status data (including nodes in subgraphs)
        # in a flat list, by referring to each node by its full path
        for node in self.flat_nodes:
            data["_status"].append(tuple_to_nested_dict(*node.component_path, node.status.name))
        return data
