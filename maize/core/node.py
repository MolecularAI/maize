"""
Node
----
Nodes are the individual atomic components of workflow graphs and encapsulate arbitrary
computational behaviour. They communicate with other nodes and the environment
only through ports, and expose parameters to the user. Custom behaviour is
implemented by subclassing and defining the `Node.run` method.

"""

from abc import abstractmethod
from collections.abc import Generator, Sequence
import importlib
import logging
import os
import random
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from typing import Any, Optional, TYPE_CHECKING

from maize.core.component import Component
from maize.core.interface import (
    Flag,
    Input,
    MultiInput,
    Interface,
    Parameter,
    FileParameter,
    PortInterrupt,
)
from maize.core.runtime import (
    Runnable,
    Status,
    StatusHandler,
    init_signal,
    setup_node_logging,
)
from maize.utilities.execution import CommandRunner, JobResourceConfig, run_single_process
from maize.utilities.resources import cpu_count
from maize.utilities.io import ScriptSpecType, expand_shell_vars, remove_dir_contents
from maize.utilities.utilities import (
    Timer,
    change_environment,
    extract_attribute_docs,
    has_module_system,
    load_modules,
    set_environment,
)
from maize.utilities.validation import Validator

if TYPE_CHECKING:
    from maize.core.graph import Graph


log = logging.getLogger("build")


class NodeBuildException(Exception):
    """Exception raised for faulty `build` methods."""


class Node(Component, Runnable, register=False):
    """
    Base class for all atomic (non-subgraph) nodes of a graph.
    Create a subclass to implement your own custom tasks.

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
    initial_status
        The initial status of the node, will be ``NOT_READY`` by default, but
        can be set otherwise to indicate that the node should not be run.
        This would be useful when starting from a partially completed graph.
    max_loops
        Run the internal `loop` method a maximum number of `max_loops` times

    Attributes
    ----------
    cpus
        Resource semaphore allowing the reservation of multiple CPUs
    gpus
        Resource semaphore allowing the reservation of multiple GPUs

    Examples
    --------
    Subclassing can be done the following way:

    >>> class Foo(Node):
    ...     out: Output[int] = Output()
    ...
    ...     def run(self):
    ...         self.out.send(42)

    """

    active: Flag = Flag(default=True)
    """Whether the node is active or can be shutdown"""

    python: FileParameter[Path] = FileParameter(default=Path(sys.executable))
    """The path to the python executable to use for this node, allows custom environments"""

    modules: Parameter[list[str]] = Parameter(default_factory=list)
    """Modules to load in addition to ones defined in the configuration"""

    scripts: Parameter[ScriptSpecType] = Parameter(default_factory=dict)
    """
    Additional script specifications require to run.

    Examples
    --------
    >>> node.scripts.set({"interpreter": /path/to/python, "script": /path/to/script})

    """

    commands: Parameter[dict[str, Path]] = Parameter(default_factory=dict)
    """Custom paths to any commands"""

    batch_options: Parameter[JobResourceConfig | None] = Parameter(default=None, optional=True)
    """If given, will run commands on the batch system instead of locally"""

    # Making status a descriptor allows us to log status updates and
    # keep track of the timers when waiting on other nodes or resources
    status = StatusHandler()

    def __init__(
        self,
        parent: Optional["Graph"] = None,
        name: str | None = None,
        description: str | None = None,
        fail_ok: bool = False,
        n_attempts: int = 1,
        level: int | str | None = None,
        cleanup_temp: bool = True,
        resume: bool = False,
        logfile: Path | None = None,
        max_cpus: int | None = None,
        max_gpus: int | None = None,
        loop: bool | None = None,
        max_loops: int = -1,
        initial_status: Status = Status.NOT_READY,
    ) -> None:
        super().__init__(
            parent=parent,
            name=name,
            description=description,
            fail_ok=fail_ok,
            n_attempts=n_attempts,
            level=level,
            cleanup_temp=cleanup_temp,
            resume=resume,
            logfile=logfile,
            max_cpus=max_cpus,
            max_gpus=max_gpus,
            loop=loop,
        )
        self.status = initial_status

        # Run loops a maximum number of times, mostly to simplify testing
        self.max_loops = max_loops

        # For the signal handler
        self.n_signals = 0

        # Construct the node I/O and check it makes sense
        self.build()
        self.check()

        # The full timer should measure the full execution time
        # no matter if there's a block or not
        self.run_timer = Timer()
        self.full_timer = Timer()

    @property
    def user_parameters(self) -> dict[str, Input[Any] | MultiInput[Any] | Parameter[Any]]:
        """Returns all settable parameters and unconnected inputs defined by the user"""
        return {
            name: para for name, para in self.parameters.items() if name not in Node.__dict__
        } | self.inputs

    def setup_directories(self, parent_path: Path | None = None) -> None:
        """Sets up the required directories."""
        if parent_path is None:
            parent_path = Path("./")
        self.work_dir = Path(parent_path / f"node-{self.name}")
        self.work_dir.mkdir()

    def build(self) -> None:
        """
        Builds the node by instantiating all interfaces from descriptions.

        Examples
        --------
        >>> class Foo(Node):
        ...     def build(self):
        ...         self.inp = self.add_input(
        ...             "inp", datatype="pdb", description="Example input")
        ...         self.param = self.add_parameter("param", default=42)

        """
        docs = extract_attribute_docs(self.__class__)
        for name in dir(self):
            attr = getattr(self, name)
            if isinstance(attr, Interface):
                interface = attr.build(name=name, parent=self)
                interface.doc = docs.get(name, None)
                setattr(self, name, interface)

    def check(self) -> None:
        """
        Checks if the node was built correctly.

        Raises
        ------
        NodeBuildException
            If the node didn't declare at least one port

        """
        if len(self.inputs) == 0 and len(self.outputs) == 0:
            raise NodeBuildException(f"Node {self.name} requires at least one port")

        if self.status == Status.NOT_READY:
            self.status = Status.READY

    def check_dependencies(self) -> None:
        """
        Check if all node dependencies are met by running the `prepare` method

        Raises
        ------
        NodeBuildException
            If required callables were not found
        ImportError
            If required python packages were not found

        """
        if self.__class__.is_checked():
            log.debug("Already checked '%s', skipping...", self.name)
            return

        log.debug("Checking if required dependencies are available for '%s'...", self.name)
        try:
            run_single_process(self._prepare, name=self.name, executable=self.python.filepath)
        finally:
            self.__class__.set_checked()

    def run_command(
        self,
        command: str | list[str],
        working_dir: Path | None = None,
        validators: Sequence[Validator] | None = None,
        verbose: bool = False,
        raise_on_failure: bool = True,
        command_input: str | None = None,
        pre_execution: str | list[str] | None = None,
        batch_options: JobResourceConfig | None = None,
        prefer_batch: bool = False,
        timeout: float | None = None,
        cuda_mps: bool = False,
    ) -> subprocess.CompletedProcess[bytes]:
        """
        Runs an external command.

        Parameters
        ----------
        command
            Command to run as a single string, or a list of strings
        working_dir
            Working directory for the command
        validators
            One or more `Validator` instances that will
            be called on the result of the command.
        verbose
            If ``True`` will also log any STDOUT or STDERR output
        raise_on_failure
            Whether to raise an exception when encountering a failure
        command_input
            Text string used as input for command
        pre_execution
            Command to run directly before the main one
        batch_options
            Job options for the batch system, if given,
            will attempt run on the batch system
        prefer_batch
            Whether to prefer submitting a batch job rather than running locally. Note that
            supplying batch options directly will automatically set this to ``True``.
        timeout
            Maximum runtime for the command in seconds, or unlimited if ``None``
        cuda_mps
            Use the multi-process service to run multiple CUDA job processes on a single GPU

        Returns
        -------
        subprocess.CompletedProcess[bytes]
            Result of the execution, including STDOUT and STDERR

        Raises
        ------
        ProcessError
            If any of the validators failed or the returncode was not zero

        Examples
        --------
        To run a single command:

        >>> self.run_command("echo foo", validators=[SuccessValidator("foo")])

        To run on a batch system, if configured:

        >>> self.run_command("echo foo", batch_options=JobResourceConfig(nodes=1))

        """
        self.status = Status.WAITING_FOR_COMMAND
        cmd = CommandRunner(
            working_dir=working_dir or self.work_dir,
            validators=validators,
            raise_on_failure=raise_on_failure,
            prefer_batch=prefer_batch and (batch_options is not None or self.batch_options.is_set),
            rm_config=self.config.batch_config,
        )
        if batch_options is None and self.batch_options.is_set:
            batch_options = self.batch_options.value

        if batch_options is not None:
            self.logger.debug("Using batch options: %s", batch_options)
        res = cmd.run_validate(
            command=command,
            verbose=verbose,
            command_input=command_input,
            config=batch_options,
            pre_execution=pre_execution,
            timeout=timeout,
            cuda_mps=cuda_mps,
        )
        self.status = Status.RUNNING
        return res

    def run_multi(
        self,
        commands: Sequence[str | list[str]],
        working_dirs: Sequence[Path] | None = None,
        command_inputs: Sequence[str | None] | None = None,
        validators: Sequence[Validator] | None = None,
        verbose: bool = False,
        raise_on_failure: bool = True,
        n_jobs: int = 1,
        pre_execution: str | list[str] | None = None,
        batch_options: JobResourceConfig | None = None,
        timeout: float | None = None,
        cuda_mps: bool = False,
        n_batch: int | None = None,
        batchsize: int | None = None,
    ) -> list[subprocess.CompletedProcess[bytes]]:
        """
        Runs multiple commands in parallel.

        Parameters
        ----------
        commands
            Commands to run as a list of strings, or a nested list of strings
        working_dirs
            Working directories for each command
        command_inputs
            Text string used as input for each command
        validators
            One or more `Validator` instances that will
            be called on the result of the command.
        verbose
            If ``True`` will also log any STDOUT or STDERR output
        raise_on_failure
            Whether to raise an exception when encountering a failure
        n_jobs
            Max number of processes to spawn at once, should generally be
            compatible with the number of available CPUs
        pre_execution
            Command to run directly before the main one
        batch_options
            Job options for the batch system, if given,
            will attempt run on the batch system
        timeout
            Maximum runtime for the command in seconds, or unlimited if ``None``
        cuda_mps
            Use the multi-process service to run multiple CUDA job processes on a single GPU
        n_batch
            Number of batches to divide all the commands between. Incompatible with ``batchsize``.
        batchsize
            Number of commands to put into 1 batch. Incompatible with ``n_batch``.

        Returns
        -------
        list[subprocess.CompletedProcess[bytes]]
            Result of the execution, including STDOUT and STDERR

        Raises
        ------
        ProcessError
            If any of the validators failed or a returncode was not zero

        Examples
        --------
        To run multiple commands, but only two at a time:

        >>> self.run_multi(["echo foo", "echo bar", "echo baz"], n_jobs=2)

        To run on a batch system, if configured (note that batch settings are per-command):

        >>> self.run_command(["echo foo", "echo bar"], batch_options=JobResourceConfig(nodes=1))

        """
        self.status = Status.WAITING_FOR_COMMAND
        batch = batch_options is not None or self.batch_options.is_set
        if n_jobs > cpu_count() and not batch:
            self.logger.warning(
                "Requested number of jobs (%s) is higher than available cores (%s)",
                n_jobs,
                cpu_count(),
            )

        cmd = CommandRunner(
            validators=validators,
            raise_on_failure=raise_on_failure,
            prefer_batch=batch,
            rm_config=self.config.batch_config,
        )
        reserved = n_jobs if not batch else 0
        with self.cpus(reserved):
            res = cmd.run_parallel(
                commands=commands,
                working_dirs=working_dirs,
                command_inputs=command_inputs,
                verbose=verbose,
                n_jobs=n_jobs,
                validate=True,
                config=(batch_options or self.batch_options.value) if batch else None,
                pre_execution=pre_execution,
                timeout=timeout,
                cuda_mps=cuda_mps,
                n_batch=n_batch,
                batchsize=batchsize,
            )
        self.status = Status.RUNNING
        return res

    # No cover because we change the environment, which breaks pytest-cov
    def _prepare(self) -> None:  # pragma: no cover
        """
        Prepares the execution environment for `run`.

        Performs the following:

        * Changing the python environment, if required
        * Setting of environment variables
        * Setting of parameters from the config
        * Loading LMOD modules
        * Importing python packages listed in `required_packages`
        * Checking if software in `required_callables` is available

        """
        # Change environment based on python executable set by `RunPool`
        python = self.node_config.python
        if not self.python.is_default:
            python = self.python.value
        change_environment(expand_shell_vars(python))

        # Custom preset parameters
        config_params = self.node_config.parameters
        for key, val in config_params.items():
            if key in self.parameters and not (param := self.parameters[key]).changed:
                param.set(val)

        # Load any required modules if possible from the global config,
        # they don't neccessarily have to contain the executable, but
        # might be required for running it
        if has_module_system():
            load_modules(*self.node_config.modules)

            # And then locally defined ones
            for mod in self.modules.value:
                load_modules(mod)

        # Environment variables
        set_environment(self.config.environment)

        # Check we can import any required modules, now
        # that we might be in a different environment
        for package in self.required_packages:
            importlib.import_module(package)

        for exe in self.required_callables:
            # Prepare any interpreter - script pairs, prioritize local
            if exe in (script_dic := self.node_config.scripts | self.scripts.value):
                interpreter = os.path.expandvars(script_dic[exe].get("interpreter", ""))
                loc_path = expand_shell_vars(script_dic[exe]["location"])
                location = loc_path.absolute().as_posix() if loc_path.exists() else loc_path.name
                self.runnable[exe] = f"{interpreter} {location}"

            # Prepare custom command locations
            elif exe in (com_dic := self.node_config.commands | self.commands.value):
                self.runnable[exe] = expand_shell_vars(Path(com_dic[exe])).absolute().as_posix()

            # It's already in our $PATH
            elif shutil.which(exe) is not None:
                self.runnable[exe] = exe

            else:
                raise NodeBuildException(
                    f"Could not find a valid executable for '{exe}'. Add an appropriate entry "
                    f"in your global configuration under '[{self.__class__.__name__.lower()}]', "
                    f"e.g. 'commands.{exe} = \"path/to/executable\"', "
                    f"'scripts.{exe}.interpreter = \"path/to/interpreter\"' and "
                    f"'scripts.{exe}.location = \"path/to/script\"' or "
                    f"load an appropriate module with 'modules = [\"module_with_{exe}\"]'"
                )

        # Run any required user setup
        self.prepare()

    # No cover because we change the environment, which breaks pytest-cov
    def execute(self) -> None:  # pragma: no cover
        """
        This is the main entrypoint for node execution.

        Raises
        ------
        KeyboardInterrupt
            If the underlying process gets interrupted or receives ``SIGINT``

        """
        # Prepare environment
        self._prepare()

        # This will hold a traceback-exception for sending to the main process
        tbe = None

        # Signal handler for interrupts will make sure the process has a chance
        # to shutdown gracefully, by setting the shutdown signal
        init_signal(self)

        # This replaces the build-logger with the process-safe message based logger
        self.logger = setup_node_logging(
            name=self.name,
            logging_queue=self._logging_queue,
            level=self.level,
            color=self.logfile is None,
        )
        self.logger.debug("Using executable at")
        self.logger.debug("'%s'", sys.executable)

        os.chdir(self.work_dir)
        self.logger.debug("Running in '%s'", self.work_dir.as_posix())

        # Wait a short random time to make testing more reliable,
        # this shouldn't matter in production too much
        time.sleep(random.random())

        # The `run_timer` is controlled by the `StatusHandler`
        # descriptor, so no need to start it here
        self.full_timer.start()
        self.logger.debug("Starting up")
        try:
            # Main execution
            tbe = self._attempt_loop()

        finally:
            # We exhausted all our attempts, we now set the shutdown signal
            # (if the task is not allowed to fail, otherwise we don't care)
            if self.status == Status.FAILED and not self.fail_ok:
                self.signal.set()
                if tbe is not None:
                    self.send_update(exception=tbe)

            run_time, full_time = self.run_timer.stop(), self.full_timer.stop()
            self.logger.debug("Shutting down, runtime: %ss", run_time)
            self.logger.debug("Shutting down, total time: %ss", full_time)

            # It's very important we shutdown all ports,
            # so other processes can follow suit
            self._shutdown()

            # The final update will have a completion status, indicating to
            # the master process that this node has finished processing
            self.send_update()

    def cleanup(self) -> None:
        if self.cleanup_temp and self.work_dir.exists():
            shutil.rmtree(self.work_dir)
        for inp in self.inputs.values():
            if inp.channel is not None:
                inp.channel.kill()

    def prepare(self) -> None:
        """
        Prepare the node for execution.

        This method is called just before :meth:`~maize.core.node.Node.run`
        and can be used for setup code if desired. This can be useful with
        looping nodes, as initial variables can be set before the actual execution.

        Examples
        --------
        >>> class Foo(Node):
        ...     def prepare(self):
        ...         self.val = 0
        ...
        ...     def run(self):
        ...         val = self.inp.receive()
        ...         self.val += val
        ...         self.out.send(self.val)

        """

    @abstractmethod
    def run(self) -> None:
        """
        This is the main high-level node execution point.

        It should be overridden by the user to provide custom node functionality,
        and should return normally at completion. Exception handling, log message passing,
        and channel management are handled by the wrapping `execute` method.

        Examples
        --------
        >>> class Foo(Node):
        ...     def run(self):
        ...         val = self.inp.receive()
        ...         new = val * self.param.value
        ...         self.out.send(new)

        """

    def _shutdown(self) -> None:
        """
        Shuts down the component gracefully.

        This should not be called by the user directly,
        as it is called at node shutdown by `execute()`.

        """
        if self.status not in (Status.STOPPED, Status.FAILED):
            self.status = Status.COMPLETED

        # Shutdown all ports, it's important that we do this for
        # every port, not just the ones that appear active, as
        # some port closures can only be performed on one side
        # (e.g. file channels can only be closed after the receiver
        # has moved the file out of the channel directory).
        for name, port in self.ports.items():
            port.close()
            self.logger.debug("Closed port %s", name)

    def _loop(self, step: float = 0.5) -> Generator[int, None, None]:
        """
        Allows continuous looping of the main routine, it handles graceful
        shutdown of the node and checks for changes in the run conditions.
        Do not use this function directly, instead pass ``loop=True`` to
        the component constructor.

        Parameters
        ----------
        step
            Timestep in seconds to take between iterations

        Returns
        -------
        Generator[None, None, None]
            Generator allowing infinite looping

        """
        i = 0
        while not self.signal.is_set():
            # Inactive but required ports should stop the process
            if not self.ports_active():
                self.logger.debug("Shutting down due to inactive port")
                self.status = Status.STOPPED
                return

            # In a testing setup we will only execute a limited number
            # of times, as we are testing the node in isolation
            if self.max_loops > 0 and i >= self.max_loops:
                self.logger.debug("Maximum loops reached (%s/%s)", i, self.max_loops)
                return

            time.sleep(step)
            yield i
            i += 1

    def _iter_run(self, cleanup: bool = False) -> None:
        """
        Runs the node (in a loop if `self.looped` is set).

        Parameters
        ----------
        cleanup
            Whether to remove working directory contents between iterations

        """
        # In some cases we might have branches in our workflow dedicated to providing
        # a potentially optional input for a downstream node. This means we don't want
        # to force the user to set this value, so we want to be able to override the
        # originating parameter to be optional. This should then cause the loading node
        # to not run at all, which will cause this branch to shutdown. These conditions
        # here check that this is the case, i.e. if all parameters are unset and optional,
        # and none of the inputs are connected, then we return immediately.
        if all(para.skippable for para in self.user_parameters.values()):
            self.logger.warning(
                "Inputs / parameters are unset, optional, or unconnected, not running node"
            )
            return

        if self.looped:
            for it in self._loop():
                if cleanup and it != 0:
                    self.logger.debug("Removing all items in '%s'", self.work_dir.absolute())
                    remove_dir_contents(self.work_dir)
                    for inp in self.flat_inputs:
                        if self.parent is not None and not inp.cached:
                            remove_dir_contents(self.parent.work_dir / f"{self.name}-{inp.name}")
                self.run()
        else:
            self.run()

    def _attempt_loop(self) -> traceback.TracebackException | None:
        """
        Attempt to execute the `run` method multiple times. Internal use only.

        Returns
        -------
        TracebackException | None
            Object containing a traceback in case of an error encountered in `run`

        Raises
        ------
        KeyboardInterrupt
            If the underlying process gets interrupted or receives ``SIGINT``

        """
        # Skip execution if inactive
        if not self.active.value:
            self.logger.info("Node inactive, stopping...")
            self.status = Status.STOPPED
            return None

        tbe = None
        for attempt in range(self.n_attempts):
            # Reset the status in case of failure
            self.status = Status.RUNNING
            try:
                self._iter_run(cleanup=True)

            # Raised to immediately quit a node in the case of a dead input,
            # as we want to avoid propagating ``None`` while the graph is
            # shutting down
            except PortInterrupt as inter:
                self.logger.debug("Port '%s' shutdown, exiting now...", inter.name)
                self.status = Status.STOPPED
                break

            # This could come from the system or ctrl-C etc. and
            # should always abort any attempts
            except KeyboardInterrupt:
                self.logger.info("Received interrupt")
                self.status = Status.STOPPED

                # This should have been set if we're here, just making sure
                self.signal.set()
                raise

            # Error in run()
            except Exception as err:  # pylint: disable=broad-except
                self.status = Status.FAILED
                msg = "Attempt %s of %s failed due to exception"
                self.logger.error(msg, attempt + 1, self.n_attempts, exc_info=err)

                # Save the traceback to send to the main process in the 'finally'
                # block, as we don't yet know whether to raise or fail silently
                # and (maybe) try again
                tbe = traceback.TracebackException.from_exception(err)

                # Can we even start up again? Check if ports are still open
                if not self.ports_active():
                    self.logger.info("Cannot restart due to closed ports")
                    self.signal.set()
                    break

            # Success
            else:
                if self.n_attempts > 1:
                    self.logger.info("Attempt %s of %s succeeded", attempt + 1, self.n_attempts)
                break

        return tbe


class LoopedNode(Node):
    """Node variant that loops its `run` method by default"""

    def __init__(
        self, max_loops: int = -1, initial_status: Status = Status.NOT_READY, **kwargs: Any
    ):
        kwargs["loop"] = True
        super().__init__(max_loops=max_loops, initial_status=initial_status, **kwargs)
