"""Utilities to execute external software."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from enum import auto
import io
import logging
from multiprocessing import get_context
import os
from pathlib import Path
from queue import Queue
import shlex
import stat
import subprocess
import sys
from tempfile import mkdtemp
import time
from typing import TYPE_CHECKING, Any, Concatenate, Literal, ParamSpec, TypeVar, cast
from typing_extensions import Self
import xml.etree.ElementTree as ET

from psij import (
    InvalidJobException,
    Job,
    JobAttributes,
    JobExecutor,
    JobExecutorConfig,
    JobSpec,
    JobStatus,
    ResourceSpecV1,
    SubmitException,
    JobState,
)
from psij.executors.batch.slurm import SlurmExecutorConfig

from maize.utilities.utilities import (
    make_list,
    split_list,
    unique_id,
    split_multi,
    set_environment,
    StrEnum,
)

if TYPE_CHECKING:
    from maize.utilities.validation import Validator


class ProcessError(Exception):
    """Error called for failed commands."""


# Maize can in theory use either 'fork' or 'spawn' for process handling. However some external
# dependencies (notably PSI/J) can cause subtle problems when changing this setting due to their
# own use of threading / multiprocessing. See this issue for more details:
# https://github.com/ExaWorks/psij-python/issues/387
DEFAULT_CONTEXT = "fork"


def _format_command(result: subprocess.CompletedProcess[bytes] | subprocess.Popen[bytes]) -> str:
    """Format a command from a completed process or ``Popen`` instance."""
    return " ".join(cast(list[str], result.args))


def _log_command_output(stdout: bytes | None, stderr: bytes | None) -> str:
    """Write any command output to the log."""
    msg = "Command output:\n"
    if stdout is not None and len(stdout) > 0:
        msg += "---------------- STDOUT ----------------\n"
        msg += stdout.decode(errors="ignore") + "\n"
        msg += "---------------- STDOUT ----------------\n"
    if stderr is not None and len(stderr) > 0:
        msg += "---------------- STDERR ----------------\n"
        msg += stderr.decode(errors="ignore") + "\n"
        msg += "---------------- STDERR ----------------\n"
    return msg


def _simple_run(command: list[str] | str) -> subprocess.CompletedProcess[bytes]:
    """Run a command and return a `subprocess.CompletedProcess` instance."""
    if isinstance(command, str):
        command = shlex.split(command)

    return subprocess.run(command, check=False, capture_output=True)


class GPUMode(StrEnum):
    DEFAULT = auto()
    EXCLUSIVE_PROCESS = auto()


@dataclass
class GPU:
    """
    Indicates the status of the system's GPUs, if available.

    Attributes
    ----------
    available
        Whether a GPU is available in the system
    free
        Whether the GPU can run a process right now
    free_with_mps
        Whether the GPU can run a process if the Nvidia multi-process service (MPS) is used
    mode
        The compute mode of the GPU, currently just one of (``DEFAULT``,
        ``EXCLUSIVE_PROCESS``).  In ``EXCLUSIVE_PROCESS`` only one process
        can be run at a time, unless MPS is used. ``DEFAULT`` allows any
        process to be run normally.

    """

    available: bool
    free: bool
    free_with_mps: bool = False
    mode: GPUMode | None = None

    @classmethod
    def from_system(cls) -> list[Self]:
        """Provides information on GPU capabilities"""
        cmd = CommandRunner(raise_on_failure=False)
        ret = cmd.run_only("nvidia-smi -q -x")
        if ret.returncode != 0:
            return [cls(available=False, free=False)]

        tree = ET.parse(io.StringIO(ret.stdout.decode()))

        devices = []
        for gpu in tree.findall("gpu"):
            # No need for MPS in default compute mode :)
            if (res := gpu.find("compute_mode")) is not None and res.text == "Default":
                devices.append(cls(available=True, free=True, mode=GPUMode.DEFAULT))
                continue

            names = (name.text or "" for name in gpu.findall("processes/*/process_name"))
            types = (typ.text or "" for typ in gpu.findall("processes/*/type"))

            # No processes implies an empty GPU
            if not len(list(names)):
                devices.append(cls(available=True, free=True, mode=GPUMode.EXCLUSIVE_PROCESS))
                continue

            mps_only = all(
                "nvidia-cuda-mps-server" in id or typ == "M+C" for id, typ in zip(names, types)
            )
            gpu_ok = all(typ == "G" for typ in types)
            devices.append(cls(
                available=True, free=gpu_ok, free_with_mps=mps_only, mode=GPUMode.EXCLUSIVE_PROCESS
            ))
        return devices


def gpu_info() -> tuple[bool, bool]:
    """
    Provides information on GPU capabilities

    Returns
    -------
    tuple[bool, bool]
        The first boolean indicates whether the GPU is free to use, the second
        indicates whether the GPU can be used, but only using the nvidia MPS daemon.
        If no GPU is available returns ``(False, False)``.

    """
    cmd = CommandRunner(raise_on_failure=False)
    ret = cmd.run_only("nvidia-smi -q -x")
    if ret.returncode != 0:
        return False, False

    tree = ET.parse(io.StringIO(ret.stdout.decode()))

    # No need for MPS in default compute mode :)
    if (res := tree.find("gpu/compute_mode")) is not None and res.text == "Default":
        return True, False

    names = (name.text or "" for name in tree.findall("gpu/processes/*/process_name"))
    types = (typ.text or "" for typ in tree.findall("gpu/processes/*/type"))

    # No processes implies an empty GPU
    if not len(list(names)):
        return True, False

    mps_only = all("nvidia-cuda-mps-server" in id or typ == "M+C" for id, typ in zip(names, types))
    gpu_ok = all(typ == "G" for typ in types)
    return gpu_ok, mps_only


def job_from_id(job_id: str, backend: str) -> Job:
    """Creates a job object from an ID"""
    job = Job()
    job._native_id = job_id
    executor = JobExecutor.get_instance(backend)
    executor.attach(job, job_id)
    return job


class WorkflowStatus(StrEnum):
    RUNNING = auto()
    """Currently running with no IO interaction"""

    COMPLETED = auto()
    """Successfully completed everything"""

    FAILED = auto()
    """Failed via exception"""

    QUEUED = auto()
    """Queued for execution in the resource manager"""

    CANCELLED = auto()
    """Cancelled by user"""

    UNKNOWN = auto()
    """Unknown job status"""

    @classmethod
    def from_psij(cls, status: JobState) -> "WorkflowStatus":
        """Convert a PSIJ job status to a Maize workflow status"""
        return PSIJSTATUSTRANSLATION[str(status)]


PSIJSTATUSTRANSLATION: dict[str, "WorkflowStatus"] = {
    str(JobState.NEW): WorkflowStatus.QUEUED,
    str(JobState.QUEUED): WorkflowStatus.QUEUED,
    str(JobState.ACTIVE): WorkflowStatus.RUNNING,
    str(JobState.FAILED): WorkflowStatus.FAILED,
    str(JobState.CANCELED): WorkflowStatus.CANCELLED,
    str(JobState.COMPLETED): WorkflowStatus.COMPLETED,
}


def check_executable(command: list[str] | str) -> bool:
    """
    Checks if a command can be run.

    Parameters
    ----------
    command
        Command to execute

    Returns
    -------
    bool
        ``True`` if running the command was successfull, ``False`` otherwise

    """
    exe = make_list(command)
    try:
        # We cannot use `CommandRunner` here, as initializing the Exaworks PSI/J job executor in
        # the main process (where this code will be run, as we're checking if the nodes have all
        # the required tools to start) may cause the single process reaper to lock up all child
        # jobs. See this related bug: https://github.com/ExaWorks/psij-python/issues/387
        res = _simple_run(exe)
        res.check_returncode()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False
    return True


def _parse_slurm_walltime(time_str: str) -> timedelta:
    """
    Parses a SLURM walltime string

    Parameters
    ----------
    time_str
        String with the `SLURM time format <https://slurm.schedmd.com/sbatch.html#OPT_time>`_.

    Returns
    -------
    timedelta
        Timedelta object with the parsed time interval

    """
    match split_multi(time_str, "-:"):
        case [days, hours, minutes, seconds]:
            delta = timedelta(
                days=int(days), hours=int(hours), minutes=int(minutes), seconds=int(seconds)
            )
        case [days, hours, minutes] if "-" in time_str:
            delta = timedelta(days=int(days), hours=int(hours), minutes=int(minutes))
        case [days, hours] if "-" in time_str:
            delta = timedelta(days=int(days), hours=int(hours))
        case [hours, minutes, seconds]:
            delta = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
        case [minutes, seconds]:
            delta = timedelta(minutes=int(minutes), seconds=int(seconds))
        case [minutes]:
            delta = timedelta(minutes=int(minutes))
    return delta


# This would normally be part of `run_single_process`, but
# pickling restrictions force us to place it at the top level
def _wrapper(
    func: Callable[[], Any], error_queue: "Queue[Exception]", *args: Any, **kwargs: Any
) -> None:  # pragma: no cover
    try:
        func(*args, **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        error_queue.put(err)


def run_single_process(
    func: Callable[[], Any], name: str | None = None, executable: Path | None = None
) -> None:
    """
    Runs a function in a separate process.

    Parameters
    ----------
    func
        Function to call in a separate process
    name
        Optional name of the function
    executable
        Optional python executable to use

    """
    ctx = get_context(DEFAULT_CONTEXT)

    # In some cases we might need to change python environments to get the dependencies
    exec_path = sys.executable if executable is None else executable.as_posix()
    ctx.set_executable(exec_path)

    # The only way to reliably get raised exceptions in the main process
    # is by passing them through a shared queue and re-raising. So we just
    # wrap the function of interest to catch any exceptions and pass them on
    queue = ctx.Queue()

    proc = ctx.Process(  # type: ignore
        target=_wrapper,
        name=name,
        args=(
            func,
            queue,
        ),
    )
    proc.start()

    proc.join(timeout=2.0)
    if proc.is_alive():
        proc.terminate()
    if not queue.empty():
        raise queue.get_nowait()


def check_returncode(
    result: subprocess.CompletedProcess[bytes],
    raise_on_failure: bool = True,
    logger: logging.Logger | None = None,
) -> None:
    """
    Check the returncode of the process and raise or log a warning.

    Parameters
    ----------
    result
        Completed process to check
    raise_on_failure
        Whether to raise an exception on failure
    logger
        Logger instance to use for command output

    """
    if logger is None:
        logger = logging.getLogger()

    # Raise the expected FileNotFoundError if the command
    # couldn't be found (to mimic subprocess)
    if result.returncode == 127:
        msg = f"Command {result.args[0]} not found (returncode {result.returncode})"
        if raise_on_failure:
            raise FileNotFoundError(msg)
        logger.warning(msg)
    elif result.returncode != 0:
        msg = f"Command {_format_command(result)} failed with returncode {result.returncode}"
        logger.warning(_log_command_output(result.stdout, result.stderr))
        if raise_on_failure:
            raise ProcessError(msg)
        logger.warning(msg)


class ProcessBase:
    def __init__(self) -> None:
        self.logger = logging.getLogger(f"run-{os.getpid()}")

    def check_returncode(
        self,
        result: subprocess.CompletedProcess[bytes],
        raise_on_failure: bool = True,
    ) -> None:
        """
        Check the returncode of the process and raise or log a warning.

        Parameters
        ----------
        result
            Completed process to check
        raise_on_failure
            Whether to raise an exception on failure

        """
        return check_returncode(result, raise_on_failure=raise_on_failure, logger=self.logger)

    def _job_to_completed_process(self, job: Job) -> subprocess.CompletedProcess[bytes]:
        """Converts a finished PSI/J job to a `CompletedProcess` instance"""

        # Basically just for mypy
        if (
            job.spec is None
            or job.spec.stderr_path is None
            or job.spec.stdout_path is None
            or job.spec.executable is None
            or job.spec.arguments is None
        ):  # pragma: no cover
            raise ProcessError("Job was not initialized correctly")

        command = [job.spec.executable, *job.spec.arguments]

        # There seem to be situations in which STDOUT / STDERR is not available
        # (although it should just be an empty file in case of no output). Rather
        # than crash here we let a potential validator check if the command maybe
        # was successful anyway.
        stdout = stderr = b""
        if job.spec.stdout_path.exists():
            with job.spec.stdout_path.open("rb") as out:
                stdout = out.read()
        else:
            self.logger.warning(
                "STDOUT at %s not found, this may indicate problems writing output",
                job.spec.stdout_path.as_posix(),
            )

        if job.spec.stdout_path.exists():
            with job.spec.stderr_path.open("rb") as err:
                stderr = err.read()
        else:
            self.logger.warning(
                "STDERR at %s not found, this may indicate problems writing output",
                job.spec.stderr_path.as_posix(),
            )

        res = subprocess.CompletedProcess(
            command,
            # Exit code is 130 if we cancelled due to timeout
            returncode=job.status.exit_code if job.status.exit_code is not None else 130,
            stdout=stdout,
            stderr=stderr,
        )
        return res


BatchSystemType = Literal["slurm", "rp", "pbspro", "lsf", "flux", "cobalt", "local"]


@dataclass
class ResourceManagerConfig:
    """Configuration for job resource managers"""

    system: BatchSystemType = "local"
    max_jobs: int = 100
    queue: str | None = None
    project: str | None = None
    launcher: str | None = None
    walltime: str = "24:00:00"
    polling_interval: int = 120


@dataclass
class JobResourceConfig:
    """Configuration for job resources"""

    nodes: int | None = None
    processes_per_node: int | None = None
    processes: int | None = None
    cores_per_process: int | None = None
    gpus_per_process: int | None = None
    exclusive_use: bool = False
    queue: str | None = None
    walltime: str | None = None
    custom_attributes: dict[str, Any] = field(default_factory=dict)

    def format_custom_attributes(self, system: BatchSystemType) -> dict[str, Any]:
        """Provides custom attributes formatted for PSI/J"""
        return {f"{system}.{key}": value for key, value in self.custom_attributes.items()}


class _JobCounterSemaphore:
    def __init__(self, val: int = 0) -> None:
        self._val = val
        self._total = 0

    @property
    def val(self) -> int:
        """Value of the semaphore"""
        return self._val

    @property
    def total(self) -> int:
        """Total number of submissions"""
        return self._total

    def inc(self) -> None:
        """Increment the counter"""
        self._val += 1

    def dec(self) -> None:
        """Decrement the counter"""
        self._val -= 1
        self._total += 1


class _SingletonJobHandler(type):
    _instances: dict[type, type] = {}

    def __call__(cls, *args: Any, **kwds: Any) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwds)
        singleton = cls._instances[cls]

        # We always reset the counter, otherwise successive `run_multi` calls will fail
        if hasattr(singleton, "_counter") and "counter" in kwds:
            singleton._counter = kwds["counter"]
        return singleton


class JobHandler(metaclass=_SingletonJobHandler):
    """
    Handles safe job cancellation in case something goes wrong.

    This class is implemented as a singleton so that there is
    only one `JobHandler` instance per process. This allows safe
    job cancellation and shutdowns through the signal handler.

    """

    def __init__(self, counter: _JobCounterSemaphore | None = None) -> None:
        self._jobs: dict[str, Job] = {}
        self._counter = counter or _JobCounterSemaphore()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        self.cancel_all()

    def add(self, job: Job) -> None:
        """Register a job with the handler"""
        self._counter.inc()
        self._jobs[job.id] = job

    def wait_for_slot(self, timeout: float | None = None, n_jobs: int | None = None) -> None:
        """Wait for a slot for a new job"""
        if n_jobs is None:
            return

        start = time.time()
        while self._counter.val >= n_jobs:
            time.sleep(0.1)
            if timeout is not None and (time.time() - start) >= timeout:
                self._stop_all()

    def wait(self, timeout: float | None = None) -> list[Job]:
        """Wait for all jobs to complete"""
        start = time.time()
        while self._counter.val > 0:
            time.sleep(0.1)
            if timeout is not None and (time.time() - start) >= timeout:
                self._stop_all()

        return list(self._jobs.values())

    def cancel_all(self) -> None:
        """Cancel all jobs"""
        while self._jobs:
            _, job = self._jobs.popitem()
            if not job.status.final:
                job.cancel()

    def _stop_all(self) -> None:
        """Stop all jobs"""
        for job in self._jobs.values():
            job.cancel()
            job.wait()
        return


class RunningProcess(ProcessBase):
    def __init__(self, job: Job, verbose: bool = False, raise_on_failure: bool = True) -> None:
        super().__init__()
        self.job = job
        self.verbose = verbose
        self.raise_on_failure = raise_on_failure
        self.stdout_path = None if job.spec is None else job.spec.stdout_path
        self.stderr_path = None if job.spec is None else job.spec.stderr_path

    @property
    def id(self) -> str | None:
        """Provides the job ID of the batch system"""
        return self.job.native_id

    def is_alive(self) -> bool:
        """
        Whether the process is alive and running

        Returns
        -------
        bool
            ``True`` if the process is alive and running

        """
        return not self.job.status.final

    def kill(self, timeout: int = 0) -> None:
        """
        Kill the process

        Parameters
        ----------
        timeout
            Timeout in seconds before forcefully killing

        """
        time.sleep(timeout)
        if not self.job.status.final:
            self.job.cancel()

    def wait(self) -> subprocess.CompletedProcess[bytes]:
        """
        Wait for the process to complete

        Returns
        -------
        subprocess.CompletedProcess[bytes]
            Result of the execution, including STDOUT and STDERR

        """
        self.job.wait()
        result = self._job_to_completed_process(self.job)

        self.check_returncode(result, raise_on_failure=self.raise_on_failure)

        if self.verbose:
            self.logger.debug(_log_command_output(result.stdout, result.stderr))

        return result


# This decorator exists to avoid always creating a new PSI/J `JobExecutor` instance with
# its own queue polling thread, causing potentially high loads on a batch system
_T = TypeVar("_T")
_P = ParamSpec("_P")
_memoized = {}


# I attempted to type this without explicitly adding the name keyword argument
# to `CommandRunner`, but concatenating keyword arguments with `ParamSpec`s is
# explicitly disallowed by PEP612 :(
def _memoize(cls: Callable[_P, _T]) -> Callable[_P, _T]:
    def inner(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        name = kwargs.get("name", None)
        if name is None:
            return cls(*args, **kwargs)
        if name not in _memoized:
            _memoized[name] = cls(*args, **kwargs)
        return _memoized[name]

    return inner


class CommandRunnerPSIJ(ProcessBase):
    """
    Command running utility based on PSI/J.

    Instantiate with preferred options and use a `run` method with your command.

    .. danger::
       It is not recommended to instantiate this class in the main process
       (i.e. outside of your nodes ``run()`` and ``prepare()`` methods).  This is due to
       `possible subtle threading problems <https://github.com/ExaWorks/psij-python/issues/387>`_
       from the interplay of maize and PSI/J.

    .. danger::
       The default user-facing class uses memoization. If you're instantiating multiple
       `CommandRunner` instances in the same process, they will by default refer to the
       same instance. To create separate instances with potentially different parameters,
       supply a custom name.

    Parameters
    ----------
    name
        The name given to this instance. The user-accessible version of this class
        is memoized, so the same name will refer to the same instance.
    raise_on_failure
        Whether to raise an exception on failure, or whether to just return `False`.
    working_dir
        The working directory to use for execution, will use the current one by default.
    validators
        One or more `Validator` instances that will be called on the result of the command.
    prefer_batch
        Whether to prefer running on a batch submission system such as SLURM, if available
    rm_config
        Configuration of the resource manager
    max_retries
        How often to reattempt job submission for batch systems

    """

    def _make_callback(
        self, count: _JobCounterSemaphore, max_count: int = 0
    ) -> Callable[[Job, JobStatus], None]:
        def _callback(job: Job, status: JobStatus) -> None:
            if status.final:
                self.logger.info("Job completed (%s/%s)", count.total + 1, max_count)
                if job.status.exit_code is not None and job.status.exit_code > 0:
                    res = self._job_to_completed_process(job)
                    self.logger.warning(
                        "Job %s failed with exit code %s (%s)",
                        job.native_id,
                        res.returncode,
                        job.status.state,
                    )
                    self.logger.warning(_log_command_output(stdout=res.stdout, stderr=res.stderr))
                count.dec()

        return _callback

    def __init__(
        self,
        *,
        name: str | None = None,
        raise_on_failure: bool = True,
        working_dir: Path | None = None,
        validators: Sequence["Validator"] | None = None,
        prefer_batch: bool = False,
        rm_config: ResourceManagerConfig | None = None,
        max_retries: int = 3,
    ) -> None:
        super().__init__()
        self.name = name
        self.raise_on_failure = raise_on_failure
        self.working_dir = working_dir.absolute() if working_dir is not None else Path.cwd()
        self._write_output_to_temp = working_dir is None
        self.validators = validators or []
        self.config = ResourceManagerConfig() if rm_config is None else rm_config
        self.max_retries = max_retries

        # We're going to be doing local execution most of the time,
        # most jobs we run are going to be relatively short, and we'll
        # already have a reservation for the main maize workflow job
        system = "local"
        exec_config = JobExecutorConfig()
        if prefer_batch:
            self.logger.debug("Attempting to run on batch system")
            # FIXME check for any submission system
            if check_executable("sinfo"):
                system = self.config.system

                # We override the default 30s polling interval to go easy on delicate batch systems
                exec_config = SlurmExecutorConfig(
                    queue_polling_interval=self.config.polling_interval
                )
            elif self.config.system != "local":
                self.logger.warning(
                    "'%s' was not found on your system, running locally", self.config.system
                )
        self._executor = JobExecutor.get_instance(system, config=exec_config)

    def validate(self, result: subprocess.CompletedProcess[bytes]) -> None:
        """
        Validate a process result.

        Parameters
        ----------
        result
            Process result to validate

        Raises
        ------
        ProcessError
            If any of the validators failed or the returncode was not zero

        """

        for validator in self.validators:
            if not validator(result):
                msg = (
                    f"Validation failure for command '{_format_command(result)}' "
                    f"with validator '{validator}'"
                )
                self.logger.warning(_log_command_output(result.stdout, result.stderr))
                if self.raise_on_failure:
                    raise ProcessError(msg)
                self.logger.warning(msg)

    def run_async(
        self,
        command: list[str] | str,
        verbose: bool = False,
        working_dir: Path | None = None,
        command_input: str | None = None,
        pre_execution: list[str] | str | None = None,
        cuda_mps: bool = False,
        config: JobResourceConfig | None = None,
        n_retries: int = 3,
    ) -> RunningProcess:
        """
        Run a command locally.

        Parameters
        ----------
        command
            Command to run as a single string, or a list of strings
        verbose
            If ``True`` will also log any STDOUT or STDERR output
        working_dir
            Optional working directory
        command_input
            Text string used as input for command
        pre_execution
            Command to run directly before the main one
        cuda_mps
            Use the multi-process service to run multiple CUDA job processes on a single GPU
        config
            Job submission config if using batch system
        n_retries
            Number of submission retries

        Returns
        -------
        RunningProcess
            Process handler allowing waiting, killing, or monitoring the running command

        """

        job = self._create_job(
            command,
            config=config,
            working_dir=working_dir,
            verbose=verbose,
            command_input=command_input,
            pre_execution=pre_execution,
            cuda_mps=cuda_mps,
        )
        for i in range(n_retries):
            self._attempt_submission(job)
            proc = RunningProcess(job, verbose=verbose, raise_on_failure=self.raise_on_failure)
            if proc.id is not None:
                return proc
            self.logger.warning(
                "Job did not receive a valid ID, retrying... (%s/%s)", i + 1, n_retries
            )
        raise ProcessError(f"Unable to successfully submit job after {n_retries} retries")

    def run_only(
        self,
        command: list[str] | str,
        verbose: bool = False,
        working_dir: Path | None = None,
        command_input: str | None = None,
        config: JobResourceConfig | None = None,
        pre_execution: list[str] | str | None = None,
        timeout: float | None = None,
        cuda_mps: bool = False,
    ) -> subprocess.CompletedProcess[bytes]:
        """
        Run a command locally and block.

        Parameters
        ----------
        command
            Command to run as a single string, or a list of strings
        verbose
            If ``True`` will also log any STDOUT or STDERR output
        working_dir
            Optional working directory
        command_input
            Text string used as input for command
        config
            Resource configuration for jobs
        pre_execution
            Command to run directly before the main one
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
            If the returncode was not zero

        """

        job = self._create_job(
            command,
            working_dir=working_dir,
            verbose=verbose,
            command_input=command_input,
            config=config,
            pre_execution=pre_execution,
            cuda_mps=cuda_mps,
        )

        with JobHandler(counter=_JobCounterSemaphore()) as hand:
            self._attempt_submission(job)
            hand.add(job)
            job.wait(timeout=timedelta(seconds=timeout) if timeout is not None else None)

        result = self._job_to_completed_process(job)

        self.check_returncode(result, raise_on_failure=self.raise_on_failure)
        if verbose:
            self.logger.debug(_log_command_output(result.stdout, result.stderr))

        return result

    def run_validate(
        self,
        command: list[str] | str,
        verbose: bool = False,
        working_dir: Path | None = None,
        command_input: str | None = None,
        config: JobResourceConfig | None = None,
        pre_execution: list[str] | str | None = None,
        timeout: float | None = None,
        cuda_mps: bool = False,
    ) -> subprocess.CompletedProcess[bytes]:
        """
        Run a command and validate.

        Parameters
        ----------
        command
            Command to run as a single string, or a list of strings
        verbose
            If ``True`` will also log any STDOUT or STDERR output
        working_dir
            Optional working directory
        command_input
            Text string used as input for command
        config
            Resource configuration for jobs
        pre_execution
            Command to run directly before the main one
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

        """

        result = self.run_only(
            command=command,
            verbose=verbose,
            working_dir=working_dir,
            command_input=command_input,
            config=config,
            pre_execution=pre_execution,
            timeout=timeout,
            cuda_mps=cuda_mps,
        )
        self.validate(result=result)
        return result

    # Convenience alias for simple calls to `run_validate`
    run = run_validate

    def run_parallel(
        self,
        commands: Sequence[list[str] | str],
        verbose: bool = False,
        n_jobs: int = 1,
        validate: bool = False,
        working_dirs: Sequence[Path | None] | None = None,
        command_inputs: Sequence[str | None] | None = None,
        config: JobResourceConfig | None = None,
        pre_execution: list[str] | str | None = None,
        timeout: float | None = None,
        cuda_mps: bool = False,
        n_batch: int | None = None,
        batchsize: int | None = None,
    ) -> list[subprocess.CompletedProcess[bytes]]:
        """
        Run multiple commands locally in parallel and block.

        Parameters
        ----------
        commands
            Commands to run as a list of single strings, or a list of lists
        verbose
            If ``True`` will also log any STDOUT or STDERR output
        n_jobs
            Number of processes to spawn at once
        validate
            Whether to validate the command execution
        working_dirs
            Directories to execute each command in
        command_input
            Text string used as input for each command, or ``None``
        config
            Resource configuration for jobs
        pre_execution
            Command to run directly before the main one
        timeout
            Maximum runtime for the command in seconds, or unlimited if ``None``
        cuda_mps
            Use the multi-process service to run multiple CUDA job processes on a single GPU
        n_batch
            Number of batches of commands to run together
            sequentially. Incompatible with ``batchsize``.
        batchsize
            Size of command batches to run sequentially. Incompatible with ``n_batch``.

        Returns
        -------
        list[subprocess.CompletedProcess[bytes]]
            Result of the execution, including STDOUT and STDERR

        Raises
        ------
        ProcessError
            If the returncode was not zero

        """
        if command_inputs is not None and (n_batch is not None or batchsize is not None):
            raise ValueError(
                "Using command inputs and batching of commands is currently not compatible"
            )

        queue: Queue[tuple[list[str] | str, Path | None, str | None]] = Queue()
        working_dirs = [None for _ in commands] if working_dirs is None else list(working_dirs)
        if command_inputs is None:
            command_inputs = [None for _ in commands]

        if n_batch is not None or batchsize is not None:
            commands = [
                self._make_batch(comms)
                for comms in split_list(list(commands), n_batch=n_batch, batchsize=batchsize)
            ]
            new_wds = []
            for wds in split_list(working_dirs, n_batch=n_batch, batchsize=batchsize):
                if len(set(wds)) > 1:
                    raise ValueError("Working directories are not consistent within batches")
                new_wds.append(wds[0])
            working_dirs = new_wds
            command_inputs = [None for _ in commands]

        for command, work_dir, cmd_input in zip(commands, working_dirs, command_inputs):
            queue.put((command, work_dir, cmd_input))

        # If we're using a batch system we can submit as many jobs as we like,
        # up to a reasonable maximum set in the system config
        n_jobs = max(self.config.max_jobs, n_jobs) if self.config.system != "local" else n_jobs

        # Keeps track of completed jobs via a callback and a counter,
        # and thus avoids using a blocking call to `wait` (or threads)
        counter = _JobCounterSemaphore()
        callback = self._make_callback(counter, max_count=len(commands))
        self._executor.set_job_status_callback(callback)

        with JobHandler(counter=counter) as hand:
            while not queue.empty():
                hand.wait_for_slot(timeout=timeout, n_jobs=n_jobs)
                command, work_dir, cmd_input = queue.get()
                tentative_job = self._create_job(
                    command,
                    working_dir=work_dir,
                    verbose=verbose,
                    command_input=cmd_input,
                    config=config,
                    pre_execution=pre_execution,
                    cuda_mps=cuda_mps,
                )

                # Some failure modes necessitate recreating the job from the spec to
                # play it safe, which is why we have a potentially new job instance here
                job = self._attempt_submission(tentative_job)
                hand.add(job)

            # Wait for all jobs to complete
            jobs = hand.wait(timeout=timeout)

        # Collect all results
        results: list[subprocess.CompletedProcess[bytes]] = []
        for job in jobs:
            result = self._job_to_completed_process(job)
            self.check_returncode(result, raise_on_failure=self.raise_on_failure)
            if verbose:
                self.logger.debug(_log_command_output(result.stdout, result.stderr))

            if validate:
                self.validate(result)
            results.append(result)
        return results

    def _attempt_submission(self, job: Job) -> Job:
        """Attempts to submit a job"""

        # No special treatment for normal local commands
        if self._executor.name == "local":
            self._executor.submit(job)
            self.logger.info(
                "Running job with PSI/J id=%s, stdout=%s, stderr=%s",
                job.id,
                None if job.spec is None else job.spec.stdout_path,
                None if job.spec is None else job.spec.stderr_path,
            )
            return job

        # Batch systems like SLURM may have the occassional
        # hiccup, we try multiple times to make sure
        for i in range(self.max_retries):
            try:
                self._executor.submit(job)
            except SubmitException as err:
                self.logger.warning(
                    "Error submitting job, trying again (%s/%s)",
                    i + 1,
                    self.max_retries,
                    exc_info=err,
                )
                time.sleep(10)
            except InvalidJobException as err:
                self.logger.warning(
                    "Error submitting, possible duplicate, trying again (%s/%s)",
                    i + 1,
                    self.max_retries,
                    exc_info=err,
                )
                time.sleep(10)

                # Recreate job to be safe and get a new unique id
                job = Job(job.spec)
            else:
                self.logger.info(
                    "Submitted %s job with PSI/J id=%s, native id=%s, stdout=%s, stderr=%s",
                    None if job.executor is None else job.executor.name,
                    job.id,
                    job.native_id,
                    None if job.spec is None else job.spec.stdout_path,
                    None if job.spec is None else job.spec.stderr_path,
                )
                return job

        # Final try
        self._executor.submit(job)
        return job

    @staticmethod
    def _make_batch(commands: Sequence[list[str] | str]) -> str:
        """Batches commands together into a script"""
        file = Path(mkdtemp(prefix="maize-")) / "commands.sh"
        with file.open("w") as out:
            for command in commands:
                if isinstance(command, list):
                    command = " ".join(command)
                out.write(command + "\n")
        file.chmod(stat.S_IRWXU | stat.S_IRWXG)
        return file.as_posix()

    def _create_job(
        self,
        command: list[str] | str,
        working_dir: Path | None = None,
        verbose: bool = False,
        command_input: str | None = None,
        config: JobResourceConfig | None = None,
        pre_execution: list[str] | str | None = None,
        cuda_mps: bool = False,
    ) -> Job:
        """Creates a PSI/J job description"""

        # We can't fully rely on `subprocess.run()` here, so we split it ourselves
        if isinstance(command, str):
            command = shlex.split(command)

        if verbose:
            self.logger.debug("Running command: %s", " ".join(command))

        exe, *arguments = command

        # General resource manager options, options that shouldn't change much from job to job
        delta = _parse_slurm_walltime(self.config.walltime)
        job_attr = JobAttributes(
            queue_name=self.config.queue
            if config is None or config.queue is None
            else config.queue,
            project_name=self.config.project,
            duration=delta
            if config is None or config.walltime is None
            else _parse_slurm_walltime(config.walltime),
            custom_attributes=None
            if config is None
            else config.format_custom_attributes(self.config.system),
        )

        # Resource configuration
        if config is not None:
            resource_attr = ResourceSpecV1(
                node_count=config.nodes,
                processes_per_node=config.processes_per_node,
                process_count=config.processes,
                cpu_cores_per_process=config.cores_per_process,
                gpu_cores_per_process=config.gpus_per_process,
                exclusive_node_use=config.exclusive_use,
            )
        else:
            resource_attr = None

        # Annoyingly, we can't access STDOUT / STDERR directly when
        # using PSI/J, so we always have to create temporary files
        base_dir = (
            Path(mkdtemp()) if self._write_output_to_temp else (working_dir or self.working_dir)
        )
        base = base_dir / f"job-{unique_id()}"
        stdout = base.with_name(base.name + "-out")
        stderr = base.with_name(base.name + "-err")

        # Similarly, command inputs need to be written to a file and cannot be piped
        stdin = None
        if command_input is not None:
            stdin = base.with_name(base.name + "-in")
            with stdin.open("wb") as inp:
                inp.write(command_input.encode())

        env: dict[str, str] = {}

        # We may need to run the multi-process daemon to allow multiple processes on a single GPU
        if cuda_mps:
            mps_dir = mkdtemp(prefix="mps")
            env |= {
                "CUDA_MPS_LOG_DIRECTORY": mps_dir,
                "CUDA_MPS_PIPE_DIRECTORY": mps_dir,
                "MPS_DIR": mps_dir,
            }
            set_environment(env)
            self.logger.debug("Spawning job with CUDA multi-process service in %s", mps_dir)

        # Pre-execution script, we generally avoid this but in some cases it can be required
        pre_script = base.with_name(base.name + "-pre")
        if pre_execution is not None and isinstance(pre_execution, str):
            pre_execution = shlex.split(pre_execution)

        if cuda_mps:
            if pre_execution is None:
                pre_execution = ["nvidia-cuda-mps-control", "-d"]
            else:
                pre_execution.extend(["nvidia-cuda-mps-control", "-d"])

        if pre_execution is not None:
            if self.config.launcher == "srun":
                self.logger.warning(
                    "When using launcher 'srun' pre-execution commands may not propagate"
                )

            with pre_script.open("w") as pre:
                pre.write("#!/bin/bash\n")
                pre.write(" ".join(pre_execution))
                self.logger.debug("Using pre-execution command: %s", " ".join(pre_execution))

        # For some versions of PSI/J passing the environment explicitly will produce multiple
        # `--export` statements, of which the last one will always override all the previous
        # ones, resulting in an incomplete environment. So we instead rely on full environment
        # inheritance for now.
        spec = JobSpec(
            executable=exe,
            arguments=arguments,
            directory=working_dir or self.working_dir,
            stderr_path=stderr,
            stdout_path=stdout,
            stdin_path=stdin,
            attributes=job_attr,
            inherit_environment=True,
            launcher=self.config.launcher,
            resources=resource_attr,
            pre_launch=pre_script if pre_execution is not None else None,
        )
        return Job(spec)


CommandRunner = _memoize(CommandRunnerPSIJ)

# This is required for testing purposes, as pytest will attempt to run many tests concurrently,
# leading to tests sharing `CommandRunner` instances due to memoization when they shouldn't
# (as we're testing different parameters). This is not a problem in actual use, since
# `CommandRunner` will generally only be used from inside separate processes.
_UnmemoizedCommandRunner = CommandRunnerPSIJ
