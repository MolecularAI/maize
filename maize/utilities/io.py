"""Various input / output functionality."""

import argparse
from collections.abc import Sequence
from dataclasses import dataclass, field
import importlib
import importlib.util
import json
import logging
from pathlib import Path
import pkgutil
import os
import shutil
import sys
from tempfile import mkdtemp
from types import ModuleType
from typing import Any, Literal, TypeVar, TYPE_CHECKING, TypedDict
from typing_extensions import Self, assert_never

import toml
import yaml

from maize.utilities.execution import ResourceManagerConfig

if TYPE_CHECKING:
    from maize.core.workflow import Workflow

ScriptSpecType = dict[str, dict[Literal["interpreter", "location"], Path]]


class NodeConfigDict(TypedDict):
    """Dictionary form of `NodeConfig`"""

    python: str
    modules: list[str]
    scripts: dict[str, dict[Literal["interpreter", "location"], str]]
    commands: dict[str, str]


T = TypeVar("T")


XDG = "XDG_CONFIG_HOME"
MAIZE_CONFIG_ENVVAR = "MAIZE_CONFIG"


log_build = logging.getLogger("build")
log_run = logging.getLogger("run")


# See: https://xdgbasedirectoryspecification.com/
def _valid_xdg_path() -> bool:
    """Checks the XDG path spec is valid"""
    return XDG in os.environ and bool(os.environ[XDG]) and Path(os.environ[XDG]).is_absolute()


def expand_shell_vars(path: Path) -> Path:
    """Expands paths containing shell variables to the full path"""
    return Path(os.path.expandvars(path))


def remove_dir_contents(path: Path) -> None:
    """Removes all items contained in a directory"""
    items = list(path.glob("*"))
    log_run.debug("Found %s items to remove", len(items))
    for item in items:
        log_run.debug("Removing '%s'", item)
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink(missing_ok=True)


def common_parent(files: Sequence[Path]) -> Path:
    """
    Provides the common parent directory for a list of files.

    Parameters
    ----------
    files
        List of paths

    Returns
    -------
    Path
        Common parent directory

    """
    files = [file.absolute() for file in files]
    if len(files) == 1:
        return files[0].parent

    # This gets us the common parent among all source paths
    file_parts = (file.parts for file in files)
    return Path(*(part[0] for part in zip(*file_parts) if len(set(part)) == 1))


def sendtree(
    files: dict[T, Path], dest: Path, mode: Literal["move", "copy", "link"] = "copy"
) -> dict[T, Path]:
    """
    Links, copies or moves multiple files to a destination directory and preserves the structure.

    Parameters
    ----------
    files
        Paths to link / copy
    dest
        Destination directory
    mode
        Whether to link, copy or move the files

    Returns
    -------
    dict[Any, Path]
        Created links / copies

    """
    files = {k: file.absolute() for k, file in files.items()}
    common = common_parent(list(files.values()))

    results: dict[T, Path] = {}
    for k, file in files.items():
        dest_path = dest.absolute() / file.relative_to(common)
        if not dest_path.exists():
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            if mode == "link":
                os.symlink(file, dest_path)
            elif mode == "copy":
                shutil.copy(file, dest_path)
            elif mode == "move":
                shutil.move(file, dest_path)
            else:
                assert_never()
        results[k] = dest_path
    return results


@dataclass
class NodeConfig:
    """
    Node specific configuration.

    Parameters
    ----------
    python
        Python interpreter to use to run the node
    modules
        Map from callables to modules
    scripts
        Map from callables to interpreter - script pairs
    commands
        Paths to specific commands
    parameters
        Default parameter settings

    """

    python: Path = field(default_factory=lambda: Path(sys.executable))
    modules: list[str] = field(default_factory=list)
    commands: dict[str, Path | str] = field(default_factory=dict)
    scripts: ScriptSpecType = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Any) -> Self:
        """
        Generate from a dictionary.

        Parameters
        ----------
        data
            Dictionary read in from a config file

        """
        config = cls()
        if "python" in data:
            config.python = data["python"]
        if "modules" in data:
            config.modules = data["modules"]
        if "scripts" in data:
            config.scripts = data["scripts"]

            # Current python executable is the default
            for dic in config.scripts.values():
                if "interpreter" not in dic:
                    dic["interpreter"] = Path(sys.executable)

                # Make sure we have paths
                for k in dic:
                    dic[k] = Path(dic[k])

        if "commands" in data:
            config.commands = {exe: Path(path) for exe, path in data["commands"].items()}
        if "parameters" in data:
            config.parameters = data["parameters"]

        return config

    def generate_template(self, required_callables: list[str]) -> NodeConfigDict:
        """
        Generate a template configuration

        Parameters
        ----------
        required_callables
            The list of software to generate a template for

        Returns
        -------
        NodeConfigDict
            Dictionary that can be serialized or used directly

        """
        res: NodeConfigDict = {
            "python": self.python.as_posix(),
            "modules": self.modules,
            "commands": {prog: f"path/to/{prog}" for prog in required_callables},
            "scripts": {
                prog: {"interpreter": "path/to/python", "location": f"path/to/{prog}"}
                for prog in required_callables
            },
        }
        return res

    def generate_template_toml(self, name: str, required_callables: list[str]) -> str:
        """
        Generate a template configuration as a TOML string

        Parameters
        ----------
        required_callables
            The list of software to generate a template for

        Returns
        -------
        str
            TOML config string

        """
        return toml.dumps({name: self.generate_template(required_callables)})


@dataclass
class Config:
    """
    Global configuration options.

    Parameters
    ----------
    packages
        List of namespace packages to load
    scratch
        The directory the workflow should be created in. Uses a temporary directory by default.
    batch_config
        Default options to be passed to the batch submission system
    environment
        Any environment variables to be set in the execution context
    nodes
        Entries specific to each node

    Examples
    --------
    Here's an example configuration file with all sections:

    .. literalinclude:: ../../../examples/config.toml
       :language: toml
       :linenos:

    """

    packages: list[str] = field(default_factory=lambda: ["maize.steps.mai", "maize.graphs.mai"])
    scratch: Path = Path(mkdtemp())
    batch_config: ResourceManagerConfig = field(default_factory=ResourceManagerConfig)
    environment: dict[str, str] = field(default_factory=dict)
    nodes: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_default(cls) -> Self:
        """
        Create a default configuration from:

        * A path specified using the ``MAIZE_CONFIG`` environment variable
        * A config file at ``~/.config/.maize.toml``

        """
        config = cls()
        if MAIZE_CONFIG_ENVVAR in os.environ:
            config_file = Path(os.environ[MAIZE_CONFIG_ENVVAR])
            config.update(config_file)
            log_build.debug("Using config at %s", config_file.as_posix())
        elif _valid_xdg_path() and (config_file := Path(os.environ[XDG]) / "maize.toml").exists():
            config.update(config_file)
            log_build.debug("Using config at %s", config_file.as_posix())
        else:
            msg = "Could not find a config file"
            if not _valid_xdg_path():
                msg += f" (${XDG} is not set)"
            log_build.warning(msg)
        return config

    def update(self, file: Path) -> None:
        """
        Read a maize configuration file.

        Parameters
        ----------
        file
            Path to the configuration file

        """
        data = read_input(file)
        log_build.debug("Updating config with %s", file.as_posix())

        for key, item in data.items():
            match key:
                case "batch":
                    self.batch_config = ResourceManagerConfig(**item)
                case "scratch":
                    self.scratch = Path(item)
                case "environment":
                    self.environment = item
                case "packages":
                    self.packages.extend(item)
                case _:
                    self.nodes[key.lower()] = NodeConfig.from_dict(item)


# It's enough to import the base namespace package and let importlib
# find all modules. Any defined custom nodes will then be registered
# internally, and we don't have to refer to the explicit module path
# for workflow definitions. See the namespace package discovery documentation:
# https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/
def get_plugins(package: ModuleType | str) -> dict[str, ModuleType]:
    """
    Finds packages in a given namespace.

    Parameters
    ----------
    package
        Base namespace package to load

    Returns
    -------
    dict[str, ModuleType]
        Dictionary of module names and loaded modules

    """
    if isinstance(package, str):
        package = importlib.import_module(package)
    return {
        name: importlib.import_module(name)
        for _, name, _ in pkgutil.iter_modules(package.__path__, package.__name__ + ".")
    }


def load_file(file: Path | str, name: str | None = None) -> ModuleType:
    """
    Load a python file as a module.

    Parameters
    ----------
    file
        Python file to load
    name
        Optional name to use for the module, will use the filename if not given

    Returns
    -------
    ModuleType
        The loaded module

    """
    file = Path(file)
    name = file.name if name is None else name
    spec = importlib.util.spec_from_file_location(name, file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to import file '{file.as_posix()}'")

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def parse_groups(parser: argparse.ArgumentParser) -> dict[str, argparse.Namespace]:
    """
    Parse commandline arguments into separate groups.

    Parameters
    ----------
    parser
        Parser with initialised groups and added arguments

    Returns
    -------
    dict[str, dict[str, Any]]
        Parsed arguments sorted by group

    """
    args = parser.parse_args()
    groups = {}
    for group in parser._action_groups:  # pylint: disable=protected-access
        if group.title is None:
            continue
        actions = {
            arg.dest: getattr(args, arg.dest, None)
            for arg in group._group_actions  # pylint: disable=protected-access
        }
        groups[group.title] = argparse.Namespace(**actions)
    return groups


def create_default_parser() -> argparse.ArgumentParser:
    """
    Creates the default maize commandline arguments.

    Returns
    -------
    argparse.ArgumentParser
        The created parser object

    """
    parser = argparse.ArgumentParser(description="Flow-based graph execution engine")
    conf = parser.add_argument_group("maize")
    conf.add_argument(
        "-c",
        "--check",
        action="store_true",
        default=False,
        help="Check if the graph was built correctly and exit",
    )
    conf.add_argument(
        "-l", "--list", action="store_true", default=False, help="List all available nodes and exit"
    )
    conf.add_argument(
        "-o",
        "--options",
        action="store_true",
        default=False,
        help="List all exposed workflow parameters and exit",
    )
    conf.add_argument(
        "-d", "--debug", action="store_true", default=False, help="Provide debugging information"
    )
    conf.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help="Silence all output except errors and warnings",
    )
    conf.add_argument("--keep", action="store_true", default=False, help="Keep all output files")
    conf.add_argument("--config", type=Path, help="Global configuration file to use")
    conf.add_argument("--parameters", type=Path, help="Additional parameters in JSON format")
    conf.add_argument("--log", type=Path, help="Logfile to use")
    return parser


def setup_workflow(workflow: "Workflow") -> None:
    """
    Sets up an initialized workflow so that it can be run on the commandline as a script.

    Parameters
    ----------
    workflow
        The built workflow object to expose

    """
    # Argument parsing - we create separate groups for
    # global settings and workflow specific options
    parser = create_default_parser()
    parser.description = workflow.description

    # Workflow-specific settings
    flow = parser.add_argument_group(workflow.name)
    flow = workflow.add_arguments(flow)
    groups = parse_groups(parser)

    # Global settings
    args = groups["maize"]
    workflow.update_settings_with_args(args)
    workflow.update_parameters(**vars(groups[workflow.name]))

    # Execution
    workflow.check()
    if args.check:
        workflow.logger.info("Workflow compiled successfully")
        return

    workflow.execute()


def with_keys(data: dict[T, Any], keys: Sequence[T]) -> dict[T, Any]:
    """Provide a dictionary subset based on keys."""
    return {k: v for k, v in data.items() if k in keys}


def with_fields(data: Any, keys: Sequence[T]) -> dict[T, Any]:
    """Provide a dictionary based on a subset of object attributes."""
    return with_keys(data.__dict__, keys=keys)


class _PathEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Path):
            return o.as_posix()
        return json.JSONEncoder.default(self, o)


def read_input(path: Path) -> dict[str, Any]:
    """Reads an input file in JSON, YAML or TOML format and returns a dictionary."""
    if not path.exists():
        raise FileNotFoundError(f"File at {path.as_posix()} not found")

    data: dict[str, Any]
    with path.open("r") as file:
        suffix = path.suffix.lower()[1:]
        if suffix == "json":
            data = json.load(file)
        elif suffix in ("yaml", "yml"):
            data = yaml.safe_load(file.read())
        elif suffix == "toml":
            data = toml.loads(file.read())
        else:
            raise ValueError(f"Unknown type '{suffix}'. Valid types: 'json', 'yaml', 'toml'")

    return data


def write_input(path: Path, data: dict[str, Any]) -> None:
    """Saves a dictionary in JSON, YAML or TOML format."""
    with path.open("w") as file:
        suffix = path.suffix.lower()[1:]
        if suffix == "json":
            file.write(json.dumps(data, indent=4, cls=_PathEncoder))
        elif suffix in ("yaml", "yml"):
            file.write(yaml.dump(data))
        elif suffix == "toml":
            file.write(toml.dumps(data))
        else:
            raise ValueError(f"Unknown type '{suffix}'. Valid types: 'json', 'yaml', 'toml'")
