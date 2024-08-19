"""Various unclassifiable utilities."""

import ast
from collections.abc import Generator, Callable
from contextlib import redirect_stderr
import contextlib
import datetime
from enum import Enum
import functools
import inspect
import io
import itertools
import math
import os
from pathlib import Path
import random
import re
import shlex
import time
import string
import sys
from types import UnionType
from typing import (
    TYPE_CHECKING,
    AnyStr,
    Literal,
    TypeVar,
    Any,
    Annotated,
    Union,
    TypeAlias,
    cast,
    get_args,
    get_origin,
)
from typing_extensions import assert_never
import warnings

from beartype.door import is_subhint
import networkx as nx

if TYPE_CHECKING:
    from maize.core.graph import Graph
    from maize.core.component import Component

T = TypeVar("T")
U = TypeVar("U")


def unique_id(length: int = 6) -> str:
    """
    Creates a somewhat unique identifier.

    Parameters
    ----------
    length
        Length of the generated ID

    Returns
    -------
    str
        A random string made up of lowercase ASCII letters and digits

    """
    # This should be safer than truncating a UUID
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


class StrEnum(Enum):
    """Allows use of enum names as auto string values. See `StrEnum` in Python 3.11."""

    @staticmethod
    def _generate_next_value_(name: str, *_: Any) -> str:
        return name


# See this SO answer:
# https://stackoverflow.com/questions/39372708/spawn-multiprocessing-process-under-different-python-executable-with-own-path
def change_environment(exec_path: str | Path) -> None:  # pragma: no cover
    """
    Changes the python environment based on the executable.

    Parameters
    ----------
    exec_path
        Path to the python executable (normally ``sys.executable``)

    """
    old_path = os.environ.get("PATH", "")
    exec_abs = Path(exec_path).parent.absolute()
    os.environ["PATH"] = exec_abs.as_posix() + os.pathsep + old_path
    base = exec_abs.parent
    site_packages = base / "lib" / f"python{sys.version[:4]}" / "site-packages"
    old_sys_path = list(sys.path)

    import site  # pylint: disable=import-outside-toplevel

    site.addsitedir(site_packages.as_posix())
    sys.prefix = base.as_posix()
    new_sys_path = []
    for item in list(sys.path):
        if item not in old_sys_path:
            new_sys_path.append(item)
            sys.path.remove(item)
    sys.path[:0] = new_sys_path


def set_environment(env: dict[str, str]) -> None:
    """
    Set global system environment variables.

    Parameters
    ----------
    env
        Dictionary of name-value pairs

    """
    for key, value in env.items():
        os.environ[key] = value


def has_module_system() -> bool:
    """Checks whether the system can use modules."""
    return "LMOD_DIR" in os.environ


# https://stackoverflow.com/questions/5427040/loading-environment-modules-within-a-python-script
def load_modules(*names: str) -> None:
    """
    Loads environment modules using ``lmod``.

    Parameters
    ----------
    names
        Module names to load

    """
    lmod = Path(os.environ["LMOD_DIR"])
    sys.path.insert(0, (lmod.parent / "init").as_posix())
    from env_modules_python import module  # pylint: disable=import-outside-toplevel,import-error

    for name in names:
        out = io.StringIO()  # pylint: disable=no-member
        with redirect_stderr(out):
            module("load", name)
        if "error" in out.getvalue():
            raise OSError(f"Error loading module '{name}'")


def _extract_single_class_docs(node_type: type["Component"]) -> dict[str, str]:
    """Extracts docs from a class definition in the style of :pep:`258`."""
    docs = {}

    # A lot of things can go wrong here, so we err on the side of caution
    with contextlib.suppress(Exception):
        source = inspect.getsource(node_type)
        for node in ast.iter_child_nodes(ast.parse(source)):
            for anno, expr in itertools.pairwise(ast.iter_child_nodes(node)):
                match anno, expr:
                    case ast.AnnAssign(
                        target=ast.Name(name),
                        annotation=ast.Subscript(
                            value=ast.Name(
                                id="Input" | "Output" | "Parameter" | "FileParameter" | "Flag"
                            )
                        ),
                    ), ast.Expr(value=ast.Constant(doc)):
                        docs[name] = doc
    return docs


# This is *slightly* evil, but it allows us to get docs that are
# both parseable by sphinx and usable for commandline help messages
def extract_attribute_docs(node_type: type["Component"]) -> dict[str, str]:
    """
    Extracts attribute docstrings in the style of :pep:`258`.

    Parameters
    ----------
    node_type
        Node class to extract attribute docstrings from

    Returns
    -------
    dict[str, str]
        Dictionary of attribute name - docstring pairs

    """
    docs: dict[str, str] = {}
    for cls in (node_type, *node_type.__bases__):
        docs |= _extract_single_class_docs(cls)
    return docs


def typecheck(value: Any, datatype: Any) -> bool:
    """
    Checks if a value is valid using type annotations.

    Parameters
    ----------
    value
        Value to typecheck
    datatype
        Type to check against. Can also be a Union or Annotated.

    Returns
    -------
    bool
        ``True`` if the value is valid, ``False`` otherwise

    """
    # This means we have probably omitted types
    if datatype is None or isinstance(datatype, TypeVar) or datatype == Any:
        return True

    # In some cases (e.g. file types) we'll hopefully have an
    # `Annotated` type, using a custom callable predicate for
    # validation. This could be a check for the correct file
    # extension or a particular range for a numerical input
    if get_origin(datatype) == Annotated:
        cls, *predicates = get_args(datatype)
        return typecheck(value, cls) and all(pred(value) for pred in predicates)

    if get_origin(datatype) == Literal:  # pylint: disable=comparison-with-callable
        options = get_args(datatype)
        return value in options

    # Any kind of container type
    if len(anno := get_args(datatype)) > 0:
        if get_origin(datatype) == UnionType:
            return any(typecheck(value, cls) for cls in anno)
        if get_origin(datatype) in (tuple, list):
            return all(typecheck(val, arg) for val, arg in zip(value, get_args(datatype)))
        if get_origin(datatype) == dict:
            key_type, val_type = get_args(datatype)
            return all(typecheck(key, key_type) for key in value) and all(
                typecheck(val, val_type) for val in value.values()
            )

        # Safe fallback in case we don't know what this is, this should avoid false positives
        datatype = get_origin(datatype)
    return isinstance(value, datatype)


_U = TypeVar("_U", Callable[[Any], Any], type[Any])


def deprecated(
    msg: str | None = None,
) -> Callable[[_U], _U]:
    """Inserts a deprecation warning for a class or a function"""

    msg = "." if msg is None else ", " + msg

    def _warn(obj: Any) -> None:
        warnings.simplefilter("always", DeprecationWarning)
        warnings.warn(
            f"{obj.__name__} is deprecated" + msg,
            category=DeprecationWarning,
            stacklevel=2,
        )
        warnings.simplefilter("default", DeprecationWarning)

    def deprecator(obj: _U) -> _U:
        if inspect.isclass(obj):
            orig_init = obj.__init__

            def __init__(self: T, *args: Any, **kwargs: Any) -> None:
                _warn(obj)
                orig_init(self, *args, **kwargs)

            # Mypy complains, recommended workaround seems to be to just ignore:
            # https://github.com/python/mypy/issues/2427
            obj.__init__ = __init__
            return obj

        # Order matters here, as classes are also callable
        elif callable(obj):

            def inner(*args: Any, **kwargs: Any) -> Any:
                _warn(obj)
                return obj(*args, **kwargs)

            return cast(_U, inner)

        else:
            assert_never(obj)

    return deprecator


class Timer:
    """
    Timer with start, pause, and stop functionality.

    Examples
    --------
    >>> t = Timer()
    ... t.start()
    ... do_something()
    ... t.pause()
    ... do_something_else()
    ... print(t.stop())

    """

    def __init__(self) -> None:
        self._start = 0.0
        self._running = False
        self._elapsed_time = 0.0

    @property
    def elapsed_time(self) -> datetime.timedelta:
        """Returns the elapsed time."""
        if self.running:
            self.pause()
            self.start()
        return datetime.timedelta(seconds=self._elapsed_time)

    @property
    def running(self) -> bool:
        """Returns whether the timer is currently running."""
        return self._running

    def start(self) -> None:
        """Start the timer."""
        self._start = time.time()
        self._running = True

    def pause(self) -> None:
        """Temporarily pause the timer."""
        self._elapsed_time += time.time() - self._start
        self._running = False

    def stop(self) -> datetime.timedelta:
        """Stop the timer and return the elapsed time in seconds."""
        if self.running:
            self.pause()
        return datetime.timedelta(seconds=self._elapsed_time)


def graph_cycles(graph: "Graph") -> list[list[str]]:
    """Returns whether the graph contains cycles."""
    mdg = graph_to_nx(graph)
    return list(nx.simple_cycles(mdg))


def graph_to_nx(graph: "Graph") -> nx.MultiDiGraph:
    """
    Converts a workflow graph to a ``networkx.MultiDiGraph`` object.

    Parameters
    ----------
    graph
        Workflow graph

    Returns
    -------
    nx.MultiDiGraph
        Networkx graph instance

    """
    mdg = nx.MultiDiGraph()
    unique_nodes = {node.component_path for node in graph.flat_nodes}
    unique_channels = {(inp[:-1], out[:-1]) for inp, out in graph.flat_channels}
    mdg.add_nodes_from(unique_nodes)
    mdg.add_edges_from(unique_channels)
    return mdg


# `Unpack` just seems completely broken with 3.10 and mypy 0.991
# See PEP646 on tuple unpacking:
# https://peps.python.org/pep-0646/#unpacking-unbounded-tuple-types
NestedDict: TypeAlias = dict[T, Union["NestedDict[T, U]", U]]


def tuple_to_nested_dict(*data: Any) -> NestedDict[T, U]:
    """Convert a tuple into a sequentially nested dictionary."""
    out: NestedDict[T, U]
    ref: NestedDict[T, U]
    out = ref = {}
    *head, semifinal, final = data
    for token in head:
        ref[token] = ref = {}
    ref[semifinal] = final
    return out


def nested_dict_to_tuple(__data: NestedDict[T, U]) -> tuple[T | U, ...]:
    """Convert a sequentially nested dictionary into a tuple."""
    out: list[T | U] = []
    data: U | NestedDict[T, U] = __data
    while data is not None:
        if not isinstance(data, dict):
            out.append(data)
            break
        first, data = next(iter(data.items()))
        out.append(first)
    return tuple(out)


def has_file(path: Path) -> bool:
    """Returns whether the specified directory contains files."""
    return path.exists() and len(list(path.iterdir())) > 0


def make_list(item: list[T] | set[T] | tuple[T, ...] | T) -> list[T]:
    """Makes a single item or sequence of items into a list."""
    if isinstance(item, list | set | tuple):
        return list(item)
    return [item]


def chunks(data: list[T], n: int) -> Generator[list[T], None, None]:
    """Splits a dataset into ``n`` chunks"""
    size, rem = divmod(len(data), n)
    for i in range(n):
        si = (size + 1) * (i if i < rem else rem) + size * (0 if i < rem else i - rem)
        yield data[si : si + (size + 1 if i < rem else size)]


def split_list(
    arr: list[T], *, n_batch: int | None = None, batchsize: int | None = None
) -> list[list[T]]:
    """
    Splits a list into smaller lists.

    Parameters
    ----------
    arr
        List to split
    n_batch
        Number of batches to generate
    batchsize
        Size of the batches

    Returns
    -------
    list[list[T]]
        List of split lists

    """
    if batchsize is None and n_batch is not None:
        batchsize = math.ceil(len(arr) / n_batch)
    elif (batchsize is None and n_batch is None) or n_batch is not None:
        raise ValueError("You must specify either the number of batches or the batchsize")
    assert batchsize is not None  # Only needed to shutup mypy
    return [arr[i : i + batchsize] for i in range(0, len(arr), batchsize)]


def split_multi(string: str, chars: str) -> list[str]:
    """
    Split string on multiple characters

    Parameters
    ----------
    string
        String to split
    chars
        Characters to split on

    Returns
    -------
    list[str]
        List of string splitting results

    """
    if not chars:
        return [string]
    splits = []
    for chunk in string.split(chars[0]):
        splits.extend(split_multi(chunk, chars[1:]))
    return splits


def format_datatype(dtype: Any) -> str:
    """
    Formats a datatype to print nicely.

    Parameters
    ----------
    dtype
        Datatype to format

    Returns
    -------
    str
        Formatted datatype

    """
    if hasattr(dtype, "__origin__"):
        return str(dtype)
    return str(dtype.__name__)


def get_all_annotations(cls: type, visited: set[type] | None = None) -> dict[str, Any]:
    """
    Recursively collect all annotations from a class and its superclasses.

    Parameters
    ----------
    cls
        The class to find annotations from

    visited
        Set of visited classes

    Returns
    -------
    dict[str, Any]:
        Dictionary of annotations
    """
    if visited is None:
        visited = set()
    if cls in visited:
        return {}
    visited.add(cls)
    # Start with an empty dictionary for annotations
    annotations: dict[str, Any] = {}
    # First, recursively collect and merge annotations from base classes
    for base in cls.__bases__:
        annotations |= get_all_annotations(base, visited)
    # Then, merge those with the current class's annotations, ensuring they take precedence
    annotations |= cls.__annotations__ if hasattr(cls, "__annotations__") else {}
    return annotations


# If the type is not given in the constructor (e.g. `out = Output[int]()`),
# it's hopefully given as an annotation (e.g. `out: Output[int] = Output()`)
def extract_superclass_type(owner: Any, name: str) -> Any:
    """
    Extract type annotations from superclasses.

    Parameters
    ----------
    owner
        Parent object with annotations in the form of ``x: str = ...``
    name
        Name of the variable

    Returns
    -------
    Any
        The annotated type, ``None`` if there wasn't one found

    """
    # __annotations__ does not include super class
    # annotations, so we combine all of them first
    annotations = get_all_annotations(owner.__class__)
    annotations |= owner.__annotations__
    if name in annotations:
        return get_args(annotations[name])[0]
    return None


def extract_type(obj: Any) -> Any:
    """
    Extract type annotations from an object or class.

    Parameters
    ----------
    obj
        Object with type annotations in the form of ``Object[...]``

    Returns
    -------
    Any
        The annotated type, ``None`` if there wasn't one found

    """
    # This is an undocumented implementation detail to retrieve type
    # arguments from an instance to use for dynamic / runtime type checking,
    # and could break at some point. We should revisit this in the future,
    # but for now it conveniently removes a bit of `Node` definition boilerplate.
    # See also this SO question:
    # https://stackoverflow.com/questions/57706180/generict-base-class-how-to-get-type-of-t-from-within-instance
    if hasattr(obj, "__orig_class__"):
        return get_args(obj.__orig_class__)[0]
    if hasattr(obj, "__args__"):
        return obj.__args__[0]
    return None


def is_path_type(arg1: Any) -> bool:
    """
    Checks if type is a `Path`-like type.

    Parameters
    ----------
    arg1
        The datatype to check

    Returns
    -------
    bool
        ``True`` if the type is a `Path`-like, ``False`` otherwise

    """
    return is_subhint(arg1, Path | list[Path] | Annotated[Path, ...] | dict[Any, Path])


# FIXME There seems to be an issue with python 3.11 and NDArray typehints. We're sticking with
# python 3.10 for now, but ideally we would drop the dependency on beartype at some point.
def matching_types(arg1: Any, arg2: Any, strict: bool = False) -> bool:
    """
    Checks if two types are matching.

    Parameters
    ----------
    arg1, arg2
        The datatypes to compare
    strict
        If set, will only output ``True`` if the types match exactly

    Returns
    -------
    bool
        ``True`` if the types are compatible, ``False`` otherwise

    """
    if strict:
        return is_subhint(arg1, arg2) and is_subhint(arg2, arg1)
    if None in (arg1, arg2):
        return True
    return is_subhint(arg1, arg2) or is_subhint(arg2, arg1)


def find_probable_files_from_command(command: str | list[str]) -> list[Path]:
    """
    Finds possible files from a command string.

    Should not be fully relied upon, as a file located in the current
    directory with no suffix will not be easily identifiable as such.

    Parameters
    ----------
    command
        String or list of tokens to check for files

    Returns
    -------
    list[Path]
        Listing of `Path` objects, or an empty list if no files were found

    """
    if isinstance(command, str):
        command = shlex.split(command)
    # The `Path` constructor will never throw an exception
    # as long as we supply `str` (or a `str` subtype)
    return [Path(token) for token in command if any(c in token for c in ("/", "."))]


def match_context(match: re.Match[AnyStr], chars: int = 100) -> AnyStr:
    """
    Provides context to a regular expression match.

    Parameters
    ----------
    match
        Regular expression match object
    chars
        Number of characters of context to provide

    Returns
    -------
    AnyStr
        Match context

    """
    return match.string[match.start() - chars : match.end() + chars]
