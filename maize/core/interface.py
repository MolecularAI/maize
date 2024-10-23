"""
Interface
---------
This module encompasses all possible node interfaces. This includes parameters,
such as configuration files (using `FileParameter`) and simple values (using `Parameter`),
but also `Input` (`MultiInput`) and `Output` (`MultiOutput`) ports allowing the attachment of
(multiple) channels to communicate with other nodes. All interfaces expose a ``datatype``
attribute that is used to ensure the usage of correct types when constructing a workflow graph.

"""

from abc import abstractmethod
from collections.abc import Iterable, Callable, Sequence, Generator
from datetime import datetime
import inspect
import logging
import os
from pathlib import Path
from typing import (
    Annotated,
    Literal,
    TypeGuard,
    TypeVar,
    ClassVar,
    Generic,
    Union,
    Any,
    cast,
    get_args,
    get_origin,
    TYPE_CHECKING,
)

from maize.core.runtime import Status
from maize.core.channels import Channel, ChannelFull
from maize.utilities.utilities import (
    extract_superclass_type,
    extract_type,
    format_datatype,
    tuple_to_nested_dict,
    NestedDict,
    typecheck,
)

if TYPE_CHECKING:
    # In commit #6ec9884 we introduced a setter into Component, this seems
    # to have the effect of mypy not being able to import the full class
    # definition at type-checking time. The results are various "Cannot
    # determine type of ... [has-type]", and "Incompatible return value
    # type (got "T", expected "T | None") [return-value]" errors. See
    # https://github.com/python/mypy/issues/16337 for a potentially
    # related error.
    from maize.core.component import Component

# I have temporarily removed dynamic typing for now, but if we
# reintroduce it intersection types will come in handy:
# https://github.com/python/typing/issues/213

T = TypeVar("T")


log = logging.getLogger(f"run-{os.getpid()}")


UPDATE_INTERVAL = 60


class Suffix:
    """
    Utility class to annotate paths with restrictions on possible suffixes.

    Parameters
    ----------
    suffixes
        Any number of possible file suffixes without the leading dot

    Examples
    --------
    >>> path: Annotated[Path, Suffix("pdb", "gro", "tpr")] = Path("conf.xyz")
    >>> pred = get_args(path)[1]
    >>> pred(path)
    False

    In practice it might look like this:

    >>> class Example(Node):
    ...     para = FileParameter[Annotated[Path, Suffix("pdb", "gro")]]()
    ...     out = Output[int]()
    ...     def run(self) -> None: ...

    This will then raise a `ValueError` when trying to set the
    parameter with a file that doesn't have the correct suffix.

    See Also
    --------
    FileParameter
        Parameter subclass using these annotations

    """

    def __init__(self, *suffixes: str) -> None:
        self._valid = frozenset(suf.lstrip(".") for suf in suffixes)

    def __call__(self, path: Path) -> bool:
        return path.suffix.lstrip(".") in self._valid

    def __repr__(self) -> str:
        return f"Suffix({', '.join(self._valid)})"

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, self.__class__):
            return False
        return len(self._valid.intersection(__value._valid)) > 0

    def __hash__(self) -> int:
        return hash(self._valid)


# Default port polling timeout, i.e. the frequency with which I/O for nodes is polled
DEFAULT_TIMEOUT = 0.5


# The idea behind the interface base class is that we can declare it
# as a class attribute in the node, with type information and some
# settings. Then when the node constructor is executed, we create a
# copy of each interface to make sure each node instance has an
# individual instance of an interface. This is done by saving the
# constructor arguments (using `__new__`) and reinitializing the
# class by calling the constructor again in `build`.
class Interface(Generic[T]):
    """
    Interface parent class, handles behaviour common to ports and parameters.

    Attributes
    ----------
    name
        Unique interface name
    parent
        Parent node instance this interface is part of
    datatype
        The datatype of the associated value. This may be a type from the ``typing``
        library and thus not always usable for checks with ``isinstance()``

    """

    _TInter = TypeVar("_TInter", bound="Interface[Any]")

    name: str
    parent: "Component"
    datatype: Any
    doc: str | None

    # These allow `build` to create a shallow copy of the interface
    _args: tuple[Any, ...]
    _kwargs: dict[str, Any]

    # Where to place the interface in the parent component
    _target: ClassVar[str]

    # See comment in `build`, this is for the typechecker
    __orig_class__: ClassVar[Any]

    def __new__(cls: type[_TInter], *args: Any, **kwargs: Any) -> _TInter:
        inst = super().__new__(cls)
        inst._args = args
        inst._kwargs = kwargs
        return inst

    def __repr__(self) -> str:
        if not hasattr(self, "name"):
            return f"{self.__class__.__name__}()"
        return (
            f"{self.__class__.__name__}[{self.datatype}]"
            f"(name='{self.name}', parent='{self.parent.name}')"
        )

    @property
    def path(self) -> tuple[str, ...]:
        """Provides a unique path to the interface."""
        return *self.parent.component_path, self.name

    @staticmethod
    def _update_generic_type(obj: _TInter) -> None:
        """Update the contained generic datatype with parent information."""
        if isinstance(obj.datatype, TypeVar) and obj.parent.datatype is not None:
            obj.datatype = obj.parent.datatype

        # Check for the case where the datatype is GenericAlias but the argument is
        # a TypeVar e.g. List[~T]
        if get_origin(obj.datatype) is not None:
            args = get_args(obj.datatype)
            if isinstance(args[0], TypeVar) and obj.parent.datatype is not None:
                obj.datatype = get_origin(obj.datatype)[obj.parent.datatype]

    @property
    def serialized(self) -> dict[str, Any]:
        """Provides a serialized summary of the parameter"""
        dtype = Any if not hasattr(self, "datatype") else self.datatype
        return {"type": str(dtype), "kind": self.__class__.__name__}

    # What we would really need to type this correctly is a `TypeVar`
    # that is bound to a generic, i.e. `TypeVar("U", bound=Interface[T])`,
    # (or higher-kinded types) but this doesn't seem possible at the moment, see:
    # https://github.com/python/mypy/issues/2756
    # https://github.com/python/typing/issues/548
    def build(self: _TInter, name: str, parent: "Component") -> _TInter:
        """
        Instantiate an interface from the description.

        Parameters
        ----------
        name
            Name of the interface, will typically be the attribute name of the parent object
        parent
            Parent component instance

        Returns
        -------
        _TInter
            Copy of the current instance, with references to the name and the parent

        """
        inst = self.__class__(*self._args, **self._kwargs)
        inst.name = name
        inst.parent = parent
        inst.datatype = None
        inst.doc = None

        inst.datatype = extract_type(self)
        if inst.datatype is None:
            inst.datatype = extract_superclass_type(parent, name)

        # If the parent component is generic and was instantiated using
        # type information, we can use that information to update the
        # interface's datatype (in case it was also generic). This is
        # unfortunately not a comprehensive solution, as we might have
        # multiple typevars or containerized types, but those are quite
        # tricky to handle properly.
        self._update_generic_type(inst)

        # Register in parent inputs/outputs/parameters dictionary
        target_dict = getattr(parent, self._target)

        # We don't want to overwrite an already existing multiport instance
        if name not in target_dict:
            target_dict[name] = inst
        return inst

    def is_file(self) -> bool:
        """Returns if the interface is wrapping a file"""
        dtype = self.datatype
        if get_origin(dtype) == Annotated:
            dtype = get_args(dtype)[0]
        return isinstance(dtype, type) and issubclass(dtype, Path)

    def is_file_iterable(self) -> bool:
        """Returns if the interface is wrapping an iterable containing files"""
        dtype = self.datatype
        if hasattr(dtype, "__iter__"):
            if not get_args(dtype):
                return False
            for sub_dtype in get_args(dtype):
                if get_origin(sub_dtype) == Annotated:
                    sub_dtype = get_args(sub_dtype)[0]
                if not inspect.isclass(sub_dtype):
                    return False
                if not issubclass(sub_dtype, Path):
                    return False

            # All sub-datatypes are paths
            return True

        # Not iterable
        return False

    def check(self, value: T) -> bool:
        """
        Checks if a value is valid using type annotations.

        Parameters
        ----------
        value
            Value to typecheck

        Returns
        -------
        bool
            ``True`` if the value is valid, ``False`` otherwise

        """
        if self.is_file() and typecheck(value, str):
            # At this point we know we can safely cast to Path and check the predicates
            value = Path(value)  # type: ignore
        if self.is_file_iterable():
            value = type(value)(Path(v) for v in value) # type: ignore
        return typecheck(value, self.datatype)


class ParameterException(Exception):
    """Exception raised for parameter issues."""


class Parameter(Interface[T]):
    """
    Task parameter container.

    Parameters
    ----------
    default
        Default value for the parameter
    default_factory
        The default factory in case of mutable values
    optional
        If ``True``, the parent node will not check if this parameter has been set

    Attributes
    ----------
    name
        Name of the parameter
    parent
        Parent component
    datatype
        The datatype of the associated parameter. This may be a type from the ``typing``
        library and thus not always usable for checks with ``isinstance()``

    See Also
    --------
    FileParameter
        Allows specifying a read-only file as a parameter
    Parameter
        Allows specifying any data value as a parameter
    Flag
        Alias for boolean parameters

    """

    _target = "parameters"

    def __init__(
        self,
        default: T | None = None,
        default_factory: Callable[[], T] | None = None,
        optional: bool = False,
    ) -> None:
        self.default = default
        if self.default is None and default_factory is not None:
            self.default = default_factory()
        self.optional = optional or self.default is not None
        self._value: T | None = self.default
        self._changed = False

    @property
    def changed(self) -> bool:
        """Returns whether the parameter was explicitly set"""
        return self._changed

    @property
    def is_default(self) -> bool:
        """Returns whether the default value is set"""
        comp = self.default == self._value
        if isinstance(comp, Iterable):
            return all(comp)
        return comp

    @property
    def is_set(self) -> bool:
        """Indicates whether the parameter has been set to a non-``None`` value."""
        return self._value is not None

    @property
    def value(self) -> T:
        """Provides the value of the parameter."""
        if self._value is None:
            raise ParameterException(
                f"Parameter '{self.name}' of node '{self.parent.name}' must be set"
            )
        return self._value

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"default": self.default, "optional": self.optional}

    @property
    def skippable(self) -> bool:
        """Indicates whether this parameter can be skipped"""
        return self.optional and not self.changed

    def set(self, value: T) -> None:
        """
        Set the parameter to a specified value.

        Parameters
        ----------
        value
            Value to set the parameter to

        Raises
        ------
        ValueError
            If the datatype doesn't match with the parameter type

        Examples
        --------
        >>> foo.val.set(42)

        See Also
        --------
        graph.Graph.add
            Allows setting parameters at the point of node addition
        component.Component.update_parameters
            Sets multiple parameters over a node, graph, or workflow at once

        """
        if not self.check(value):
            raise ValueError(
                f"Error validating value '{value}' of type '{type(value)}'"
                f" against parameter type '{self.datatype}'"
            )
        self._changed = True
        self._value = value


Flag = Parameter[bool]


P = TypeVar("P", bound=Path | list[Path])


class FileParameter(Parameter[P]):
    """
    Allows provision of files as parameters to nodes.

    Parameters
    ----------
    exist_required
        If ``True`` will raise an exception if the specified file can't be found

    Raises
    ------
    FileNotFoundError
        If the input file doesn't exist

    See Also
    --------
    Parameter
        Allows specifying values as parameters

    """

    def __init__(
        self, default: P | None = None, exist_required: bool = True, optional: bool = False
    ) -> None:
        super().__init__(default, default_factory=None, optional=optional)
        self.exist_required = exist_required

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"exist_required": self.exist_required}

    @property
    def filepath(self) -> P:
        """Provides the path to the file."""
        paths = self.value if isinstance(self.value, list) else [self.value]
        if self.exist_required:
            for path in paths:
                if path is None or not path.exists():
                    path_str = path.as_posix() if path is not None else "None"
                    raise FileNotFoundError(f"Parameter file at '{path_str}' not found")
        return self.value

    def set(self, value: P) -> None:
        if isinstance(value, list):
            path: Path | list[Path] = [Path(val).absolute() for val in value]
        elif isinstance(value, Path | str):
            path = Path(value).absolute()
        super().set(cast(P, path))


ParameterMappingType = dict[str, str | T | list[NestedDict[str, str]] | None]


S = TypeVar("S")


class MultiParameter(Parameter[T], Generic[T, S]):
    """
    Container for multiple parameters. Allows setting multiple
    low-level parameters with a single high-level one.

    When constructing subgraphs, one will often want to map a
    single parameter to multiple component nodes. This is where
    `MultiParameter` is useful, as it will automatically set those
    component parameters. If you wish to perform some more elaborate
    processing instead, subclass `MultiParameter` and overwrite the
    `MultiParameter.set` method.

    Do not use this class directly, instead make use of the
    :meth:`maize.core.graph.Graph.combine_parameters` method.

    Parameters
    ----------
    parameters
        Sequence of `Parameter` instances that
        will be updated with a call to `set`
    default
        Default value for the parameter
    optional
        Whether the parameter will be considered optional
    hook
        An optional hook function mapping from ``T`` to ``S``

    Attributes
    ----------
    name
        Name of the parameter
    parent
        Parent component

    See Also
    --------
    graph.Graph.combine_parameters
        Uses `MultiParameter` in the background to combine multiple parameters
        from separate nodes into a single parameter for a subgraph.

    """

    _TInter = TypeVar("_TInter", bound="MultiParameter[Any, Any]")

    def __init__(
        self,
        parameters: Sequence[Parameter[S] | "Input[S]"],
        default: T | None = None,
        optional: bool | None = None,
        hook: Callable[[T], S] | None = None,
    ) -> None:
        def _id(x: T) -> S:
            return cast(S, x)

        super().__init__(default=default)

        if len(dtypes := {param.datatype for param in parameters}) > 1:
            raise ParameterException(f"Inconsistent datatypes in supplied parameters: {dtypes}")

        self._parameters = list(parameters)
        self.doc = parameters[0].doc
        self.hook = _id if hook is None else hook

        if hook is None:
            self.datatype = parameters[0].datatype

        if parameters[0].is_set and default is None and hook is None:
            self._value = cast(T, parameters[0].value)
        elif default is not None:
            self.set(default)

        # This allows us to set parameters as optional on the workflow level
        if optional is not None:
            self.optional = optional
            for para in self._parameters:
                para.optional = optional

    @property
    def parents(self) -> list["Component"]:
        """Provides the original parent nodes of all contained parameters."""
        return [param.parent for param in self._parameters]

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"default": self.default}

    @property
    def original_names(self) -> list[str]:
        """Provides the original names of the contained parameters"""
        return [para.name for para in self._parameters]

    def build(self: _TInter, name: str, parent: "Component") -> _TInter:
        self.name = name
        self.parent = parent
        return self

    def set(self, value: T) -> None:
        """
        Sets all contained parameters to the value.

        Parameters
        ----------
        value
            The value to be set for all contained parameters
        check
            Whether to check the value against the parameter datatype

        """
        self._value = value
        for param in self._parameters:
            param.set(self.hook(value))

    def as_dict(self) -> ParameterMappingType[T]:
        """
        Provide a dictionary representation of the parameter mapping.

        Returns
        -------
        ParameterMappingType[T]
            Dictionary representation of the `MultiParameter`

        """
        val = None if not self.is_set else self.value
        data: ParameterMappingType[T] = dict(
            name=self.name, value=val, type=format_datatype(self.datatype)
        )
        mapping: list[NestedDict[str, str]] = []
        for para in self._parameters:
            mapping.append(tuple_to_nested_dict(para.parent.name, para.name))
        data["map"] = mapping
        return data


class PortException(Exception):
    """Exception raised for channel issues."""


_PortType = TypeVar("_PortType", bound="Port[Any]")


class Port(Interface[T]):
    """
    Port parent class, use the `Input` or `Output` classes to specify user connections.

    Parameters
    ----------
    timeout
        Timeout used for continuously polling the connection
        for data on a blocking `receive` call.
    optional
        Whether this port is required for the process to stay alive.
        If the connection to an optional port is closed by a neighbouring
        process, the current node will not shutdown.
    mode
        Whether to ``'link'``, ``'move'`` or ``'copy'`` (default) files.
    hook
        An optional function to be called on the data being sent or received.

    Attributes
    ----------
    name
        Unique port name
    parent
        Parent node instance this port is part of

    Raises
    ------
    PortException
        If the port is used without a connected channel

    """

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        optional: bool = False,
        mode: Literal["copy", "link", "move"] = "copy",
        hook: Callable[[T], T] = lambda x: x,
    ) -> None:
        self.timeout = timeout
        self.optional = optional
        self.mode = mode
        self.hook = hook
        self.channel: Channel[T] | None = None

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"optional": self.optional, "mode": self.mode}

    @property
    def active(self) -> bool:
        """Specifies whether the port is active or not."""
        return Port.is_connected(self) and self.channel.active

    @property
    def connected(self) -> bool:
        """Specifies whether the port is connected."""
        return self.is_connected(self)

    @property
    def size(self) -> int:
        """Returns the approximate number of items waiting in the channel"""
        return 0 if self.channel is None else self.channel.size

    @staticmethod
    def is_connected(port: _PortType) -> TypeGuard["_PortChannel[T]"]:
        """Specifies whether the port is connected."""
        return port.channel is not None

    def set_channel(self, channel: Channel[T]) -> None:
        """
        Set the channel associated with the port. This needs
        to be called when connecting two ports together.

        Parameters
        ----------
        channel
            An instantiated subclass of `Channel`

        """
        self.channel = channel

    def close(self) -> None:
        """
        Closes the port.

        This can be detected by neighbouring nodes waiting on
        the port, and subsequently cause them to shut down.

        Raises
        ------
        PortException
            If the port is not connected

        """
        if self.channel is not None:
            self.channel.close()


# Mypy type guard, see: https://stackoverflow.com/questions/71805426/how-to-tell-a-python-type-checker-that-an-optional-definitely-exists
class _PortChannel(Port[T]):
    channel: Channel[T]
    _value: T | None
    _preloaded: bool
    cached: bool


class Output(Port[T]):
    """
    Output port to allow sending arbitrary data.

    Parameters
    ----------
    timeout
        Timeout used for continuously polling the connection
        for sending data into a potentially full channel.
    mode
        Whether to ``'link'``, ``'move'`` or ``'copy'`` (default) files.
    hook
        An optional function to be called on the data being sent or received.

    Attributes
    ----------
    name
        Unique port name
    parent
        Parent node instance this port is part of

    Raises
    ------
    PortException
        If the port is used without a connected channel

    """

    _target = "outputs"

    # Convenience function allowing connections by
    # using the shift operator on ports like so:
    # a.out >> b.in
    def __rshift__(self, other: "Input[T]") -> None:
        if self.parent.parent is not None:
            self.parent.parent.connect(receiving=other, sending=self)

    def send(self, item: T) -> None:
        """
        Sends data through the channel.

        Parameters
        ----------
        item
            Item to send through the channel

        Raises
        ------
        PortException
            If trying to send through an unconnected port

        Examples
        --------
        In the `run` method of the sending node:

        >>> self.output.send(42)

        """
        if not Port.is_connected(self):
            raise PortException("Attempting send through an inactive port")

        # Get the current time to allow regular status update communication
        current_time = datetime.now()

        self.parent.status = Status.WAITING_FOR_OUTPUT
        while not self.parent.signal.is_set():
            # Send an update if it's taking a while
            delta = datetime.now() - current_time
            if delta.seconds > UPDATE_INTERVAL:
                self.parent.send_update()
                current_time = datetime.now()

            # If our connection partner shuts down while we are trying to
            # send something, that means we should shutdown too
            if not self.active:
                self.parent.status = Status.STOPPED
                raise PortInterrupt("Port is dead, stopping node", name=self.name)

            try:
                self.channel.send(self.hook(cast(Any, item)), timeout=self.timeout)

            # This essentially allows the FBP concept of 'back-pressure'
            except ChannelFull:
                continue
            else:
                self.parent.status = Status.RUNNING
                return


class PortInterrupt(KeyboardInterrupt):
    """
    Interrupt raised to quit a process immediately to avoid
    propagating ``None`` values to downstream nodes.

    """

    def __init__(self, *args: object, name: str | None = None) -> None:
        self.name = name
        super().__init__(*args)


class Input(Port[T]):
    """
    Input port to allow receiving or parameterising data.

    An input port can either be connected to another node's output
    port to dynamically receive data, or set to a value (with optional
    default) before workflow execution to obtain a static value,
    making it behave analogously to `Parameter`.

    Parameters
    ----------
    default
        Default value for the parameter
    default_factory
        The default factory in case of mutable values
    timeout
        Timeout used for continuously polling the connection
        for data on a blocking `receive` call.
    optional
        If ``True``, this port does not have to be connected. Nodes should
        then check if data is available first (by a call to ``ready()``)
        before accessing it. Also determines whether this port is required
        for the process to stay alive. If the connection to an optional port
        is closed by a neighbouring process, the current node will not shutdown.
    mode
        Whether to ``'link'``, ``'move'`` or ``'copy'`` (default) files.
    cached
        If ``True``, will cache the latest received value and immediately return
        this value when calling `receive` while the channel is empty. This is useful
        in cases where a node will run in a loop, but some inputs stay constant, as
        those constant inputs will only need to receive a value a single time.
    hook
        An optional function to be called on the data being sent or received.

    Attributes
    ----------
    name
        Unique port name
    parent
        Parent node instance this port is part of

    Raises
    ------
    PortException
        If the port is used without a connected channel

    """

    _target = "inputs"
    _value: T | None
    _preloaded: bool

    def __init__(
        self,
        default: T | None = None,
        default_factory: Callable[[], T] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        optional: bool = False,
        mode: Literal["copy", "link", "move"] = "copy",
        cached: bool = False,
        hook: Callable[[T], T] = lambda x: x,
    ) -> None:
        super().__init__(timeout, optional, mode)
        self.default = default
        self.cached = cached
        self.hook = hook
        if self.default is None and default_factory is not None:
            self.default = default_factory()
        self._value = self.default
        self._changed = False
        self._preloaded = False

    # Convenience function allowing connections by
    # using the shift operator on ports like so:
    # a.in << b.out
    def __lshift__(self, other: "Output[T]") -> None:
        if self.parent.parent is not None:
            self.parent.parent.connect(receiving=self, sending=other)

    @property
    def changed(self) -> bool:
        """Returns whether the input was explicitly set"""
        return self._changed

    @property
    def is_default(self) -> bool:
        """Returns whether the default value is set"""
        return self.default == self._value

    @property
    def is_set(self) -> bool:
        """Indicates whether the input has been set to a non-``None`` value or is an active port."""
        return self._value is not None or self.active

    @property
    def active(self) -> bool:
        return super().active or self.ready() or (self.cached and self._value is not None)

    @property
    def value(self) -> T:
        """Receives a value. Alias for `receive()`"""
        return self.receive()

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"default": self.default, "cached": self.cached}

    @property
    def skippable(self) -> bool:
        """Indicates whether this input can be skipped"""
        return self.optional and not self.is_set

    def set(self, value: T) -> None:
        """
        Set the input to a static value.

        Parameters
        ----------
        value
            Value to set the input to

        Raises
        ------
        ValueError
            If the datatype doesn't match with the parameter type
        ParameterException
            When attempting to set a value for a connected port

        """
        if not self.check(value):
            raise ValueError(
                f"Error validating value '{value}' of type '{type(value)}'"
                f" against parameter type '{self.datatype}'"
            )
        if Port.is_connected(self):
            raise ParameterException("Can't set an Input that is already connected")

        if self.is_file():
            value = Path(value).absolute()  # type: ignore
        if self.is_file_iterable():
            value = type(value)(p.absolute() for p in value) # type: ignore

        self._changed = True
        self._value = value

    def preload(self, value: T) -> None:
        """
        Preload a value on an input. Internal use only.

        Parameters
        ----------
        value
            Value to set the parameter to

        Raises
        ------
        ValueError
            If the datatype doesn't match with the parameter type

        """
        if not self.check(value):
            raise ValueError(
                f"Error validating value '{value}' of type '{type(value)}'"
                f" against parameter type '{self.datatype}'"
            )
        self._preloaded = True
        self._value = value

    def dump(self) -> list[T]:
        """
        Dump any data contained in the channel.

        Returns
        -------
        list[T]
            List or all items contained in the channel

        """
        if self.channel is None:
            if self._value is None:
                raise PortException("Cannot dump from unconnected ports")
            return [self.hook(self._value)]
        return [self.hook(val) for val in self.channel.flush()]

    def ready(self) -> bool:
        """
        Specifies whether the input has data available to read.

        This allows checking for data without a blocking
        receive, thus allowing nodes to use optional inputs.

        Returns
        -------
        bool
            ``True`` if there is data in the channel ready to be read

        Examples
        --------
        >>> if self.input.ready():
        ...     val = self.input.receive()
        ... else:
        ...     val = 42

        """
        if self._preloaded:
            return True
        if Port.is_connected(self):
            return self.channel.ready
        return self._value is not None

    def receive(self) -> T:
        """
        Receives data from the port and blocks.

        Returns
        -------
        T
            Item received from the channel.

        Raises
        ------
        PortInterrupt
            Special signal to immediately quit the node,
            without any further processing
        PortException
            If trying to receive from an unconnected port,
            or if the received value is ``None``

        Examples
        --------
        >>> self.input.receive()
        42

        See Also
        --------
        Input.receive_optional
            Can potentially return 'None', use this
            method when utilising optional inputs

        """
        val = self.receive_optional()
        if val is None:
            raise PortException("Received 'None', use 'receive_optional' with optional ports")
        return val

    def receive_optional(self) -> T | None:
        """
        Receives data from the port and blocks, potentially returning ``None``.

        In nearly all cases you will want to use `Input.receive` instead. This method
        is intended to be used with optional inputs with upstream branches that may or
        may not run. In those cases, use this method and handle a potential ``None``
        value indicating that the optional data is not available. If you expect the
        data to always be available instantly, you can use `Input.ready` to check if
        there's data in the channel to be read.

        Returns
        -------
        T | None
            Item received from the channel.

        Raises
        ------
        PortInterrupt
            Special signal to immediately quit the node,
            without any further processing
        PortException
            If trying to receive from an unconnected port

        Examples
        --------
        >>> self.input.receive()
        42

        See Also
        --------
        Input.receive
            Raises a `PortException` instead of returning ``None``

        """
        if not Port.is_connected(self):
            if self._value is None:
                if not self.optional:
                    raise PortException(
                        f"Attempting receive from unconnected port '{self.parent.name}-{self.name}'"
                    )
                else:
                    return None
            return self.hook(self._value)

        if self._preloaded:
            self._preloaded = False
            if self._value is None:
                return None
            # See comment under the TYPE_CHECKING condition above, this seems to be a mypy quirk
            return cast(T, self.hook(self._value))

        # Warn the user if we receive multiple times from an unlooped node, as this is
        # unusual and may cause unexpected premature graph shutdowns
        if self._value is not None and not self.parent.looped and not self.optional:
            log.warning(
                "Receiving multiple times from the same port ('%s'), "
                "but the node is not looping. This may cause unexpected "
                "behaviour and earlier graph shutdowns if not accounted for.",
                self.name,
            )

        # Get the current time to allow regular status update communication
        current_time = datetime.now()

        # And now for the tricky bit
        self.parent.status = Status.WAITING_FOR_INPUT
        while not self.parent.signal.is_set():
            # First try to get data, even if the upstream process is dead
            item = self.channel.receive(timeout=self.timeout)  # type: ignore
            if item is not None:
                self.parent.status = Status.RUNNING
                self._value = item
                return cast(T, self.hook(item))

            # If we attempt to receive a value, but we don't have a new one
            # available and `cached` is True, we return the last cached value
            if self.cached and self._value is not None:
                return cast(T, self.hook(self._value))

            # Check for channel termination signal, this should cause
            # immediate port closure and component shutdown (in most cases)
            if not self.active:
                break

            # Send an update if it's taking a while
            delta = datetime.now() - current_time
            if delta.seconds > UPDATE_INTERVAL:
                self.parent.send_update()
                current_time = datetime.now()

        # Optional ports should really be probed for data with 'ready()'
        if not self.optional:
            self.parent.status = Status.STOPPED
            raise PortInterrupt("Port is dead, stopping node", name=self.name)

        # This means the channel and by extension upstream port shut down. Returning `None`
        # is okay provided that we have an optional port and the user handles this situation.
        return None


class MultiPort(Port[T]):
    """
    Aggregate Port parent class, allowing multiple
    ports to be integrated into one instance.

    Parameters
    ----------
    timeout
        Timeout used for continuously polling the connection
        for data on a blocking `receive` call.
    optional
        Whether this port is required for the process to stay alive.
        If the connection to an optional port is closed by a neighbouring
        process, the current node will not shutdown.
    n_ports
        The number of ports to instantiate, if not given will allow
        dynamic creation of new ports when `set_channel` is called
    mode
        Whether to ``'link'``, ``'move'`` or ``'copy'`` (default) files.

    Attributes
    ----------
    name
        Unique port name
    parent
        Parent node instance this port is part of

    Raises
    ------
    PortException
        If the port is used without a connected channel

    Examples
    --------
    Accessing individual ports through indexing:

    >>> out[0].send(item)

    """

    _ports: list[Port[T]]
    _type: type[Port[T]]
    _TInter = TypeVar("_TInter", bound="MultiPort[Any]")

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        optional: bool = False,
        mode: Literal["copy", "link", "move"] = "copy",
    ) -> None:
        self._ports = []
        super().__init__(timeout=timeout, optional=optional, mode=mode)

    @abstractmethod
    def __getitem__(self, key: int) -> Port[T]:  # pragma: no cover
        pass

    def __setitem__(self, key: int, value: Port[T]) -> None:
        self._ports[key] = value

    @abstractmethod
    def __iter__(self) -> Generator[Port[T], None, None]:  # pragma: no cover
        pass

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"n_ports": len(self._ports)}

    def __len__(self) -> int:
        return len(self._ports)

    def set_channel(self, channel: Channel[T]) -> None:
        # This occurs *after* `build`, we thus need to provide
        # a full port with name and parent parameters
        port = self._type(optional=self.optional, timeout=self.timeout).build(
            name=self.name, parent=self.parent
        )
        port.set_channel(channel=channel)
        self._ports.append(port)

    @staticmethod
    def is_connected(port: _PortType) -> TypeGuard["_PortChannel[T]"]:
        if not isinstance(port, MultiPort):  # pragma: no cover
            return port.is_connected(port)
        if len(port) == 0:
            return False
        return all(subport.is_connected(subport) for subport in port)

    def close(self) -> None:
        for _, port in enumerate(self._ports):
            port.close()


class MultiOutput(MultiPort[T]):
    """
    Aggregation of multiple output ports into a single port.

    Index into the port to access a normal `Output` instance with a `send` method.

    Parameters
    ----------
    timeout
        Timeout used for continuously polling the connection
        for free space on a potentially blocking `send` call.
    optional
        Whether this port is required for the process to stay alive.
        If the connection to an optional port is closed by a neighbouring
        process, the current node will not shutdown.
    n_ports
        The number of ports to instantiate, if not given will allow
        dynamic creation of new ports when `set_channel` is called
    mode
        Whether to ``'link'``, ``'move'`` or ``'copy'`` (default) files.

    Attributes
    ----------
    name
        Unique port name
    parent
        Parent node instance this port is part of

    Raises
    ------
    PortException
        If the port is used without a connected channel

    Examples
    --------
    >>> class Example(Node):
    ...     out = MultiOutput[int](n_ports=2)
    ...
    ...     def run(self):
    ...         self.out[0].send(42)
    ...         self.out[1].send(69)

    """

    _target = "outputs"
    _type = Output

    # Typing this correctly will require higher-kinded types I think
    # and won't be possible for the foreseeable future :(
    # But this is enough to get correct static type checking at the
    # graph assembly stage, and that's the most important thing
    _ports: list[Output[T]]  # type: ignore

    def __getitem__(self, key: int) -> Output[T]:
        return self._ports[key]

    def __iter__(self) -> Generator[Output[T], None, None]:
        yield from self._ports

    # Convenience function allowing connections by
    # using the shift operator on ports like so:
    # a.out >> b.in
    def __rshift__(self, other: Union[Input[T], "MultiInput[T]"]) -> None:
        if self.parent.parent is not None:
            self.parent.parent.connect(receiving=other, sending=self)


class MultiInput(MultiPort[T]):
    """
    Aggregation of multiple input ports into a single port.

    Index into the port to access a normal `Input` instance with a `receive` method.

    Parameters
    ----------
    timeout
        Timeout used for continuously polling the connection
        for data on a blocking `receive` call.
    optional
        Whether this port is required for the process to stay alive.
        If the connection to an optional port is closed by a neighbouring
        process, the current node will not shutdown.
    n_ports
        The number of ports to instantiate, if not given will allow
        dynamic creation of new ports when `set_channel` is called
    mode
        Whether to ``'link'``, ``'move'`` or ``'copy'`` (default) files.

    Attributes
    ----------
    name
        Unique port name
    parent
        Parent node instance this port is part of

    Raises
    ------
    PortException
        If the port is used without a connected channel

    Examples
    --------
    >>> class Example(Node):
    ...     inp = MultiInput[int](n_ports=2)
    ...
    ...     def run(self):
    ...         a = self.inp[0].receive()
    ...         b = self.inp[1].receive()

    """

    _target = "inputs"
    _type = Input
    _ports: list[Input[T]]  # type: ignore

    def __getitem__(self, key: int) -> Input[T]:
        return self._ports[key]

    def __iter__(self) -> Generator[Input[T], None, None]:
        yield from self._ports

    # Convenience function allowing connections by
    # using the shift operator on ports like so:
    # a.in << b.out
    def __lshift__(self, other: Union[Output[T], "MultiOutput[T]"]) -> None:
        if self.parent.parent is not None:
            self.parent.parent.connect(receiving=self, sending=other)

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        optional: bool = False,
        mode: Literal["copy", "link", "move"] = "copy",
        cached: bool = False,
    ) -> None:
        self.cached = cached
        super().__init__(timeout, optional, mode)

    @property
    def is_set(self) -> bool:
        """Indicates whether all inputs have been set"""
        return all(inp.is_set for inp in self._ports)

    @property
    def default(self) -> T | None:
        """Provides the default value, if available"""
        return self._ports[0].default

    @property
    def skippable(self) -> bool:
        """Indicates whether this input can be skipped"""
        return self.optional and not self.is_set and not self.is_connected(self)

    def set(self, value: T) -> None:
        """Set unconnected ports to a specified value"""
        port = self._type(optional=self.optional, timeout=self.timeout).build(
            name=self.name, parent=self.parent
        )
        port.set(value)
        self._ports.append(port)

    def set_channel(self, channel: Channel[T]) -> None:
        # This occurs *after* `build`, we thus need to provide
        # a full port with name and parent parameters
        port = self._type(optional=self.optional, timeout=self.timeout, cached=self.cached).build(
            name=self.name, parent=self.parent
        )
        port.set_channel(channel=channel)
        self._ports.append(port)

    def dump(self) -> list[list[T]]:
        """Dump any data contained in any of the inputs."""
        return [port.dump() for port in self._ports]

    def preload(self, data: list[T]) -> None:
        """Preload the input with data, to allow resuming from a checkpoint."""
        for port, datum in zip(self._ports, data):
            port.preload(datum)
