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
from datetime import datetime
from pathlib import Path
from typing import (
    Literal,
    TypeGuard,
    TypeVar,
    ClassVar,
    Generic,
    Union,
    Any,
    cast,
    get_args,
    TYPE_CHECKING,
)
from collections.abc import Callable, Sequence, Generator

from maize.core.runtime import Status
from maize.core.channels import Channel, ChannelFull
from maize.utilities.utilities import (
    extract_parent_type,
    extract_type,
    tuple_to_nested_dict,
    NestedDict,
    typecheck,
)

if TYPE_CHECKING:
    from maize.core.component import Component

# I have temporarily removed dynamic typing for now, but if we
# reintroduce it intersection types will come in handy:
# https://github.com/python/typing/issues/213

T = TypeVar("T")


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

    name: str
    parent: "Component"
    datatype: Any
    doc: str | None

    # These allow `build` to create a shallow copy of the interface
    _args: tuple[Any, ...]
    _kwargs: dict[str, Any]

    _target: ClassVar[str]
    _TInter = TypeVar("_TInter", bound="Interface[Any]")

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
        Self[T]
            Copy of the current instance, with references to the name and the parent

        """
        inst = self.__class__(*self._args, **self._kwargs)
        inst.name = name
        inst.parent = parent
        inst.datatype = None
        inst.doc = None

        inst.datatype = extract_type(self)
        if inst.datatype is None:
            inst.datatype = extract_parent_type(parent, name)

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

    """

    _target = "parameters"

    def __init__(
        self,
        default: T | None = None,
        default_factory: Callable[[], T] | None = None,
        optional: bool = False,
    ) -> None:
        self.optional = optional
        self.default = default
        if self.default is None and default_factory is not None:
            self.default = default_factory()
        self._value: T | None = self.default
        self._changed = False

    @property
    def changed(self) -> bool:
        """Returns whether the parameter was explicitly set"""
        return self._changed

    @property
    def is_default(self) -> bool:
        """Returns whether the default value is set"""
        return self.default == self._value

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

    def set(self, value: T) -> None:
        """
        Set the parameter.

        Will typically not be called directly, but by the `add` method
        of the associated class and the constructor of the node, or
        with the `update_parameters` graph method.

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
        self._changed = True
        self._value = value


Flag = Parameter[bool]


P = TypeVar("P", bound=Path)


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
        path = self.value
        if self.exist_required and (path is None or not path.exists()):
            path_str = path.as_posix() if path is not None else "None"
            raise FileNotFoundError(f"Parameter file at '{path_str}' not found")
        return path

    def set(self, value: P) -> None:
        # Attempt a cast from string to Path to accomodate serialization
        value = Path(value)  # type: ignore
        super().set(value)
        self._value = value.absolute()


ParameterMappingType = dict[str, str | T | list[NestedDict[str, str]] | None]


class MultiParameter(Parameter[T]):
    """
    Container for multiple parameters. Allows setting multiple
    low-level parameters with a single high-level one.

    When constructing subgraphs, one will often want to map a
    single parameter to multiple component nodes. This is where
    `MultiParameter` is useful, as it will automatically set those
    component parameters. If you wish to perform some more elaborate
    processing instead, subclass `MultiParameter` and overwrite the
    `MultiParameter.set` method.

    Parameters
    ----------
    default
        Default value for the parameter
    parameters
        Sequence of `Parameter` instances that
        will be updated with a call to `set`

    Attributes
    ----------
    name
        Name of the parameter
    parent
        Parent component

    """

    _TInter = TypeVar("_TInter", bound="MultiParameter[Any]")

    def __init__(self, parameters: Sequence[Parameter[T]], default: T | None = None) -> None:
        super().__init__(default=default)
        if len(dtypes := {param.datatype for param in parameters}) > 1:
            raise ParameterException(f"Inconsistent datatypes in supplied parameters: {dtypes}")
        self._parameters = list(parameters)
        self.datatype = parameters[0].datatype
        self.doc = parameters[0].doc
        if parameters[0].is_set and default is None:
            self._value = parameters[0].value

    @property
    def parents(self) -> list["Component"]:
        """Provides the original parent nodes of all contained parameters."""
        return [param.parent for param in self._parameters]

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"default": self.default}

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

        """
        self._value = value
        for param in self._parameters:
            param.set(value)

    def as_dict(self) -> ParameterMappingType[T]:
        """
        Provide a dictionary representation of the parameter mapping.

        Returns
        -------
        ParameterMappingType[T]
            Dictionary representation of the `MultiParameter`

        """
        val = None if not self.is_set else self.value
        data: ParameterMappingType[T] = dict(name=self.name, value=val)
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
    ) -> None:
        self.timeout = timeout
        self.optional = optional
        self.mode = mode
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
    _cached: bool


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
                raise PortInterrupt("Port is dead, stopping node")

            try:
                self.channel.send(cast(Any, item), timeout=self.timeout)

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
    default) before workflow execution to obtain a static value.

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
    ) -> None:
        super().__init__(timeout, optional, mode)
        self.default = default
        if self.default is None and default_factory is not None:
            self.default = default_factory()
        self._value = self.default
        self._changed = False
        self._preloaded = False
        self._cached = cached

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
        return super().active or self.ready() or (self._cached and self._value is not None)

    @property
    def value(self) -> T:
        """Receives a value. Alias for `receive()`"""
        return self.receive()

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"default": self.default, "cached": self._cached}

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
            return [self._value]
        return self.channel.flush()

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
        Receives data from the port and blocks.

        If the port has the `optional` flag and isn't
        checked using `ready()` the output can be ``None``.
        To avoid this behaviour use the `receive` method instead.

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
                raise PortException(
                    f"Attempting receive from unconnected port '{self.parent.name}-{self.name}'"
                )
            return self._value

        if self._preloaded:
            self._preloaded = False
            return self._value

        # Get the current time to allow regular status update communication
        current_time = datetime.now()

        # And now for the tricky bit
        self.parent.status = Status.WAITING_FOR_INPUT
        while not self.parent.signal.is_set():
            # First try to get data, even if the upstream process is dead
            item = self.channel.receive(timeout=self.timeout)
            if item is not None:
                self.parent.status = Status.RUNNING
                self._value = item
                return item

            # If we attempt to receive a value, but we don't have a new one
            # available and `cached` is True, we return the last cached value
            if self._cached and self._value is not None:
                return self._value

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
        self.parent.status = Status.STOPPED
        if not self.optional:
            raise PortInterrupt("Port is dead, stopping node", name=self.name)

        # Just to satisfy the type-checker, only way to return from
        # this function is with data, or by shutting down the process
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
        n_ports: int | None = None,
        mode: Literal["copy", "link", "move"] = "copy",
    ) -> None:
        self._ports = []
        self._dynamic = n_ports is None
        if n_ports is not None:
            for _ in range(n_ports):
                self._ports.append(self._type(timeout=timeout, optional=optional, mode=mode))
        super().__init__(timeout=timeout, optional=optional)

    @abstractmethod
    def __getitem__(self, key: int) -> Port[T]:
        pass

    def __setitem__(self, key: int, value: Port[T]) -> None:
        self._ports[key] = value

    @abstractmethod
    def __iter__(self) -> Generator[Port[T], None, None]:
        pass

    @property
    def serialized(self) -> dict[str, Any]:
        return super().serialized | {"n_ports": len(self._ports), "dynamic": self._dynamic}

    def __len__(self) -> int:
        return len(self._ports)

    def set_channel(self, channel: Channel[T]) -> None:
        for port in self._ports:
            if not port.is_connected(port):
                port.set_channel(channel=channel)
                return
        if self._dynamic:
            # This occurs *after* `build`, we thus need to provide
            # a full port with name and parent parameters
            port = self._type(optional=self.optional, timeout=self.timeout).build(
                name=self.name, parent=self.parent
            )
            port.set_channel(channel=channel)
            self._ports.append(port)
            return
        raise PortException(
            f"MultiPort '{self.name}' already has all" "ports connected and is not dynamic."
        )

    @staticmethod
    def is_connected(port: _PortType) -> TypeGuard["_PortChannel[T]"]:
        if not isinstance(port, MultiPort):
            return port.is_connected(port)
        if len(port) == 0:
            return False
        return all(subport.is_connected(subport) for subport in port)

    def close(self) -> None:
        for _, port in enumerate(self._ports):
            port.close()

    def build(self: _TInter, name: str, parent: "Component") -> _TInter:
        new = super().build(name, parent)

        dtype = new.datatype
        if hasattr(new, "__orig_class__"):
            dtype = get_args(new.__orig_class__)[0]

        for i, port in enumerate(new):
            # We register all subports under the same name,
            # and then set the MultiPort to use that name
            new_port = port.build(name=name, parent=parent)
            new_port.datatype = dtype
            # No idea what pylint's problem is here
            new[i] = new_port  # pylint: disable=unsupported-assignment-operation
        return new


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

    def dump(self) -> list[list[T]]:
        """Dump any data contained in any of the inputs."""
        return [port.dump() for port in self._ports]

    def preload(self, data: list[T]) -> None:
        """Preload the input with data, to allow resuming from a checkpoint."""
        for port, datum in zip(self._ports, data):
            port.preload(datum)
