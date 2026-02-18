"""Port testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

from multiprocessing import Process
from pathlib import Path
import time
from typing import Annotated, Any

import numpy as np
from numpy.typing import NDArray
import pytest

from maize.core.interface import (
    PortInterrupt,
    Interface,
    Suffix,
    Parameter,
    FileParameter,
    MultiParameter,
    ParameterException,
    MultiInput,
    MultiOutput,
    PortException,
)


@pytest.fixture
def annotated():
    return Annotated[Path, Suffix("abc", "xyz")]


@pytest.fixture
def annotated_list():
    return list[Annotated[Path, Suffix("abc", "xyz")]]


@pytest.fixture
def interface():
    return Interface()


@pytest.fixture
def interface_typed():
    return Interface[int]()


@pytest.fixture
def interface_annotated(annotated):
    return Interface[annotated]()


@pytest.fixture
def parameter(mock_component):
    return Parameter[int](default=42).build(name="test", parent=mock_component)


@pytest.fixture
def parameter2(mock_component):
    return Parameter[int](default=42).build(name="test2", parent=mock_component)


@pytest.fixture
def parameter_array(mock_component):
    return Parameter[NDArray[Any]](default=np.array([1, 2, 3])).build(
        name="test2", parent=mock_component
    )


@pytest.fixture
def parameter_nodefault(mock_component):
    return Parameter[int]().build(name="test", parent=mock_component)


@pytest.fixture
def file_parameter(mock_component, annotated):
    return FileParameter[annotated]().build(name="test", parent=mock_component)


@pytest.fixture
def file_list_parameter(mock_component, annotated_list):
    return FileParameter[annotated_list]().build(name="test", parent=mock_component)


@pytest.fixture
def multi_parameter(mock_component, parameter, parameter2):
    return MultiParameter(parameters=(parameter, parameter2), default=3).build(
        "foo", parent=mock_component
    )


@pytest.fixture
def multi_parameter_hook(mock_component, parameter, parameter2):
    return MultiParameter[str, int](parameters=(parameter, parameter2), hook=int).build(
        "foo", parent=mock_component
    )


@pytest.fixture
def multi_parameter_hook2(mock_component, parameter, parameter2):
    return MultiParameter[str, int](parameters=(parameter, parameter2), hook=int, default="5").build(
        "foo", parent=mock_component
    )


@pytest.fixture
def multi_input(mock_component):
    return MultiInput[int]().build(name="test", parent=mock_component)


@pytest.fixture
def multi_output(mock_component):
    return MultiOutput[int]().build(name="test", parent=mock_component)


@pytest.fixture
def multi_output_fixed(mock_component):
    return MultiOutput[int](n_ports=2).build(name="test", parent=mock_component)


@pytest.fixture
def suffix1():
    return Suffix("pdb", "gro")


@pytest.fixture
def suffix2():
    return Suffix("pdb")


@pytest.fixture
def suffix3():
    return Suffix("xyz")


class Test_Suffix:
    def test_suffix(self, suffix1):
        assert suffix1(Path("a.pdb"))
        assert suffix1(Path("a.gro"))
        assert not suffix1(Path("a.xyz"))

    def test_eq(self, suffix1, suffix2, suffix3, mock_component):
        assert suffix1 != mock_component
        assert suffix1 == suffix2
        assert suffix1 != suffix3


class Test_Interface:
    def test_build(self, interface, mock_component):
        interface._target = "parameters"
        inter = interface.build(name="test", parent=mock_component)
        assert inter.datatype is None
        assert mock_component.parameters["test"] == inter

    def test_typed(self, interface_typed, mock_component):
        interface_typed._target = "parameters"
        inter = interface_typed.build(name="test", parent=mock_component)
        assert inter.datatype == int

    def test_typed_check(self, interface_typed, mock_component):
        interface_typed._target = "parameters"
        inter = interface_typed.build(name="test", parent=mock_component)
        assert inter.check(42)
        assert not inter.check("foo")

    def test_annotated(self, interface_annotated, mock_component, annotated):
        interface_annotated._target = "parameters"
        inter = interface_annotated.build(name="test", parent=mock_component)
        assert inter.datatype == annotated

    def test_annotated_check(self, interface_annotated, mock_component):
        interface_annotated._target = "parameters"
        inter = interface_annotated.build(name="test", parent=mock_component)
        assert inter.check(Path("file.abc"))
        assert not inter.check(Path("file.pdb"))
        assert inter.check("foo.abc")
        assert not inter.check("foo")

    def test_path(self, interface, mock_component):
        interface._target = "parameters"
        inter = interface.build(name="test", parent=mock_component)
        assert inter.path == ("mock", "test")

    def test_serialized(self, interface):
        assert interface.serialized == {"type": "typing.Any", "kind": "Interface"}


class Test_Parameter:
    def test_set(self, parameter):
        assert parameter.is_set
        parameter.set(17)
        assert parameter.is_set
        assert parameter.value == 17
        with pytest.raises(ValueError):
            parameter.set("foo")

    def test_default(self, parameter_array):
        assert parameter_array.is_set
        assert parameter_array.is_default
        parameter_array.set(np.array([1, 2, 3]))
        assert parameter_array.is_default
        assert all(parameter_array.value == np.array([1, 2, 3]))

    def test_set_none(self, parameter_nodefault):
        assert not parameter_nodefault.is_set
        parameter_nodefault.set(17)
        assert parameter_nodefault.is_set

    def test_changed(self, parameter):
        assert not parameter.changed
        parameter.set(17)
        assert parameter.changed

    def test_serialized(self, parameter):
        assert parameter.serialized == {
            "type": "<class 'int'>",
            "kind": "Parameter",
            "default": 42,
            "optional": True,
        }

    def test_file(self, file_parameter, shared_datadir):
        with pytest.raises(ParameterException):
            file_parameter.filepath
        file_parameter.set(Path("nonexistent.abc"))
        assert file_parameter.is_set
        with pytest.raises(FileNotFoundError):
            file_parameter.filepath
        file_parameter.set(shared_datadir / "testorigin.abc")
        assert file_parameter.filepath

    def test_file_list(self, file_list_parameter, shared_datadir):
        with pytest.raises(ParameterException):
            file_list_parameter.filepath
        file_list_parameter.set([Path("nonexistent.abc"), Path("other_nonexistent.xyz")])
        assert file_list_parameter.is_set
        with pytest.raises(FileNotFoundError):
            file_list_parameter.filepath
        file_list_parameter.set([shared_datadir / "testorigin.abc"])
        assert file_list_parameter.filepath

    def test_file_serialized(self, file_parameter):
        assert file_parameter.serialized["kind"] == "FileParameter"
        assert file_parameter.serialized["default"] is None
        assert not file_parameter.serialized["optional"]
        assert file_parameter.serialized["exist_required"]

    def test_multi(self, multi_parameter, mock_component):
        assert multi_parameter.default == 3
        assert multi_parameter.datatype == int
        assert multi_parameter.is_set
        assert multi_parameter.value == 3
        multi_parameter.set(10)
        assert multi_parameter.value == 10
        assert multi_parameter._parameters[0].value == 10
        assert multi_parameter.parents == [mock_component, mock_component]

    def test_multi_as_dict(self, multi_parameter):
        assert multi_parameter.as_dict() == {
            "name": "foo",
            "value": 3,
            "type": "int",
            "map": [{"mock": "test"}, {"mock": "test2"}],
        }

    def test_multi_serialized(self, multi_parameter):
        assert multi_parameter.serialized == {
            "type": "<class 'int'>",
            "kind": "MultiParameter",
            "default": 3,
            "optional": True,
        }

    def test_multi_bad(self, parameter, file_parameter):
        with pytest.raises(ParameterException):
            MultiParameter(parameters=(parameter, file_parameter), default=3)

    def test_multi_hook(self, multi_parameter_hook, mock_component):
        assert multi_parameter_hook.default is None
        assert not multi_parameter_hook.is_set
        multi_parameter_hook.set("10")
        assert multi_parameter_hook.value == "10"
        assert multi_parameter_hook._parameters[0].value == 10
        assert multi_parameter_hook._parameters[1].value == 10
        assert multi_parameter_hook.parents == [mock_component, mock_component]

    def test_multi_hook2(self, multi_parameter_hook2, mock_component):
        assert multi_parameter_hook2.default == "5"
        assert multi_parameter_hook2.is_set
        assert multi_parameter_hook2.value == "5"
        assert multi_parameter_hook2._parameters[0].value == 5
        assert multi_parameter_hook2._parameters[1].value == 5
        multi_parameter_hook2.set("10")
        assert multi_parameter_hook2.value == "10"
        assert multi_parameter_hook2._parameters[0].value == 10
        assert multi_parameter_hook2._parameters[1].value == 10
        assert multi_parameter_hook2.parents == [mock_component, mock_component]


class Test_Output:
    def test_connection(self, connected_output):
        assert connected_output.connected

    def test_serialized(self, connected_output):
        assert connected_output.serialized == {
            "type": "None",
            "kind": "Output",
            "optional": False,
            "mode": "copy",
        }

    def test_active(self, connected_output):
        assert connected_output.active

    def test_close(self, connected_output):
        connected_output.close()
        assert not connected_output.active

    def test_send(self, connected_output):
        connected_output.send(42)

    def test_send_hook(self, connected_output):
        connected_output.hook = lambda x: x + 2
        connected_output.send(42)

    def test_send_file(self, connected_file_output, datafile):
        connected_file_output.send(datafile)

    def test_send_unconnected(self, unconnected_output):
        with pytest.raises(PortException):
            unconnected_output.send(42)

    def test_send_closed(self, connected_output):
        connected_output.close()
        with pytest.raises(PortInterrupt):
            connected_output.send(42)


class Test_Input:
    def test_connection(self, connected_input):
        assert connected_input.connected

    def test_serialized(self, connected_input):
        assert connected_input.serialized == {
            "type": "None",
            "kind": "Input",
            "optional": False,
            "mode": "copy",
            "default": None,
            "cached": False
        }

    def test_no_connection(self, unconnected_input):
        assert not unconnected_input.connected

    def test_set(self, unconnected_input):
        unconnected_input.set(42)
        assert unconnected_input.value == 42
        unconnected_input.datatype = str
        with pytest.raises(ValueError):
            unconnected_input.set(42)

    def test_active(self, connected_input):
        assert connected_input.active

    def test_close(self, connected_input):
        connected_input.close()
        assert connected_input.active
        assert connected_input.ready()
        connected_input.receive()
        assert not connected_input.active

    def test_inactive(self, unconnected_input):
        assert not unconnected_input.active
        with pytest.raises(PortException):
            unconnected_input.receive()

    def test_preload(self, connected_input):
        assert connected_input.receive() == 42
        assert not connected_input.ready()
        connected_input.preload(42)
        assert connected_input.ready()
        connected_input.datatype = int
        with pytest.raises(ValueError):
            connected_input.preload("foo")

    def test_preload_hook(self, connected_input):
        connected_input.hook = lambda x: x + 2
        connected_input.preload(42)
        assert connected_input.ready()
        assert connected_input.receive() == 44

    def test_default_factory(self, connected_input_default_factory):
        assert connected_input_default_factory.ready()
        assert connected_input_default_factory.receive() == [17]

    def test_dump(self, connected_input):
        assert connected_input.dump() == [42]

    def test_dump_hook(self, connected_input):
        connected_input.hook = lambda x: x + 2
        assert connected_input.dump() == [44]

    def test_dump_unconnected(self, unconnected_input):
        with pytest.raises(PortException):
            unconnected_input.dump()

    def test_receive(self, connected_input):
        assert connected_input.receive() == 42

    def test_receive_cached(self, connected_input):
        connected_input.cached = True
        assert connected_input.receive() == 42
        assert connected_input.receive() == 42

    def test_receive_hook(self, connected_input):
        connected_input.hook = lambda x: x + 2
        assert connected_input.receive() == 44

    def test_receive_optional(self, connected_input):
        connected_input.optional = True
        connected_input.preload(None)
        with pytest.raises(PortException):
            connected_input.receive()

    def test_receive_optional_none(self, connected_input):
        connected_input.optional = True
        connected_input.preload(None)
        assert connected_input.receive_optional() is None

    def test_receive_optional_unconnected(self, unconnected_input):
        unconnected_input.optional = True
        unconnected_input.set(None)
        assert unconnected_input.receive_optional() is None

    def test_ready_receive(self, connected_input):
        assert connected_input.ready()
        assert connected_input.receive() == 42

    def test_close_receive(self, connected_input):
        connected_input.close()
        assert connected_input.ready()
        assert connected_input.receive() == 42
        assert not connected_input.ready()

    def test_receive_multi_attempt(self, connected_input):
        assert connected_input.receive() == 42
        connected_input.close()
        with pytest.raises(PortInterrupt):
            connected_input.receive()

    def test_receive_multi_success(self, connected_input_multi):
        assert connected_input_multi.receive() == 42
        assert connected_input_multi.receive() == -42
        assert not connected_input_multi.ready()

    def test_file_receive(self, connected_file_input):
        assert connected_file_input.receive().exists()

    def test_file_ready_receive(self, connected_file_input):
        assert connected_file_input.ready()
        assert connected_file_input.receive().exists()

    def test_file_close_receive(self, connected_file_input):
        connected_file_input.close()
        assert connected_file_input.ready()
        assert connected_file_input.receive().exists()
        assert not connected_file_input.ready()

    def test_default(self, connected_input_default):
        assert connected_input_default.is_set
        assert connected_input_default.is_default
        assert not connected_input_default.changed
        assert connected_input_default.active
        assert connected_input_default.ready()
        assert connected_input_default.receive() == 42
        assert not connected_input_default.ready()
        with pytest.raises(ParameterException):
            connected_input_default.set(1)

    def test_unconnected_default(self, unconnected_input_default):
        assert unconnected_input_default.is_set
        assert unconnected_input_default.is_default
        assert not unconnected_input_default.changed
        assert unconnected_input_default.active
        assert unconnected_input_default.ready()
        assert unconnected_input_default.receive() == 17
        assert unconnected_input_default.ready()
        assert unconnected_input_default.receive() == 17
        unconnected_input_default.set(39)
        assert unconnected_input_default.receive() == 39
        assert not unconnected_input_default.is_default
        assert unconnected_input_default.changed
        assert unconnected_input_default.active


class Test_both:
    def test_send_receive(self, connected_pair):
        inp, out = connected_pair
        out.send(42)
        assert inp.receive() == 42

    def test_send_receive_full(self, connected_pair):
        inp, out = connected_pair

        def send():
            for _ in range(4):
                out.send(42)

        p = Process(target=send)
        p.start()
        time.sleep(5)
        assert inp.receive() == 42
        assert inp.receive() == 42
        assert inp.receive() == 42
        assert inp.receive() == 42
        p.join()

    def test_send_receive_wait(self, connected_pair):
        inp, out = connected_pair

        def recv():
            inp.receive()

        p = Process(target=recv)
        p.start()
        time.sleep(5)
        out.send(42)
        p.join()

    def test_send_receive_hook(self, connected_pair):
        inp, out = connected_pair
        inp.hook = out.hook = lambda x: x + 2
        out.send(42)
        assert inp.receive() == 46

    def test_send_receive_multi(self, connected_pair):
        inp, out = connected_pair
        out.send(42)
        out.send(101010)
        out.send(-42)
        assert inp.receive() == 42
        assert inp.receive() == 101010
        assert inp.ready()
        assert inp.receive() == -42
        assert not inp.ready()

    def test_file_send_receive(self, connected_file_pair, datafile):
        inp, out = connected_file_pair
        out.send(datafile)
        assert inp.ready()
        assert inp.receive().exists()
        assert not inp.ready()


class Test_MultiInput:
    def test_properties(self, multi_input, empty_channel, loaded_channel):
        assert not multi_input.connected
        multi_input.set_channel(empty_channel)
        multi_input.set_channel(loaded_channel)
        assert multi_input.connected
        assert multi_input[0].channel is empty_channel
        assert multi_input[1].channel is loaded_channel
        assert len(multi_input) == 2
        assert multi_input.default is None

    def test_serialized(self, multi_input):
        assert multi_input.serialized == {
            "type": "<class 'int'>",
            "kind": "MultiInput",
            "n_ports": 0,
            "optional": False,
            "mode": "copy",
        }

    def test_receive(self, multi_input, loaded_datachannel2):
        multi_input.set_channel(loaded_datachannel2)
        multi_input.set_channel(loaded_datachannel2)
        assert multi_input[0].receive() == 42
        assert multi_input[1].receive() == -42

    def test_receive_set(self, multi_input):
        multi_input.set(39)
        for port in multi_input:
            assert port.receive() == 39

    def test_close(self, multi_input, loaded_channel):
        multi_input.set_channel(loaded_channel)
        multi_input.set_channel(loaded_channel)
        assert multi_input[0].active
        assert multi_input[1].active
        multi_input.close()
        assert multi_input[0].active
        assert multi_input[1].active
        multi_input[0].receive()
        assert not multi_input[0].active
        assert not multi_input[1].active

    def test_dump(self, multi_input, loaded_datachannel2):
        multi_input.set_channel(loaded_datachannel2)
        multi_input.set_channel(loaded_datachannel2)
        assert multi_input.dump() == [[42, -42], []]

    def test_preload(self, multi_input, empty_datachannel):
        multi_input.set_channel(empty_datachannel)
        multi_input.set_channel(empty_datachannel)
        multi_input.preload([1, 2])
        assert multi_input[0].receive() == 1
        assert multi_input[1].receive() == 2


class Test_MultiOutput:
    def test_properties(self, multi_output, empty_channel, loaded_channel):
        assert not multi_output.connected
        multi_output.set_channel(empty_channel)
        multi_output.set_channel(loaded_channel)
        assert multi_output.connected
        assert multi_output[0].channel is empty_channel
        assert multi_output[1].channel is loaded_channel
        assert len(multi_output)

    def test_send(self, multi_output, empty_datachannel):
        multi_output.set_channel(empty_datachannel)
        multi_output.set_channel(empty_datachannel)
        multi_output[0].send(42)
        multi_output[1].send(42)
        assert empty_datachannel.receive() == 42
        assert empty_datachannel.receive() == 42

    def test_close(self, multi_output, loaded_channel):
        multi_output.set_channel(loaded_channel)
        multi_output.set_channel(loaded_channel)
        assert multi_output[0].active
        assert multi_output[1].active
        multi_output.close()
        assert not multi_output[0].active
        assert not multi_output[1].active
