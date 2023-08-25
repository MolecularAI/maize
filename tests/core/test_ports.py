"""Port testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

from pathlib import Path
from typing import Annotated

import pytest

from maize.core.interface import PortInterrupt, Interface, Suffix, Parameter, FileParameter, MultiParameter, ParameterException, MultiInput, MultiOutput, PortException


@pytest.fixture
def annotated():
    return Annotated[Path, Suffix("abc", "xyz")]

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
def parameter_nodefault(mock_component):
    return Parameter[int]().build(name="test", parent=mock_component)


@pytest.fixture
def file_parameter(mock_component, annotated):
    return FileParameter[annotated]().build(name="test", parent=mock_component)


@pytest.fixture
def multi_parameter(mock_component, parameter, parameter2):
    return MultiParameter(parameters=(parameter, parameter2), default=3).build(
        "foo", parent=mock_component)


@pytest.fixture
def multi_input(mock_component):
    return MultiInput[int]().build(name="test", parent=mock_component)


@pytest.fixture
def multi_input_fixed(mock_component):
    return MultiInput[int](n_ports=2).build(name="test", parent=mock_component)


@pytest.fixture
def multi_output(mock_component):
    return MultiOutput[int]().build(name="test", parent=mock_component)


@pytest.fixture
def multi_output_fixed(mock_component):
    return MultiOutput[int](n_ports=2).build(name="test", parent=mock_component)


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
        assert not inter.check("foo.abc")
        assert not inter.check("foo")

    def test_path(self, interface, mock_component):
        interface._target = "parameters"
        inter = interface.build(name="test", parent=mock_component)
        assert inter.path == ("mock", "test")


class Test_Parameter:
    def test_set(self, parameter):
        assert parameter.is_set
        parameter.set(17)
        assert parameter.is_set
        assert parameter.value == 17
        with pytest.raises(ValueError):
            parameter.set("foo")

    def test_set_none(self, parameter_nodefault):
        assert not parameter_nodefault.is_set
        parameter_nodefault.set(17)
        assert parameter_nodefault.is_set

    def test_file(self, file_parameter, shared_datadir):
        with pytest.raises(ParameterException):
            file_parameter.filepath
        file_parameter.set(Path("nonexistent.abc"))
        assert file_parameter.is_set
        with pytest.raises(FileNotFoundError):
            file_parameter.filepath
        file_parameter.set(shared_datadir / "testorigin.abc")
        assert file_parameter.filepath

    def test_multi(self, multi_parameter):
        assert multi_parameter.default == 3
        assert multi_parameter.datatype == int
        assert multi_parameter.is_set
        assert multi_parameter.value == 3
        multi_parameter.set(10)
        assert multi_parameter.value == 10
        assert multi_parameter._parameters[0].value == 10

    def test_multi_as_dict(self, multi_parameter):
        assert multi_parameter.as_dict() == {
            "name": "foo", "value": 3, "map": [{"mock": "test"}, {"mock": "test2"}]}

    def test_multi_bad(self, parameter, file_parameter):
        with pytest.raises(ParameterException):
            MultiParameter(parameters=(parameter, file_parameter), default=3)


class Test_Output:
    def test_connection(self, connected_output):
        assert connected_output.connected

    def test_active(self, connected_output):
        assert connected_output.active

    def test_close(self, connected_output):
        connected_output.close()
        assert not connected_output.active

    def test_send(self, connected_output):
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

    def test_no_connection(self, unconnected_input):
        assert not unconnected_input.connected

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

    def test_dump(self, connected_input):
        assert connected_input.dump() == [42]

    def test_dump_unconnected(self, unconnected_input):
        with pytest.raises(PortException):
            unconnected_input.dump()

    def test_receive(self, connected_input):
        assert connected_input.receive() == 42

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

    def test_properties_fixed(self, multi_input_fixed, empty_channel):
        assert not multi_input_fixed.connected
        assert len(multi_input_fixed) == 2
        multi_input_fixed.set_channel(empty_channel)
        multi_input_fixed.set_channel(empty_channel)
        assert multi_input_fixed.connected

    def test_receive(self, multi_input, loaded_datachannel2):
        multi_input.set_channel(loaded_datachannel2)
        multi_input.set_channel(loaded_datachannel2)
        assert multi_input[0].receive() == 42
        assert multi_input[1].receive() == -42

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

    def test_properties_fixed(self, multi_output_fixed, empty_channel):
        assert not multi_output_fixed.connected
        assert len(multi_output_fixed) == 2
        multi_output_fixed.set_channel(empty_channel)
        multi_output_fixed.set_channel(empty_channel)
        assert multi_output_fixed.connected
        with pytest.raises(PortException):
            multi_output_fixed.set_channel(empty_channel)

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
