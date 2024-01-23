"""Core testing data"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

import dill
import pytest

from maize.core.channels import DataChannel, FileChannel
from maize.core.component import Component
from maize.core.interface import Output, Input


@pytest.fixture
def datafile(shared_datadir):
    return shared_datadir / "testorigin.abc"


@pytest.fixture
def datafile2(shared_datadir):
    return shared_datadir / "nested" / "testorigin2.abc"


@pytest.fixture
def nested_datafiles(datafile, datafile2):
    return [datafile, datafile2]


@pytest.fixture
def nested_datafiles_dict(datafile, datafile2):
    return {"foo": datafile, 42: datafile2}


@pytest.fixture
def data():
    return 17


@pytest.fixture
def filedata(tmp_path):
    return tmp_path / "chan-test"


@pytest.fixture
def loaded_datachannel():
    channel = DataChannel(size=1)
    channel.preload(dill.dumps(42))
    return channel


@pytest.fixture
def loaded_datachannel2():
    channel = DataChannel(size=2)
    channel.preload(dill.dumps(42))
    channel.preload(dill.dumps(-42))
    return channel


@pytest.fixture
def empty_datachannel():
    channel = DataChannel(size=3)
    return channel


@pytest.fixture
def empty_filechannel(tmp_path):
    channel = FileChannel()
    channel.setup(destination=tmp_path / "chan-test")
    return channel


@pytest.fixture
def empty_filechannel2(tmp_path):
    channel = FileChannel(mode="link")
    channel.setup(destination=tmp_path / "chan-test")
    return channel


@pytest.fixture
def loaded_filechannel(empty_filechannel, datafile):
    empty_filechannel.preload(datafile)
    return empty_filechannel


@pytest.fixture
def loaded_filechannel2(empty_filechannel, nested_datafiles):
    empty_filechannel.preload(nested_datafiles)
    return empty_filechannel


@pytest.fixture
def loaded_filechannel3(empty_filechannel2, nested_datafiles):
    empty_filechannel2.preload(nested_datafiles)
    return empty_filechannel2


@pytest.fixture
def loaded_filechannel4(empty_filechannel, nested_datafiles_dict):
    empty_filechannel.preload(nested_datafiles_dict)
    return empty_filechannel


@pytest.fixture
def loaded_filechannel5(tmp_path, datafile):
    channel = FileChannel()
    chan_path = tmp_path / "chan-test"
    chan_path.mkdir()
    (chan_path / datafile.name).touch()
    channel.setup(destination=chan_path)
    return channel


# If this breaks, check this:
# https://stackoverflow.com/questions/42014484/pytest-using-fixtures-as-arguments-in-parametrize
@pytest.fixture(params=["loaded_datachannel", "loaded_filechannel"])
def loaded_channel(request):
    return request.getfixturevalue(request.param)


@pytest.fixture(params=["empty_datachannel", "empty_filechannel"])
def empty_channel(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def mock_component():
    return Component(name="mock")


@pytest.fixture
def connected_output(empty_datachannel, mock_component):
    out = Output().build(name="Test", parent=mock_component)
    out.set_channel(empty_datachannel)
    return out


@pytest.fixture
def connected_input(loaded_datachannel, mock_component):
    inp = Input().build(name="Test", parent=mock_component)
    inp.set_channel(loaded_datachannel)
    return inp


@pytest.fixture
def connected_input_default(loaded_datachannel, data, mock_component):
    inp = Input(default=data).build(name="Test", parent=mock_component)
    inp.set_channel(loaded_datachannel)
    return inp


@pytest.fixture
def connected_input_default_factory(data, mock_component):
    inp = Input[list[int]](default_factory=lambda: [data]).build(name="Test", parent=mock_component)
    return inp


@pytest.fixture
def connected_input_multi(loaded_datachannel2, mock_component):
    inp = Input().build(name="Test", parent=mock_component)
    inp.set_channel(loaded_datachannel2)
    return inp


@pytest.fixture
def unconnected_input(mock_component):
    inp = Input().build(name="Test", parent=mock_component)
    return inp


@pytest.fixture
def unconnected_input_default(mock_component, data):
    inp = Input(default=data).build(name="Test", parent=mock_component)
    return inp


@pytest.fixture
def unconnected_output(mock_component):
    out = Output().build(name="Test", parent=mock_component)
    return out


@pytest.fixture
def connected_pair(empty_datachannel, mock_component):
    inp = Input().build(name="Test", parent=mock_component)
    out = Output().build(name="Test", parent=mock_component)
    out.set_channel(empty_datachannel)
    inp.set_channel(empty_datachannel)
    return inp, out


@pytest.fixture
def connected_file_output(empty_filechannel, mock_component):
    out = Output().build(name="Test", parent=mock_component)
    out.set_channel(empty_filechannel)
    return out


@pytest.fixture
def connected_file_output_full(loaded_filechannel, mock_component):
    out = Output().build(name="Test", parent=mock_component)
    out.set_channel(loaded_filechannel)
    return out


@pytest.fixture
def connected_file_input(loaded_filechannel, mock_component):
    inp = Input().build(name="Test", parent=mock_component)
    inp.set_channel(loaded_filechannel)
    return inp


@pytest.fixture
def connected_file_pair(empty_filechannel, mock_component):
    inp = Input().build(name="Test", parent=mock_component)
    out = Output().build(name="Test", parent=mock_component)
    out.set_channel(empty_filechannel)
    inp.set_channel(empty_filechannel)
    return inp, out
