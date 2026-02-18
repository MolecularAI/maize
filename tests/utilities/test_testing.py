"""Testing testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

import pytest

from maize.utilities.testing import MockChannel
from maize.utilities.macros import node_to_function


@pytest.fixture
def empty_mock_channel():
    channel = MockChannel()
    return channel


@pytest.fixture
def loaded_mock_channel():
    channel = MockChannel(items=[1, 2, 3])
    return channel


class Test_MockChannel:
    def test_channel_send(self, empty_mock_channel):
        empty_mock_channel.send(42)
        assert empty_mock_channel.ready

    def test_channel_receive(self, loaded_mock_channel):
        assert loaded_mock_channel.receive() == 1

    def test_channel_receive_empty(self, empty_mock_channel):
        assert empty_mock_channel.receive(timeout=1) is None

    def test_channel_close_receive(self, loaded_mock_channel):
        loaded_mock_channel.close()
        assert loaded_mock_channel.receive() == 1

    def test_flush(self, loaded_mock_channel):
        assert loaded_mock_channel.flush() == [1, 2, 3]


def test_node_to_function(example_a):
    afunc = node_to_function(example_a)
    assert afunc(val=42) == {"out": 42}
