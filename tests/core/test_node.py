"""Node testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

from pathlib import Path
import time
from traceback import TracebackException
from types import MethodType
from typing import Any

import pytest

from maize.core.component import Component
from maize.core.interface import FileParameter, Parameter, PortException, Input, Output
from maize.core.node import Node, NodeBuildException
from maize.core.runtime import Status, setup_build_logging
from maize.utilities.testing import MockChannel
from maize.utilities.validation import SuccessValidator
from maize.utilities.execution import ProcessError


@pytest.fixture
def mock_parent():
    return Component(name="parent")


@pytest.fixture
def node(mock_parent):
    class TestNode(Node):
        inp: Input[Any] = Input()
        out: Output[Any] = Output()
        data: Parameter[Any] = Parameter()
        file: FileParameter[Path] = FileParameter()

        def build(self):
            super().build()
            self.logger = setup_build_logging("build")

        def run(self):
            pass

    return TestNode(parent=mock_parent)


@pytest.fixture
def node_hybrid(mock_parent):
    class TestNode(Node):
        inp: Input[int] = Input()
        inp_default: Input[int] = Input(default=17)
        inp_optional: Input[int] = Input(optional=True)
        out: Output[int] = Output()

        def run(self):
            a = self.inp.receive()
            b = self.inp_default.receive()
            if self.inp_optional.ready():
                c = self.inp_optional.receive()
            self.out.send(a + b + c)

    return TestNode(parent=mock_parent)


@pytest.fixture
def node_with_channel(node):
    node.inp.set_channel(MockChannel())
    node.out.set_channel(MockChannel())
    node.logger = setup_build_logging(name="test")
    return node


@pytest.fixture
def node_with_run_fail(node_with_channel):
    def fail(self):
        raise Exception("test")
    node_with_channel.run = MethodType(fail, node_with_channel)
    return node_with_channel


@pytest.fixture
def node_with_run_fail_first(node_with_channel):
    def fail(self):
        i = self.i
        self.i += 1
        if i == 0:
            raise Exception("test")
    node_with_channel.i = 0
    node_with_channel.run = MethodType(fail, node_with_channel)
    return node_with_channel


@pytest.fixture
def node_with_run_int(node_with_channel):
    def fail(self):
        raise KeyboardInterrupt("test")
    node_with_channel.run = MethodType(fail, node_with_channel)
    return node_with_channel


@pytest.fixture
def invalid_node_class():
    class InvalidTestNode(Node):
        inp: Input[Any] = Input()

    return InvalidTestNode


@pytest.fixture
def invalid_node_class_no_ports():
    class InvalidTestNode(Node):
        def build(self):
            pass

        def run(self):
            pass

    return InvalidTestNode


class Test_Node:
    def test_init(self, node, mock_parent):
        assert len(node.inputs) == 1
        assert len(node.outputs) == 1
        assert len(node.ports) == 2
        assert len(node.parameters) == 8
        assert node.data.name == "data"
        assert node.file.name == "file"
        assert not node.ports_active()
        assert node.parent is mock_parent
        assert node.status == Status.READY

    def test_setup(self, node):
        node.setup_directories()
        assert Path(f"node-{node.name}").exists()

    def test_setup_path(self, node, tmp_path):
        node.setup_directories(tmp_path)
        assert (tmp_path / f"node-{node.name}").exists()

    def test_init_fail_abc(self, invalid_node_class):
        with pytest.raises(TypeError):
            invalid_node_class()

    def test_init_fail_no_ports(self, invalid_node_class_no_ports):
        with pytest.raises(NodeBuildException):
            invalid_node_class_no_ports()

    def test_node_user_parameters(self, node):
        assert "inp" in node.user_parameters
        assert "data" in node.user_parameters
        assert "file" in node.user_parameters
        assert "python" not in node.user_parameters
        assert "modules" not in node.user_parameters
        assert "scripts" not in node.user_parameters

    def test_shutdown(self, node_with_channel):
        node_with_channel._shutdown()
        assert node_with_channel.status == Status.COMPLETED

    def test_ports(self, node_with_channel):
        assert node_with_channel.ports_active()

    def test_run_command(self, node):
        assert node.run_command("echo hello").returncode == 0
        val = SuccessValidator("hello")
        assert node.run_command("echo hello", validators=[val]).returncode == 0
        with pytest.raises(ProcessError):
            assert node.run_command("echo other", validators=[val]).returncode == 0

    def test_run_multi(self, node):
        for ret in node.run_multi(["echo hello", "echo foo"]):
            assert ret.returncode == 0
        results = node.run_multi(["echo hello" for _ in range(6)], n_batch=2)
        assert len(results) == 2
        for res in results:
            assert res.returncode == 0
        val = SuccessValidator("hello")
        assert node.run_multi(["echo hello"], validators=[val])[0].returncode == 0
        with pytest.raises(ProcessError):
            assert node.run_multi(["echo other"], validators=[val])[0].returncode == 0

    def test_loop(self, node_with_channel):
        node_with_channel.max_loops = 2
        start = time.time()
        for _ in node_with_channel._loop(step=1.0):
            continue
        total = time.time() - start
        assert 1.5 < total < 2.5

    def test_loop_shutdown(self, node_with_channel):
        looper = node_with_channel._loop()
        next(looper)
        assert node_with_channel.ports_active()
        node_with_channel.inp.close()
        assert not node_with_channel.inp.active
        try:
            next(looper)
        except StopIteration:
            pass
        assert node_with_channel.status == Status.STOPPED

    def test_prepare(self, node_with_channel):
        node_with_channel.required_packages = ["numpy"]
        node_with_channel.required_callables = ["echo"]
        node_with_channel._prepare()
        with pytest.raises(ModuleNotFoundError):
            node_with_channel.required_packages = ["nonexistentpackage"]
            node_with_channel._prepare()
        with pytest.raises(NodeBuildException):
            node_with_channel.required_packages = []
            node_with_channel.required_callables = ["idontexist"]
            node_with_channel._prepare()

    def test_execute(self, node_with_channel):
        node_with_channel.execute()
        assert node_with_channel.status == Status.COMPLETED
        assert not node_with_channel.ports_active()
        assert not node_with_channel.signal.is_set()
        time.sleep(0.5)
        updates = []
        while not node_with_channel._message_queue.empty():
            updates.append(node_with_channel._message_queue.get())
        summary = updates[-1]
        assert summary.name == node_with_channel.name
        assert summary.status == Status.COMPLETED

    def test_execute_run_fail(self, node_with_run_fail):
        node_with_run_fail.execute()
        assert node_with_run_fail.status == Status.FAILED
        assert not node_with_run_fail.ports_active()
        assert node_with_run_fail.signal.is_set()
        time.sleep(0.5)
        updates = []
        while not node_with_run_fail._message_queue.empty():
            updates.append(node_with_run_fail._message_queue.get())
        summary = updates[-1]
        assert isinstance(updates[-2].exception, TracebackException)
        assert summary.name == node_with_run_fail.name
        assert summary.status == Status.FAILED

    def test_execute_inactive(self, node_with_channel):
        node_with_channel.active.set(False)
        node_with_channel.execute()
        assert node_with_channel.status == Status.STOPPED
        assert not node_with_channel.ports_active()
        assert not node_with_channel.signal.is_set()
        time.sleep(0.5)
        updates = []
        while not node_with_channel._message_queue.empty():
            updates.append(node_with_channel._message_queue.get())
        summary = updates[-1]
        assert summary.name == node_with_channel.name
        assert summary.status == Status.STOPPED

    def test_execute_run_int(self, node_with_run_int):
        with pytest.raises(KeyboardInterrupt):
            node_with_run_int.execute()
        assert node_with_run_int.status == Status.STOPPED
        assert not node_with_run_int.ports_active()
        assert node_with_run_int.signal.is_set()
        time.sleep(0.5)
        updates = []
        while not node_with_run_int._message_queue.empty():
            updates.append(node_with_run_int._message_queue.get())
        summary = updates[-1]
        assert summary.name == node_with_run_int.name
        assert summary.status == Status.STOPPED

    def test_execute_run_fail_ok(self, node_with_run_fail):
        node_with_run_fail.fail_ok = True
        node_with_run_fail.execute()
        assert node_with_run_fail.status == Status.FAILED
        assert not node_with_run_fail.ports_active()
        assert not node_with_run_fail.signal.is_set()
        time.sleep(0.5)
        updates = []
        while not node_with_run_fail._message_queue.empty():
            updates.append(node_with_run_fail._message_queue.get())
        summary = updates[-1]
        assert summary.name == node_with_run_fail.name
        assert summary.status == Status.FAILED

    def test_execute_run_fail_2_attempts(self, node_with_run_fail_first):
        node_with_run_fail_first.n_attempts = 3
        node_with_run_fail_first.execute()
        assert node_with_run_fail_first.status == Status.COMPLETED
        assert not node_with_run_fail_first.ports_active()
        assert not node_with_run_fail_first.signal.is_set()
        time.sleep(0.5)
        updates = []
        while not node_with_run_fail_first._message_queue.empty():
            updates.append(node_with_run_fail_first._message_queue.get())
        summary = updates[-1]
        assert summary.name == node_with_run_fail_first.name
        assert summary.status == Status.COMPLETED
