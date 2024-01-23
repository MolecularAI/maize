"""Miscallaneous testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

from pathlib import Path
import shutil
import time
import pytest

from maize.core.component import Component
from maize.core.interface import Input, Output, Parameter
from maize.core.runtime import Status, setup_build_logging
from maize.core.node import Node
from maize.core.workflow import Workflow
from maize.steps.io import LoadData, LoadFile, LoadFiles, Log, Return, SaveFile, SaveFiles
from maize.steps.plumbing import Copy, Delay, RoundRobin, Scatter, Multiply
from maize.utilities.testing import TestRig, MockChannel
from maize.utilities.macros import node_to_function, function_to_node, parallel, lambda_node


@pytest.fixture
def mock_parent():
    return Component(name="parent")


@pytest.fixture
def node_hybrid():
    class TestNode(Node):
        inp: Input[int] = Input()
        inp_default: Input[int] = Input(default=17)
        inp_optional: Input[int] = Input(optional=True)
        out: Output[int] = Output()

        def run(self):
            a = self.inp.receive()
            b = self.inp_default.receive()
            c = 0
            if self.inp_optional.ready():
                c = self.inp_optional.receive()
            self.out.send(a + b + c)

    return TestNode


@pytest.fixture
def node_optional():
    class TestNode(Node):
        inp: Input[Path] = Input(mode="copy")
        inp_opt: Input[Path] = Input(mode="copy", optional=True)
        out: Output[Path] = Output(mode="copy")

        def build(self):
            super().build()
            self.logger = setup_build_logging("build")

        def run(self) -> None:
            file = self.inp.receive()
            time.sleep(1)
            if self.inp_opt.ready():
                file_extra = self.inp_opt.receive()
                self.out.send(file_extra)
            else:
                self.out.send(file)

    return TestNode


@pytest.fixture
def node_receive_optional():
    class TestNode(Node):
        inp: Input[Path] = Input(mode="copy")
        inp_opt: Input[Path] = Input(mode="copy", optional=True)
        out: Output[Path] = Output(mode="copy")

        def build(self):
            super().build()
            self.logger = setup_build_logging("build")

        def run(self) -> None:
            file = self.inp.receive()
            file_extra = self.inp_opt.receive_optional()
            if file_extra is not None:
                self.out.send(file_extra)
            else:
                self.out.send(file)

    return TestNode


@pytest.fixture
def node_input():
    class TestNode(Node):
        inp: Input[Path] = Input(mode="copy", optional=True)
        out: Output[Path] = Output(mode="copy")

        def build(self):
            super().build()
            self.logger = setup_build_logging("build")

        def run(self) -> None:
            file = self.inp.receive()
            self.out.send(file)

    return TestNode


@pytest.fixture
def node_optional_with_channel(node_optional, shared_datadir, mock_parent):
    node = node_optional(parent=mock_parent)
    channel = MockChannel(shared_datadir / "testorigin.abc")
    node.inp.set_channel(channel)
    node.out.set_channel(channel)
    node.logger = setup_build_logging(name="test")
    return node


class Test_interfaces:
    def test_input(self, node_hybrid):
        rig = TestRig(node_hybrid)
        res = rig.setup_run(inputs={"inp": [42]})
        assert res["out"].get() == 42 + 17

        rig = TestRig(node_hybrid)
        res = rig.setup_run(inputs={"inp": [42], "inp_optional": [2]})
        assert res["out"].get() == 42 + 17 + 2

        rig = TestRig(node_hybrid)
        res = rig.setup_run(inputs={"inp": [42], "inp_default": [16], "inp_optional": [2]})
        assert res["out"].get() == 42 + 16 + 2


def test_lambda_node():
    rig = TestRig(lambda_node(lambda x: x + 2))
    res = rig.setup_run(inputs={"inp": 42})
    assert res["out"].get() == 44


def test_function_to_node():
    def func(a: int, b: bool = True, c: str = "foo") -> int:
        return a + 1 if b else a

    rig = TestRig(function_to_node(func))
    res = rig.setup_run(inputs={"inp": [42]})
    assert res["out"].get() == 43

    res = rig.setup_run(inputs={"inp": [42]}, parameters={"b": False})
    assert res["out"].get() == 42


def test_node_to_function(node_hybrid):
    func = node_to_function(node_hybrid)
    assert func(inp=42, inp_default=17, inp_optional=0)["out"] == 42 + 17


def test_multi_file_load_save(shared_datadir, tmp_path):
    flow = Workflow(level="debug")
    data = flow.add(
        LoadFiles[Path],
        parameters={
            "files": [shared_datadir / "testorigin.abc", shared_datadir / "testorigin2.abc"]
        },
    )
    save = flow.add(SaveFiles[Path], parameters={"destination": tmp_path})
    flow.connect(data.out, save.inp)
    flow.execute()
    assert (tmp_path / "testorigin.abc").exists()
    assert (tmp_path / "testorigin2.abc").exists()


def test_multi_file_load_copy_save(shared_datadir, tmp_path):
    dest1 = tmp_path / "save1"
    dest2 = tmp_path / "save2"
    dest1.mkdir(), dest2.mkdir()
    flow = Workflow(level="debug")
    data = flow.add(
        LoadFiles[Path],
        parameters={
            "files": [shared_datadir / "testorigin.abc", shared_datadir / "testorigin2.abc"]
        },
    )
    copy = flow.add(Copy[list[Path]])
    log = flow.add(Log[list[Path]])
    save1 = flow.add(SaveFiles[Path], name="save1", parameters={"destination": dest1})
    save2 = flow.add(SaveFiles[Path], name="save2", parameters={"destination": dest2})
    flow.connect(data.out, log.inp)
    flow.connect(log.out, copy.inp)
    flow.connect(copy.out, save1.inp)
    flow.connect(copy.out, save2.inp)
    flow.execute()
    assert (dest1 / "testorigin.abc").exists()
    assert (dest1 / "testorigin2.abc").exists()
    assert (dest2 / "testorigin.abc").exists()
    assert (dest2 / "testorigin2.abc").exists()


def test_parallel_multi_file(shared_datadir, tmp_path):

    class Dictionize(Node):
        inp: Input[list[Path]] = Input()
        out: Output[dict[int, Path]] = Output()

        def run(self) -> None:
            files = self.inp.receive()
            for i, file in enumerate(files):
                self.out.send({i: file})

    class DeDictionize(Node):
        inp: Input[dict[int, Path]] = Input()
        out: Output[list[Path]] = Output()
        n: Parameter[int] = Parameter(default=2)

        def run(self) -> None:
            files = []
            for _ in range(self.n.value):
                files.extend(list(self.inp.receive().values()))
            self.out.send(files)

    dest = tmp_path / "save"
    dest.mkdir()
    flow = Workflow(level="debug")
    data = flow.add(
        LoadFiles[Path],
        parameters={
            "files": [shared_datadir / "testorigin.abc", shared_datadir / "testorigin2.abc"]
        },
    )
    dicz = flow.add(Dictionize)
    log = flow.add(parallel(Log[dict[int, Path]], n_branches=2, loop=True))
    dedi = flow.add(DeDictionize)
    save = flow.add(SaveFiles[Path], parameters={"destination": dest})
    flow.connect(data.out, dicz.inp)
    flow.connect(dicz.out, log.inp)
    flow.connect(log.out, dedi.inp)
    flow.connect(dedi.out, save.inp)
    flow.execute()
    assert (dest / "testorigin.abc").exists()
    assert (dest / "testorigin2.abc").exists()


def test_parallel_file(shared_datadir, tmp_path):
    flow = Workflow(level="debug")
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    mult = flow.add(Multiply[Path], parameters={"n_packages": 2})
    scat = flow.add(Scatter[Path])
    dela = flow.add(parallel(Log[Path], n_branches=2, loop=True))
    roro = flow.add(RoundRobin[Path])
    out1 = flow.add(SaveFile[Path], name="out1", parameters={"destination": tmp_path / "test1.abc"})
    out2 = flow.add(SaveFile[Path], name="out2", parameters={"destination": tmp_path / "test2.abc"})
    flow.connect(data.out, mult.inp)
    flow.connect(mult.out, scat.inp)
    flow.connect(scat.out, dela.inp)
    flow.connect(dela.out, roro.inp)
    flow.connect(roro.out, out1.inp)
    flow.connect(roro.out, out2.inp)
    flow.execute()
    assert (tmp_path / "test1.abc").exists()
    assert (tmp_path / "test2.abc").exists()


def test_optional_file(shared_datadir, tmp_path, node_optional):
    flow = Workflow(level="debug", cleanup_temp=False)
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    data_opt = flow.add(LoadFile[Path], name="opt")
    data_opt.file.optional = True
    test = flow.add(node_optional)
    out = flow.add(SaveFile[Path], parameters={"destination": tmp_path / "test.abc"})

    flow.connect(data.out, test.inp)
    flow.connect(data_opt.out, test.inp_opt)
    flow.connect(test.out, out.inp)
    flow.execute()
    assert (tmp_path / "test.abc").exists()


def test_receive_optional_file(shared_datadir, tmp_path, node_receive_optional):
    flow = Workflow(level="debug", cleanup_temp=False)
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    data_opt = flow.add(LoadFile[Path], name="opt")
    delay = flow.add(Delay[Path], name="del", parameters={"delay": 2})
    data_opt.file.optional = True
    test = flow.add(node_receive_optional)
    out = flow.add(SaveFile[Path], parameters={"destination": tmp_path})

    flow.connect(data.out, test.inp)
    flow.connect(data_opt.out, delay.inp)
    flow.connect(delay.out, test.inp_opt)
    flow.connect(test.out, out.inp)
    flow.execute()
    assert (tmp_path / "testorigin.abc").exists()


def test_receive_optional_file_alt(shared_datadir, tmp_path, node_receive_optional):
    flow = Workflow(level="debug", cleanup_temp=False)
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    data_opt = flow.add(
        LoadFile[Path], name="opt", parameters={"file": shared_datadir / "testorigin2.abc"}
    )
    delay = flow.add(Delay[Path], name="del", parameters={"delay": 2})
    test = flow.add(node_receive_optional)
    out = flow.add(SaveFile[Path], parameters={"destination": tmp_path})

    flow.connect(data.out, test.inp)
    flow.connect(data_opt.out, delay.inp)
    flow.connect(delay.out, test.inp_opt)
    flow.connect(test.out, out.inp)
    flow.execute()
    assert (tmp_path / "testorigin2.abc").exists()


def test_optional_file_input(shared_datadir, tmp_path, node_optional, node_input):
    flow = Workflow(level="debug", cleanup_temp=False)
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    data_opt = flow.add(node_input, name="opt")
    data_opt.inp.set(shared_datadir / "testorigin2.abc")
    test = flow.add(node_optional)
    out = flow.add(SaveFile[Path], parameters={"destination": tmp_path})

    flow.connect(data.out, test.inp)
    flow.connect(data_opt.out, test.inp_opt)
    flow.connect(test.out, out.inp)
    flow.execute()
    assert (tmp_path / "testorigin2.abc").exists()


def test_execute_optional(node_optional_with_channel):
    node_optional_with_channel.execute()
    assert node_optional_with_channel.status == Status.STOPPED
    assert not node_optional_with_channel.ports_active()
    assert not node_optional_with_channel.signal.is_set()


def test_parallel_file_many(shared_datadir, tmp_path):
    class _Test(Node):
        inp: Input[Path] = Input(mode="copy")
        out: Output[Path] = Output(mode="copy")

        def run(self) -> None:
            file = self.inp.receive()
            out = Path("local.abc")
            shutil.copy(file, out)
            self.out.send(out)

    n_files = 2
    flow = Workflow(level="debug", cleanup_temp=False)
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    mult = flow.add(Multiply[Path], parameters={"n_packages": n_files})
    scat = flow.add(Scatter[Path])
    dela = flow.add(parallel(_Test, n_branches=4, loop=True))
    roro = flow.add(RoundRobin[Path])
    for i in range(n_files):
        out = flow.add(
            SaveFile[Path], name=f"out{i}", parameters={"destination": tmp_path / f"test{i}.abc"}
        )
        flow.connect(roro.out, out.inp)

    flow.connect(data.out, mult.inp)
    flow.connect(mult.out, scat.inp)
    flow.connect(scat.out, dela.inp)
    flow.connect(dela.out, roro.inp)
    flow.execute()
    for i in range(n_files):
        assert (tmp_path / f"test{i}.abc").exists()


def test_file_copy(shared_datadir, tmp_path):
    flow = Workflow()
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    copy = flow.add(Copy[Path])
    out1 = flow.add(SaveFile[Path], name="out1", parameters={"destination": tmp_path / "test1.abc"})
    out2 = flow.add(SaveFile[Path], name="out2", parameters={"destination": tmp_path / "test2.abc"})
    flow.connect(data.out, copy.inp)
    flow.connect(copy.out, out1.inp)
    flow.connect(copy.out, out2.inp)
    flow.execute()
    assert (tmp_path / "test1.abc").exists()
    assert (tmp_path / "test2.abc").exists()


def test_file_copy_delay(shared_datadir, tmp_path):
    flow = Workflow()
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    copy = flow.add(Copy[Path])
    del1 = flow.add(Delay[Path], name="del1", parameters={"delay": 2})
    del2 = flow.add(Delay[Path], name="del2", parameters={"delay": 5})
    out1 = flow.add(SaveFile[Path], name="out1", parameters={"destination": tmp_path / "test1.abc"})
    out2 = flow.add(SaveFile[Path], name="out2", parameters={"destination": tmp_path / "test2.abc"})
    flow.connect(data.out, del1.inp)
    flow.connect(del1.out, copy.inp)
    flow.connect(copy.out, out2.inp)
    flow.connect(copy.out, del2.inp)
    flow.connect(del2.out, out1.inp)
    flow.execute()
    assert (tmp_path / "test1.abc").exists()
    assert (tmp_path / "test2.abc").exists()


def test_nested_file_copy(shared_datadir, tmp_path):
    flow = Workflow()
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    copy1 = flow.add(Copy[Path], name="copy1")
    copy2 = flow.add(Copy[Path], name="copy2")
    out1 = flow.add(SaveFile[Path], name="out1", parameters={"destination": tmp_path / "test1.abc"})
    out2 = flow.add(SaveFile[Path], name="out2", parameters={"destination": tmp_path / "test2.abc"})
    out3 = flow.add(SaveFile[Path], name="out3", parameters={"destination": tmp_path / "test3.abc"})
    flow.connect(data.out, copy1.inp)
    flow.connect(copy1.out, out1.inp)
    flow.connect(copy1.out, copy2.inp)
    flow.connect(copy2.out, out2.inp)
    flow.connect(copy2.out, out3.inp)
    flow.execute()
    assert (tmp_path / "test1.abc").exists()
    assert (tmp_path / "test2.abc").exists()
    assert (tmp_path / "test3.abc").exists()


def test_file_load(shared_datadir, tmp_path):
    flow = Workflow()
    data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
    out = flow.add(SaveFile[Path], parameters={"destination": tmp_path / "test.abc"})
    flow.connect(data.out, out.inp)
    flow.execute()
    assert (tmp_path / "test.abc").exists()
    assert (shared_datadir / "testorigin.abc").exists()
