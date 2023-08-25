"""Miscallaneous testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

from pathlib import Path
import shutil
import pytest

from maize.core.component import Component
from maize.core.interface import Input, Output
from maize.core.node import Node
from maize.core.workflow import Workflow
from maize.steps.io import LoadFile, LoadFiles, Log, SaveFile, SaveFiles
from maize.steps.plumbing import Copy, Delay, RoundRobin, Scatter, Multiply
from maize.utilities.testing import TestRig
from maize.utilities.macros import parallel


@pytest.fixture
def mock_parent():
    return Component(name="parent")


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
            c = 0
            if self.inp_optional.ready():
                c = self.inp_optional.receive()
            self.out.send(a + b + c)

    return TestNode


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
