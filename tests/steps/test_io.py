"""Tests for IO nodes"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init

from pathlib import Path
import shutil

import dill
import pytest

from maize.utilities.testing import TestRig
from maize.steps.io import LoadFile, LoadData, SaveFile, Return


@pytest.fixture
def simple_file(shared_datadir):
    return shared_datadir / "testorigin.abc"


class Test_io:
    def test_loadfile(self, simple_file):
        t = TestRig(LoadFile[Path])
        out = t.setup_run(parameters={"file": simple_file})
        assert out["out"].get() == simple_file

    def test_loaddata(self):
        t = TestRig(LoadData[int])
        out = t.setup_run(parameters={"data": 42})
        assert out["out"].get() == 42

    def test_savefile(self, simple_file, tmp_path):
        t = TestRig(SaveFile[Path])
        t.setup_run(
            inputs={"inp": simple_file}, parameters={"destination": tmp_path / simple_file.name}
        )
        assert (tmp_path / simple_file.name).exists()

    def test_savefile_dir(self, simple_file, tmp_path):
        t = TestRig(SaveFile[Path])
        t.setup_run(inputs={"inp": simple_file}, parameters={"destination": tmp_path})
        assert (tmp_path / simple_file.name).exists()

    def test_savefile_parent(self, simple_file, tmp_path):
        t = TestRig(SaveFile[Path])
        t.setup_run(
            inputs={"inp": simple_file},
            parameters={"destination": tmp_path / "folder" / simple_file.name},
        )
        assert (tmp_path / "folder" / simple_file.name).exists()

    def test_savefile_overwrite(self, simple_file, tmp_path):
        dest = tmp_path / simple_file.name
        shutil.copy(simple_file, dest)
        first_time = dest.stat().st_mtime
        t = TestRig(SaveFile[Path])
        t.setup_run(
            inputs={"inp": simple_file}, parameters={"destination": dest, "overwrite": True}
        )
        assert dest.stat().st_mtime > first_time

    def test_return(self):
        t = TestRig(Return[int])
        t.setup_run(inputs={"inp": 42})
        assert dill.loads(t.node.ret_queue.get()) == 42
