"""Tests for IO nodes"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init

from pathlib import Path
import shutil

import dill
import pytest

from maize.utilities.testing import TestRig
from maize.steps.io import (
    Dummy,
    FileBuffer,
    LoadFile,
    LoadData,
    LoadFiles,
    Log,
    LogResult,
    SaveFile,
    Return,
    SaveFiles,
    Void,
)


@pytest.fixture
def simple_file(shared_datadir):
    return shared_datadir / "testorigin.abc"


@pytest.fixture
def simple_file2(shared_datadir):
    return shared_datadir / "testorigin2.abc"


class Test_io:
    def test_dummy(self):
        t = TestRig(Dummy)
        t.setup_run()

    def test_void(self):
        t = TestRig(Void)
        t.setup_run(inputs={"inp": 42})

    @pytest.mark.skip(reason="Fails when run together with all other tests, passes alone")
    def test_loadfile(self, simple_file):
        t = TestRig(LoadFile[Path])
        out = t.setup_run(parameters={"file": simple_file})
        assert out["out"].get() == simple_file
        with pytest.raises(FileNotFoundError):
            t.setup_run(parameters={"file": Path("nofile")})

    def test_loadfiles(self, simple_file, simple_file2):
        t = TestRig(LoadFiles[Path])
        out = t.setup_run(parameters={"files": [simple_file, simple_file2]})
        assert out["out"].get() == [simple_file, simple_file2]
        with pytest.raises(FileNotFoundError):
            t.setup_run(parameters={"files": [Path("nofile"), simple_file]})

    def test_loaddata(self):
        t = TestRig(LoadData[int])
        out = t.setup_run(parameters={"data": 42})
        assert out["out"].get() == 42

    def test_logresult(self):
        t = TestRig(LogResult)
        t.setup_run(inputs={"inp": 42})

    def test_log(self):
        t = TestRig(Log[int])
        out = t.setup_run(inputs={"inp": 42})
        assert out["out"].get() == 42

    def test_log2(self, simple_file):
        t = TestRig(Log[Path])
        out = t.setup_run(inputs={"inp": simple_file})
        assert out["out"].get() == simple_file

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

    def test_savefiles(self, simple_file, simple_file2, tmp_path):
        t = TestRig(SaveFiles[Path])
        t.setup_run(
            inputs={"inp": [[simple_file, simple_file2]]}, parameters={"destination": tmp_path}
        )
        assert (tmp_path / simple_file.name).exists()
        assert (tmp_path / simple_file2.name).exists()

    def test_filebuffer(self, simple_file, tmp_path):
        t = TestRig(FileBuffer[Path])
        location = tmp_path / simple_file.name
        t.setup_run(inputs={"inp": [simple_file]}, parameters={"file": location})
        assert location.exists()

    def test_filebuffer_receive(self, simple_file, tmp_path):
        location = tmp_path / simple_file.name
        shutil.copy(simple_file, location)
        t = TestRig(FileBuffer[Path])
        res = t.setup_run(inputs={"inp": [simple_file]}, parameters={"file": location})
        file = res["out"].get()
        assert file is not None
        assert file.exists()

    def test_return(self):
        t = TestRig(Return[int])
        t.setup_run(inputs={"inp": 42})
        assert dill.loads(t.node.ret_queue.get()) == 42
