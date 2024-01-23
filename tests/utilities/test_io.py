"""IO testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

import argparse
from dataclasses import dataclass
from pathlib import Path
import subprocess
from types import ModuleType
import typing
from typing import Literal

import dill
import pytest
from maize.utilities.execution import ResourceManagerConfig

from maize.utilities.io import (
    args_from_function,
    common_parent,
    get_plugins,
    load_file,
    remove_dir_contents,
    sendtree,
    parse_groups,
    setup_workflow,
    wait_for_file,
    with_keys,
    with_fields,
    read_input,
    write_input,
    Config
)


@pytest.fixture
def parser_groups():
    parser = argparse.ArgumentParser()
    a = parser.add_argument_group("a")
    a.add_argument("-a", type=int)
    b = parser.add_argument_group("b")
    b.add_argument("-b", type=str)
    return parser


@pytest.fixture
def obj():
    @dataclass
    class obj:
        a: int = 42
        b: str = "foo"

    return obj


@pytest.fixture
def files(tmp_path):
    paths = [
        Path("a/b/c/d"),
        Path("a/b/c/e"),
        Path("a/b/f/g"),
        Path("a/g/h/e"),
    ]
    return [tmp_path / path for path in paths]


def test_remove_dir_contents(tmp_path):
    path = tmp_path / "dir"
    path.mkdir()
    file = path / "file"
    file.touch()
    remove_dir_contents(path)
    assert not file.exists()
    assert path.exists()


def test_wait_for_file(tmp_path):
    path = tmp_path / "file.dat"
    subprocess.Popen(f"sleep 5 && echo 'foo' > {path.as_posix()}", shell=True)
    wait_for_file(path)
    assert path.exists()


def test_wait_for_file2(tmp_path):
    path = tmp_path / "file.dat"
    subprocess.Popen(f"sleep 1 && echo 'foo' > {path.as_posix()}", shell=True)
    wait_for_file(path, timeout=2)
    assert path.exists()


def test_wait_for_file3(tmp_path):
    path = tmp_path / "file.dat"
    subprocess.Popen(f"sleep 1 && touch {path.as_posix()}", shell=True)
    wait_for_file(path, timeout=2, zero_byte_check=False)
    assert path.exists()


def test_wait_for_file_to(tmp_path):
    path = tmp_path / "file.dat"
    subprocess.Popen(f"sleep 5 && touch {path.as_posix()}", shell=True)
    with pytest.raises(TimeoutError):
        wait_for_file(path, timeout=2)
        assert path.exists()


def test_common_parent(files):
    com = common_parent(files)
    assert com.relative_to(com.parent) == Path("a")
    com = common_parent(files[:3])
    assert com.relative_to(com.parent) == Path("b")
    com = common_parent(files[:2])
    assert com.relative_to(com.parent) == Path("c")
    com = common_parent([files[1], files[3]])
    assert com.relative_to(com.parent) == Path("a")


class TestConfig:
    def test_env_init(self, mocker):
        conf = Path("conf.toml")
        with conf.open("w") as out:
            out.write("scratch = 'test'")
        mocker.patch("os.environ", {"MAIZE_CONFIG": conf.absolute().as_posix()})
        config = Config.from_default()
        assert config.scratch == Path("test")

    def test_default(self, mocker):
        mocker.patch("os.environ", {})
        config = Config.from_default()
        assert config.nodes == {}
        assert config.environment == {}
        assert config.batch_config == ResourceManagerConfig()


def test_get_plugins():
    assert "pytest.__main__" in get_plugins("pytest")


def test_load_file(tmp_path):
    module_file = tmp_path / "module.py"
    with module_file.open("w") as mod:
        mod.write("CONSTANT = 42\n")
    assert isinstance(load_file(module_file), ModuleType)


def test_args_from_function(mocker):
    def func(foo: int, flag: bool, lit: typing.Literal["foo", "bar"]) -> str:
        return "baz"

    mocker.patch("sys.argv", ["testing", "--foo", "42", "--flag", "--lit", "foo"])
    parser = argparse.ArgumentParser()
    parser = args_from_function(parser, func)
    args = parser.parse_args()
    assert args.foo == 42
    assert args.flag
    assert args.lit == "foo"


def test_sendtree_link(files, tmp_path):
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    res = sendtree({i: file for i, file in enumerate(files)}, tmp_path, mode="link")
    for file in res.values():
        assert (tmp_path / file).exists()
        assert (tmp_path / file).is_symlink()


def test_sendtree_copy(files, tmp_path):
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    res = sendtree({i: file for i, file in enumerate(files)}, tmp_path, mode="copy")
    for file in res.values():
        assert (tmp_path / file).exists()
        assert not (tmp_path / file).is_symlink()


def test_sendtree_move(files, tmp_path):
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    res = sendtree({i: file for i, file in enumerate(files)}, tmp_path, mode="move")
    for file in res.values():
        assert (tmp_path / file).exists()
        assert not (tmp_path / file).is_symlink()


def test_parse_groups(parser_groups, mocker):
    mocker.patch("sys.argv", ["testing", "-a", "42", "-b", "foo"])
    groups = parse_groups(parser_groups)
    assert vars(groups["a"]) == {"a": 42}
    assert vars(groups["b"]) == {"b": "foo"}


def test_setup_workflow(nested_graph_with_params, mocker):
    mocker.patch("sys.argv", ["testing", "--val", "38", "--quiet", "--debug"])
    setup_workflow(nested_graph_with_params)
    t = nested_graph_with_params.nodes["t"]
    assert t.get() == 50


def test_setup_workflow_check(nested_graph_with_params, mocker):
    mocker.patch("sys.argv", ["testing", "--val", "38", "--check"])
    setup_workflow(nested_graph_with_params)


def test_with_keys():
    assert with_keys({"a": 42, "b": 50}, {"a"}) == {"a": 42}


def test_with_fields(obj):
    assert with_fields(obj, ("a",)) == {"a": 42}


def test_read_input(shared_datadir):
    res = read_input(shared_datadir / "dict.yaml")
    assert res == {"a": 42, "b": "foo", "c": {"d": ["a", "b"]}}
    res = read_input(shared_datadir / "dict.json")
    assert res == {"a": 42, "b": "foo", "c": {"d": ["a", "b"]}}
    res = read_input(shared_datadir / "dict.toml")
    assert res == {"a": 42, "b": "foo", "c": {"d": ["a", "b"]}}
    with pytest.raises(ValueError):
        read_input(shared_datadir / "testorigin.abc")
    with pytest.raises(FileNotFoundError):
        read_input(shared_datadir / "nothere.yaml")


def test_write_input(tmp_path):
    data = {"a": 42, "b": "foo", "c": {"d": ["a", "b"]}}
    write_input(tmp_path / "dict.yaml", data)
    res = read_input(tmp_path / "dict.yaml")
    assert res == data
