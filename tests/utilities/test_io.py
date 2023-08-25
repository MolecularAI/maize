"""IO testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

import argparse
from dataclasses import dataclass
from pathlib import Path

import dill
import pytest

from maize.utilities.io import (
    sendtree,
    parse_groups,
    setup_workflow,
    with_keys,
    with_fields,
    read_input,
    write_input,
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


def test_linktree(files, tmp_path):
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    res = sendtree(files, tmp_path, mode="link")
    for file in res:
        assert (tmp_path / file).exists()
        assert (tmp_path / file).is_symlink()


def test_parse_groups(parser_groups, mocker):
    mocker.patch("sys.argv", ["testing", "-a", "42", "-b", "foo"])
    groups = parse_groups(parser_groups)
    assert vars(groups["a"]) == {"a": 42}
    assert vars(groups["b"]) == {"b": "foo"}


def test_setup_workflow(nested_graph_with_params, mocker):
    mocker.patch("sys.argv", ["testing", "--val", "38", "--quiet", "--debug"])
    setup_workflow(nested_graph_with_params)
    t = nested_graph_with_params.nodes["t"]
    assert dill.loads(t.ret_queue.get(timeout=10)) == 50


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
