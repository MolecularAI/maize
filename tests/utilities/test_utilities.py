"""Utilities testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name

from pathlib import Path
import re
import string
from typing import Annotated, Any, Literal

from maize.core.graph import Graph
from maize.core.workflow import Workflow
from maize.steps.plumbing import Delay, Merge
from maize.utilities.utilities import (
    deprecated,
    unique_id,
    graph_cycles,
    typecheck,
    Timer,
    tuple_to_nested_dict,
    nested_dict_to_tuple,
    has_file,
    make_list,
    matching_types,
    find_probable_files_from_command,
    match_context,
)


def test_unique_id():
    assert len(unique_id()) == 6
    assert set(unique_id()).issubset(set(string.ascii_lowercase + string.digits))


def test_graph_cycles(example_a):
    class SubGraph(Graph):
        def build(self):
            a = self.add(example_a, "a")
            m = self.add(Merge[int], "m")
            a.out >> m.inp
            self.inp = self.map_port(m.inp, "inp")
            self.out = self.map_port(m.out, "out")

    g = Workflow()
    a = g.add(SubGraph, "a")
    b = g.add(Delay[int], "b")
    c = g.add(Delay[int], "c")
    a >> b >> c
    c.out >> a.inp
    cycles = graph_cycles(g)
    assert len(cycles) > 0


def test_typecheck():
    assert typecheck(42, int)
    assert not typecheck("foo", int)
    assert typecheck(42, Annotated[int, lambda x: x > 10])
    assert typecheck(42, Annotated[int, lambda x: x > 10, lambda x: x < 50])
    assert not typecheck(8, Annotated[int, lambda x: x > 10])
    assert typecheck(42, Annotated[int | float, lambda x: x > 10])
    assert typecheck(42, int | str)
    assert typecheck(42, None)
    assert typecheck(42, Any)
    assert typecheck(42, Literal[42, 17])
    assert typecheck({"foo": 42}, dict[str, int])
    assert typecheck({"foo": 42, "bar": 39}, dict[str, int])


def test_deprecated():
    def func(a: int) -> int:
        return a + 1

    new = deprecated("func is deprecated")(func)
    assert new(17) == 18

    class cls:
        def func(self, a: int) -> int:
            return a + 1

    new = deprecated("cls is deprecated")(cls)
    assert new().func(17) == 18


def test_Timer():
    t = Timer()
    ini_time = t.elapsed_time
    assert not t.running
    t.start()
    assert t.running
    assert t.elapsed_time > ini_time
    assert t.running
    t.pause()
    assert not t.running
    assert t.stop() > ini_time


def test_tuple_to_nested_dict():
    res = tuple_to_nested_dict("a", "b", "c", 42)
    assert res == {"a": {"b": {"c": 42}}}


def test_nested_dict_to_tuple():
    res = nested_dict_to_tuple({"a": {"b": {"c": 42}}})
    assert res == ("a", "b", "c", 42)


def test_has_file(shared_datadir, tmp_path):
    assert has_file(shared_datadir)
    empty = tmp_path / "empty"
    empty.mkdir()
    assert not has_file(empty)


def test_make_list():
    assert make_list(42) == [42]
    assert make_list([42]) == [42]
    assert make_list({42, 17}) == sorted([42, 17])
    assert make_list((42, 17)) == [42, 17]


def test_matching_types():
    assert matching_types(int, int)
    assert matching_types(bool, int)
    assert not matching_types(bool, int, strict=True)
    assert not matching_types(str, int)
    assert matching_types(str, None)
    assert matching_types(Annotated[int, "something"], int)


def test_find_probable_files_from_command():
    assert find_probable_files_from_command("cat ./test") == [Path("./test")]
    assert find_probable_files_from_command("cat test") == []
    assert find_probable_files_from_command("cat test.abc") == [Path("test.abc")]


def test_match_context():
    match = re.search("foo", "-----foo-----")
    assert match_context(match, chars=3) == "---foo---"
