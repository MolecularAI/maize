"""Graph testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import, unused-variable, unused-argument

import logging
from pathlib import Path

import pytest

from maize.core.channels import FileChannel
from maize.core.graph import Graph, GraphBuildException
from maize.core.runtime import Status
from maize.core.workflow import Workflow, CheckpointException, ParsingException
from maize.steps.plumbing import Merge, Delay
from maize.steps.io import LoadFile, Return, SaveFile
from maize.utilities.io import read_input


@pytest.fixture
def graph_dict(shared_datadir):
    return read_input(shared_datadir / "graph.yaml")


@pytest.fixture(params=["nodes", "parameters"])
def graph_dict_missing_field(graph_dict, request):
    graph_dict[request.param][0].pop("name")
    return graph_dict


@pytest.fixture
def graph_dict_missing_channel_field(graph_dict):
    del graph_dict["channels"][0]["sending"]
    return graph_dict


@pytest.fixture
def graph_dict_missing_channel(graph_dict):
    graph_dict["channels"].pop()
    return graph_dict


@pytest.fixture
def two_node_graph(example_a):
    g = Workflow()
    a = g.add(example_a, "a")
    t = g.add(Return[int], "term")
    g.connect(a.out, t.inp)
    return g


@pytest.fixture
def two_node_graph_param(example_a):
    g = Workflow(level="DEBUG")
    a = g.add(example_a, "a", parameters=dict(val=42))
    t = g.add(Return[int], "term")
    g.connect(a.out, t.inp)
    return g


@pytest.fixture
def graph_with_params(two_node_graph_param):
    param = two_node_graph_param.nodes["a"].parameters["val"]
    two_node_graph_param.combine_parameters(param, name="value")
    return two_node_graph_param


@pytest.fixture
def graph_with_all_params(two_node_graph_param):
    node = two_node_graph_param.nodes["a"]
    for param in node.parameters.values():
        two_node_graph_param.combine_parameters(param, name=param.name)
    return two_node_graph_param


class Test_Graph_init:
    def test_graph_add(self, example_a):
        g = Workflow()
        g.add(example_a, "a")
        assert len(g.nodes) == 1

    def test_two_node_init(self, two_node_graph):
        assert len(two_node_graph.channels) == 1
        assert len(two_node_graph.nodes) == 2
        assert len(two_node_graph.nodes["a"].parameters) == 8

    def test_from_dict(self, graph_dict, example_a, example_b, subgraph):
        g = Workflow.from_dict(graph_dict)
        assert len(g.nodes) == 4
        assert len(g.channels) == 4
        assert len(g.parameters) == 1
        assert g.parameters["val"].value == 42

    def test_from_dict_missing_node_field(
        self, graph_dict_missing_field, example_a, example_b, subgraph):
        A, B = example_a, example_b
        with pytest.raises(ParsingException):
            g = Workflow.from_dict(graph_dict_missing_field)

    def test_from_dict_missing_channel_field(
        self, graph_dict_missing_channel_field, example_a, example_b, subgraph):
        A, B = example_a, example_b
        with pytest.raises(ParsingException):
            g = Workflow.from_dict(graph_dict_missing_channel_field)

    def test_from_dict_missing_channel(
        self, graph_dict_missing_channel, example_a, example_b, subgraph):
        A, B = example_a, example_b
        with pytest.raises(GraphBuildException):
            g = Workflow.from_dict(graph_dict_missing_channel)

    def test_from_yaml(self, shared_datadir, example_a, example_b, subgraph):
        A, B = example_a, example_b
        g = Workflow.from_file(shared_datadir / "graph.yaml")
        assert len(g.nodes) == 4
        assert len(g.channels) == 4
        assert len(g.parameters) == 1
        assert g.parameters["val"].value == 42

    def test_to_dict(self, two_node_graph):
        data = two_node_graph.to_dict()
        assert len(data["nodes"]) == 2
        assert len(data["channels"]) == 1

    def test_nested_to_dict(self, nested_graph):
        data = nested_graph.to_dict()
        assert len(data["nodes"]) == 4
        assert len(data["channels"]) == 4

    def test_to_from_dict(self, two_node_graph):
        data = two_node_graph.to_dict()
        graph = Workflow.from_dict(data)
        assert two_node_graph.nodes.keys() == graph.nodes.keys()
        assert two_node_graph.channels.keys() == graph.channels.keys()
        new_data = graph.to_dict()
        assert new_data == data

    def test_to_from_dict_complex(self, nested_graph, example_a, example_b):
        A, B = example_a, example_b
        data = nested_graph.to_dict()
        graph = Workflow.from_dict(data)
        assert nested_graph.nodes.keys() == graph.nodes.keys()
        assert nested_graph.channels.keys() == graph.channels.keys()

    @pytest.mark.parametrize("suffix,length", [("json", 49), ("yml", 45), ("toml", 33)])
    def test_to_file(self, two_node_graph, tmp_path, suffix, length):
        file = tmp_path / f"two-node.{suffix}"
        two_node_graph.to_file(file)
        assert file.exists()
        assert len(file.read_text().split("\n")) == length

    def test_to_checkpoint(self, two_node_graph):
        file = two_node_graph.work_dir.glob(f"ckp-{two_node_graph.name}-*.yaml")
        two_node_graph.to_checkpoint(fail_ok=False)
        assert next(file).exists()

    def test_to_checkpoint_given_file(self, two_node_graph, tmp_path):
        file = tmp_path / "checkpoint.yaml"
        two_node_graph.to_checkpoint(file, fail_ok=False)
        assert file.exists()

    def test_to_checkpoint_given_file_fail(self, two_node_graph, tmp_path):
        file = tmp_path / "non-existent" / "checkpoint.yaml"
        with pytest.raises(CheckpointException):
            two_node_graph.to_checkpoint(file, fail_ok=False)

    def test_to_checkpoint_given_file_fail_ok(self, two_node_graph, tmp_path):
        file = tmp_path / "non-existent" / "checkpoint.yaml"
        two_node_graph.to_checkpoint(file, fail_ok=True)

    def test_from_checkpoint(self, shared_datadir, example_a, example_b):
        A, B = example_a, example_b
        file = shared_datadir / "checkpoint.yaml"
        g = Workflow.from_checkpoint(file)
        assert "a" in g.nodes
        assert "term" in g.nodes
        assert isinstance(g.nodes["a"], A)
        assert g.nodes["a"].status == Status.READY
        assert not g.nodes["a"].fail_ok

    def test_to_checkpoint_nested(self, nested_graph, tmp_path):
        file = tmp_path / "checkpoint-nested.yaml"
        nested_graph.to_checkpoint(file)
        assert file.exists()
        assert len(file.read_text().split("\n")) == 103

    def test_from_checkpoint_nested(
        self, shared_datadir, example_a, example_b, subgraph, subsubgraph):
        A, B = example_a, example_b
        SubGraph, SubSubGraph = subgraph, subsubgraph
        file = shared_datadir / "checkpoint-nested.yaml"
        g = Workflow.from_checkpoint(file)

    @pytest.mark.parametrize("extra_options", [["--value", "42"], ["--value", "-2"]])
    def test_update_with_args(self, graph_with_params, extra_options):
        setting = int(extra_options[-1])
        graph_with_params.update_with_args(extra_options)
        assert graph_with_params.parameters["value"].value == setting

    def test_update_with_args_all(self, graph_with_all_params):
        extra_options = ["--val", "42", "--flag", "--file", "file.pdb"]
        graph_with_all_params.update_with_args(extra_options)
        assert graph_with_all_params.parameters["val"].value == 42
        assert graph_with_all_params.parameters["flag"].value
        assert graph_with_all_params.parameters["file"].value == Path("file.pdb")

    def test_update_with_args_all_fail1(self, graph_with_all_params):
        extra_options = ["--val", "seven", "--flag", "--file", "file.pdb"]
        with pytest.raises(ParsingException):
            graph_with_all_params.update_with_args(extra_options)

    def test_update_with_args_all_fail2(self, graph_with_all_params):
        extra_options = ["--val", "42", "--flag", "--blah", "file.pdb"]
        with pytest.raises(ParsingException):
            graph_with_all_params.update_with_args(extra_options)

    def test_update_with_args_all_fail3(self, graph_with_all_params):
        extra_options = ["--val", "42", "--flag", "--file", "file.xyz"]
        with pytest.raises(ValueError):
            graph_with_all_params.update_with_args(extra_options)


class Test_Graph_properties:
    def test_get_node(self, nested_graph):
        assert nested_graph.get_node("b").name == "b"
        assert nested_graph.get_node("sg", "delay").name == "delay"
        assert nested_graph.get_node("sg", "ssg", "a").name == "a"
        assert nested_graph.get_node("sg", "ssg", "delay").name == "delay"
        assert (nested_graph.get_node("sg", "delay") is not
                nested_graph.get_node("sg", "ssg", "delay"))

    def test_get_parameter(self, nested_graph):
        assert nested_graph.get_parameter("sg", "ssg", "a", "val").name == "val"
        assert nested_graph.get_parameter("sg", "delay", "delay").name == "delay"

    def test_get_port(self, nested_graph):
        assert nested_graph.get_port("sg", "ssg", "out").name == "out"
        assert nested_graph.get_port("m", "inp").name == "inp"


    def test_flat_nodes(self, nested_graph):
        assert ({node.name for node in nested_graph.flat_nodes} ==
                {"a", "b", "delay", "t", "m"})

    def test_as_dict(self, nested_graph):
        assert len(nested_graph.as_dict()["name"]) == 6

    def test_directories(self, nested_graph, tmp_path):
        nested_graph.setup_directories(tmp_path)
        assert nested_graph.work_dir.name.startswith("graph-")
        assert nested_graph.work_dir.exists()
        assert nested_graph.get_node("b").work_dir.name == "node-b"
        assert nested_graph.get_node("sg", "delay").work_dir.name == "node-delay"
        assert nested_graph.get_node("sg", "delay").work_dir.parent.name == "graph-sg"


class Test_Graph_build:
    def test_add(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a")
        assert "a" in g.nodes
        assert a in g.flat_nodes

    def test_add_duplicate(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a")
        with pytest.raises(GraphBuildException):
            a = g.add(example_a, "a")

    def test_add_param(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a", parameters=dict(val=42, flag=True))
        assert "a" in g.nodes
        assert a in g.flat_nodes
        assert a.parameters["val"].value == 42
        assert a.parameters["flag"].value

    def test_add_param_fail(self, example_a):
        g = Workflow()
        with pytest.raises(KeyError):
            a = g.add(example_a, "a", parameters=dict(nonexistent=1))

    def test_add_subgraph_looped(self, subgraph, example_b):
        g = Workflow()
        sg = g.add(subgraph, "sg", loop=True)
        b = g.add(example_b, "b", loop=True)
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        g.connect(sg.out, m.inp)
        g.connect(b.out, m.inp)
        g.connect(m.out, b.inp)
        g.connect(b.out_final, t.inp)
        assert sg.nodes["delay"].looped
        assert sg.nodes["ssg"].looped
        assert sg.nodes["ssg"].nodes["delay"].looped
        assert sg.nodes["ssg"].nodes["a"].looped

    def test_check(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a")
        t = g.add(Return[int], "t")
        with pytest.raises(GraphBuildException):
            g.check()

    def test_connect(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a")
        t = g.add(Return[int], "t")
        g.connect(a.out, t.inp)
        g.check()
        assert a.status == Status.READY
        assert t.status == Status.READY

    def test_connect_file(self, tmp_path):
        g = Workflow(level=logging.DEBUG)
        a = g.add(LoadFile[Path], "a")
        t = g.add(SaveFile[Path], "t")
        g.connect(a.out, t.inp)
        a.file.set(Path("fake"))
        t.destination.set(Path("fake"))
        g.check()
        g.setup_directories(tmp_path)
        assert len(g.channels) == 1
        assert isinstance(g.channels.popitem()[1], FileChannel)
        assert (t.parent.work_dir / f"{t.name}-{t.inp.name}").exists()
        assert a.status == Status.READY
        assert t.status == Status.READY

    def test_connect_bad_types(self, example_b):
        g = Workflow()
        a = g.add(LoadFile[Path], "a")
        b = g.add(example_b, "b", loop=True)
        t1 = g.add(Return[int], "t1")
        t2 = g.add(Return[int], "t2")
        with pytest.raises(GraphBuildException):
            g.connect(a.out, b.inp)
            g.connect(b.out, t1.inp)
            g.connect(b.out_final, t2.inp)

    def test_autoconnect(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a")
        t = g.add(Return[int], "t")
        g.auto_connect(a, t)
        g.check()
        assert a.status == Status.READY
        assert t.status == Status.READY

    def test_connect_large(self, subgraph, example_b):
        g = Workflow()
        sg = g.add(subgraph, "sg")
        b = g.add(example_b, "b", loop=True)
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        g.connect(sg.out, m.inp)
        g.connect(b.out, m.inp)
        g.connect(m.out, b.inp)
        g.connect(b.out_final, t.inp)
        assert b.status == Status.READY
        assert m.status == Status.READY
        assert t.status == Status.READY
        assert len(g.nodes) == 4
        assert len(g.flat_nodes) == 6
        assert len(g.channels) == 4

    def test_connect_large_shorthand(self, subgraph, example_b):
        g = Workflow()
        sg = g.add(subgraph, "sg")
        b = g.add(example_b, "b", loop=True)
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        sg >> m
        m.inp << b.out
        m.out >> b.inp
        t << b
        assert b.status == Status.READY
        assert m.status == Status.READY
        assert t.status == Status.READY
        assert len(g.nodes) == 4
        assert len(g.flat_nodes) == 6
        assert len(g.channels) == 4

    def test_chain(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a")
        d1 = g.add(Delay[int], "d1")
        d2 = g.add(Delay[int], "d2")
        d3 = g.add(Delay[int], "d3")
        t = g.add(Return[int], "t")
        g.chain(a, d1, d2, d3, t)
        assert a.status == Status.READY
        assert d1.status == Status.READY
        assert d2.status == Status.READY
        assert d3.status == Status.READY
        assert t.status == Status.READY
        assert len(g.nodes) == 5
        assert len(g.flat_nodes) == 5
        assert len(g.channels) == 4

    def test_map_parameters(self, example_a):
        g = Workflow()
        a = g.add(example_a, "a")
        t = g.add(Return[int], "t")
        g.connect(a.out, t.inp)
        g.combine_parameters(a.val, name="val")
        assert "val" in g.parameters

    def test_build(self, newgraph, newgraph2):
        g = Workflow()
        ng = g.add(newgraph, "ng")
        ng2 = g.add(newgraph2, "ng2")
        g.connect(ng.out, ng2.inp)
        g.check()
        assert "out" in ng.ports
        assert "inp" in ng2.ports
        assert len(ng.ports) == 1
        assert len(ng2.ports) == 1
        assert len(g.nodes) == 2
        assert len(g.flat_nodes) == 5
        assert len(g.channels) == 1
