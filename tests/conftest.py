"""Global fixtures"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import, unused-variable, unused-argument

from pathlib import Path
import shutil
from typing import Annotated, Any

import pytest

from maize.core.interface import Input, Output, Parameter, FileParameter, Suffix
from maize.core.graph import Graph
from maize.core.node import Node
from maize.core.workflow import Workflow
from maize.steps.plumbing import Copy, Merge, Delay
from maize.steps.io import Return, Void


@pytest.fixture
def temp_working_dir(tmp_path: Any, monkeypatch: Any) -> None:
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def example_a():
    class A(Node):
        out = Output[int]()
        val = Parameter[int](default=3)
        file = FileParameter[Annotated[Path, Suffix("pdb")]](default=Path("./fake"))
        flag = Parameter[bool](default=False)

        def run(self):
            self.out.send(self.val.value)

    return A


@pytest.fixture
def example_b():
    class B(Node):
        fail: bool = False
        inp = Input[int]()
        out = Output[int]()
        out_final = Output[int]()

        def run(self):
            if self.fail:
                self.fail = False
                raise RuntimeError("This is a test exception")

            val = self.inp.receive()
            self.logger.debug("%s received %s", self.name, val)
            if val > 48:
                self.logger.debug("%s stopping", self.name)
                self.out_final.send(val)
                return
            self.out.send(val + 2)

    return B


@pytest.fixture
def node_with_file():
    class NodeFile(Node):
        inp: Input[int] = Input()

        def run(self) -> None:
            file = Path("test.out")
            file.unlink(missing_ok=True)
            data = self.inp.receive()
            self.logger.debug("received %s", data)
            with file.open("w") as f:
                f.write(str(data))
    return NodeFile


@pytest.fixture
def subsubgraph(example_a):
    class SubSubGraph(Graph):
        def build(self):
            a = self.add(example_a, "a", parameters=dict(val=36))
            d = self.add(Delay[int], "delay", parameters=dict(delay=1))
            self.connect(a.out, d.inp)
            self.out = self.map_port(d.out, name="out")
            self.combine_parameters(a.val, name="val")

    return SubSubGraph


@pytest.fixture
def subgraph(subsubgraph):
    class SubGraph(Graph):
        def build(self):
            a = self.add(subsubgraph, "ssg", parameters=dict(val=36))
            d = self.add(Delay[int], "delay", parameters=dict(delay=1))
            self.connect(a.out, d.inp)
            self.out = self.map_port(d.out, "out")

    return SubGraph


@pytest.fixture
def subgraph_multi(example_a):
    class SubgraphMulti(Graph):
        def build(self):
            a = self.add(example_a, parameters=dict(val=36))
            copy = self.add(Copy[int])
            void = self.add(Void)
            self.connect(a.out, copy.inp)
            self.connect(copy.out, void.inp)
            self.map(copy.out)
    return SubgraphMulti


@pytest.fixture
def nested_graph(subgraph, example_b):
    g = Workflow()
    sg = g.add(subgraph, "sg")
    b = g.add(example_b, "b", loop=True)
    m = g.add(Merge[int], "m")
    t = g.add(Return[int], "t")
    g.connect(sg.out, m.inp)
    g.connect(b.out, m.inp)
    g.connect(m.out, b.inp)
    g.connect(b.out_final, t.inp)
    return g


@pytest.fixture
def nested_graph_with_params(subsubgraph, example_b):
    g = Workflow()
    sg = g.add(subsubgraph, "sg")
    b = g.add(example_b, "b", loop=True)
    m = g.add(Merge[int], "m")
    t = g.add(Return[int], "t")
    g.connect(sg.out, m.inp)
    g.connect(b.out, m.inp)
    g.connect(m.out, b.inp)
    g.connect(b.out_final, t.inp)
    g.combine_parameters(sg.parameters["val"], name="val")
    return g


@pytest.fixture
def newgraph(example_a, example_b):
    class NewGraph(Graph):
        def build(self):
            a = self.add(example_a, "a")
            b = self.add(example_b, "b", loop=True)
            t = self.add(Return[int], "t")
            self.connect(a.out, b.inp)
            self.connect(b.out_final, t.inp)
            self.out = self.map_port(b.out, "out")

    return NewGraph


@pytest.fixture
def newgraph2():
    class NewGraph2(Graph):
        def build(self):
            d = self.add(Delay[int], "d")
            t = self.add(Return[int], "t")
            self.connect(d.out, t.inp)
            self.inp = self.map_port(d.inp, "inp")

    return NewGraph2
