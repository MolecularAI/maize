"""Global fixtures"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import, unused-variable, unused-argument

from pathlib import Path
from typing import Annotated

import pytest

from maize.core.interface import Input, Output, Parameter, FileParameter, Suffix
from maize.core.graph import Graph
from maize.core.node import Node
from maize.core.workflow import Workflow
from maize.steps.plumbing import Merge, Delay
from maize.steps.io import Return


class A(Node):
    out = Output[int]()
    val = Parameter[int](default=3)
    file = FileParameter[Annotated[Path, Suffix("pdb")]](default=Path("./fake"))
    flag = Parameter[bool](default=False)

    def run(self):
        self.out.send(self.val.value)


@pytest.fixture
def example_a():
    return A


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


@pytest.fixture
def example_b():
    return B


class SubSubGraph(Graph):
    def build(self):
        a = self.add(A, "a", parameters=dict(val=36))
        d = self.add(Delay[int], "delay", parameters=dict(delay=1))
        self.connect(a.out, d.inp)
        self.out = self.map_port(d.out, name="out")
        self.combine_parameters(a.val, name="val")


@pytest.fixture
def subsubgraph():
    return SubSubGraph


class SubGraph(Graph):
    def build(self):
        a = self.add(SubSubGraph, "ssg", parameters=dict(val=36))
        d = self.add(Delay[int], "delay", parameters=dict(delay=1))
        self.connect(a.out, d.inp)
        self.out = self.map_port(d.out, "out")


@pytest.fixture
def subgraph():
    return SubGraph


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


class NewGraph(Graph):
    def build(self):
        a = self.add(A, "a")
        b = self.add(B, "b", loop=True)
        t = self.add(Return[int], "t")
        self.connect(a.out, b.inp)
        self.connect(b.out_final, t.inp)
        self.out = self.map_port(b.out, "out")

class NewGraph2(Graph):
    def build(self):
        d = self.add(Delay[int], "d")
        t = self.add(Return[int], "t")
        self.connect(d.out, t.inp)
        self.inp = self.map_port(d.inp, "inp")

@pytest.fixture
def newgraph():
    return NewGraph

@pytest.fixture
def newgraph2():
    return NewGraph2
