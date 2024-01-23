"""Simple directed-cyclic-graph example with a subgraph"""

from maize.core.graph import Graph
from maize.core.interface import Parameter, Input, Output
from maize.core.node import Node
from maize.core.workflow import Workflow

from maize.steps.plumbing import Delay, Merge
from maize.steps.io import Return


class A(Node):
    out: Output[int] = Output()
    send_val: Parameter[int] = Parameter()

    def run(self) -> None:
        self.out.send(self.send_val.value)


class B(Node):
    inp: Input[int] = Input()
    out: Output[int] = Output()
    final: Output[int] = Output()

    def run(self) -> None:
        val = self.inp.receive()
        if val > 48:
            self.logger.debug("%s stopping", self.name)
            self.final.send(val)
            return
        self.out.send(val + 2)

class SubGraph(Graph):
    def build(self) -> None:
        a = self.add(A, parameters=dict(send_val=36))
        d = self.add(Delay[int], parameters=dict(delay=1))
        self.connect(a.out, d.inp)
        self.out = self.map_port(d.out)
        self.val = self.combine_parameters(a.send_val, name="val")


flow = Workflow(name="test")
sg = flow.add(SubGraph)
b = flow.add(B, loop=True)
merge = flow.add(Merge[int])
ret = flow.add(Return[int])
flow.connect(sg.out, merge.inp)
flow.connect(merge.out, b.inp)
flow.connect(b.out, merge.inp)
flow.connect(b.final, ret.inp)
flow.combine_parameters(sg.val, name="val")
flow.check()
flow.execute()
