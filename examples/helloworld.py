"""A simple hello-world-ish example graph."""

from maize.core.interface import Parameter, Output, MultiInput
from maize.core.node import Node
from maize.core.workflow import Workflow

# Define the nodes
class Example(Node):
    data: Parameter[str] = Parameter(default="Hello")
    out: Output[str] = Output()

    def run(self) -> None:
        self.out.send(self.data.value)


class ConcatAndPrint(Node):
    inp: MultiInput[str] = MultiInput()

    def run(self) -> None:
        result = " ".join(inp.receive() for inp in self.inp)
        self.logger.info("Received: '%s'", result)


# Build the graph
flow = Workflow(name="hello")
ex1 = flow.add(Example, name="ex1")
ex2 = flow.add(Example, name="ex2", parameters=dict(data="maize"))
concat = flow.add(ConcatAndPrint)
flow.connect(ex1.out, concat.inp)
flow.connect(ex2.out, concat.inp)

# Check and run!
flow.check()
flow.execute()
