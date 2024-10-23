"""Workflow execution testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import, unused-variable, unused-argument

from dataclasses import dataclass
import logging
from pathlib import Path
import random
import sys
import time
from typing import Annotated

import networkx
import numpy as np
import pytest

from maize.core.interface import (
    Input,
    Output,
    Parameter,
    FileParameter,
    MultiInput,
    MultiOutput,
    Suffix,
)
from maize.core.graph import Graph
from maize.core.node import Node
from maize.core.runtime import DEFAULT_CONTEXT, NodeException
from maize.core.workflow import Workflow, FutureWorkflowResult, expose, wait_for_all
from maize.steps.plumbing import Merge, Delay
from maize.steps.io import LoadData, Return, LoadFile, SaveFile
from maize.utilities.execution import JobResourceConfig, WorkflowStatus, check_executable


ENV_EXEC = Path(sys.executable).parents[2] / "maize-test" / "bin" / "python"


class A(Node):
    out = Output[int]()
    val = Parameter[int]()
    file = FileParameter[Annotated[Path, Suffix("pdb")]]()
    flag = Parameter[bool]()

    def run(self):
        self.out.send(self.val.value)


@pytest.fixture
def example_a():
    return A


class Aany(Node):
    out = Output()
    val = Parameter()
    file = FileParameter()
    flag = Parameter()

    def run(self):
        self.out.send(self.val.value)


@pytest.fixture
def example_a_any():
    return Aany


class Aenv(Node):
    out = Output[int]()
    val = Parameter[int](default=42)
    file = FileParameter[Annotated[Path, Suffix("pdb")]]()
    flag = Parameter[bool]()

    def run(self):
        import scipy

        self.out.send(self.val.value)


@pytest.fixture
def example_a_env():
    return Aenv


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


@pytest.fixture
def loopable_subgraph(example_b):
    class Loopable(Graph):
        def build(self) -> None:
            dela = self.add(Delay[int], parameters={"delay": 1})
            b = self.add(example_b)
            self.connect(dela.out, b.inp)
            self.map(dela.inp, b.out, b.out_final)

    return Loopable


class SubSubGraph(Graph):
    def build(self):
        a = self.add(A, "a", parameters=dict(val=36))
        d = self.add(Delay[int], "delay", parameters=dict(delay=1))
        self.connect(a.out, d.inp)
        self.out = self.map_port(d.out, "out")
        self.combine_parameters(a.val, name="val")


@pytest.fixture
def subsubgraph(example_a):
    return SubSubGraph


class Random(Node):
    """Perform a random linear combination of inputs and send results to outputs."""

    inp = MultiInput[float](optional=True)
    out = MultiOutput[float](optional=True)
    fail = Parameter[bool](default=False)

    def run(self):
        n_inputs = len(self.inp)
        n_outputs = len(self.out)
        # Starting nodes
        if n_inputs == 0:
            ins = np.array([1])
        else:
            ins = np.array([ip.receive() for ip in self.inp])
        weights = np.random.random(size=(max(n_outputs, 1), max(n_inputs, 1)))
        res = weights @ ins
        time.sleep(random.random())
        if self.fail.value and (random.random() < 0.01):
            raise Exception("fail test")
        for i, out in enumerate(self.out):
            if out.active:
                out.send(res[i])


@pytest.fixture
def random_node():
    return Random


class RandomLoop(Node):
    inp = MultiInput[float](optional=True)
    out = MultiOutput[float](optional=True)
    fail = Parameter[bool](default=False)

    def run(self):
        n_inputs = len(self.inp)
        n_outputs = len(self.out)
        ins = np.ones(max(1, n_inputs))
        for i, inp in enumerate(self.inp):
            if inp.ready():
                self.logger.debug("Receiving via input %s", i)
                ins[i] = inp.receive()
        weights = np.random.random(size=(max(n_outputs, 1), max(n_inputs, 1)))
        res = weights @ ins
        time.sleep(random.random())
        if self.fail.value and (random.random() < 0.1):
            raise RuntimeError("This is a test exception")
        if random.random() < 0.05:
            return

        for i, out in enumerate(self.out):
            if out.active:
                self.logger.debug("Sending via output %s", i)
                out.send(res[i])


@pytest.fixture
def random_loop_node():
    return RandomLoop


class RandomResources(Node):
    inp = MultiInput[float](optional=True)
    out = MultiOutput[float](optional=True)
    fail = Parameter[bool](default=False)

    def run(self):
        n_inputs = len(self.inp)
        n_outputs = len(self.out)
        # Starting nodes
        if n_inputs == 0:
            ins = np.array([1])
        else:
            ins = np.array([inp.receive() for inp in self.inp])

        with self.cpus(8):
            weights = np.random.random(size=(max(n_outputs, 1), max(n_inputs, 1)))
            res = weights @ ins
            time.sleep(random.random())
            if self.fail.value and (random.random() < 0.1):
                raise Exception("fail test")
            for i, out in enumerate(self.out):
                if out.active:
                    out.send(res[i])


@pytest.fixture
def random_resource_node():
    return RandomResources


@pytest.fixture
def graph_mp_fixed_file(shared_datadir):
    return shared_datadir / "graph-mp-fixed.yaml"


@pytest.fixture
def simple_data():
    return 42


@dataclass
class TestData:
    x: int
    y: list[str]
    z: dict[tuple[int, int], bytes]


@pytest.fixture
def normal_data():
    return TestData(x=39, y=["hello", "göteborg"], z={(0, 1): b"foo"})


@pytest.fixture
def weird_data():
    return {("foo", 4j): lambda x: print(x + 1)}


@pytest.fixture(params=[10, 50, 100])
def random_dag_gen(request):
    def _random_dag(fail, node_type):
        dag = networkx.gnc_graph(request.param)
        g = Workflow()
        for i in range(dag.number_of_nodes()):
            node = g.add(node_type, str(i), parameters=dict(fail=fail))

        for sen, rec in dag.edges:
            g.connect(sending=g.nodes[str(sen)].out, receiving=g.nodes[str(rec)].inp)

        term = g.add(Return[float], "term")
        for node in g.nodes.values():
            if "out" in node.outputs:
                g.connect(sending=node.outputs["out"], receiving=term.inp)
                break

        g.ret = term.ret_queue
        print({k: {k: v.connected for k, v in n.ports.items()} for k, n in g.nodes.items()})
        return g

    return _random_dag


@pytest.fixture(params=[10, 50, 100])
def random_dcg_gen(request, random_loop_node):
    def _random_dcg(fail):
        dcg = networkx.random_k_out_graph(request.param, k=2, alpha=0.2)
        g = Workflow(level=logging.DEBUG)
        for i in range(dcg.number_of_nodes()):
            _ = g.add(random_loop_node, str(i), loop=True, parameters=dict(fail=fail))

        for sen, rec, _ in dcg.edges:
            g.logger.info("Connecting %s -> %s", sen, rec)
            g.connect(
                sending=g.nodes[str(sen)].out,
                receiving=g.nodes[str(rec)].inp,
                size=request.param * 5,
            )

        return g

    return _random_dcg


class Test_workflow:
    def test_register(self):
        Workflow.register(name="test", factory=lambda: Workflow())
        assert Workflow.get_workflow_summary("test") == ""
        assert callable(Workflow.get_available_workflows().pop())
        assert Workflow.from_name("test")
        with pytest.raises(KeyError):
            Workflow.from_name("nope")

    def test_template(self, random_dag_gen, random_node):
        wf = random_dag_gen(fail=False, node_type=random_node)
        assert wf.generate_config_template() == ""

    def test_expose(self, simple_data, example_a, mocker, shared_datadir):
        @expose
        def flow() -> Workflow:
            g = Workflow()
            a = g.add(example_a, "a", parameters=dict(val=simple_data))
            t = g.add(Return[int], "term")
            g.connect(a.out, t.inp)
            g.map(a.val, a.file, a.flag)
            return g

        mocker.patch("sys.argv", ["testing", "--val", "foo", "--check"])
        with pytest.raises(SystemExit):
            flow()

        file = Path("file.pdb")
        file.touch()
        mocker.patch(
            "sys.argv", ["testing", "--val", "17", "--file", file.as_posix(), "--flag", "--check"]
        )
        flow()


class Test_graph_run:
    def test_single_execution_channel_simple(self, simple_data, example_a):
        g = Workflow()
        a = g.add(example_a, "a", parameters=dict(val=simple_data))
        t = g.add(Return[int], "term")
        g.connect(a.out, t.inp)
        g.execute()
        assert t.get() == simple_data

    def test_single_execution_channel_simple_file(self, node_with_file):
        g = Workflow(cleanup_temp=False, level="debug")
        g.config.scratch = Path("./")
        l = g.add(LoadData[int], parameters={"data": 42}, loop=True, max_loops=3)
        a = g.add(node_with_file, loop=True)
        g.connect(l.out, a.inp)
        g.execute()
        assert g.work_dir.exists()
        assert (g.work_dir / "node-loaddata").exists()
        with pytest.raises(StopIteration):
            next((g.work_dir / "node-loaddata").iterdir()).exists()

    def test_single_execution_channel_normal(self, normal_data, example_a_any):
        g = Workflow()
        a = g.add(example_a_any, "a", parameters=dict(val=normal_data))
        t = g.add(Return[int], "term")
        g.connect(a.out, t.inp)
        g.execute()
        assert t.get() == normal_data

    @pytest.mark.skipif(
        DEFAULT_CONTEXT == "spawn",
        reason=(
            "Acceptable datatypes for channels are restricted "
            "when using multiprocessing 'spawn' context"
        ),
    )
    def test_single_execution_channel_weird(self, weird_data, example_a_any):
        g = Workflow()
        a = g.add(example_a_any, "a", parameters=dict(val=weird_data))
        t = g.add(Return[int], "term")
        g.connect(a.out, t.inp)
        g.execute()
        assert t.get().keys() == weird_data.keys()

    @pytest.mark.skipif(
        not ENV_EXEC.exists(),
        reason=(
            "Testing alternative environment execution requires "
            "the `maize-test` environment to be installed"
        ),
    )
    def test_single_execution_alt_env(self, example_a_env, simple_data):
        g = Workflow(level=logging.DEBUG)
        a = g.add(example_a_env, "a", parameters=dict(python=ENV_EXEC, val=simple_data))
        t = g.add(Return[int], "term")
        g.connect(a.out, t.inp)
        g.execute()
        assert t.get() == simple_data

    def test_single_execution_complex_graph(self, example_a, example_b):
        g = Workflow()
        a = g.add(example_a, "a", parameters=dict(val=40))
        b = g.add(example_b, "b", loop=True)
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        g.connect(a.out, m.inp)
        g.connect(b.out, m.inp)
        g.connect(m.out, b.inp)
        g.connect(b.out_final, t.inp)
        g.execute()
        assert t.get() == 50

    def test_single_execution_complex_graph_with_subgraph(self, subsubgraph, example_b):
        g = Workflow(level="DEBUG")
        a = g.add(subsubgraph, "a", parameters=dict(val=40))
        b = g.add(example_b, "b", loop=True)
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        g.connect(a.out, m.inp)
        g.connect(b.out, m.inp)
        g.connect(m.out, b.inp)
        g.connect(b.out_final, t.inp)
        g.execute()
        assert t.get() == 50

    def test_execution_subgraph_looped(self, loopable_subgraph, example_a):
        g = Workflow(level="DEBUG")
        a = g.add(example_a, "a", parameters={"val": 36})
        sg = g.add(loopable_subgraph, "sg", loop=True)
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        g.connect(a.out, m.inp)
        g.connect(sg.out, m.inp)
        g.connect(m.out, sg.inp)
        g.connect(sg.out_final, t.inp)
        g.execute()
        assert t.get() == 50

    def test_multi_execution_complex_graph(self, example_a, example_b):
        g = Workflow()
        a = g.add(example_a, "a", parameters=dict(val=40))
        b = g.add(example_b, "b", n_attempts=2, loop=True)
        b.fail = True
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        g.connect(a.out, m.inp)
        g.connect(b.out, m.inp)
        g.connect(m.out, b.inp)
        g.connect(b.out_final, t.inp)
        g.execute()
        assert t.get() == 50

    def test_multi_execution_complex_graph_fail(self, example_a, example_b):
        g = Workflow()
        a = g.add(example_a, "a", parameters=dict(val=40))
        b = g.add(example_b, "b", n_attempts=1, loop=True)
        b.fail = True
        m = g.add(Merge[int], "m")
        t = g.add(Return[int], "t")
        g.connect(a.out, m.inp)
        g.connect(b.out, m.inp)
        g.connect(m.out, b.inp)
        g.connect(b.out_final, t.inp)
        with pytest.raises(NodeException):
            g.execute()

    @pytest.mark.skipif(
        not check_executable("sinfo"),
        reason="Testing slurm requires a functioning Slurm batch system",
    )
    def test_submit(self, tmp_path, shared_datadir):
        flow = Workflow()
        data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
        out = flow.add(SaveFile[Path], parameters={"destination": tmp_path / "test.abc"})
        flow.connect(data.out, out.inp)
        res = flow.submit(folder=tmp_path, config=JobResourceConfig(walltime="00:02:00"))

        assert res.id
        assert res.query() in (WorkflowStatus.QUEUED, WorkflowStatus.RUNNING)
        assert res.wait() == WorkflowStatus.COMPLETED
        assert (tmp_path / "test.abc").exists()

    @pytest.mark.skipif(
        not check_executable("sinfo"),
        reason="Testing slurm requires a functioning Slurm batch system",
    )
    def test_submit_wait_all(self, tmp_path, shared_datadir):
        res = {}
        for name in ("a", "b"):
            flow = Workflow(name=name)
            data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
            out = flow.add(SaveFile[Path], parameters={"destination": tmp_path / f"test-{name}.abc"})
            flow.connect(data.out, out.inp)
            res[name] = flow.submit(folder=tmp_path / name, config=JobResourceConfig(walltime="00:02:00"))

        wait_for_all(list(res.values()))
        for name in ("a", "b"):
            assert res[name].id
            assert res[name].wait() == WorkflowStatus.COMPLETED
            assert (tmp_path / f"test-{name}.abc").exists()

    @pytest.mark.skipif(
        not check_executable("sinfo"),
        reason="Testing slurm requires a functioning Slurm batch system",
    )
    def test_submit_cancel(self, tmp_path, shared_datadir):
        flow = Workflow()
        data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
        delay = flow.add(Delay[Path], parameters={"delay": 60})
        out = flow.add(SaveFile[Path], parameters={"destination": tmp_path / "test.abc"})
        flow.connect(data.out, delay.inp)
        flow.connect(delay.out, out.inp)
        res = flow.submit(folder=tmp_path, config=JobResourceConfig(walltime="00:02:00"))

        assert res.id
        assert res.query() in (WorkflowStatus.QUEUED, WorkflowStatus.RUNNING)
        res.cancel()
        time.sleep(10)  # Wait a bit for slow queueing systems
        assert res.query() == WorkflowStatus.CANCELLED
        assert not (tmp_path / "test.abc").exists()

    @pytest.mark.skipif(
        not check_executable("sinfo"),
        reason="Testing slurm requires a functioning Slurm batch system",
    )
    def test_submit_serdes(self, tmp_path, shared_datadir):
        flow = Workflow()
        data = flow.add(LoadFile[Path], parameters={"file": shared_datadir / "testorigin.abc"})
        delay = flow.add(Delay[Path], parameters={"delay": 30})
        out = flow.add(SaveFile[Path], parameters={"destination": tmp_path / "test.abc"})
        flow.connect(data.out, delay.inp)
        flow.connect(delay.out, out.inp)
        res = flow.submit(folder=tmp_path, config=JobResourceConfig(walltime="00:02:00"))

        assert res.id
        assert res.query() in (WorkflowStatus.QUEUED, WorkflowStatus.RUNNING)
        serdes = FutureWorkflowResult.from_dict(res.to_dict())
        assert serdes.query() in (WorkflowStatus.QUEUED, WorkflowStatus.RUNNING)
        assert serdes.wait() == WorkflowStatus.COMPLETED
        assert (tmp_path / "test.abc").exists()

    @pytest.mark.random
    def test_random_dags(self, random_dag_gen, random_node):
        random_dag_gen(fail=False, node_type=random_node).execute()

    @pytest.mark.random
    def test_random_dags_resources(self, random_dag_gen, random_resource_node):
        random_dag_gen(fail=False, node_type=random_resource_node).execute()

    @pytest.mark.random
    def test_random_dcgs(self, random_dcg_gen):
        random_dcg_gen(fail=False).execute()

    @pytest.mark.random
    def test_random_dcgs_with_fail(self, random_dcg_gen):
        with pytest.raises(NodeException):
            random_dcg_gen(fail=True).execute()
