"""Tests for plumbing nodes"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init

import time

import pytest

from maize.utilities.testing import TestRig
from maize.steps.plumbing import (
    Accumulate,
    Batch,
    Choice,
    Combine,
    CopyEveryNIter,
    Delay,
    IndexDistribute,
    IntegerMap,
    Merge,
    Copy,
    MergeLists,
    Multiplex,
    Multiply,
    RoundRobin,
    Scatter,
    Barrier,
    TimeDistribute,
    Yes,
)


class Test_plumbing:
    def test_multiply(self):
        t = TestRig(Multiply)
        out = t.setup_run(inputs={"inp": [42]}, parameters={"n_packages": 3})
        assert out["out"].get() == [42, 42, 42]

    def test_yes(self):
        t = TestRig(Yes)
        out = t.setup_run(inputs={"inp": [42]}, max_loops=3)
        assert out["out"].get() == 42
        assert out["out"].get() == 42
        assert out["out"].get() == 42

    def test_barrier(self):
        t = TestRig(Barrier)
        out = t.setup_run(inputs={"inp": [[1, 2], [3, 4]], "inp_signal": [False, True]})
        assert out["out"].get() == [3, 4]

    def test_batch(self):
        t = TestRig(Batch)
        out = t.setup_run(inputs={"inp": [[1, 2, 3, 4]]}, parameters={"n_batches": 3})
        assert out["out"].get() == [1, 2]
        assert out["out"].get() == [3]
        assert out["out"].get() == [4]

    def test_combine(self):
        t = TestRig(Combine)
        out = t.setup_run(inputs={"inp": [[1, 2], [3], [4]]}, parameters={"n_batches": 3})
        assert out["out"].get() == [1, 2, 3, 4]

    def test_merge_lists(self):
        t = TestRig(MergeLists)
        out = t.setup_run(inputs={"inp": [[[1, 2]], [[3]], [[4, 5]]]}, max_loops=1)
        assert out["out"].get() == [1, 2, 3, 4, 5]

    def test_merge(self):
        t = TestRig(Merge)
        out = t.setup_run(inputs={"inp": [1, 2]}, max_loops=1)
        assert out["out"].get() == 1
        assert out["out"].get() == 2

    def test_multiplex(self):
        t = TestRig(Multiplex)
        out = t.setup_run_multi(
            inputs={"inp": [0, 1, 2], "inp_single": [3, 4, 5]}, n_outputs=3, max_loops=1
        )
        assert out["out_single"].get() == 0
        assert out["out"][0].get() == 3
        assert out["out_single"].get() == 1
        assert out["out"][1].get() == 4
        assert out["out_single"].get() == 2
        assert out["out"][2].get() == 5

    def test_copy(self):
        t = TestRig(Copy)
        out = t.setup_run_multi(inputs={"inp": 1}, n_outputs=2, max_loops=1)
        assert out["out"][0].get() == 1
        assert out["out"][1].get() == 1

    def test_round_robin(self):
        t = TestRig(RoundRobin)
        out = t.setup_run_multi(inputs={"inp": [1, 2]}, n_outputs=2, max_loops=2)
        assert out["out"][0].get() == 1
        assert out["out"][1].get() == 2

    def test_integer_map(self):
        t = TestRig(IntegerMap)
        out = t.setup_run(
            inputs={"inp": [0, 1, 2, 3]}, parameters={"pattern": [0, 2, -1]}, max_loops=4, loop=True
        )
        assert out["out"].get() == 1
        assert out["out"].get() == 1
        assert out["out"].get() == 2
        assert out["out"].get() == 2

    def test_choice(self):
        t = TestRig(Choice)
        out = t.setup_run(
            inputs={"inp": ["foo", "bar"], "inp_index": [0]}, parameters={"clip": False}
        )
        assert out["out"].get() == "foo"

    def test_choice2(self):
        t = TestRig(Choice)
        out = t.setup_run(
            inputs={"inp": ["foo", "bar"], "inp_index": [1]}, parameters={"clip": False}
        )
        assert out["out"].get() == "bar"

    def test_choice_clip(self):
        t = TestRig(Choice)
        out = t.setup_run(
            inputs={"inp": ["foo", "bar"], "inp_index": [3]}, parameters={"clip": True}
        )
        assert out["out"].get() == "bar"

    def test_index_distribute(self):
        t = TestRig(IndexDistribute)
        out = t.setup_run_multi(
            inputs={"inp": ["foo"], "inp_index": [0]},
            parameters={"clip": False},
            n_outputs=3,
        )
        assert out["out"][0].get() == "foo"
        assert not out["out"][1].ready
        assert not out["out"][2].ready

    def test_index_distribute2(self):
        t = TestRig(IndexDistribute)
        out = t.setup_run_multi(
            inputs={"inp": ["foo"], "inp_index": [1]},
            parameters={"clip": True},
            n_outputs=3,
        )
        assert not out["out"][0].ready
        assert out["out"][1].get() == "foo"
        assert not out["out"][2].ready

    def test_index_distribute_clip(self):
        t = TestRig(IndexDistribute)
        out = t.setup_run_multi(
            inputs={"inp": ["foo"], "inp_index": [4]},
            parameters={"clip": True},
            n_outputs=3,
        )
        assert not out["out"][0].ready
        assert not out["out"][1].ready
        assert out["out"][2].get() == "foo"

    def test_time_distribute(self):
        t = TestRig(TimeDistribute)
        out = t.setup_run_multi(
            inputs={"inp": [1, 2, 3, 4, 5, 6, 7, 8]},
            parameters={"pattern": [2, 1, 5, 0]},
            n_outputs=4,
        )
        for i in range(2):
            assert out["out"][0].get() == i + 1
        assert out["out"][1].get() == 3
        for i in range(5):
            assert out["out"][2].get() == 4 + i
        assert not out["out"][3].flush()

    def test_time_distribute_inf(self):
        t = TestRig(TimeDistribute)
        out = t.setup_run_multi(
            inputs={"inp": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]},
            parameters={"pattern": [2, 1, 4, -1]},
            n_outputs=4,
        )
        for i in range(2):
            assert out["out"][0].get() == i + 1
        assert out["out"][1].get() == 3
        for i in range(4):
            assert out["out"][2].get() == 4 + i
        for i in range(3):
            assert out["out"][3].get() == 8 + i
        assert not out["out"][3].flush()

    def test_time_distribute_cycle(self):
        t = TestRig(TimeDistribute)
        out = t.setup_run_multi(
            inputs={"inp": [1, 2, 3, 1, 2, 3]},
            parameters={"pattern": [2, 1], "cycle": True},
            n_outputs=2,
            max_loops=6,
        )
        for _ in range(2):
            assert out["out"][0].get() == 1
            assert out["out"][0].get() == 2
            assert out["out"][1].get() == 3

    def test_copy_every_n_iter(self):
        t = TestRig(CopyEveryNIter)
        out = t.setup_run_multi(
            inputs={"inp": [1, 2, 3, 4]}, parameters={"freq": 2}, n_outputs=2, max_loops=4
        )
        for i in range(4):
            assert out["out"][0].get() == i + 1
        assert out["out"][1].get() == 2
        assert out["out"][1].get() == 4

    def test_accumulate(self):
        t = TestRig(Accumulate)
        out = t.setup_run(inputs={"inp": [1, 2]}, parameters={"n_packets": 2}, max_loops=1)
        assert out["out"].get() == [1, 2]

    def test_scatter(self):
        t = TestRig(Scatter)
        out = t.setup_run(inputs={"inp": [[1, 2]]}, max_loops=1)
        assert out["out"].get() == 1
        assert out["out"].get() == 2

    def test_scatter_fail(self):
        t = TestRig(Scatter)
        with pytest.raises(ValueError):
            _ = t.setup_run(inputs={"inp": [1]}, max_loops=1)

    def test_delay(self):
        t = TestRig(Delay)
        start_time = time.time()
        out = t.setup_run(inputs={"inp": 1}, parameters={"delay": 5.0}, max_loops=1)
        end_time = time.time() - start_time
        assert out["out"].get() == 1
        assert 4.5 < end_time < 10
