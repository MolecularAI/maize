"""Tests for plumbing nodes"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init

import time

import pytest

from maize.utilities.testing import TestRig
from maize.steps.plumbing import (
    Accumulate,
    Batch,
    Combine,
    Delay,
    Merge,
    Copy,
    RoundRobin,
    Scatter,
    Barrier,
    Yes,
)


class Test_plumbing:
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

    def test_merge(self):
        t = TestRig(Merge)
        out = t.setup_run(inputs={"inp": [1, 2]}, max_loops=1)
        assert out["out"].get() == 1
        assert out["out"].get() == 2

    def test_copy(self):
        t = TestRig(Copy)
        out = t.setup_run(inputs={"inp": 1}, n_outputs=2, max_loops=1)
        assert out["out"][0].get() == 1
        assert out["out"][1].get() == 1

    def test_round_robin(self):
        t = TestRig(RoundRobin)
        out = t.setup_run(inputs={"inp": [1, 2]}, n_outputs=2, max_loops=2)
        assert out["out"][0].get() == 1
        assert out["out"][1].get() == 2

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
