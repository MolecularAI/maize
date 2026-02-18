"""Resources testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name

import pytest

from maize.utilities.resources import gpu_count, ChunkedSemaphore, Resources


def test_gpu_count():
    assert gpu_count() in range(1024)


def test_ChunkedSemaphore():
    sem = ChunkedSemaphore(10, sleep=1)
    sem.acquire(5)
    sem.acquire(5)
    with pytest.raises(ValueError):
        sem.acquire(20)
    sem.release(5)
    sem.release(5)
    with pytest.raises(ValueError):
        sem.release(5)

def test_Resources(mock_component):
    sem = Resources(10, parent=mock_component)
    with sem(5):
        pass

    with pytest.raises(ValueError):
        with sem(20):
            pass
