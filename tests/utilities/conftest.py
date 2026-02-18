"""Utility testing data"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

import pytest

from maize.core.component import Component


@pytest.fixture
def mock_component():
    return Component()
