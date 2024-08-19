"""Visualization testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name

import pytest
from maize.utilities.visual import nested_graphviz


class Test_Visual:
    def test_nested_graphviz(self, nested_graph):
        dot = nested_graphviz(nested_graph)
        assert "subgraph" in dot.body[0]
        assert "cluster-sg" in dot.body[0]
        assert "cluster-ssg" in dot.body[4]
