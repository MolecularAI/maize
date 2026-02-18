"""Utilities for graph and workflow visualization."""

import sys
from typing import Any, Literal, Union, cast, TYPE_CHECKING, get_args, get_origin

from matplotlib.colors import to_hex

from maize.utilities.execution import check_executable
from maize.core.runtime import Status

try:
    import graphviz
except ImportError:
    HAS_GRAPHVIZ = False
else:
    HAS_GRAPHVIZ = True
    if not check_executable(["dot", "-V"]):
        HAS_GRAPHVIZ = False

if TYPE_CHECKING:
    from maize.core.graph import Graph

# AZ colors
_COLORS = {
    "mulberry": (131, 0, 81),
    "navy": (0, 56, 101),
    "purple": (60, 16, 83),
    "gold": (240, 171, 0),
    "lime-green": (196, 214, 0),
    "graphite": (63, 68, 68),
    "light-blue": (104, 210, 223),
    "magenta": (208, 0, 111),
    "platinum": (157, 176, 172),
}

_STATUS_COLORS = {
    Status.NOT_READY: _COLORS["platinum"],
    Status.READY: _COLORS["graphite"],
    Status.RUNNING: _COLORS["light-blue"],
    Status.COMPLETED: _COLORS["lime-green"],
    Status.FAILED: _COLORS["magenta"],
    Status.STOPPED: _COLORS["purple"],
    Status.WAITING_FOR_INPUT: _COLORS["gold"],
    Status.WAITING_FOR_OUTPUT: _COLORS["gold"],
    Status.WAITING_FOR_RESOURCES: _COLORS["navy"],
    Status.WAITING_FOR_COMMAND: _COLORS["navy"],
}


def _rgb(red: int, green: int, blue: int) -> Any:
    return to_hex((red / 255, green / 255, blue / 255))


def _pprint_dtype(dtype: Any) -> str:
    """Prints datatypes in a concise way"""
    ret = ""
    if (origin := get_origin(dtype)) is not None:
        ret += f"{origin.__name__}"
    if args := get_args(dtype):
        ret += f"[{', '.join(_pprint_dtype(arg) for arg in args)}]"
    elif hasattr(dtype, "__name__"):
        ret += dtype.__name__
    return ret


HEX_COLORS = {k: _rgb(*c) for k, c in _COLORS.items()}
HEX_STATUS_COLORS = {k: _rgb(*c) for k, c in _STATUS_COLORS.items()}
COLOR_SEQ = list(HEX_COLORS.values())
GRAPHVIZ_STYLE = dict(
    node_attr={
        "fillcolor": "#66666622",
        "fontname": "Consolas",
        "fontsize": "11",
        "shape": "box",
        "style": "rounded,filled",
        "penwidth": "2.0",
    },
    graph_attr={"bgcolor": "#ffffff00"},
    edge_attr={
        "fontname": "Consolas",
        "fontsize": "9",
        "penwidth": "2.0",
        "color": HEX_COLORS["graphite"],
    },
)


def nested_graphviz(
    flow: "Graph",
    max_level: int = sys.maxsize,
    coloring: Literal["nesting", "status"] = "nesting",
    labels: bool = True,
    _dot: Union["graphviz.Digraph", None] = None,
    _level: int = 0,
) -> "graphviz.Digraph":
    """
    Create a graphviz digraph instance from a workflow or graph.

    Parameters
    ----------
    flow
        Workflow to convert
    _dot
        Graphviz dot for recursive internal passing
    _level
        Current nesting level for internal passing

    Returns
    -------
    graphviz.Digraph
        Graphviz object for visualization

    """
    if _dot is None:
        _dot = graphviz.Digraph(flow.name, **GRAPHVIZ_STYLE)
    for name, node in flow.nodes.items():
        # Can't check for graph due to circular imports
        if hasattr(node, "nodes") and _level < max_level:
            # We need to prefix the name with "cluster" to make
            # sure graphviz recognizes it as a subgraph
            with _dot.subgraph(name="cluster-" + name) as subgraph:
                color = COLOR_SEQ[_level] if coloring == "nesting" else HEX_COLORS["platinum"]
                subgraph.attr(label=name)
                subgraph.attr(**GRAPHVIZ_STYLE["node_attr"])
                subgraph.attr(color=color)
                nested_graphviz(
                    cast("Graph", node),
                    max_level=max_level,
                    coloring=coloring,
                    labels=labels,
                    _dot=subgraph,
                    _level=_level + 1,
                )
        else:
            # Because we can have duplicate names in subgraphs, we need to refer
            # to each node by its full path (and construct the edges this way too)
            color = COLOR_SEQ[_level] if coloring == "nesting" else HEX_STATUS_COLORS[node.status]
            unique_name = "-".join(node.component_path)
            _dot.node(unique_name, label=name, color=color)

    for (*out_path, out_port_name), (*inp_path, inp_port_name) in flow.channels:
        root = flow.root
        out = root.get_port(*out_path, out_port_name)
        inp = root.get_port(*inp_path, inp_port_name)
        dtype_label = _pprint_dtype(out.datatype)
        out_name = "-".join(out_path[: max_level + 1])
        inp_name = "-".join(inp_path[: max_level + 1])
        headlabel = (
            inp_port_name.removeprefix("inp_")
            if len(inp.parent.inputs) > 1 and inp_port_name != "inp"
            else None
        )
        taillabel = (
            out_port_name.removeprefix("out_")
            if len(out.parent.outputs) > 1 and out_port_name != "out"
            else None
        )
        _dot.edge(
            out_name,
            inp_name,
            label=dtype_label if labels else None,
            headlabel=headlabel if labels else None,
            taillabel=taillabel if labels else None,
            **GRAPHVIZ_STYLE["edge_attr"],
        )
    return _dot
