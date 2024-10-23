"""
maize
==========
maize is a graph-based workflow manager for computational chemistry pipelines.

"""

# pylint: disable=import-outside-toplevel, unused-import

__version__ = "0.8.5"

import argparse
from contextlib import suppress
import multiprocessing
from pathlib import Path
import sys

# TODO This doesn't work for editable installs for some reason
# (ImportError when attempting to import from maize directly).
from maize.core.workflow import Workflow
from maize.core.component import Component
from maize.utilities.execution import DEFAULT_CONTEXT
from maize.utilities.io import (
    Config,
    create_default_parser,
    get_plugins,
)

# Importing these will trigger all contained nodes to be
# registered and discovered by `Component.get_available_nodes()`
import maize.steps.io
import maize.steps.plumbing


def main() -> None:
    """Main maize execution entrypoint."""

    # Import builtin steps to register them

    parser = create_default_parser()

    # We only add the file arg if we're not just showing available nodes
    if "--list" not in sys.argv:
        parser.add_argument(
            "file",
            type=Path,
            help="Path to a JSON, YAML or TOML input file",
        )

    # extra_options are parameters passed to the graph itself
    args, extra_options = parser.parse_known_args()
    config = Config.from_default()
    if args.config:
        config.update(args.config)

    # Import namespace packages defined in config
    for package in config.packages:
        with suppress(ImportError):
            get_plugins(package)

    if args.list:
        # TODO Would be cool to partition this list into modules etc and get a tree-like overview
        # of nodes, this would require keeping track of the __module__ at component registration
        names = {comp.__name__: comp for comp in Component.get_available_nodes()}
        min_length = max(len(name) for name in names) + 1
        print("The following node types are available:\n")
        print(
            "\n".join(
                f"  {name:<{min_length}} {names[name].get_summary_line():>{min_length}}"
                for name in sorted(names)
            ),
            "\n",
        )
        return

    graph = Workflow.from_file(args.file)

    # We create a separate parser for graph parameters
    extra_parser = argparse.ArgumentParser()
    extra_parser = graph.add_arguments(extra_parser)
    if args.options:
        print("The following workflow parameters are available:")
        extra_parser.print_help()
        return

    graph.update_settings_with_args(args)
    graph.update_with_args(extra_options, extra_parser)

    graph.check()
    if args.check:
        print("Workflow compiled successfully")
        return
    graph.execute()


if __name__ == "__main__":
    multiprocessing.set_start_method(DEFAULT_CONTEXT)
    main()
