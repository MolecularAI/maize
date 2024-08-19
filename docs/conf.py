# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# pylint: disable=all

import sys
import time
import os

sys.path.insert(0, os.path.abspath(".."))
import maize.maize

project = "maize"
copyright = f"{time.localtime().tm_year}, Molecular AI group"
author = "Thomas Löhr"
release = version = maize.maize.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.graphviz",
]

autosummary_generate = True
add_module_names = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "maize-contrib": ("https://molecularai.github.io/maize-contrib/doc", None),
}


# These handlers ensure that the value for `required_callables` is always emitted
def process_required_callables_sig(app, what, name, obj, options, signature, return_annotation):
    if what == "attribute" and (
        name.endswith("required_callables") or name.endswith("required_packages")
    ):
        options["no-value"] = False
    else:
        options["no-value"] = True


def include_interfaces(app, what, name, obj, skip, options):
    if what == "attribute" and (
        name.endswith("required_callables") or name.endswith("required_packages")
    ):
        return False
    return None


def setup(app):
    app.connect("autodoc-process-signature", process_required_callables_sig)
    app.connect("autodoc-skip-member", include_interfaces)


# -- AZ Colors
_COLORS = {
    "mulberry": "rgb(131,0,81)",
    "lime-green": "rgb(196,214,0)",
    "navy": "rgb(0,56,101)",
    "graphite": "rgb(63,68,68)",
    "light-blue": "rgb(104,210,223)",
    "magenta": "rgb(208,0,111)",
    "purple": "rgb(60,16,83)",
    "gold": "rgb(240,171,0)",
    "platinum": "rgb(157,176,172)",
}

graphviz_output_format = "svg"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_title = "maize"
html_logo = "resources/maize-logo.svg"
html_theme = "furo"
html_theme_options = {
    "sidebar_hide_name": True,
    "light_css_variables": {
        "color-brand-primary": _COLORS["mulberry"],
        "color-brand-content": _COLORS["mulberry"],
        "color-api-name": _COLORS["navy"],
        "color-api-pre-name": _COLORS["navy"],
    },
}
html_static_path = ["_static"]
