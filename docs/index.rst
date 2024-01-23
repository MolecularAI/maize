.. maize documentation master file, created by
   sphinx-quickstart on Thu Jan 12 09:54:03 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

maize
=====
This is the documentation for *maize* |version|.

*maize* is a graph-based workflow manager for computational chemistry pipelines. It is based on the principles of `flow-based programming <https://github.com/flowbased/flowbased.org/wiki>`_ and thus allows arbitrary graph topologies, including cycles, to be executed. Each task in the workflow (referred to as *nodes*) is run as a separate process and interacts with other nodes in the graph by communicating through unidirectional *channels*, connected to *ports* on each node. Every node can have an arbitrary number of input or output ports, and can read from them at any time, any number of times. This allows complex task dependencies and cycles to be modelled effectively.

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Guide

   docs/quickstart
   docs/userguide
   docs/cookbook
   docs/examples
   docs/steps/index
   docs/glossary

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Development

   docs/roadmap
   docs/design
   docs/development
   docs/reference

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: External

   Steps <https://molecularai.github.io/maize-contrib/doc/steps/index.html>
   Utilities <https://molecularai.github.io/maize-contrib/doc/utilities.html>

Installation
------------

If you plan on using `maize-contrib <https://github.com/MolecularAI/maize-contrib>`_, then you should just follow the installation instructions for the latter. Maize will be installed automatically as a dependency.

To get started quickly with running maize, you can install from an environment file:

.. code-block:: bash

   conda env create -f env-users.yml

Teaser
------
A taste for defining and running workflows with *maize*.

.. literalinclude:: ../examples/helloworld.py
   :language: python
   :linenos:
       

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`