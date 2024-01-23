Quickstart
==========

Installation
------------
If you plan on using `maize-contrib <https://github.com/MolecularAI/maize-contrib>`_, then you should just follow the `installation instructions <https://molecularai.github.io/maize-contrib/doc/index.html#installation>`_ for the latter. Maize will be installed automatically as a dependency.

Note that `maize-contrib <https://github.com/MolecularAI/maize-contrib>`_ requires several additional domain-specific packages, and you should use its own environment file instead if you plan on using these extensions.

To get started quickly with running maize, you can install from an environment file:

.. code-block:: bash

   conda env create -f env-users.yml

If you want to develop the code or run the tests, use the development environment and install the package in editable mode:

.. code-block:: bash

   conda env create -f env-dev.yml
   conda activate maize-dev
   pip install --no-deps -e ./

.. caution::
   If you want to develop both maize and *maize-contrib* you may need to install both using legacy editable packages by adding ``--config-settings editable_mode=compat`` as arguments to ``pip``, as not doing so will stop tools like ``pylint`` and ``pylance`` from finding the imports. See this `setuptools issue <https://github.com/pypa/setuptools/issues/3518>`_.

Manual install
^^^^^^^^^^^^^^
Maize requires the following packages and also depends on python 3.10:

* dill
* networkx
* pyyaml
* toml
* numpy
* matplotlib
* graphviz
* beartype
* psij-python

We also strongly recommend the installation of `mypy <https://mypy.readthedocs.io/en/stable/>`_. To install everything use the following command:

.. code-block:: bash

   conda install -c conda-forge python=3.10 dill networkx yaml toml mypy

If you wish to develop or add additional modules, the following additional packages will be required:

* pytest
* sphinx
