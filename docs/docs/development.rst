Development
===========
This is a summary of code conventions used in *maize*.

Installation
------------
If you will be working on both *maize* and *maize-contrib* it will be easiest to install the environment for *maize-contrib* first, as it encompasses all dependencies for maize and domain-specific extensions. You can then install both packages using editable installs.

Code style
----------
We use the :pep:`8` convention with a 100 character line length - you can use ``black`` as a formatter with the ``--line-length=100`` argument. The code base features full static typing, use the following `mypy <https://mypy.readthedocs.io/en/stable/>`_ command to check it:

.. code-block:: shell

   mypy --explicit-package-bases --strict maize/steps maize/core maize/utilities

You may need to install missing types using :code:`mypy --install-types`. Type hints should only be omitted when either mypy or typing doesn't yet fully support the required feature, such as `higher-kinded types <https://github.com/python/typing/issues/548>`_ or type-tuples (:pep:`646`).

.. caution::
   If you installed maize in editable mode you may need to specify its location with ``MYPYPATH`` to ensure ``mypy`` can find it. See this `setuptools issue <https://github.com/pypa/setuptools/issues/3518>`_.

Documentation
-------------
Every public class or function should feature a full docstring with a full description of parameters / attributes. We follow the `numpy docstring <https://numpydoc.readthedocs.io/en/latest/format.html>`_ standard for readability reasons. Docs are built using `sphinx <https://www.sphinx-doc.org/en/master/>`_ in the ``docs`` folder:

.. code-block:: shell
   
   cd docs
   sphinx-build -b html ./ _build/html

There will be some warnings from ``autosummary`` that can be ignored. The built docs can then be found in ``docs/_build/html``. To preview them locally you can start a local webserver running the following command in the ``docs/_build/html`` folder:

.. code-block:: shell

   python -m http.server 8000

The docs are then available at `<http://localhost:8000/index.html>`_.

If you add a new feature, you should mention the new behaviour in the :doc:`userguide`, in the :doc:`cookbook`, and ideally add an example under :doc:`examples`. If the feature necessitated a deeper change to the fundamental design, you should also update :doc:`design`.

Testing
-------
Tests are written using `pytest <https://docs.pytest.org/en/7.2.x/contents.html>`_ and cover the lower-level components as well as higher-level graph execution, and can be run with:

.. code-block:: shell

   pytest --log-cli-level=DEBUG tests/

Any new features or custom nodes should be covered by suitable tests. To make testing the latter a bit easier, you can use the :class:`~maize.utilities.testing.TestRig` class together with :class:`~maize.utilities.testing.MockChannel` if required.

Coverage can be reported using:

.. code-block:: shell

   pytest tests/ -v --cov maize --cov-report html:coverage

New versions
------------
To release a new version of maize, perform the following steps:

1. Create a new branch titled ``release-x.x.x``
2. Add your changes to ``CHANGELOG.md``
3. Increment :attr:`maize.__version__`
4. Commit your changes
5. Rebuild and update the remote documentation (see above)
6. Create a tag using :code:`git tag vx.x.x`
7. Push your changes with :code:`git push` and :code:`git push --tags`
8. Update ``master``:

   1. :code:`git checkout master`
   2. :code:`git pull origin master`
   3. :code:`git merge release-x.x.x`
   4. :code:`git push origin master`

9. Create a wheel for bundling with *maize-contrib* or other dependent repositories:

   :code:`pip wheel --no-deps .`
