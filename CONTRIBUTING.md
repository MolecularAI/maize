Contributing
============
This is a summary of code conventions and contributing guidelines for *maize*.

Issues & Pull requests
----------------------
To report bugs or suggest new features, open an issue, or submit a pull request with your changes.

Installation
------------
If you will be working on both *maize* and *maize-contrib* it will be easiest to install the environment for *maize-contrib* first, as it encompasses all dependencies for maize and domain-specific extensions. You can then install both packages using editable installs.

Code style
----------
We use the [PEP8](https://peps.python.org/pep-0008/) convention with a 100 character line length - you can use `black` as a formatter with the `--line-length=100` argument. The code base features full static typing, use the following [mypy](https://mypy.readthedocs.io/en/stable/) command to check it:

```shell
mypy --follow-imports=silent --ignore-missing-imports --strict maize
```

Type hints should only be omitted when either mypy or typing doesn't yet fully support the required feature, such as [higher-kinded types](https://github.com/python/typing/issues/548) or type-tuples ([PEP646](https://peps.python.org/pep-0646/)).

> [!IMPORTANT]
> If you installed maize in editable mode you may need to specify its location with `$MYPYPATH` to ensure `mypy` can find it. See this [setuptools issue](https://github.com/pypa/setuptools/issues/3518).

Documentation
-------------
Every public class or function should feature a full docstring with a full description of parameters / attributes. We follow the [numpy docstring](https://numpydoc.readthedocs.io/en/latest/format.html) standard for readability reasons. Docs are built using [sphinx](https://www.sphinx-doc.org/en/master/) in the `docs` folder:

```shell
make html
```

There will be some warnings from `autosummary` that can be ignored. The built docs can then be found in `docs/_build/html`. To preview them locally you can start a local webserver running the following command in the `docs/_build/html` folder:

```shell
python -m http.server 8000
```

The docs are then available at `http://localhost:8000/index.html`.

If you add a new feature, you should mention the new behaviour in the `userguide`, in the `cookbook`, and ideally add an example under `examples`. If the feature necessitated a deeper change to the fundamental design, you should also update `design`.

Testing
-------
Tests are written using [pytest](https://docs.pytest.org/en/7.2.x/contents.html) and cover the lower-level components as well as higher-level graph execution, and can be run with:

```shell
pytest --log-cli-level=DEBUG tests/
```

Any new features or custom nodes should be covered by suitable tests. To make testing the latter a bit easier, you can use the `maize.utilities.testing.TestRig` class together with `maize.utilities.testing.MockChannel` if required.

Coverage can be reported using:

```shell
pytest tests/ -v --cov maize --cov-report html:coverage
```

New versions
------------
To release a new version of maize, perform the following steps:

1. Create a new branch titled `release-x.x.x`
2. Add your changes to `CHANGELOG.md`
3. Increment `maize.__version__`
4. Commit your changes
5. Rebuild and update the remote documentation (see above)
6. Create a tag using `git tag vx.x.x`
7. Push your changes with `git push` and `git push --tags`
8. Update `master`:
    1. `git checkout master`
    2. `git pull origin master`
    3. `git merge release-x.x.x`
    4. `git push origin master`
