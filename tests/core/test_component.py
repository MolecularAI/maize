"""Component testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

import logging
import pytest

from maize.core.component import Component


@pytest.fixture
def parent_component(tmp_path):
    comp = Component(name="master")
    comp.setup_directories(tmp_path)
    return comp


@pytest.fixture
def component(parent_component):
    comp = Component(name="test", parent=parent_component)
    comp.setup_directories(parent_component.work_dir)
    return comp


@pytest.fixture
def nested_component(component):
    return Component(name="leaf", parent=component)


@pytest.fixture
def nested_component_no_name(component):
    return Component(parent=component)


class Test_Component:
    def test_no_subclass(self):
        with pytest.raises(KeyError):
            Component.get_node_class("nonexistent")

    def test_init(self, parent_component):
        assert parent_component.level == logging.INFO
        assert parent_component.name == "master"

    def test_init_child(self, component, parent_component):
        assert component.parent is parent_component
        assert component.name == "test"
        parent_component.signal.set()
        assert component.signal.is_set()

    def test_root(self, component, parent_component):
        assert component.root is parent_component
        assert parent_component.root is parent_component

    def test_parameter_fail(self, component):
        with pytest.raises(KeyError):
            component.update_parameters(non_existent=42)

    def test_component_path(self, nested_component):
        assert nested_component.component_path == ("test", "leaf",)

    def test_work_dir(self, nested_component, tmp_path):
        nested_component.setup_directories(tmp_path / nested_component.parent.work_dir)
        assert "/comp-master/comp-test/comp-leaf" in nested_component.work_dir.as_posix()
        assert nested_component.work_dir.exists()

    def test_work_dir_auto(self, nested_component_no_name, tmp_path):
        nested_component_no_name.setup_directories(
            tmp_path / nested_component_no_name.parent.work_dir)
        assert "/comp-master/comp-test/comp-" in nested_component_no_name.work_dir.as_posix()
        assert nested_component_no_name.work_dir.exists()

    def test_as_dict(self, nested_component):
        dic = nested_component.as_dict()
        assert dic["name"] == "leaf"
        assert dic["description"] is None
        assert dic["status"] == "READY"
