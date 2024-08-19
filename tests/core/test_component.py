"""Component testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

import logging
import pytest

from maize.core.component import Component
from maize.core.interface import Input, Output, Parameter
from maize.core.node import Node


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


@pytest.fixture
def example_comp_class():
    class Example(Node):
        """Example docstring"""
        required_callables = ["blah"]
        inp: Input[int] = Input()
        out: Output[int] = Output()
        para: Parameter[int] = Parameter()
    return Example


class Test_Component:
    def test_no_subclass(self):
        with pytest.raises(KeyError):
            Component.get_node_class("nonexistent")

    def test_sample_config(self, example_comp_class):
        example_comp_class._generate_sample_config(name="foo")
        assert "Example configuration" in example_comp_class.__doc__

    def test_serialized_summary(self, example_comp_class):
        data = example_comp_class.serialized_summary()
        assert data["name"] == "Example"
        assert data["inputs"][0]["name"] == "inp"
        assert data["outputs"][0]["name"] == "out"
        assert data["parameters"][0]["name"] == "para"

    def test_summary_line(self, example_comp_class):
        assert "Example docstring" in example_comp_class.get_summary_line()

    def test_get_interfaces(self, example_comp_class):
        assert {"inp", "out", "para"} == example_comp_class.get_interfaces()

    def test_get_inputs(self, example_comp_class):
        assert "inp" in example_comp_class.get_inputs()

    def test_get_outputs(self, example_comp_class):
        assert "out" in example_comp_class.get_outputs()

    def test_get_parameters(self, example_comp_class):
        assert "para" in example_comp_class.get_parameters()

    def test_get_available(self, example_comp_class):
        assert example_comp_class in Component.get_available_nodes()

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

    def test_work_dir_default(self, nested_component, temp_working_dir):
        nested_component.setup_directories()
        assert "comp-leaf" in nested_component.work_dir.as_posix()
        assert nested_component.work_dir.exists()

    def test_as_dict(self, nested_component):
        dic = nested_component.as_dict()
        assert dic["name"] == "leaf"
        assert "description" not in dic
        assert "status" not in dic
