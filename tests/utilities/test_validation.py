"""Validation testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name

import pytest
from maize.utilities.execution import CommandRunner
from maize.utilities.validation import FailValidator, SuccessValidator, FileValidator


@pytest.fixture
def echo_command():
    return "echo 'test'"

@pytest.fixture
def process_result(echo_command):
    cmd = CommandRunner()
    return cmd.run_only(echo_command)

@pytest.fixture
def tmp_file_gen(tmp_path):
    return tmp_path / "test.xyz"

@pytest.fixture
def process_result_file(tmp_file_gen):
    cmd = CommandRunner()
    return cmd.run_only(["touch", tmp_file_gen.as_posix()])


class Test_Validator:
    def test_fail_validator(self, process_result):
        val = FailValidator("test")
        assert not val(process_result)
        val = FailValidator(["test", "something"])
        assert not val(process_result)
        val = FailValidator(["something"])
        assert val(process_result)

    def test_success_validator(self, process_result):
        val = SuccessValidator("test")
        assert val(process_result)
        val = SuccessValidator(["test", "something"])
        assert not val(process_result)
        val = SuccessValidator(["something"])
        assert not val(process_result)

    def test_file_validator(self, process_result_file, tmp_file_gen):
        val = FileValidator(tmp_file_gen, zero_byte_check=False)
        assert val(process_result_file)
        val = FileValidator(tmp_file_gen, zero_byte_check=True)
        assert not val(process_result_file)
        val = FileValidator(tmp_file_gen / "fake", zero_byte_check=False)
        assert not val(process_result_file)

