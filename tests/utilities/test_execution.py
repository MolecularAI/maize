"""Execution testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name

import time

import pytest
from maize.utilities.execution import (
    ProcessError,
    ResourceManagerConfig,
    check_executable,
    _UnmemoizedCommandRunner as CommandRunner,
)
from maize.utilities.validation import FailValidator, SuccessValidator
from maize.utilities.utilities import load_modules, set_environment


@pytest.fixture
def nonexistent_command():
    return "thiscommanddoesntexist"


@pytest.fixture
def echo_command():
    return "echo 'test'"


@pytest.fixture
def sleep_command():
    return "sleep 2"


@pytest.fixture
def slurm_config():
    return ResourceManagerConfig(system="slurm", queue="core", launcher="srun", walltime="00:05:00")


class Test_CommandRunner:
    def test_run_only(self, echo_command):
        cmd = CommandRunner()
        res = cmd.run_only(echo_command, verbose=True)
        assert "test" in res.stdout.decode()

    def test_run_only_fail(self, nonexistent_command):
        with pytest.raises(FileNotFoundError):
            cmd = CommandRunner()
            cmd.run_only(nonexistent_command)

    def test_run_only_fail2(self):
        with pytest.raises(ProcessError):
            cmd = CommandRunner(name="default")
            res = cmd.run_only("cat nonexistent")
        cmd = CommandRunner(raise_on_failure=False)
        res = cmd.run_only("cat nonexistent")
        assert res.returncode == 1

    def test_run_timeout(self, sleep_command):
        cmd = CommandRunner(raise_on_failure=False)
        res = cmd.run_only(sleep_command, timeout=1)
        assert res.returncode == 130

    def test_run_timeout_error(self, sleep_command):
        cmd = CommandRunner()
        with pytest.raises(ProcessError):
            cmd.run_only(sleep_command, timeout=1)

    def test_run_only_input(self):
        cmd = CommandRunner()
        res = cmd.run_only("cat", verbose=True, command_input="foo")
        assert "foo" in res.stdout.decode()

    def test_run_async(self, echo_command):
        cmd = CommandRunner()
        proc = cmd.run_async(echo_command)
        assert "test" in proc.wait().stdout.decode()

    def test_run_async_sleep(self, sleep_command):
        cmd = CommandRunner()
        start = time.time()
        proc = cmd.run_async(sleep_command)
        assert proc.is_alive()
        proc.wait()
        assert not proc.is_alive()
        assert (time.time() - start) < 3

    def test_run_async_sleep_kill(self, sleep_command):
        cmd = CommandRunner()
        start = time.time()
        proc = cmd.run_async(sleep_command)
        assert proc.is_alive()
        proc.kill()
        assert not proc.is_alive()
        assert (time.time() - start) < 2

    def test_run_validate_no_validators(self, echo_command):
        cmd = CommandRunner()
        res = cmd.run(echo_command)
        assert "test" in res.stdout.decode()

    def test_run_validate(self, echo_command):
        cmd = CommandRunner(validators=[SuccessValidator("test")])
        res = cmd.run(echo_command)
        assert "test" in res.stdout.decode()

    def test_run_validate_fail(self, echo_command):
        cmd = CommandRunner(validators=[FailValidator("test")])
        with pytest.raises(ProcessError):
            cmd.run(echo_command)

    def test_run_multi(self, echo_command):
        cmd = CommandRunner()
        results = cmd.run_parallel([echo_command, echo_command])
        for result in results:
            assert result.returncode == 0

    def test_run_multi_batch(self, echo_command):
        cmd = CommandRunner()
        results = cmd.run_parallel([echo_command for _ in range(5)], n_batch=2)
        assert len(results) == 2
        for result in results:
            assert result.returncode == 0

    def test_run_multi_batch2(self, echo_command):
        cmd = CommandRunner()
        results = cmd.run_parallel([echo_command for _ in range(5)], batchsize=2)
        assert len(results) == 3
        for result in results:
            assert result.returncode == 0

    def test_run_multi_batch3(self, echo_command, tmp_path):
        cmd = CommandRunner()
        results = cmd.run_parallel(
            [echo_command for _ in range(5)], working_dirs=[tmp_path for _ in range(5)], batchsize=2
        )
        assert len(results) == 3
        for result in results:
            assert result.returncode == 0

    def test_run_multi_batch_fail(self, echo_command):
        cmd = CommandRunner()
        with pytest.raises(ValueError):
            cmd.run_parallel([echo_command for _ in range(5)], batchsize=2, n_batch=2)

    def test_run_multi_batch_fail2(self, echo_command):
        cmd = CommandRunner()
        with pytest.raises(ValueError):
            cmd.run_parallel(
                [echo_command for _ in range(5)],
                command_inputs=["foo" for _ in range(5)],
                batchsize=2,
            )

    def test_run_multi_batch_fail3(self, echo_command, tmp_path):
        cmd = CommandRunner()
        with pytest.raises(ValueError):
            cmd.run_parallel(
                [echo_command for _ in range(5)],
                working_dirs=[tmp_path, "foo", tmp_path, tmp_path, tmp_path],
                batchsize=2,
            )

    def test_run_multi_input(self):
        cmd = CommandRunner()
        inputs = ["foo", "bar"]
        results = cmd.run_parallel(["cat", "cat"], verbose=True, command_inputs=inputs)
        for inp, res in zip(inputs, results):
            assert inp in res.stdout.decode()

    def test_run_multi_time(self, sleep_command):
        cmd = CommandRunner()
        start = time.time()
        results = cmd.run_parallel([sleep_command, sleep_command], n_jobs=2)
        assert (time.time() - start) < 3
        for result in results:
            assert result.returncode == 0

    def test_run_multi_timeout(self, sleep_command):
        cmd = CommandRunner(raise_on_failure=False)
        start = time.time()
        results = cmd.run_parallel([sleep_command, sleep_command], n_jobs=2, timeout=1)
        assert (time.time() - start) < 2.5
        for result in results:
            assert result.returncode == 130

    def test_run_multi_timeout_error(self, sleep_command):
        cmd = CommandRunner()
        with pytest.raises(ProcessError):
            cmd.run_parallel([sleep_command, sleep_command], n_jobs=2, timeout=1)

    def test_run_multi_seq(self, sleep_command):
        cmd = CommandRunner()
        start = time.time()
        results = cmd.run_parallel([sleep_command, sleep_command], n_jobs=1)
        assert (time.time() - start) > 3
        for result in results:
            assert result.returncode == 0

    def test_run_multi_validate(self, echo_command):
        cmd = CommandRunner(validators=[SuccessValidator("test")])
        results = cmd.run_parallel([echo_command, echo_command], validate=True)
        for res in results:
            assert "test" in res.stdout.decode()

    def test_run_preexec(self):
        cmd = CommandRunner()
        res = cmd.run("env", pre_execution="export BLAH=foo")
        assert "BLAH=foo" in res.stdout.decode()


class Test_check_executable:
    def test_fail(self, nonexistent_command):
        assert not check_executable(nonexistent_command)

    def test_success(self):
        command = "ls"
        assert check_executable(command)


@pytest.mark.skipif(
    not check_executable("sinfo"), reason="Testing slurm requires a functioning Slurm batch system"
)
class Test_batch:
    def test_run_only(self, echo_command, slurm_config):
        cmd = CommandRunner(prefer_batch=True, rm_config=slurm_config)
        res = cmd.run_only(echo_command, verbose=True)
        assert "test" in res.stdout.decode()

    def test_run_only_timeformat(self, echo_command, slurm_config):
        slurm_config.walltime = "02-05:00"
        cmd = CommandRunner(prefer_batch=True, rm_config=slurm_config)
        res = cmd.run_only(echo_command, verbose=True)
        assert "test" in res.stdout.decode()

    def test_run_only_fail(self, nonexistent_command, slurm_config):
        with pytest.raises((FileNotFoundError, ProcessError)):
            cmd = CommandRunner(prefer_batch=True, rm_config=slurm_config)
            cmd.run_only(nonexistent_command)

    def test_run_modules(self, slurm_config):
        load_modules("GCC")
        cmd = CommandRunner(prefer_batch=True, rm_config=slurm_config)
        res = cmd.run_only("gcc --version", verbose=True)
        assert "(GCC)" in res.stdout.decode()

    def test_run_env(self, slurm_config):
        set_environment({"MAIZE_TEST": "MAIZE_TEST_SUCCESS"})
        cmd = CommandRunner(prefer_batch=True, rm_config=slurm_config)
        res = cmd.run_only("env", verbose=True)
        assert "MAIZE_TEST_SUCCESS" in res.stdout.decode()

    def test_run_validate(self, echo_command, slurm_config):
        cmd = CommandRunner(
            validators=[SuccessValidator("test")], prefer_batch=True, rm_config=slurm_config
        )
        res = cmd.run(echo_command)
        assert "test" in res.stdout.decode()

    def test_run_multi(self, echo_command, slurm_config):
        cmd = CommandRunner(prefer_batch=True, rm_config=slurm_config)
        results = cmd.run_parallel([echo_command, echo_command])
        for result in results:
            assert result.returncode == 0

    def test_run_multi_many(self, echo_command, slurm_config):
        cmd = CommandRunner(prefer_batch=True, rm_config=slurm_config)
        results = cmd.run_parallel([echo_command for _ in range(50)])
        for result in results:
            assert result.returncode == 0
