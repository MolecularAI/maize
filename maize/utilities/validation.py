"""Defines some simple process run validators."""


from abc import abstractmethod
from pathlib import Path
import subprocess
from maize.utilities.io import wait_for_file

from maize.utilities.utilities import make_list


class Validator:
    """
    Validate if a command has run successfully or not.

    Calling an instance with a ``subprocess.CompletedProcess`` object
    should return boolean indicating the success of the command.

    """

    @abstractmethod
    def __call__(self, result: subprocess.CompletedProcess[bytes]) -> bool:
        """
        Calls the validator with the instantiated search string on a command result.

        Parameters
        ----------
        result
            Object containing `stdout` and `stderr` of the completed command

        Returns
        -------
        bool
            ``True`` if the command succeeded, ``False`` otherwise

        """


class OutputValidator(Validator):
    """
    Validate the STDOUT / STDERR of an external command.

    Calling an instance with a ``subprocess.CompletedProcess`` object
    should return boolean indicating the success of the command.

    Parameters
    ----------
    expect
        String or list of strings to look for in the output

    """

    def __init__(self, expect: str | list[str]) -> None:
        self.expect = make_list(expect)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(self.expect)})"

    @abstractmethod
    def __call__(self, result: subprocess.CompletedProcess[bytes]) -> bool:
        pass


class FailValidator(OutputValidator):
    """
    Check if an external command has failed by searching for strings in the output.

    Parameters
    ----------
    expect
        String or list of strings to look for in the
        output, any match will indicate a failure.

    """

    def __call__(self, result: subprocess.CompletedProcess[bytes]) -> bool:
        for exp in self.expect:
            if (result.stderr is not None and exp in result.stderr.decode(errors="ignore")) or (
                exp in result.stdout.decode(errors="ignore")
            ):
                return False
        return True


class SuccessValidator(OutputValidator):
    """
    Check if an external command has succeeded by searching for strings in the output.

    Parameters
    ----------
    expect
        If all strings specified here are found, the command was successful

    """

    def __call__(self, result: subprocess.CompletedProcess[bytes]) -> bool:
        for exp in self.expect:
            if not (
                (result.stderr is not None and exp in result.stderr.decode(errors="ignore"))
                or (exp in result.stdout.decode(errors="ignore"))
            ):
                return False
        return True


class FileValidator(Validator):
    """
    Check if an external command has succeeded by searching for one or more generated files.

    Parameters
    ----------
    expect
        If all files specified here are found, the command was successful
    zero_byte_check
        Whether to check if the generated file is empty
    timeout
        Will wait ``timeout`` seconds for the file to appear

    """

    def __init__(
        self, expect: Path | list[Path], zero_byte_check: bool = True, timeout: int = 5
    ) -> None:
        self.expect = make_list(expect)
        self.zero_byte_check = zero_byte_check
        self.timeout = timeout

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({', '.join(p.as_posix() for p in self.expect)}, "
            f"zero_byte_check={self.zero_byte_check})"
        )

    def __call__(self, _: subprocess.CompletedProcess[bytes]) -> bool:
        for exp in self.expect:
            try:
                wait_for_file(exp, timeout=self.timeout, zero_byte_check=self.zero_byte_check)
            except TimeoutError:
                return False
        return True


class ContentValidator(Validator):
    """
    Check if an external command has succeeded by searching
    contents in one or more generated files. Empty files fail.

    Parameters
    ----------
    expect
        dictionary of file paths and list of strings to check for in each file
    timeout
        Will wait ``timeout`` seconds for the file to appear
    """

    def __init__(self, expect: dict[Path, list[str]], timeout: int = 5) -> None:
        self.expect = expect
        self.timeout = timeout

    def __repr__(self) -> str:
        arg_descr = ", ".join(
            f"{p.as_posix()}:{','.join(s for s in self.expect[p])}" for p in self.expect
        )
        return f"{self.__class__.__name__}({arg_descr})"

    def __call__(self, _: subprocess.CompletedProcess[bytes]) -> bool:
        for file_path, expected in self.expect.items():
            try:
                wait_for_file(file_path, timeout=self.timeout, zero_byte_check=True)
                with file_path.open() as f:
                    file_contents = f.read()
                    for exp_string in expected:
                        if exp_string and exp_string not in file_contents:
                            return False
            except TimeoutError:
                return False
        return True
