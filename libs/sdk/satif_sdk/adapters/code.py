import inspect
import logging
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from satif_core import CodeExecutor
from satif_core.adapters.base import Adapter
from satif_core.types import SDIFPath
from sdif_db import SDIFDatabase

from satif_sdk.code_executors import LocalCodeExecutor

logger = logging.getLogger(__name__)


class AdapterError(Exception):
    """Custom exception for adapter errors."""

    pass


class CodeAdapter(Adapter):
    """
    Executes custom Python code to adapt data within an SDIF database,
    producing a new, adapted SDIF database file.

    The adaptation logic can be provided as:
    1.  A direct Python callable:
        The function should modify the passed `SDIFDatabase` instance in-place.
        Signatures:
        - `def adapt(db: SDIFDatabase) -> None:`
        - `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> None:`

    2.  A string containing Python code or a Path to a Python script file:
        This code will be executed by a `CodeExecutor`.
        The function identified by `function_name` within the code should be
        prepared to accept specific arguments provided by the executor:
        - EITHER `db: SDIFDatabase` (an instance connected to the database to be adapted)
        - OR `conn: sqlite3.Connection` (a direct connection to that database).
        - Optionally, `context: Dict[str, Any]` if it needs `extra_context`.

        Example Signatures:
        - `def adapt(db: SDIFDatabase) -> Dict[str, Any]:`
        - `def adapt(db: SDIFDatabase, context: Dict[str, Any]) -> Dict[str, Any]:`
        - `def adapt(conn: sqlite3.Connection) -> Dict[str, Any]:`
        - `def adapt(conn: sqlite3.Connection, context: Dict[str, Any]) -> Dict[str, Any]:`

        This function **must return a dictionary** (e.g., `{}`) to comply with the
        `CodeExecutor` interface, though the dictionary content is ignored by `CodeAdapter`.
        If `conn` is used, the database being adapted is also ATTACHed with the schema name "db"
        (e.g., `conn.execute("SELECT * FROM db.my_table")` refers to a table in the main file).
        If `db: SDIFDatabase` is used, methods on the `db` object operate directly on this main database file.


    Args:
        function: The callable, code string, or file path containing the adaptation logic.
        function_name: Name of the function to execute (defaults to "adapt").
                       Used when `function` is a code string or file path.
        code_executor: Optional `CodeExecutor` instance. If `function` is code/file
                       and this is None, a `LocalCodeExecutor` will be used by default.
        extra_context: Optional dictionary of objects to make available.
                       - For direct callables: passed as `context` argument if accepted.
                       - For code via executor: passed to `executor.execute()` and
                         made available in the execution scope and as `context` argument.
        output_suffix: Suffix for the output adapted file (defaults to "_adapted").
        disable_security_warning: If True and a `LocalCodeExecutor` is auto-created,
                                  its security warning for local execution is suppressed.
                                  Defaults to False.
    """

    def __init__(
        self,
        function: Union[Callable, str, Path],
        function_name: str = "adapt",
        code_executor: Optional[CodeExecutor] = None,
        extra_context: Optional[Dict[str, Any]] = None,
        output_suffix: str = "_adapted",
        disable_security_warning: bool = False,
    ):
        self.adapt_function_obj: Optional[Callable] = None
        self.adapt_code: Optional[str] = None
        self.function_name = function_name
        self.code_executor = code_executor
        self.extra_context = extra_context or {}
        self.output_suffix = output_suffix
        self._original_function_input = function
        self.disable_security_warning = disable_security_warning

        self._current_output_path: Optional[Path] = None
        self._init_logic()

    def _init_logic(self):
        """Initialize the adaptation logic based on the input type."""
        function_input = self._original_function_input

        if callable(function_input):
            self.adapt_function_obj = function_input
            if self.function_name == "adapt":
                self.function_name = function_input.__name__
            logger.debug(
                f"Initialized CodeAdapter with direct callable: {self.function_name}"
            )
        elif isinstance(function_input, str):
            self.adapt_code = function_input
            logger.debug(
                f"Initialized CodeAdapter with code string. Target function: '{self.function_name}'"
            )
            if self.code_executor is None:
                logger.info(
                    f"No code_executor provided for CodeAdapter with code string; "
                    f"defaulting to LocalCodeExecutor(disable_security_warning={self.disable_security_warning})."
                )
                self.code_executor = LocalCodeExecutor(
                    disable_security_warning=self.disable_security_warning
                )
        elif isinstance(function_input, Path):
            try:
                with open(function_input, encoding="utf-8") as f:
                    self.adapt_code = f.read()
                logger.debug(
                    f"Initialized CodeAdapter with code from file '{function_input}'. Target function: '{self.function_name}'"
                )
                if self.code_executor is None:
                    logger.info(
                        f"No code_executor provided for CodeAdapter with code file '{function_input}'; "
                        f"defaulting to LocalCodeExecutor(disable_security_warning={self.disable_security_warning})."
                    )
                    self.code_executor = LocalCodeExecutor(
                        disable_security_warning=self.disable_security_warning
                    )
            except Exception as e:
                raise ValueError(
                    f"Failed to read adaptation logic from file '{function_input}': {e}"
                ) from e
        else:
            raise TypeError(
                "Input 'function' for CodeAdapter must be a callable, a string (code), or a Path to a script. "
                f"Got {type(function_input)}."
            )

        if not self.adapt_function_obj and not (self.adapt_code and self.code_executor):
            raise AdapterError(
                "CodeAdapter could not be initialized: No direct callable provided, "
                "and/or code logic with a CodeExecutor could not be set up."
            )

    def adapt(self, sdif: SDIFPath) -> Path:
        """
        Applies the adaptation logic to the input SDIF database file,
        producing a new adapted SDIF file.

        Args:
            sdif: The SDIF data source(s) to adapt. This can be:
                - A single SDIF file path (str or Path).
                - An `SDIFDatabase` instance.

        Returns:
            The path to the newly created adapted SDIF file.

        Raises:
            FileNotFoundError: If the input SDIF file path does not exist.
            AdapterError: If code execution or adaptation logic fails.
        """
        input_path = Path(sdif).resolve()
        if not input_path.exists() or not input_path.is_file():
            raise FileNotFoundError(f"Input SDIF file not found: {input_path}")

        output_filename = f"{input_path.stem}{self.output_suffix}{input_path.suffix}"
        self._current_output_path = (input_path.parent / output_filename).resolve()

        logger.info(
            f"Starting adaptation. Input: {input_path}, Output: {self._current_output_path}"
        )

        try:
            if self._current_output_path.exists():
                logger.warning(
                    f"Output file {self._current_output_path} exists. "
                    f"{'Overwriting due to empty output_suffix.' if not self.output_suffix else 'Overwriting.'}"
                )
                self._current_output_path.unlink()

            shutil.copy2(input_path, self._current_output_path)
            logger.debug(f"Copied {input_path} to {self._current_output_path}")

            if self.adapt_function_obj:
                logger.debug(
                    f"Executing direct callable '{self.function_name}' on {self._current_output_path}"
                )
                with SDIFDatabase(self._current_output_path, read_only=False) as db:
                    sig = inspect.signature(self.adapt_function_obj)
                    params = list(sig.parameters.keys())

                    if params and params[0] == "db":
                        if len(params) == 1:
                            result = self.adapt_function_obj(db=db)
                        elif len(params) >= 2 and params[1] == "context":
                            result = self.adapt_function_obj(
                                db=db, context=self.extra_context
                            )
                        else:
                            raise AdapterError(
                                f"Direct adaptation function '{self.function_name}' with >1 args must have 'context' as the second. Signature: {sig}"
                            )
                    else:
                        raise AdapterError(
                            f"Direct adaptation function '{self.function_name}' must accept 'db' as its first parameter. Signature: {sig}"
                        )

                    if result is not None:
                        logger.warning(
                            f"Direct adaptation function '{self.function_name}' returned a value ({type(result)}). "
                            "It should modify the SDIFDatabase instance in place and return None."
                        )
                logger.info(
                    f"Direct callable '{self.function_name}' executed successfully."
                )

            elif self.adapt_code and self.code_executor:
                logger.debug(
                    f"Executing adaptation code for function '{self.function_name}' via {type(self.code_executor).__name__} on {self._current_output_path}"
                )
                sdif_sources = {"db": self._current_output_path}

                executor_result = self.code_executor.execute(
                    code=self.adapt_code,
                    function_name=self.function_name,
                    sdif_sources=sdif_sources,
                    extra_context=self.extra_context,
                )

                if not isinstance(executor_result, dict):
                    logger.warning(
                        f"Adaptation function '{self.function_name}' via executor returned type {type(executor_result)}, "
                        f"but CodeAdapter expects the executor to ensure a dict (even if empty). Result ignored."
                    )
                logger.info(
                    f"Adaptation code via executor for '{self.function_name}' executed successfully."
                )

            else:
                raise AdapterError(
                    "CodeAdapter is not configured with valid adaptation logic."
                )

        except Exception as e:
            logger.exception(
                f"Error during adaptation process for '{self.function_name}' on {input_path}"
            )
            if self._current_output_path and self._current_output_path.exists():
                try:
                    self._current_output_path.unlink()
                    logger.debug(
                        f"Removed potentially corrupted output file: {self._current_output_path}"
                    )
                except OSError as unlink_err:
                    logger.error(
                        f"Failed to remove corrupted output file {self._current_output_path}: {unlink_err}"
                    )

            if isinstance(
                e,
                (AdapterError, FileNotFoundError, ValueError, TypeError, sqlite3.Error),
            ):
                raise
            raise AdapterError(
                f"Unexpected error during adaptation of '{self.function_name}': {e}"
            ) from e

        final_output_path = self._current_output_path
        self._current_output_path = None

        if not final_output_path or not final_output_path.exists():
            raise AdapterError(
                "Adaptation process completed, but the output file was not found or was removed due to an error."
            )

        return final_output_path
