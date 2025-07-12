from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict


class CodeExecutor(ABC):
    """
    Abstract Base Class defining the interface for code execution engines.

    This class provides a contract for different strategies of executing Python code,
    particularly for data transformation tasks. Concrete implementations might execute
    code locally, in a sandboxed environment, or through other mechanisms.

    The primary goal of a CodeExecutor is to:
    1. Accept a description of SDIF data sources.
    2. Establish the necessary database connections and setup based on these sources
       within its specific execution environment.
    3. Execute a given string of Python code to define a specific function.
    4. Call that function with the established database connection and any extra context.
    5. Return the results from the called function.
    """

    @abstractmethod
    def execute(
        self,
        code: str,
        function_name: str,
        sdif_sources: Dict[str, Path],
        extra_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Abstract method to set up a database environment from SDIF sources,
        execute a Python code string to define a function, call that function,
        and return its results.

        Concrete implementations will define how the `sdif_sources` are accessed
        (e.g., direct file access, copying to a sandbox) and how the database
        connection is established and managed within their specific environment.

        Args:
            code:
                A string containing the Python script to be executed. This script
                is expected to define the function identified by `function_name`.
            function_name:
                The name of the function, defined within the `code` string, that
                will be invoked after the `code` string itself has been executed.
            sdif_sources:
                A dictionary mapping desired schema names (strings) to the resolved
                `pathlib.Path` objects of the corresponding SDIF database files.
                The executor is responsible for using these sources to establish
                the necessary SQLite connection(s) and ATTACH operations before
                running the user's code.
            extra_context:
                A dictionary of additional objects and data to be made available
                to the transformation logic. Implementations will decide how this
                context is provided to the executed code.

        Returns:
            A dictionary, which is the result of successfully calling the user-defined
            transformation function (`function_name`). Typically, keys are output
            filenames and values are the data to be written.

        Raises:
            satif_core.exceptions.CodeExecutionError:
                If any error occurs during database setup, code loading, definition,
                or execution of the `code` or the subsequent call to `function_name`.
            NotImplementedError:
                If a concrete subclass does not implement this method.
        """
        pass
