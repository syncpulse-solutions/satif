import csv
import inspect
import io
import json
import logging
import os
import re
import sqlite3
import unicodedata
import uuid
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from satif_core import CodeExecutor
from satif_core.exceptions import CodeExecutionError
from sdif_db import cleanup_db_connection, create_db_connection
from sdif_db.database import SDIFDatabase

logger = logging.getLogger(__name__)


class LocalCodeExecutor(CodeExecutor):
    """
    Executes user-provided Python code strings locally using Python's built-in `exec`.

    This executor is responsible for:
    1. Setting up an SQLite database environment based on provided SDIF source file paths.
       This includes creating an in-memory database (if multiple sources) or connecting
       to a single source, and then ATTACHing all specified SDIF files as schemas.
    2. Executing a given `code` string in an environment where this database connection
       (or an SDIFDatabase wrapper) is accessible, along with other standard libraries
       and provided `extra_context`.
    3. Identifying a specific function within the executed `code` by its `function_name`.
    4. Calling this identified function, passing it the live SQLite connection (as `conn`)
       or an SDIFDatabase instance (as `db`), and context.
    5. Returning the result produced by the called function.
    6. Ensuring the database connection is properly closed and resources are cleaned up.

    **Security Warning:**
    This executor runs arbitrary Python code directly on the host machine where it is instantiated.
    It provides **NO SANDBOXING OR SECURITY ISOLATION**. Therefore, it should **ONLY** be used
    in trusted environments and with code from trusted sources.
    """

    _DEFAULT_INITIAL_CONTEXT: Dict[str, Any] = {
        "pd": pd,
        "json": json,
        "Path": Path,
        "sqlite3": sqlite3,
        "datetime": datetime,
        "timedelta": timedelta,
        "re": re,
        "uuid": uuid,
        "os": os,
        "io": io,
        "BytesIO": BytesIO,
        "csv": csv,
        "np": np,
        "unicodedata": unicodedata,
        "SDIFDatabase": SDIFDatabase,
    }

    def __init__(
        self,
        initial_context: Optional[Dict[str, Any]] = None,
        disable_security_warning: bool = False,
    ):
        """
        Initializes the LocalCodeExecutor.

        Args:
            initial_context:
                An optional dictionary of global variables to make available
                during code execution. These will be merged with (and can
                override) the default set of globals provided by the executor.
            disable_security_warning: If True, suppresses the security warning log.
        """
        self._resolved_initial_globals = self._DEFAULT_INITIAL_CONTEXT.copy()
        if initial_context:
            self._resolved_initial_globals.update(initial_context)
        self.disable_security_warning = disable_security_warning

    def execute(
        self,
        code: str,
        function_name: str,
        sdif_sources: Dict[str, Path],
        extra_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Sets up a database, executes the code string to define a function,
        then calls that function with the database connection (as `conn` or `db`) and context.

        Args:
            code:
                A string containing the Python script to be executed. This script
                is expected to define the function identified by `function_name`.
                It can include imports, helper functions, and class definitions
                as needed for the main transformation function.
            function_name:
                The name of the function (defined in `code`) to be invoked.
            sdif_sources:
                A dictionary mapping schema names (str) to resolved `Path` objects
                of the SDIF database files. This executor will create/connect to
                an SQLite database and ATTACH these sources.
            extra_context:
                A dictionary of additional objects and data to be made available
                to the transformation logic.
                - The entire `extra_context` dictionary is passed as the `context`
                  argument to the transformation function if its signature includes it.
                - Additionally, all key-value pairs in `extra_context` are injected
                  as global variables into the environment where the `code` string
                  is initially executed. If `extra_context` contains keys that
                  match standard globals (e.g., 'pd', 'json') or the explicitly
                  provided 'conn' or 'context' globals, they will be overwritten
                  in that global scope.

        Returns:
            A dictionary, which is the result of calling the user-defined
            transformation function (`function_name`). The keys are typically
            output filenames, and values are the data to be written.

        Raises:
            CodeExecutionError: If any error occurs during the process, including:
                - Database setup errors from `db_utils`.
                - Syntax errors in the `code` string.
                - The specified `function_name` not being found after executing `code`.
                - The identified `function_name` not being a callable function.
                - The function having an incompatible signature (e.g., not accepting `conn`).
                - The function not returning a dictionary.
                - Any exception raised during the execution of the user's transformation function.
        """
        db_conn: Optional[sqlite3.Connection] = None
        db_instance: Optional[SDIFDatabase] = None
        attached_schemas: Dict[str, Path] = {}

        try:
            execution_globals = {
                **self._resolved_initial_globals,
                "context": extra_context,
                "__builtins__": __builtins__,
                **extra_context,
            }

            if not self.disable_security_warning:
                logger.warning(
                    f"Executing user-provided code locally to define and run function '{function_name}'. "
                    "This is insecure and should only be used in trusted environments."
                )

            compiled_code = compile(code, "<code_string>", "exec")
            exec(compiled_code, execution_globals, execution_globals)

            if function_name not in execution_globals:
                raise CodeExecutionError(
                    f"Function '{function_name}' not found after executing code string."
                )

            transform_func = execution_globals[function_name]
            if not callable(transform_func):
                raise CodeExecutionError(
                    f"'{function_name}' defined in code is not a callable function."
                )

            sig = inspect.signature(transform_func)
            param_names = list(sig.parameters.keys())
            func_args: Dict[str, Any] = {}

            if "db" in param_names:
                if len(sdif_sources) != 1:
                    raise CodeExecutionError(
                        "LocalCodeExecutor: 'db' parameter requires exactly one SDIF source file."
                    )
                db_path = list(sdif_sources.values())[0]
                if not isinstance(db_path, Path):
                    db_path = Path(db_path)

                db_instance = SDIFDatabase(db_path, read_only=False)
                func_args["db"] = db_instance
            else:
                # Default to connection-based setup if 'db' parameter is not present.
                # This will also be the path if function takes no db/conn params (e.g. only context, or no params).
                db_conn, attached_schemas = create_db_connection(sdif_sources)
                if "conn" in param_names:
                    func_args["conn"] = db_conn

            if "context" in param_names:
                func_args["context"] = extra_context

            has_db_or_conn_in_func_args = "db" in func_args or "conn" in func_args

            if param_names:
                first_param_name = param_names[0]
                first_param_obj = sig.parameters[first_param_name]
                is_first_param_db_or_conn_type = first_param_name in ("db", "conn")

                if first_param_obj.default == inspect.Parameter.empty:
                    if not is_first_param_db_or_conn_type:
                        raise CodeExecutionError(
                            f"Transformation function '{function_name}'s first required parameter must be 'db' or 'conn' if other arguments are expected. Got '{first_param_name}'. Signature: {sig}"
                        )
                    elif not has_db_or_conn_in_func_args:
                        logger.error(
                            f"Internal error: First param {first_param_name} required but not prepared."
                        )
                        raise CodeExecutionError(
                            f"Transformation function '{function_name}' expected '{first_param_name}' but it was not prepared by the executor. Signature: {sig}"
                        )
            for p_name, p_obj in sig.parameters.items():
                if p_obj.default == inspect.Parameter.empty and p_name not in func_args:
                    raise CodeExecutionError(
                        f"Transformation function '{function_name}' is missing required argument '{p_name}'. Executor provides 'db' or 'conn', and 'context'. Signature: {sig}"
                    )

            func_result = transform_func(**func_args)

            if not isinstance(func_result, dict):
                raise CodeExecutionError(
                    f"Function '{function_name}' must return a Dict. Got {type(func_result)}."
                )

            return {str(k): v for k, v in func_result.items()}

        except CodeExecutionError:
            raise
        except Exception as e:
            logger.exception(
                f"Error during local execution for function '{function_name}' on source {sdif_sources}"
            )
            raise CodeExecutionError(
                f"An error occurred in LocalCodeExecutor for function '{function_name}': {e}"
            ) from e
        finally:
            if db_instance:
                try:
                    db_instance.close()
                except Exception as e:
                    logger.error(
                        f"Error closing user SDIFDatabase instance for {function_name}: {e}"
                    )
            if db_conn:
                cleanup_db_connection(db_conn, attached_schemas, should_close=True)
