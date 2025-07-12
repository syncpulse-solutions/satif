import asyncio
import contextvars
import csv
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import clevercsv
from agents import Agent, Runner, function_tool
from agents.mcp.server import MCPServerStdio
from charset_normalizer import detect
from mcp import ClientSession
from satif_core import AsyncStandardizer
from satif_core.types import Datasource, SDIFPath, StandardizationResult
from satif_sdk.standardizers.csv import (
    CSVStandardizer,
)
from satif_sdk.utils import (
    DELIMITER_SAMPLE_SIZE,
    ENCODING_SAMPLE_SIZE,
)

logger = logging.getLogger(__name__)


# TODO: maybe we want more analysis tools:
# get empty rows or same values rows.


# --- Agent Prompt Definition ---
AI_CSV_PROMPT = """
You are an expert CSV Data Standardization Agent. Your mission is to analyze a given CSV file and determine all necessary parameters and metadata so it can be correctly standardized into a well-structured SDIF table using the underlying CSVStandardizer.

**CSV File Path:** {file_path}
**Initial Guesses (Hints for you to verify or correct):**
- Encoding: {initial_encoding}
- Delimiter: '{initial_delimiter}'

**Your Task:**

1.  **Core Parsing Parameters:**
    *   Determine the correct file `encoding` (string, e.g., "utf-8", "latin-1").
    *   Determine the correct `delimiter` (string, e.g., ",", ";", "\\t").
    *   Determine if a `has_header` row exists (boolean: true/false).
    *   Determine `skip_rows` (integer for initial N rows OR a list of 0-based specific row indices to skip, e.g., metadata blocks, comments, empty lines, repeated headers). Ensure the list is sorted and contains unique, non-negative integers.

2.  **Table Definition:**
    *   Generate a concise, descriptive, and SQL-safe `table_name` for the data in this CSV (string, snake_case preferred, e.g., "customer_orders_2023_q4"). This name will be sanitized by the system, but try to make it good.
    *   Optionally, generate a `table_description` (string) providing a brief semantic overview of what the entire table represents, especially if the `table_name` isn't fully self-explanatory. (e.g., "Contains quarterly sales data for all product lines."). Only provide if it adds clear value.

3.  **Column Analysis and Definition:**
    *   For **each column** you identify that should be included in the final table:
        *   `original_identifier` (string): This is how the column is found in the *raw CSV data*.
            *   If `has_header` is true, this is the **exact original header name** from the CSV.
            *   If `has_header` is false, this is a **string representation of the 0-based column index** (e.g., "0", "1", "2").
        *   `final_column_name` (string): This is the desired name for the column in the SDIF database table. It **MUST** be:
            *   Clean and descriptive.
            *   Sanitized by you (snake_case, lowercase, no special characters besides underscore, no spaces). The system will also sanitize it, but aim for a good one.
            *   Potentially an improved/clarified version of the original header (e.g., fixing typos, expanding abbreviations).
        *   `description` (string, OPTIONAL): A concise semantic description of what the data in this specific column represents.
            *   **Provide this ONLY if the `final_column_name` is not entirely self-explanatory or if the column's content is ambiguous.**
            *   Focus on clarity and a human-understandable meaning. (e.g., for a column `order_total_usd`, a description might be "Total amount of the order in US Dollars, including taxes but excluding discounts.")
            *   If the `final_column_name` is very clear (e.g., `customer_email_address`), a separate description is likely NOT needed. Omit the field or set to null.

4.  **Final Output:**
    *   Respond ONLY with a single JSON object containing all the determined parameters and metadata.
    *   The JSON object MUST adhere strictly to the following structure:

    ```json
    {{
        "table_name": "...",
        "table_description": null, // Or string value. Null or omit if not generated.
        "encoding": "...",
        "delimiter": "...",
        "has_header": true/false,
        "skip_rows": 0, // Integer for initial N, or sorted list of 0-based indices e.g. [0, 1, 5]
        "columns": [
            {{
                "original_identifier": "original_header_or_index_string",
                "final_column_name": "sanitized_snake_case_name",
                "description": null // Or string value. Null or omit if not generated.
            }}
            // ... more column objects
        ]
    }}
    ```

**Tools Available:**
- `read_csv_sample(encoding: str, delimiter: str, skip_initial_rows: int = 0, row_limit: int = 20, include_row_indices: bool = False)`: Reads a sample from the *beginning* of the file. Crucial for header and initial structure.
- `read_raw_lines(encoding: str, line_limit: int = 50, start_line: int = 0)`: Reads raw lines. Useful for finding specific rows to skip (empty, repeated headers, footers) by their 0-based index.

**General Workflow Guidance:**
1.  **Initial Probe & Core Params:** Use `read_csv_sample` with initial hints (and `include_row_indices=True`) to examine the first few rows. Verify/correct `encoding` and `delimiter`. If `read_csv_sample` reports errors or shows garbled data. Determine `has_header` by looking at the first non-skipped row.
2.  **Identify Skip Rows:**
    *   If there's metadata/comments at the top, determine how many initial rows to skip and use that for `skip_rows` (integer value).
    *   Use `read_raw_lines` to scan for other rows to skip (e.g., empty lines, comment lines, repeated headers mid-file, summary footers). Collect all 0-based indices of such rows. If you have specific indices, `skip_rows` should be a sorted list of these indices. If you only skip initial N rows, it's an integer.
3.  **Column Identification & Definition:**
    *   After settling `skip_rows` and `has_header`, call `read_csv_sample` again with `skip_initial_rows` set appropriately (if `skip_rows` is an int) to see the clean data rows and the header (if present).
    *   If `has_header` is true, the first row from this clean sample gives you the `original_identifier` values (original header names).
    *   If `has_header` is false, the `original_identifier` for each column will be its 0-based index as a string (e.g., "0", "1", "2", ... for as many columns as you see in the first data row).
    *   For each column you decide to include:
        *   Determine its `original_identifier`.
        *   Create a clean, descriptive `final_column_name` (snake_case).
        *   If (and ONLY IF) necessary, write a `description` for that column.
4.  **Table Naming & Description:** Based on the clean data and column names, formulate a `table_name` and, if valuable, a `table_description`.
5.  **Construct Final JSON:** Ensure your output is ONLY the JSON object, perfectly matching the specified schema. Pay close attention to the format of `skip_rows` and how optional fields (`table_description`, column `description`) are handled (either omit the key or set its value to `null`).
"""


# --- Tool Context Manager ---
class AIStandardizerToolContext:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._original_context = None

    def __enter__(self):
        global _CURRENT_AI_CSV_TOOL_CONTEXT
        self._original_context = _CURRENT_AI_CSV_TOOL_CONTEXT.set(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _CURRENT_AI_CSV_TOOL_CONTEXT
        if self._original_context is not None:
            _CURRENT_AI_CSV_TOOL_CONTEXT.reset(self._original_context)
        self._original_context = None


_CURRENT_AI_CSV_TOOL_CONTEXT: contextvars.ContextVar[
    Optional[AIStandardizerToolContext]
] = contextvars.ContextVar("current_ai_csv_tool_context", default=None)


# --- Tool Implementations (Assumed to be largely the same, ensure JSON output is robust) ---
@function_tool
async def read_csv_sample(
    encoding: str,
    delimiter: str,
    skip_initial_rows: int | None,  # Made optional to match agent's potential calls
    row_limit: int | None,
    include_row_indices: bool | None,
) -> str:
    if skip_initial_rows is None:
        skip_initial_rows = 0
    if row_limit is None:
        row_limit = 20
    if include_row_indices is None:
        include_row_indices = False

    context = _CURRENT_AI_CSV_TOOL_CONTEXT.get()
    if not context or not context.file_path or not context.file_path.exists():
        return json.dumps({"error": "File path not found in tool context."})

    rows = []
    error_message = None
    processed_row_count = 0
    actual_skipped_count = 0
    try:
        with open(context.file_path, encoding=encoding, newline="") as f:
            for i in range(skip_initial_rows):
                try:
                    next(f)
                    actual_skipped_count += 1
                except StopIteration:
                    error_message = f"EOF reached while skipping initial {skip_initial_rows} rows (skipped {actual_skipped_count})."
                    break
            if error_message:
                return json.dumps(
                    {"error": error_message, "rows": [], "processed_row_count": 0}
                )

            reader = csv.reader(f, delimiter=delimiter)
            current_read_index = actual_skipped_count
            for i, row_fields in enumerate(reader):
                if i >= row_limit:
                    break
                processed_row_count += 1
                if include_row_indices:
                    rows.append([current_read_index] + row_fields)
                else:
                    rows.append(row_fields)
                current_read_index += 1
        return json.dumps(
            {"rows": rows, "processed_row_count": processed_row_count, "error": None}
        )
    except UnicodeDecodeError as e:
        error_message = f"Encoding error: {e}. Used encoding '{encoding}'."
    except csv.Error as e:
        error_message = f"CSV parsing error: {e}. Used delimiter '{delimiter}'. Check if delimiter is correct."
    except StopIteration:
        error_message = (
            "Reached end of file unexpectedly." if processed_row_count == 0 else None
        )
    except Exception as e:
        logger.error(f"Unexpected error in read_csv_sample tool: {e}", exc_info=True)
        error_message = f"Unexpected error reading sample: {str(e)}"
    return json.dumps(
        {
            "error": error_message,
            "rows": rows,
            "processed_row_count": processed_row_count,
        }
    )


@function_tool
async def read_raw_lines(
    encoding: str, line_limit: int | None, start_line: int | None
) -> str:
    if line_limit is None:
        line_limit = 50
    if start_line is None:
        start_line = 0

    context = _CURRENT_AI_CSV_TOOL_CONTEXT.get()
    if not context or not context.file_path or not context.file_path.exists():
        return json.dumps({"error": "File path not found in tool context."})
    if start_line < 0:
        return json.dumps({"error": "start_line cannot be negative."})

    lines = []
    error_message = None
    actual_start_line = 0
    lines_read_count = 0
    try:
        with open(context.file_path, encoding=encoding, newline="") as f:
            for i in range(start_line):
                try:
                    next(f)
                    actual_start_line += 1
                except StopIteration:
                    error_message = f"EOF reached while skipping to start_line {start_line} (skipped {actual_start_line})."
                    break
            if error_message:
                return json.dumps(
                    {
                        "error": error_message,
                        "lines": [],
                        "start_line_processed": actual_start_line,
                        "lines_read_count": 0,
                    }
                )

            for i, line in enumerate(f):
                if i >= line_limit:
                    break
                lines.append(line.rstrip("\r\n"))
                lines_read_count += 1
        return json.dumps(
            {
                "lines": lines,
                "start_line_processed": actual_start_line,
                "lines_read_count": lines_read_count,
                "error": None,
            }
        )
    except UnicodeDecodeError as e:
        error_message = f"Encoding error: {e}. Used encoding '{encoding}'."
    except StopIteration:
        error_message = (
            "Reached end of file unexpectedly." if lines_read_count == 0 else None
        )
    except Exception as e:
        logger.error(f"Unexpected error in read_raw_lines tool: {e}", exc_info=True)
        error_message = f"Unexpected error reading raw lines: {str(e)}"
    return json.dumps(
        {
            "error": error_message,
            "lines": lines,
            "start_line_processed": actual_start_line,
            "lines_read_count": lines_read_count,
        }
    )


# --- AICSVStandardizer Class ---
class AICSVStandardizer(
    CSVStandardizer, AsyncStandardizer
):  # Inherits from the enhanced CSVStandardizer
    def __init__(
        self,
        mcp_server: Optional[MCPServerStdio] = None,
        mcp_session: Optional[ClientSession] = None,
        llm_model: str = "gpt-4.1-2025-04-14",
        # --- Initial Hints (Optional) ---
        initial_delimiter: Optional[str] = None,
        initial_encoding: Optional[str] = None,
    ):
        # AI will determine the file_configs
        super().__init__(
            delimiter=None,
            encoding=None,
            has_header=True,
            skip_rows=0,
            skip_columns=None,
            descriptions=None,
            table_names=None,
            file_configs=None,
            column_definitions=None,
        )

        self.mcp_servers = [mcp_server] if mcp_server else []
        self.mcp_session = mcp_session
        self.llm_model = llm_model
        self._initial_delimiter_hint = initial_delimiter
        self._initial_encoding_hint = initial_encoding

    async def _get_initial_guesses(self, file_path: Path) -> Tuple[str, str]:
        """Helper to get initial encoding and delimiter guesses for a single file."""
        encoding_guess = self._initial_encoding_hint
        if not encoding_guess:
            try:
                with open(file_path, "rb") as fb_enc:
                    enc_sample = fb_enc.read(ENCODING_SAMPLE_SIZE)
                detected_enc_info = detect(enc_sample) if enc_sample else None
                encoding_guess = (
                    detected_enc_info["encoding"]
                    if detected_enc_info and detected_enc_info["encoding"]
                    else "utf-8"
                )
            except Exception as e:
                logger.warning(
                    f"Initial encoding detection for {file_path.name} failed: {e}. Using utf-8."
                )
                encoding_guess = "utf-8"

        delimiter_guess = self._initial_delimiter_hint
        if not delimiter_guess:
            try:
                with open(
                    file_path, encoding=encoding_guess, errors="ignore"
                ) as f_delim_sample:
                    delim_sample_text = f_delim_sample.read(DELIMITER_SAMPLE_SIZE)
                if delim_sample_text:
                    sniffer = clevercsv.Sniffer()
                    dialect = sniffer.sniff(delim_sample_text)
                    delimiter_guess = (
                        dialect.delimiter if dialect and dialect.delimiter else ","
                    )
                else:
                    delimiter_guess = ","
            except Exception as e:
                logger.warning(
                    f"Initial delimiter detection for {file_path.name} failed ({e}). Using ','."
                )
                delimiter_guess = ","

        logger.info(
            f"Initial guesses for {file_path.name} - Encoding: {encoding_guess}, Delimiter: '{delimiter_guess}'"
        )
        return encoding_guess, delimiter_guess

    async def _run_analysis_agent(
        self,
        file_path: Path,
        initial_encoding: str,
        initial_delimiter: str,
    ) -> Dict[str, Any]:
        with AIStandardizerToolContext(file_path):
            prompt = AI_CSV_PROMPT.format(
                file_path=str(file_path),
                initial_encoding=initial_encoding,
                initial_delimiter=initial_delimiter,
            )
            agent = Agent(
                name="CSV Detail Analyzer Agent",
                mcp_servers=self.mcp_servers,
                tools=[read_csv_sample, read_raw_lines],
                model=self.llm_model,
            )
            logger.info(f"Running CSV Detail Analyzer Agent for {file_path.name}...")
            result = await Runner.run(agent, input=prompt)

            if not result or not result.final_output:
                raise RuntimeError(
                    f"Agent execution failed or returned no output for {file_path.name}."
                )
            logger.info(
                f"Agent for {file_path.name} finished. Raw output preview: {result.final_output[:500]}..."
            )

            try:
                final_params_text = result.final_output.strip()
                match = re.search(r"```(?:json)?(.*)```", final_params_text, re.DOTALL)
                if match:
                    final_params_text = match.group(1).strip()

                ai_output = json.loads(final_params_text)

                # --- Validate Agent Output Structure ---
                if not isinstance(ai_output, dict):
                    raise ValueError("Agent did not return a valid JSON object.")

                required_top_keys = {
                    "table_name",
                    "encoding",
                    "delimiter",
                    "has_header",
                    "skip_rows",
                    "columns",
                }
                if not required_top_keys.issubset(ai_output.keys()):
                    missing = required_top_keys - ai_output.keys()
                    raise ValueError(
                        f"Agent JSON missing required top-level keys: {missing}"
                    )

                if not isinstance(ai_output["columns"], list):
                    raise ValueError("Agent JSON 'columns' must be a list.")
                if not ai_output["columns"]:  # Must have at least one column defined
                    raise ValueError("Agent JSON 'columns' list cannot be empty.")

                for col_spec in ai_output["columns"]:
                    if not isinstance(col_spec, dict):
                        raise ValueError(
                            f"Each item in 'columns' list must be a dictionary. Found: {type(col_spec)}"
                        )
                    req_col_keys = {"original_identifier", "final_column_name"}
                    if not req_col_keys.issubset(col_spec.keys()):
                        missing_col_keys = req_col_keys - col_spec.keys()
                        raise ValueError(
                            f"Column spec {col_spec.get('final_column_name', 'N/A')} missing keys: {missing_col_keys}"
                        )
                    # Ensure description is present, even if None (or agent omits it)
                    if "description" not in col_spec:
                        col_spec["description"] = None

                sr = ai_output["skip_rows"]
                if not isinstance(sr, int) and not (
                    isinstance(sr, list) and all(isinstance(i, int) for i in sr)
                ):
                    raise ValueError(
                        f"Agent JSON 'skip_rows' must be an integer or list of integers, got {type(sr)}"
                    )
                if isinstance(sr, list):
                    ai_output["skip_rows"] = sorted(list(set(i for i in sr if i >= 0)))

                # Ensure table_description is present, even if None
                if "table_description" not in ai_output:
                    ai_output["table_description"] = None

                logger.info(
                    f"Agent successfully determined parameters for {file_path.name}"
                )
                return ai_output
            except json.JSONDecodeError as e:
                logger.error(
                    f"Agent for {file_path.name} did not return valid JSON: {e}. Output: {result.final_output}",
                    exc_info=True,
                )
                raise ValueError(
                    f"Agent failed to produce valid JSON output for {file_path.name}."
                ) from e
            except ValueError as e:  # Catch our custom validation errors
                logger.error(
                    f"Invalid JSON structure or content from agent for {file_path.name}: {e}. Output: {result.final_output}",
                    exc_info=True,
                )
                raise e  # Re-raise

    async def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        overwrite: bool = False,
        config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> StandardizationResult:
        output_path_obj = Path(output_path)

        input_paths: List[Path]
        if isinstance(datasource, (str, Path)):
            input_paths = [Path(datasource)]
        elif isinstance(datasource, list) and all(
            isinstance(p, (str, Path)) for p in datasource
        ):
            input_paths = [Path(p) for p in datasource]
        else:
            raise TypeError(
                "datasource must be a file path string/Path object or a list of such paths."
            )

        if not input_paths:
            raise ValueError("No input datasource provided.")

        ai_analysis_tasks = []
        for input_file_path in input_paths:
            if not input_file_path.exists() or not input_file_path.is_file():
                raise FileNotFoundError(
                    f"Input CSV file not found or is not a file: {input_file_path}"
                )

            async def analyze_file_task(file_path_for_task: Path):
                logger.info(
                    f"--- Starting AI Analysis for file: {file_path_for_task.name} ---"
                )
                enc_guess, delim_guess = await self._get_initial_guesses(
                    file_path_for_task
                )
                # Store the raw AI output for this file, potentially to add to StandardizationResult later
                # This requires _run_analysis_agent to return the raw JSON string or parsed dict
                ai_params_for_file = await self._run_analysis_agent(
                    file_path_for_task, enc_guess, delim_guess
                )
                return file_path_for_task, ai_params_for_file  # Return path with params

            ai_analysis_tasks.append(analyze_file_task(input_file_path))

        logger.info(f"Starting AI analysis for {len(ai_analysis_tasks)} CSV file(s)...")
        all_ai_params_results_with_paths: List[Tuple[Path, Dict[str, Any]]] = []
        try:
            all_ai_params_results_with_paths = await asyncio.gather(*ai_analysis_tasks)
        except Exception as e:
            logger.exception(f"Critical error during concurrent AI analysis phase: {e}")
            raise RuntimeError("AI analysis phase failed.") from e

        logger.info(
            f"AI analysis complete for all {len(all_ai_params_results_with_paths)} file(s)."
        )

        all_ai_file_configs: List[Dict[str, Any]] = []

        for file_path, ai_params in all_ai_params_results_with_paths:
            logger.info(f"Aggregating AI parameters for: {file_path.name}")

            file_conf_for_base = {
                "table_name": ai_params["table_name"],
                "description": ai_params.get("table_description"),
                "encoding": ai_params["encoding"],
                "delimiter": ai_params["delimiter"],
                "has_header": ai_params["has_header"],
                "skip_rows": ai_params["skip_rows"],
                "column_definitions": ai_params["columns"],
            }
            all_ai_file_configs.append(file_conf_for_base)

        logger.debug(
            f"Initializing final CSVStandardizer with aggregated AI parameters: {all_ai_file_configs}"
        )
        final_processor = CSVStandardizer(file_configs=all_ai_file_configs)

        try:
            logger.info(
                f"Executing batch standardization for {len(input_paths)} file(s)..."
            )
            standardization_result = final_processor.standardize(
                datasource=input_paths,
                output_path=output_path_obj,
                overwrite=overwrite,
            )
            logger.info(
                f"AI CSV Standardization complete. Output: {standardization_result.output_path}"
            )

            return standardization_result
        except Exception as e:
            logger.exception(
                f"Error during final batch standardization step using AI parameters: {e}"
            )
            raise RuntimeError("Final batch standardization step failed.") from e
