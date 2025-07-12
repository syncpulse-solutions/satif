import inspect
import json
import logging
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Optional, Union

from agents import Agent, Runner, function_tool
from agents.mcp.server import MCPServer
from mcp import ClientSession
from satif_core.adapters.base import Adapter
from satif_core.types import Datasource, SDIFPath
from satif_sdk import SDIFDatabase
from satif_sdk.adapters.code import AdapterError, CodeAdapter

logger = logging.getLogger(__name__)


TIDY_TRANSFORMATION_PROMPT = """
You are an expert Data Tidying Agent for SDIF databases.
Your task is to write Python code to transform tables within a given SDIF database into a 'tidy' format, modifying the database *in place*.

**Tidy Data Principles:**
1. Each variable forms a column.
2. Each observation forms a row.
3. Each type of observational unit forms a table (you might need to create new tables).

**Input SDIF Context:**
You will be provided with:
- The schema of the input SDIF database (`input_schema`).
- A sample analysis of the input SDIF database (`input_sample`).

<input_schema>
{input_schema}
</input_schema>

<input_sample>
{input_sample}
</input_sample>

**Available `SDIFDatabase` Methods:**
Here are the public methods you can call on the `db` object passed to your `adapt_sdif` function:
```python
{sdif_database_methods}
```

**Your Goal:**
Generate Python code for an adaptation function named `adapt_sdif`. This function MUST:
- Accept an `SDIFDatabase` object (`db`) which represents the database to be modified.
- Perform tidying operations directly on this `db` instance using its available methods (see list above) and potentially pandas for intermediate processing.
- Examples of operations:
    - Read a messy table: `df = db.read_table('messy_table')`
    - Tidy the DataFrame using pandas (melt, split, etc.).
    - Write the tidy DataFrame back: `db.write_dataframe(tidy_df, 'tidy_table_name', source_id=1, if_exists='replace')` (obtain a source_id if needed or use a default).
    - Drop the original messy table if desired: `db.drop_table('messy_table')`
- The function should modify the `db` object **in place** and MUST return `None`.

**Tools Available:**
- `execute_sql(query: str)`: Execute a read-only SQL query against the **input** SDIF database to inspect data further before generating adaptation code.
- `execute_tidy_adaptation(code: str)`: Execute the Python code string containing your `adapt_sdif` function.
    - This tool will run your code against a **copy** of the input SDIF.
    - It will return a **sample analysis** of the **modified SDIF file**.

**Workflow:**
1. **Analyze:** Examine the `input_schema` and `input_sample`. Use `execute_sql` if needed to understand the data structure and identify tables needing tidying.
2. **Code:** Write the `adapt` Python code function using `SDIFDatabase` methods and pandas.
3. **Execute & Verify:** Use `execute_tidy_adaptation` with your code.
4. **Review:** Examine the returned `output_sample_analysis`. Check if the tables in the modified SDIF meet the tidy data principles.
5. **Refine:** If the output is not tidy, modify your Python code and repeat step 3.
6. **Finalize:** Once the `output_sample_analysis` confirms the data is tidy, respond **only** with the final, validated Python code string for the `adapt_sdif` function enclosed in triple backticks (```python ... ```). Do not include any other text before or after the code block.

**Example `adapt_sdif` function:**
```python
import pandas as pd
from satif.adapters.code import AdapterError # Import for raising errors if needed
from typing import Dict, Any

# Assume input db has table 'wide_sales' with columns: 'Region', 'Q1_Sales', 'Q2_Sales'

def adapt_sdif(db: SDIFDatabase) -> None:
    try:
        # Get a default source_id or create one if needed
        sources = db.list_sources()
        source_id = sources[0]['source_id'] if sources else db.add_source('tidy_adapter', 'script')

        # Read the table to tidy
        df_sales = db.read_table('wide_sales')

        # Tidy using pandas
        tidy_sales = pd.melt(df_sales,
                             id_vars=['Region'],
                             value_vars=['Q1_Sales', 'Q2_Sales'],
                             var_name='Quarter',
                             value_name='Sales')
        tidy_sales['Quarter'] = tidy_sales['Quarter'].str.replace('_Sales', '')

        # Write the tidy table back, replacing the original if desired,
        # or writing to a new table.
        # Here, we write to a new table and drop the old one.
        db.write_dataframe(tidy_sales,
                           'tidy_sales_data',
                           source_id=source_id,
                           if_exists='replace', # Replace if 'tidy_sales_data' already exists
                           description="Tidied sales data")

        # Optionally drop the original table
        db.drop_table('wide_sales')

    except Exception as e:
        print(f"Error during tidying: {{e}}") # Log errors
        # Re-raise the exception to signal failure to the execution framework
        raise AdapterError(f"Error in adapt_sdif: {{e}}") from e

    # IMPORTANT: Function must return None (can be implicit)
    return None
```

**Important:**
- Your Python code string MUST define the `adapt_sdif(db: SDIFDatabase)` function.
- The function MUST return `None`.
- Use `db.write_dataframe` with `if_exists='replace'` or `if_exists='append'` or write to new tables and potentially use `db.drop_table` for the old ones.
- Handle potential errors during data reading or processing within your function and raise an `AdapterError` or similar to indicate failure.
- Ensure pandas and other necessary libraries (like `typing`, `AdapterError`) are imported within the code string if you use them.
"""


TOOL_CONTEXT = {
    "copied_input_sdif_path": None,
    "temp_dir": None,
    "current_output_sdif_path": None,
}


@function_tool
async def execute_tidy_adaptation(code: str) -> str:
    """
    Tool implementation for the agent to execute the tidying adaptation code.
    Runs the code against a *copy* of the input SDIF, creating a *new* output SDIF,
    and returns a sample analysis of the modified output.
    """
    copied_input_path = TOOL_CONTEXT.get("copied_input_sdif_path")
    temp_dir = TOOL_CONTEXT.get("temp_dir")

    if not copied_input_path or not copied_input_path.exists():
        return (
            "Error: Input SDIF copy not found for transformation. Tool context issue."
        )
    if not temp_dir:
        return "Error: Temporary directory not set up. Tool context issue."

    # Define path for the *output* SDIF generated by *this tool execution*
    # This path is temporary just for this tool's run
    tool_output_sdif_path = temp_dir / "tidy_adaptation_output.sdif"
    # Update context for potential internal use, though CodeAdapter calculates its own path
    TOOL_CONTEXT["current_output_sdif_path"] = tool_output_sdif_path

    logger.info(
        f"Executing adaptation code via tool. Output will be: {tool_output_sdif_path}"
    )

    try:
        adapter = CodeAdapter(
            function=code,
            function_name="adapt_sdif",
            output_suffix="_adapted_tool_run",
        )
        # Run the adaptation. It copies `copied_input_path` and modifies the copy.
        # The returned path is the newly created, adapted file.
        adapted_sdif_path = adapter.adapt(copied_input_path)

        # 2. Get sample analysis of the *adapted* SDIF file
        with SDIFDatabase(adapted_sdif_path, read_only=True) as adapted_db_read:
            analysis = adapted_db_read.get_sample_analysis(
                num_sample_rows=5, top_n_common_values=5
            )
            # Store the path generated by the tool for reference if needed elsewhere
            TOOL_CONTEXT["current_output_sdif_path"] = adapted_sdif_path
            return json.dumps({"output_sample_analysis": analysis})

    except AdapterError as e:
        logger.error(f"Error during adaptation code execution via tool: {e}")
        # Clean up the failed output file if it exists
        if (
            TOOL_CONTEXT["current_output_sdif_path"]
            and TOOL_CONTEXT["current_output_sdif_path"].exists()
        ):
            try:
                TOOL_CONTEXT["current_output_sdif_path"].unlink()
            except OSError:
                pass
        return f"Error: Adaptation code failed: {e}"
    except (sqlite3.Error, ValueError, TypeError, FileNotFoundError) as e:
        logger.error(f"Error analyzing adapted SDIF or file issue: {e}")
        if (
            TOOL_CONTEXT["current_output_sdif_path"]
            and TOOL_CONTEXT["current_output_sdif_path"].exists()
        ):
            try:
                TOOL_CONTEXT["current_output_sdif_path"].unlink()
            except OSError:
                pass
        return f"Error: Failed to analyze adapted SDIF: {e}"
    except Exception as e:
        logger.exception("Unexpected error in execute_tidy_adaptation tool")
        if (
            TOOL_CONTEXT["current_output_sdif_path"]
            and TOOL_CONTEXT["current_output_sdif_path"].exists()
        ):
            try:
                TOOL_CONTEXT["current_output_sdif_path"].unlink()
            except OSError:
                pass
        return f"Unexpected Error: {e}"


class TidyAdapter(Adapter):
    """
    Uses an AI agent (via agents library) to generate and execute Python code
    that transforms tables in an SDIF file into a tidy format using CodeAdapter.
    """

    def __init__(
        self,
        mcp_server: MCPServer,
        mcp_session: ClientSession,
        llm_model: str = "o4-mini",
        max_iterations: int = 5,
    ):
        """
        Initialize the TidyAdapter.

        Args:
            mcp_server: An instance of MCPServer for agent communication.
            mcp_session: An instance of ClientSession for resource/prompt fetching.
            llm_model: Name of the language model to use for the agent.
            max_iterations: Maximum number of attempts the agent gets to refine the code.
        """
        self.mcp_server = mcp_server
        self.mcp_session = mcp_session
        self.llm_model = llm_model
        self.max_iterations = max_iterations  # Not directly used by Runner, but good for context/potential future use

        # Temporary environment management (could be context managed)
        self._temp_dir: Optional[Path] = None
        self._copied_input_sdif_path: Optional[Path] = None

    def _get_sdif_methods(self) -> str:
        """Introspects SDIFDatabase and returns formatted public method signatures."""
        signatures = []
        # Exclude known internal/base methods explicitly
        exclude_methods = {
            "__init__",
            "__enter__",
            "__exit__",
            "__del__",
            "_validate_connection",
            "_create_metadata_tables",
        }
        try:
            # Iterate through members of the class
            for name, member in inspect.getmembers(SDIFDatabase):
                # Check if it's a function/method and not excluded/private
                if (
                    inspect.isfunction(member)
                    and not name.startswith("_")
                    and name not in exclude_methods
                ):
                    try:
                        sig = inspect.signature(member)
                        # Format the signature string
                        sig_str = f"db.{name}{sig}"
                        signatures.append(sig_str)
                    except (ValueError, TypeError) as e:
                        # Handle methods that might not have clear signatures (e.g., built-ins if any slip through)
                        logger.debug(f"Could not get signature for method {name}: {e}")
                        signatures.append(f"db.{name}(...) # Signature unavailable")

            return "\n".join(sorted(signatures))
        except Exception as e:
            logger.error(f"Failed to introspect SDIFDatabase methods: {e}")
            return "# Failed to retrieve method signatures."

    def _setup_temp_env(self, input_sdif_path: Path) -> Path:
        """Creates a temporary directory and copies the input SDIF."""
        self._temp_dir = Path(tempfile.mkdtemp(prefix="satif_tidy_adapter_"))
        self._copied_input_sdif_path = (
            self._temp_dir / f"input_copy_{input_sdif_path.name}"
        )
        shutil.copy(input_sdif_path, self._copied_input_sdif_path)
        logger.info(
            f"Copied input SDIF to temporary location: {self._copied_input_sdif_path}"
        )
        # Set global tool context
        TOOL_CONTEXT["copied_input_sdif_path"] = self._copied_input_sdif_path
        TOOL_CONTEXT["temp_dir"] = self._temp_dir
        TOOL_CONTEXT["current_output_sdif_path"] = None  # Reset output path context
        return self._copied_input_sdif_path

    def _cleanup_temp_env(self):
        """Removes the temporary directory."""
        if self._temp_dir and self._temp_dir.exists():
            try:
                shutil.rmtree(self._temp_dir)
                logger.info(f"Cleaned up temporary directory: {self._temp_dir}")
            except Exception as e:
                logger.error(
                    f"Error cleaning up temporary directory {self._temp_dir}: {e}"
                )
        # Clear global tool context
        TOOL_CONTEXT["copied_input_sdif_path"] = None
        TOOL_CONTEXT["temp_dir"] = None
        TOOL_CONTEXT["current_output_sdif_path"] = None
        self._temp_dir = None
        self._copied_input_sdif_path = None

    def parse_code(self, code_text: str) -> Optional[str]:
        """Extracts Python code from markdown code blocks."""
        match = re.search(r"```(?:python)?(.*?)```", code_text, re.DOTALL)
        if match:
            return match.group(1).strip()
        else:
            # If no markdown block, assume the whole text might be code (less reliable)
            # Check for keywords common in the expected code
            if "def adapt_sdif(" in code_text and "SDIFDatabase" in code_text:
                logger.warning(
                    "No markdown code block found, attempting to use entire response as code."
                )
                return code_text.strip()
            return None  # Indicate no valid code found

    async def adapt(self, sdif: Union[SDIFPath, SDIFDatabase]) -> Datasource:
        """
        Transforms the data in the input SDIF to be tidy using an AI agent.

        Args:
            sdif: The input SDIF database instance. Connection will be closed.

        Returns:
            Path to the new SDIF file containing the tidied data.

        Raises:
            FileNotFoundError: If the input SDIF path doesn't exist.
            RuntimeError: If the agent fails to produce valid tidy code.
            Exception: For unexpected errors during the process.
        """
        if isinstance(sdif, SDIFDatabase):
            input_path = Path(sdif.path)
        else:
            input_path = Path(sdif)
        if not input_path.exists():
            raise FileNotFoundError(f"Input SDIF file not found: {input_path}")

        # Ensure the input DB connection is closed before copying
        try:
            sdif.close()
        except Exception:
            pass

        copied_input_path = self._setup_temp_env(input_path)

        try:
            # Get Initial Context using SDIFDatabase methods directly
            with SDIFDatabase(copied_input_path, read_only=True) as db:
                input_schema_dict = db.get_schema()
                input_sample_dict = db.get_sample_analysis()

            sdif_methods_str = self._get_sdif_methods()

            initial_context = {
                "input_schema": json.dumps(input_schema_dict, indent=2),
                "input_sample": json.dumps(input_sample_dict, indent=2),
                "sdif_database_methods": sdif_methods_str,
            }

            agent = Agent(
                name="Tidy SDIF Adapter Agent",
                mcp_servers=[self.mcp_server],
                tools=[execute_tidy_adaptation],  # Use the decorated tools
                model=self.llm_model,
            )

            logger.info(f"Running Tidy Agent with model {self.llm_model}...")
            result = await Runner.run(
                agent,
                input=TIDY_TRANSFORMATION_PROMPT.format(
                    input_schema=initial_context["input_schema"],
                    input_sample=initial_context["input_sample"],
                    sdif_database_methods=initial_context["sdif_database_methods"],
                ),
            )

            if not result or not result.final_output:
                raise RuntimeError("Agent execution failed or returned no output.")

            logger.info(
                f"Agent finished. Final output message:\n{result.final_output[:500]}..."
            )

            final_code = self.parse_code(result.final_output)

            if not final_code:
                raise RuntimeError(
                    f"Agent failed to produce valid final Python code in its response."
                    f" Full response:\n{result.final_output}"
                )

            logger.info(
                "Successfully parsed final adaptation code from agent response."
            )

            logger.info("Executing final adaptation code...")
            final_adapter = CodeAdapter(
                function=final_code,
                function_name="adapt_sdif",
                output_suffix="_tidy_final",
            )

            final_adapted_path = final_adapter.adapt(copied_input_path)

            persistent_output_path = (
                input_path.parent / final_adapted_path.name
            ).resolve()
            if persistent_output_path.exists():
                logger.warning(
                    f"Overwriting existing file at final destination: {persistent_output_path}"
                )
                persistent_output_path.unlink()

            shutil.move(str(final_adapted_path), persistent_output_path)
            logger.info(
                f"Successfully generated final tidy SDIF: {persistent_output_path}"
            )

            return persistent_output_path

        except Exception as e:
            logger.exception(f"Error during TidyAdapter adapt process: {e}")
            raise
        finally:
            self._cleanup_temp_env()
