import base64
import os
import re
from collections import defaultdict
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from agents import Agent, Runner, function_tool
from agents.mcp.server import MCPServer
from mcp import ClientSession
from satif_core import AsyncTransformationBuilder
from satif_core.types import FilePath
from satif_sdk.code_executors.local_executor import LocalCodeExecutor
from satif_sdk.comparators import get_comparator
from satif_sdk.representers import get_representer
from satif_sdk.transformers import CodeTransformer

CONTEXT_INPUT_SDIF_PATH: ContextVar[Optional[Path]] = ContextVar(
    "CONTEXT_INPUT_SDIF_PATH", default=None
)
CONTEXT_OUTPUT_TARGET_FILES: ContextVar[Optional[Dict[Union[str, Path], str]]] = (
    ContextVar("CONTEXT_OUTPUT_TARGET_FILES", default=None)
)
CONTEXT_SCHEMA_ONLY: ContextVar[Optional[bool]] = ContextVar(
    "CONTEXT_SCHEMA_ONLY", default=None
)


def _format_comparison_output(
    comparison_result: Dict[str, Any],
    schema_only_mode: Optional[bool],
    source_file_display_name: str,
    target_file_display_name: str,
) -> str:
    """
    Formats the comparison result string, with special handling for schema_only mode
    where files are equivalent due to being empty.
    """
    base_message_prefix = f"Comparison for {source_file_display_name} [SOURCE] with {target_file_display_name} [TARGET]:"

    if schema_only_mode is True and comparison_result.get("are_equivalent") is True:
        details = comparison_result.get("details", {})
        row_comparison = details.get("row_comparison", {})

        row_count1 = row_comparison.get("row_count1")
        row_count2 = row_comparison.get("row_count2")

        if (
            isinstance(row_count1, (int, float))
            and row_count1 == 0
            and isinstance(row_count2, (int, float))
            and row_count2 == 0
        ):
            return f"{base_message_prefix} Files have the same headers but are both empty (no data rows). This should not happen. Please verify the instructions and try again."

    # Default formatting if the special condition isn't met
    return f"{base_message_prefix} {comparison_result}"


@function_tool
async def execute_transformation(code: str) -> str:
    """Executes the transformation code on the input and returns the
    comparison difference between the transformed output and the target output example.

    Args:
        code: The code to execute on the input.
    """
    input_sdif_path = CONTEXT_INPUT_SDIF_PATH.get()
    output_target_files_dict = CONTEXT_OUTPUT_TARGET_FILES.get()
    schema_only_flag = CONTEXT_SCHEMA_ONLY.get()

    if input_sdif_path is None or output_target_files_dict is None:
        return "Error: Transformation context not initialized correctly via contextvars"

    code_transformer = CodeTransformer(
        function=code,
        code_executor=LocalCodeExecutor(disable_security_warning=True),
    )
    generated_output_path = code_transformer.export(input_sdif_path)

    comparisons = []
    comparator_kwargs = {}
    if schema_only_flag:
        comparator_kwargs["check_structure_only"] = True

    if os.path.isdir(generated_output_path):
        # If it's a directory, compare each file with its corresponding target
        generated_files = os.listdir(generated_output_path)

        for (
            output_base_file,
            output_target_file_name,
        ) in output_target_files_dict.items():
            if output_target_file_name in generated_files:
                generated_file_path = os.path.join(
                    generated_output_path, output_target_file_name
                )
                comparator = get_comparator(output_target_file_name.split(".")[-1])
                comparison = comparator.compare(
                    generated_file_path, output_base_file, **comparator_kwargs
                )
                formatted_message = _format_comparison_output(
                    comparison,
                    schema_only_flag,
                    generated_file_path,
                    output_target_file_name,
                )
                comparisons.append(formatted_message)
            else:
                comparisons.append(
                    f"Error: {output_target_file_name} not found in the generated output"
                )
    else:
        # If it's a single file, ensure there's only one target and compare
        if len(output_target_files_dict) == 1:
            output_file = list(output_target_files_dict.keys())[0]
            output_target_file_name = list(output_target_files_dict.values())[0]
            comparator = get_comparator(
                str(output_file).split(".")[-1]
            )  # Ensure output_file is string for split
            comparison = comparator.compare(
                generated_output_path, output_file, **comparator_kwargs
            )
            formatted_message = _format_comparison_output(
                comparison,
                schema_only_flag,
                str(generated_output_path),
                output_target_file_name,
            )
            comparisons.append(formatted_message)
        else:
            comparisons.append(
                "Error: Single output file generated but multiple target files expected"
            )

    return "\n".join(comparisons)


class SyncpulseTransformationBuilder(AsyncTransformationBuilder):
    """This class is used to build a transformation code that will be used to transform a SDIF database into a set of files following the format of the given output files."""

    def __init__(
        self,
        mcp_server: MCPServer,
        mcp_session: ClientSession,
        llm_model: str = "o4-mini",
    ):
        self.mcp_server = mcp_server
        self.mcp_session = mcp_session
        self.llm_model = llm_model

    async def build(
        self,
        sdif: Path,
        output_target_files: Dict[FilePath, str] | List[FilePath] | FilePath,
        output_sdif: Optional[Path] = None,
        instructions: str = "",
        schema_only: bool = False,
        representer_kwargs: Optional[Dict[str, Any]] = None,
    ) -> str:
        resolved_input_sdif_path = Path(sdif).resolve()

        # OUTPUT_TARGET_FILES keys are absolute paths to original example files for local reading by representers/comparators.
        # Values are agent-facing filenames.
        resolved_output_target_files: Dict[Union[str, Path], str]
        if isinstance(output_target_files, FilePath):
            resolved_output_target_files = {
                Path(output_target_files).resolve(): Path(output_target_files).name
            }
        elif isinstance(output_target_files, list):
            resolved_output_target_files = {
                Path(file_path).resolve(): Path(file_path).name
                for file_path in output_target_files
            }
        elif isinstance(output_target_files, dict):
            temp_map = {}
            for k, v in output_target_files.items():
                # Resolve Path keys to absolute paths
                key_to_resolve = k
                if (
                    isinstance(key_to_resolve, str) and Path(key_to_resolve).exists()
                ):  # Check if string is a valid path
                    key_to_resolve = Path(key_to_resolve)

                if isinstance(key_to_resolve, Path):
                    temp_map[key_to_resolve.resolve()] = v
                else:  # Keep non-Path keys as they are (e.g. if it's already a resolved string path from somewhere else)
                    temp_map[key_to_resolve] = v
            resolved_output_target_files = temp_map
        else:
            resolved_output_target_files = {}

        token_input_path = CONTEXT_INPUT_SDIF_PATH.set(resolved_input_sdif_path)
        token_output_files = CONTEXT_OUTPUT_TARGET_FILES.set(
            resolved_output_target_files
        )
        token_schema_only = CONTEXT_SCHEMA_ONLY.set(schema_only)

        try:
            # We must encode the path because special characters are not allowed in mcp read_resource()
            input_sdif_mcp_uri_path = base64.b64encode(
                str(resolved_input_sdif_path).encode()
            ).decode()
            output_sdif_mcp_uri_path = (
                base64.b64encode(str(output_sdif).encode()).decode()
                if output_sdif
                else None
            )

            input_schema = await self.mcp_session.read_resource(
                f"schema://{input_sdif_mcp_uri_path}"
            )
            input_sample = await self.mcp_session.read_resource(
                f"sample://{input_sdif_mcp_uri_path}"
            )

            output_schema_text = "N/A"
            output_sample_text = "N/A"
            if output_sdif_mcp_uri_path:
                try:
                    output_schema_content = await self.mcp_session.read_resource(
                        f"schema://{output_sdif_mcp_uri_path}"
                    )
                    if output_schema_content.contents:
                        output_schema_text = output_schema_content.contents[0].text
                except Exception as e:
                    print(
                        f"Warning: Could not read schema for output_sdif {output_sdif_mcp_uri_path}: {e}"
                    )

                try:
                    output_sample_content = await self.mcp_session.read_resource(
                        f"sample://{output_sdif_mcp_uri_path}"
                    )
                    if output_sample_content.contents:
                        output_sample_text = output_sample_content.contents[0].text
                except Exception as e:
                    print(
                        f"Warning: Could not read sample for output_sdif {output_sdif_mcp_uri_path}: {e}"
                    )
            output_representation = defaultdict(dict)
            if resolved_output_target_files:
                for file_key_abs_path in list(resolved_output_target_files.keys()):
                    agent_facing_name = resolved_output_target_files[file_key_abs_path]
                    try:
                        # Representer uses the absolute path (file_key_abs_path) to read the example file.
                        representer = get_representer(file_key_abs_path)
                        representation, used_params = representer.represent(
                            file_key_abs_path, **(representer_kwargs or {})
                        )
                        output_representation[agent_facing_name] = {
                            "representation": representation,
                            "used_params": used_params,
                        }
                    except Exception as e:
                        print(
                            f"Warning: Could not get representation for {agent_facing_name} (path {file_key_abs_path}): {e}"
                        )
                        output_representation[agent_facing_name] = (
                            f"Error representing file: {e}"
                        )

            prompt = await self.mcp_session.get_prompt(
                "create_transformation",
                arguments={
                    "input_file": Path(
                        input_sdif_mcp_uri_path  # Use the original sdif path for display name logic if needed
                    ).name,
                    "input_schema": input_schema.contents[0].text
                    if input_schema.contents
                    else "Error reading input schema",
                    "input_sample": input_sample.contents[0].text
                    if input_sample.contents
                    else "Error reading input sample",
                    "output_files": str(list(resolved_output_target_files.values())),
                    "output_schema": output_schema_text,
                    "output_sample": output_sample_text
                    if not schema_only
                    else "Sample not available. File is empty (no data).",
                    "output_representation": str(output_representation),
                    "instructions": instructions
                    or "No instructions provided. Use the output example.",
                },
            )
            agent = Agent(
                name="Transformation Builder",
                mcp_servers=[self.mcp_server],
                tools=[execute_transformation],
                model=self.llm_model,
            )
            result = await Runner.run(agent, prompt.messages[0].content.text)
            transformation_code = self.parse_code(result.final_output)
            return transformation_code
        finally:
            # Reset context variables after the task is done
            CONTEXT_INPUT_SDIF_PATH.reset(token_input_path)
            CONTEXT_OUTPUT_TARGET_FILES.reset(token_output_files)
            CONTEXT_SCHEMA_ONLY.reset(token_schema_only)

    def parse_code(self, code) -> str:
        match = re.search(r"```(?:python)?(.*?)```", code, re.DOTALL)
        if match:
            return match.group(1).strip()
        else:
            # Handle case where no code block is found
            return code.strip()
