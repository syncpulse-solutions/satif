import base64
import binascii
import json
import logging
import os
import sqlite3

from fastmcp import FastMCP
from sdif_db import SDIFDatabase

from sdif_mcp.prompt import CREATE_TRANSFORMATION

mcp = FastMCP(name="sdif")

logger = logging.getLogger(__name__)


# TODO: Find a better alternative
# We encode file_path in base64 to avoid issues with special characters
# and to be able to pass it to the MCP server as a resource string
# This is cumbersome so we should find a better solution
def _decode_path_if_base64(path_str: str) -> str:
    """Tries to decode a path string if it's Base64 encoded.
    Returns the decoded string or the original string if not valid Base64.
    """
    try:
        # Attempt to decode. b64decode expects bytes and returns bytes.
        decoded_bytes = base64.b64decode(path_str, validate=True)
        # If successful, convert bytes to string (assuming utf-8 encoding for paths)
        return decoded_bytes.decode("utf-8")
    except binascii.Error:  # This error is raised for invalid Base64 padding, etc.
        # Not a valid Base64 string, assume it's a plain path
        return path_str
    except UnicodeDecodeError:
        # If b64decode was successful but the result isn't valid UTF-8,
        # it's unlikely to be a path we want. Return original or handle as error.
        # For now, returning original, but this case might need more thought if it occurs.
        logger.warning(
            f"Successfully Base64 decoded '{path_str}' but result was not valid UTF-8. Treating as plain path."
        )
        return path_str


@mcp.prompt()
async def create_transformation(
    input_file: str,
    input_schema: str,
    input_sample: str,
    output_files: str,
    output_schema: str,
    output_sample: str,
    output_representation: str,
    instructions: str,
):
    return CREATE_TRANSFORMATION.format(
        input_file=input_file,
        input_schema=input_schema,
        input_sample=input_sample,
        output_files=output_files,
        output_schema=output_schema,
        output_sample=output_sample,
        output_representation=output_representation,
        instructions=instructions,
    )


@mcp.resource(uri="schema://{sqlite_file}")
async def get_schema(sqlite_file: str) -> str:
    """
    Get the schema of a SQLite database.

    Args:
        sqlite_file: Path to the SQLite file (can be plain string or Base64 encoded)

    Returns:
        String representation of the database schema
    """
    actual_sqlite_file = _decode_path_if_base64(sqlite_file)
    logger.debug(
        f"get_schema: original path='{sqlite_file}', decoded path='{actual_sqlite_file}'"
    )

    if not os.path.exists(actual_sqlite_file):
        return f"Error: SQLite file {actual_sqlite_file} not found or not authorized"

    try:
        with SDIFDatabase(actual_sqlite_file, read_only=True) as db:
            schema_info = db.get_schema()

        return schema_info

    except sqlite3.Error as e:
        return f"Error getting schema: {str(e)}"


@mcp.resource(uri="sample://{sqlite_file}")
async def get_sample(sqlite_file: str) -> str:
    """Get the sample of a SQLite database."""
    actual_sqlite_file = _decode_path_if_base64(sqlite_file)
    logger.debug(
        f"get_sample: original path='{sqlite_file}', decoded path='{actual_sqlite_file}'"
    )

    if not os.path.exists(actual_sqlite_file):
        return f"Error: SQLite file {actual_sqlite_file} not found or not authorized"

    try:
        with SDIFDatabase(actual_sqlite_file, read_only=True) as db:
            sample_analysis = db.get_sample_analysis(
                num_sample_rows=5,
                top_n_common_values=10,
            )
    except sqlite3.Error as e:
        return f"Error getting sample: {str(e)}"

    return sample_analysis


@mcp.tool()
async def execute_sql(sqlite_file: str, query: str) -> str:
    """
    Execute SQL query on a SQLite database.

    Args:
        sqlite_file: Path to the SQLite file (can be plain string or Base64 encoded)
        query: SQL query to execute

    Returns:
        Results of the query as a formatted string
    """
    actual_sqlite_file = _decode_path_if_base64(sqlite_file)
    logger.debug(
        f"execute_sql: original path='{sqlite_file}', decoded path='{actual_sqlite_file}'"
    )

    # Validate the file exists and is in our allowed list
    if not os.path.exists(actual_sqlite_file):
        return f"Error: SQLite file {actual_sqlite_file} not found or not authorized"

    try:
        with SDIFDatabase(actual_sqlite_file, read_only=True) as db:
            result = db.query(query, return_format="dict")
            MAX_RESULTS = 50
            if len(result) > MAX_RESULTS:
                return json.dumps(
                    {
                        "data": result[:MAX_RESULTS],
                        "truncated": True,
                        "total_rows": len(result),
                    }
                )
            else:
                return json.dumps({"data": result, "truncated": False})
    except PermissionError as e:
        return f"Error: Query refused. {e}"
    except sqlite3.Error as e:
        return f"Error executing query: {e}"
    except Exception as e:
        logger.exception("Unexpected error in execute_sql tool")
        return f"Unexpected Error: {e}"


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport="stdio")
