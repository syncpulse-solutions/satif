import logging
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from sdif_db.database import SDIFDatabase

from xlsx_to_sdif.spreadsheet.aspose_cells import AsposeCellsManager, get_workbook
from xlsx_to_sdif.state import State, Table
from xlsx_to_sdif.utils import parse_json_from_last_message

# Constants
SAMPLE_SIZE_FOR_TYPE_INFERENCE = 100

logger = logging.getLogger(__name__)


def _sanitize_name(name: str, prefix: str = "item") -> str:
    """Clean up a string to be a safe SQL identifier, transliterating accents."""
    if not isinstance(name, str):
        name = str(name)

    # 1. Transliterate using unicodedata to handle accents (e.g., é -> e)
    # NFKD decomposes characters, encode/decode removes combining marks
    try:
        name = (
            unicodedata.normalize("NFKD", name)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    except Exception:
        # Fallback if normalization fails for some reason
        pass

    # 2. Convert to lowercase and strip leading/trailing whitespace
    name = name.lower().strip()

    # 3. Replace sequences of non-alphanumeric characters (excluding underscore) with a single underscore
    name = re.sub(r"[^a-z0-9_]+", "_", name)

    # 4. Remove leading/trailing underscores that might result from replacements
    name = name.strip("_")

    # 5. Ensure it starts with a letter or underscore if not empty
    safe_name = name  # Already processed non-alphanumeric, just need start check
    if safe_name and not (safe_name[0].isalpha() or safe_name[0] == "_"):
        safe_name = f"_{safe_name}"

    # 6. Prevent using SQLite keywords (basic list, might need expansion)
    sqlite_keywords = {
        "select",
        "insert",
        "update",
        "delete",
        "from",
        "where",
        "table",
        "column",
        "index",
        "create",
        "alter",
        "drop",
        "primary",
        "key",
        "foreign",
        "references",
        "null",
        "not",
        "default",
        "check",
        "unique",
        "constraint",
        "order",
        "group",
        "by",
        "limit",
        "offset",
        "join",
        "left",
        "right",
        "inner",
        "outer",
        "on",
        "using",
        "values",
        "set",
    }
    if safe_name in sqlite_keywords:
        safe_name = f"_{safe_name}"  # Add prefix if it's a keyword

    # 7. Return prefix if name becomes empty after all processing
    return safe_name or prefix


def _infer_column_types(
    sample_data: List[Dict[str, Any]], column_keys: List[str]
) -> Dict[str, str]:
    """Infer SQLite types (INTEGER, REAL, TEXT) from sample data."""
    potential_types: Dict[str, set] = {
        key: {"INTEGER", "REAL", "TEXT"} for key in column_keys
    }

    for row in sample_data:
        for col_key in column_keys:
            # Use .get() in case of missing keys in sample (shouldn't happen if prepared correctly)
            value = row.get(col_key)
            if value is None or value == "":  # Treat empty strings/None as compatible
                continue

            # Convert value to string for consistent checking, unless it's already numeric
            if not isinstance(value, (int, float)):
                value_str = str(value).strip()
                if value_str == "":
                    continue
            else:
                value_str = str(value)  # Keep original type for later checks

            current_potentials = potential_types[col_key]
            if "TEXT" in current_potentials and len(current_potentials) == 1:
                continue  # Already determined as TEXT

            # Check Integer
            is_int = False
            if "INTEGER" in current_potentials:
                try:
                    int(value_str)
                    is_int = True
                except (ValueError, TypeError):
                    current_potentials.discard("INTEGER")

            # Check Real (Float)
            is_real = False
            if "REAL" in current_potentials:
                try:
                    float_val = float(value_str)
                    is_real = True
                    # If it was also a valid int, check if it has a fractional part
                    if is_int and not float_val.is_integer():
                        current_potentials.discard(
                            "INTEGER"
                        )  # Prefer REAL if fractional
                except (ValueError, TypeError):
                    current_potentials.discard("REAL")

            # If neither int nor real, it must be TEXT
            if not is_int and not is_real:
                potential_types[col_key] = {"TEXT"}

    # Determine final types
    final_types = {}
    for col_key, potentials in potential_types.items():
        if "INTEGER" in potentials:
            final_types[col_key] = "INTEGER"
        elif "REAL" in potentials:
            final_types[col_key] = "REAL"
        else:
            final_types[col_key] = "TEXT"  # Default to TEXT

    return final_types


def transform_to_sdif(state: State) -> Dict[str, Optional[str]]:
    """Transforms extracted tables from a spreadsheet into an SDIF database file.

    Reads table data based on ranges identified in the state, infers schemas,
    stores table data, metadata, and semantic links in a new SDIF file.
    """
    logger.info("--- Transforming Extracted Tables to SDIF ---")
    extracted_json = parse_json_from_last_message(state)
    if not extracted_json:
        logger.warning("Warning: No tables extracted in the previous step.")
        return {"output_sdif_path": None}
    if not isinstance(extracted_json, list):
        logger.warning(
            f"Warning: Expected a list of tables, but got type {type(extracted_json)}. Cannot proceed."
        )
        return {"output_sdif_path": None}

    try:
        extracted_tables = [Table(**table) for table in extracted_json]
    except Exception as e:
        logger.error(f"Error: Failed to parse extracted table data: {e}")
        # Potentially add the raw JSON to messages for debugging?
        return {"output_sdif_path": None}  # Indicate failure

    if not extracted_tables:
        logger.warning("Warning: Parsed table list is empty.")
        return {"output_sdif_path": None}

    spreadsheet_path = Path(state["spreadsheet_path"])
    output_sdif_path = spreadsheet_path.parent / spreadsheet_path.name.replace(
        spreadsheet_path.suffix, ".sdif"
    )
    processed_table_names: Set[str] = set()  # Keep track of generated table names

    logger.info(f"Creating SDIF database at: {output_sdif_path}")
    try:
        # Use overwrite=True as this node generates the file from scratch each time
        with SDIFDatabase(output_sdif_path, overwrite=True) as db:
            # 1. Add Source
            source_id = db.add_source(
                file_name=spreadsheet_path.name,
                file_type=spreadsheet_path.suffix.lstrip("."),  # e.g., 'xlsx'
                description=f"Data extracted from spreadsheet: {spreadsheet_path.name}",
            )
            logger.info(
                f"Added source '{spreadsheet_path.name}' with source_id: {source_id}"
            )

            # Initialize Spreadsheet Manager once
            spreadsheet_manager = AsposeCellsManager(
                workbook=get_workbook(state["spreadsheet_path"]),
            )

            # 2. Process Each Table
            for idx, table in enumerate(extracted_tables):
                logger.info(
                    f"\nProcessing extracted table {idx + 1}: '{table.title}' (Range: {table.range})"
                )
                try:
                    # a. Read Table Data
                    # Ensure range format is compatible if needed, assume Aspose handles "Sheet1!A1:C5"
                    table_data: List[List[Any]] = spreadsheet_manager.read_cells(
                        table.range
                    )

                    if (
                        not table_data or len(table_data) < 1
                    ):  # Need at least a header row potentially
                        logger.warning(
                            f"Warning: No data found or read for range '{table.range}'. Skipping table."
                        )
                        continue

                    # b. Determine Table Name (Sanitize & Handle Duplicates)
                    base_table_name = _sanitize_name(table.title, f"table_{idx}")
                    final_table_name = base_table_name
                    counter = 1
                    while final_table_name in processed_table_names:
                        final_table_name = f"{base_table_name}_{counter}"
                        counter += 1
                    processed_table_names.add(final_table_name)
                    logger.info(f"  Sanitized table name: {final_table_name}")

                    # c. Handle Headers (Assume first row is header) & Prepare Data
                    raw_headers = [
                        str(h) if h is not None else "" for h in table_data[0]
                    ]  # Convert potential non-strings
                    data_rows = table_data[1:]

                    if not raw_headers:
                        logger.warning(
                            f"Warning: Table '{final_table_name}' range '{table.range}' seems to have no header row. Skipping."
                        )
                        continue

                    col_name_counts = {}
                    final_columns_ordered: List[str] = []
                    original_headers_map: Dict[
                        str, str
                    ] = {}  # Map final name to original
                    column_definitions: Dict[str, Dict[str, Any]] = {}

                    for header_idx, header in enumerate(raw_headers):
                        base_col_name = _sanitize_name(header, f"column_{header_idx}")
                        final_col_name = base_col_name
                        count = col_name_counts.get(base_col_name, 0) + 1
                        col_name_counts[base_col_name] = count
                        if count > 1:
                            final_col_name = f"{base_col_name}_{count - 1}"

                        final_columns_ordered.append(final_col_name)
                        original_headers_map[final_col_name] = header
                        column_definitions[final_col_name] = {
                            "type": "TEXT",  # Default, will be inferred
                            "description": f"Original header: '{header}'.",
                            "original_format": None,  # Could potentially be inferred later
                        }

                    if not final_columns_ordered:
                        logger.warning(
                            f"Warning: No valid column names derived for table '{final_table_name}'. Skipping."
                        )
                        continue

                    # Prepare data as list of dicts
                    prepared_data: List[Dict[str, Any]] = []
                    for row_idx, data_row in enumerate(data_rows):
                        row_dict = {}
                        # Handle rows that might be shorter/longer than header
                        for i, col_name in enumerate(final_columns_ordered):
                            if i < len(data_row):
                                row_dict[col_name] = data_row[i]
                            else:
                                row_dict[col_name] = None  # Pad shorter rows with None
                        prepared_data.append(row_dict)

                    # d. Infer Column Types
                    sample_for_inference = prepared_data[
                        :SAMPLE_SIZE_FOR_TYPE_INFERENCE
                    ]
                    if sample_for_inference:
                        try:
                            inferred_types = _infer_column_types(
                                sample_for_inference, final_columns_ordered
                            )
                            for col_name, inferred_type in inferred_types.items():
                                column_definitions[col_name]["type"] = inferred_type
                            logger.info(f"  Inferred column types: {inferred_types}")
                        except Exception as infer_e:
                            logger.warning(
                                f"Warning: Type inference failed for table '{final_table_name}'. Defaulting all columns to TEXT. Error: {infer_e}"
                            )
                            # Types remain TEXT default

                    # e. Create SDIF Table
                    table_description = f"Data extracted from spreadsheet '{spreadsheet_path.name}', range '{table.range}'."
                    if table.metadata and table.metadata.get("description"):
                        table_description += (
                            f" Provided Description: {table.metadata['description']}"
                        )

                    db.create_table(
                        table_name=final_table_name,
                        columns=column_definitions,
                        source_id=source_id,
                        description=table_description,
                        original_identifier=f"{table.title} ({table.range})",  # Combine title and range
                    )
                    logger.info(f"  Created SDIF table '{final_table_name}' metadata.")

                    # f. Insert SDIF Data
                    if prepared_data:
                        db.insert_data(table_name=final_table_name, data=prepared_data)
                        logger.info(
                            f"  Inserted {len(prepared_data)} rows into '{final_table_name}'."
                        )
                    else:
                        logger.info(
                            f"  No data rows found to insert for '{final_table_name}'."
                        )

                    # g. Store Metadata as Object
                    if table.metadata:
                        metadata_object_name = f"metadata_{final_table_name}"
                        # Ensure metadata is JSON serializable (convert complex types if needed)
                        try:
                            db.add_object(
                                object_name=metadata_object_name,
                                json_data=table.metadata,
                                source_id=source_id,
                                description=f"Extracted metadata associated with table '{final_table_name}' (original range: {table.range}).",
                                schema_hint=None,  # Could generate a basic schema if needed
                            )
                            logger.info(
                                f"  Stored extracted metadata as SDIF object '{metadata_object_name}'."
                            )

                            # h. Add Semantic Link (Table <-> Metadata Object)
                            db.add_semantic_link(
                                link_type="annotation",
                                from_element_type="object",
                                from_element_spec={"object_name": metadata_object_name},
                                to_element_type="table",
                                to_element_spec={"table_name": final_table_name},
                                description=f"Links table '{final_table_name}' to its extracted metadata.",
                            )
                            logger.info(
                                f"  Added semantic link between table '{final_table_name}' and object '{metadata_object_name}'."
                            )
                        except TypeError as json_e:
                            logger.warning(
                                f"Warning: Metadata for table '{final_table_name}' is not JSON serializable. Skipping object/link creation. Error: {json_e}"
                            )
                        except Exception as db_e:
                            logger.warning(
                                f"Warning: Failed to store metadata object or link for table '{final_table_name}'. Error: {db_e}"
                            )
                    else:
                        logger.warning(
                            f"  No metadata provided for table '{final_table_name}', skipping object/link creation."
                        )

                except Exception as table_e:
                    logger.error(
                        f"Error processing table {idx + 1} ('{table.title}'): {table_e}"
                    )
                    # Continue processing other tables if possible

    except Exception as e:
        logger.error(
            f"Error creating or writing to SDIF database '{output_sdif_path}': {e}"
        )
        return {"output_sdif_path": None}  # Indicate failure

    logger.info(f"--- SDIF Transformation Complete: {output_sdif_path} ---")
    return {"output_sdif_path": str(output_sdif_path)}
