import re
from pathlib import Path
from typing import (
    Dict,
    List,
    NotRequired,
    Optional,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

T = TypeVar("T")


class ColumnSpec(TypedDict):
    original_identifier: str
    final_column_name: str
    description: NotRequired[str]


ColumnDefinitionsInput = Union[
    List[ColumnSpec],
    Dict[str, List[ColumnSpec]],  # Map of table_name to column specs
]
ColumnDefinitionsConfig = Optional[
    Union[ColumnDefinitionsInput, List[Optional[ColumnDefinitionsInput]]]
]

SkipRowsConfig = Optional[Union[int, List[int], Set[int]]]
SkipColumnsConfig = Optional[
    Union[int, str, List[Union[int, str]], Set[Union[int, str]]]
]

ENCODING_SAMPLE_SIZE = 1024 * 12  # Bytes for encoding detection
DELIMITER_SAMPLE_SIZE = 1024 * 16  # Bytes for delimiter detection


def sanitize_sql_identifier(name: str, prefix: str = "item") -> str:
    """
    Clean up a string to be a safe SQL identifier.
    Replaces problematic characters with underscores, ensures it starts with a
    letter or underscore, and appends an underscore if it's a basic SQL keyword.
    """
    name = (
        name.strip().lower()
    )  # Trim whitespace and convert to lowercase for snake_case
    # Replace common problematic characters with underscores
    name = re.sub(r"[^\w\s-]", "", name)  # Keep word chars, whitespace, hyphen
    name = re.sub(r"[-\s]+", "_", name)  # Replace hyphens/whitespace with underscore
    # Ensure it starts with a letter or underscore if not empty
    safe_name = "".join(c for c in name if c.isalnum() or c == "_")

    # Ensure it's not a reserved SQL keyword (basic check)
    # TODO: Consider a more comprehensive list of SQL keywords from a library if precision is critical
    if safe_name.upper() in {
        "TABLE",
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "FROM",
        "WHERE",
        "GROUP",
        "ORDER",
        "BY",
        "INDEX",
        "ALTER",
        "CREATE",
        "DROP",
        "VALUES",
    }:
        safe_name = f"{safe_name}_"
    return safe_name or prefix  # Return prefix if name becomes empty


def normalize_list_argument(
    arg_value: Optional[Union[T, List[Optional[T]]]],
    arg_name_for_error: str,
    expected_len: int,
) -> List[Optional[T]]:
    """
    Normalizes an argument that can be a single item or a list into a list
    of a specific expected length.

    If arg_value is a single item, it's repeated expected_len times.
    If arg_value is a list, its length must match expected_len.
    If arg_value is None, a list of Nones of expected_len is returned.
    """
    if arg_value is None:
        return [None] * expected_len

    # isinstance check for dict should be specific if T can be Dict,
    # but for general purpose, checking if it's NOT a list first is safer.
    if not isinstance(arg_value, list) or isinstance(
        arg_value, dict
    ):  # Treat dict as a single item unless T is List[Dict] explicitly
        # Special handling for file_configs which can be a single dict
        if arg_name_for_error == "File configs" and isinstance(arg_value, dict):
            return [arg_value] * expected_len  # type: ignore -> T might be Dict
        # General case for single items (non-list)
        if not isinstance(arg_value, list):
            return [arg_value] * expected_len  # type: ignore -> T might be T

    # If it is a list
    if isinstance(arg_value, list):
        if len(arg_value) != expected_len:
            raise ValueError(
                f"{arg_name_for_error} list length ({len(arg_value)}) must match "
                f"input files count ({expected_len})."
            )
        return arg_value  # type: ignore -> T might be List[Optional[T]]

    # Fallback, though logically the above should cover typical single item or list cases.
    # This might be hit if T is complex and isn't caught by 'not isinstance(arg_value, list)'
    # For instance, if arg_value is a custom iterable object that isn't a list.
    # Defaulting to repeating it if it's not a list.
    return [arg_value] * expected_len  # type: ignore


def validate_skip_rows_config(
    config: SkipRowsConfig, file_name_for_error: Optional[str] = None
) -> SkipRowsConfig:
    """Validate types and values for skip_rows config."""
    error_context = f" (file: {file_name_for_error})" if file_name_for_error else ""
    if config is None:
        return 0
    if isinstance(config, int):
        if config < 0:
            raise ValueError(
                f"skip_rows integer value cannot be negative{error_context}."
            )
        return config
    elif isinstance(config, (list, set)):
        validated_set = set()
        for item in config:
            if not isinstance(item, int):
                raise TypeError(
                    f"skip_rows list/set must contain only integers{error_context}."
                )
            validated_set.add(item)
        return validated_set
    else:
        raise TypeError(
            f"skip_rows must be an integer, a list/set of integers, or None{error_context}."
        )


def validate_skip_columns_config(
    config: SkipColumnsConfig, file_name_for_error: Optional[str] = None
) -> SkipColumnsConfig:
    """Validate types and values for skip_columns config."""
    error_context = f" (file: {file_name_for_error})" if file_name_for_error else ""
    if config is None:
        return None
    if isinstance(config, (int, str)):
        if isinstance(config, int) and config < 0:
            raise ValueError(
                f"skip_columns integer index cannot be negative{error_context}."
            )
        return config
    elif isinstance(config, (list, set)):
        validated_items = []
        for item in config:
            if isinstance(item, int):
                if item < 0:
                    raise ValueError(
                        f"skip_columns indices in list/set cannot be negative{error_context}."
                    )
                validated_items.append(item)
            elif isinstance(item, str):
                validated_items.append(item)
            else:
                raise TypeError(
                    f"skip_columns list/set must contain only integers or strings{error_context}."
                )
        return validated_items
    else:
        raise TypeError(
            f"skip_columns must be an int, str, list/set of int/str, or None{error_context}."
        )


def parse_skip_rows_config(skip_rows_config: SkipRowsConfig) -> Union[int, Set[int]]:
    """Parse validated skip_rows config into int (for initial skip) or Set[int] (for indexed skip)."""
    if skip_rows_config is None:
        return 0
    if isinstance(skip_rows_config, int):
        return skip_rows_config
    elif isinstance(skip_rows_config, (list, set)):
        return set(skip_rows_config)
    else:
        raise TypeError(
            "Internal Error: Invalid type for processed skip_rows_config, expected validated config."
        )


def parse_skip_columns_config(
    skip_columns_config: SkipColumnsConfig,
) -> Tuple[Set[int], Set[str]]:
    """Parse validated skip_columns config into separate sets for indices and names."""
    skip_indices: Set[int] = set()
    skip_names: Set[str] = set()
    if skip_columns_config is None:
        return skip_indices, skip_names
    if isinstance(skip_columns_config, int):
        skip_indices.add(skip_columns_config)
    elif isinstance(skip_columns_config, str):
        skip_names.add(skip_columns_config)
    elif isinstance(skip_columns_config, (list, set)):
        for item in skip_columns_config:  # Assumes already validated
            if isinstance(item, int):
                skip_indices.add(item)
            elif isinstance(item, str):
                skip_names.add(item)
    else:
        raise TypeError(
            "Internal Error: Invalid type for processed skip_columns_config, expected validated config."
        )
    return skip_indices, skip_names


def detect_file_encoding(
    file_path: Path, sample_size: int = ENCODING_SAMPLE_SIZE
) -> str:
    """Detect file encoding using charset-normalizer."""
    from charset_normalizer import detect as charset_detect

    try:
        with open(file_path, "rb") as fb:
            data = fb.read(sample_size)
            if not data:
                return "utf-8"
            best_guess = charset_detect(data)
            if best_guess and best_guess.get("encoding"):
                return best_guess["encoding"]
            else:
                raise ValueError(
                    f"Encoding detection failed for {file_path.name}: No suitable encoding found by charset_normalizer."
                )
    except (
        ValueError
    ):  # Re-raise ValueError specifically if charset_detect couldn't find one
        raise
    except Exception as e:
        raise RuntimeError(
            f"Error during encoding detection for {file_path.name}: {e}"
        ) from e


def detect_csv_delimiter(sample_text: str) -> str:
    """Detect CSV delimiter using clevercsv.Sniffer."""
    import clevercsv

    if not sample_text:
        raise ValueError("Cannot detect delimiter from empty sample text.")
    try:
        sniffer = clevercsv.Sniffer()
        dialect = sniffer.sniff(sample_text)
        if dialect and dialect.delimiter:
            return dialect.delimiter
        else:
            raise ValueError("CleverCSV could not reliably determine the delimiter.")
    except clevercsv.Error as e:
        raise ValueError(f"Could not automatically detect CSV delimiter: {e}") from e
    except Exception as e:
        raise RuntimeError(
            f"An unexpected error occurred during delimiter detection: {e}"
        ) from e
