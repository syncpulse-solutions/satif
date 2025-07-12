import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from satif_core.representers.base import Representer

from satif_sdk.utils import detect_csv_delimiter, detect_file_encoding

log = logging.getLogger(__name__)


class CSVRepresenter(Representer):
    """
    Generates representation for CSV files.
    Can be initialized with default encoding and delimiter.
    """

    def __init__(
        self,
        default_delimiter: Optional[str] = None,
        default_encoding: str = "utf-8",
        default_num_rows: int = 10,
    ):
        """
        Initialize CSVRepresenter.

        Args:
            default_delimiter: Default CSV delimiter. Auto-detected if None.
            default_encoding: Default file encoding.
            default_num_rows: Default number of data rows to represent.
        """
        self.default_delimiter = default_delimiter
        self.default_encoding = default_encoding
        self.default_num_rows = default_num_rows

    def represent(
        self,
        file_path: Union[str, Path],
        num_rows: Optional[int] = None,  # Allow None to use instance default
        **kwargs: Any,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Generates a string representation of a CSV file by showing
        the header and the first N data rows.

        Kwargs Options:
            encoding (str): File encoding. Overrides instance default.
            delimiter (str): CSV delimiter. Overrides instance default.

        Returns:
            Tuple[str, Dict[str, Any]]:
                - The string representation.
                - A dictionary containing used parameters: 'encoding' and 'delimiter'.
        """
        file_path = Path(file_path)
        actual_num_rows = num_rows if num_rows is not None else self.default_num_rows

        used_params: Dict[str, Any] = {}
        representation_lines: List[str] = []

        if not file_path.is_file():
            err_msg = f"File not found: {file_path}"
            log.error(err_msg)
            used_params["error"] = err_msg
            return f"[{err_msg}]", used_params

        # 1. Determine Encoding
        final_encoding: str = kwargs.get("encoding")
        if final_encoding:
            log.debug(f"Using encoding from kwargs: {final_encoding} for {file_path}")
        elif self.default_encoding:  # self.default_encoding is always set in __init__
            final_encoding = self.default_encoding
            log.debug(
                f"Using instance default encoding: {final_encoding} for {file_path}"
            )
        else:
            try:
                final_encoding = detect_file_encoding(file_path)
                log.debug(f"Detected encoding: {final_encoding} for {file_path}")
            except Exception:
                err_msg = f"Error detecting encoding for {file_path}"
                log.error(err_msg)
                used_params["encoding_error"] = err_msg
                return f"[{err_msg}]", used_params
        used_params["encoding"] = final_encoding

        # 2. Determine Delimiter
        final_delimiter: Optional[str] = kwargs.get("delimiter")
        if final_delimiter:
            log.debug(
                f"Using delimiter from kwargs: '{final_delimiter}' for {file_path}"
            )
        elif self.default_delimiter is not None:
            final_delimiter = self.default_delimiter
            log.debug(
                f"Using instance default delimiter: '{final_delimiter}' for {file_path}"
            )
        else:
            final_delimiter = detect_csv_delimiter(file_path, final_encoding)
            if final_delimiter:
                log.debug(f"Detected delimiter: '{final_delimiter}' for {file_path}")
            else:
                log.warning(
                    f"Failed to detect delimiter for {file_path}. Defaulting to ','"
                )
                final_delimiter = ","  # Fallback delimiter
        used_params["delimter"] = final_delimiter

        try:
            with open(
                file_path, newline="", encoding=final_encoding, errors="replace"
            ) as f:
                reader = csv.reader(f, delimiter=final_delimiter)
                try:
                    header = next(reader)
                    representation_lines.append(final_delimiter.join(header))
                    rows_read_count = 0
                    for row in reader:
                        if rows_read_count >= actual_num_rows:
                            break
                        representation_lines.append(final_delimiter.join(map(str, row)))
                        rows_read_count += 1

                    if rows_read_count < actual_num_rows and rows_read_count > -1:
                        log.debug(
                            f"Read {rows_read_count} data rows from {file_path} (less than requested {actual_num_rows})."
                        )
                    if not representation_lines:  # Empty file even before header
                        log.debug(
                            f"CSV file {file_path} appears empty before header read."
                        )
                        return "[CSV file appears empty]", used_params

                except StopIteration:
                    if representation_lines:  # Header was read but no data rows
                        log.debug(f"CSV file {file_path} has header but no data rows.")
                        representation_lines.append("[No data rows found]")
                    else:  # File was completely empty or unreadable by csv.reader
                        log.debug(
                            f"CSV file {file_path} is empty or could not be parsed by CSV reader."
                        )
                        return "[CSV file is empty or unparsable]", used_params
                except csv.Error as e:  # Catch specific CSV parsing errors
                    err_msg = f"CSV parsing error in {file_path}: {e}"
                    log.error(err_msg)
                    used_params["error"] = err_msg
                    return f"[{err_msg}]", used_params
                except Exception as e:  # Catch other unexpected errors during reading
                    err_msg = f"Error reading CSV content from {file_path}: {e}"
                    log.error(err_msg, exc_info=True)
                    used_params["error"] = err_msg
                    return f"[{err_msg}]", used_params

        except FileNotFoundError:  # Should be caught earlier, but defensive
            err_msg = f"File not found: {file_path}"
            log.error(err_msg)
            used_params["error"] = err_msg
            return f"[{err_msg}]", used_params
        except UnicodeDecodeError as e:
            err_msg = f"Encoding error opening {file_path} with encoding '{final_encoding}': {e}"
            log.error(err_msg, exc_info=True)
            used_params["error"] = err_msg
            used_params["encoding_tried"] = final_encoding
            return f"[{err_msg}]", used_params
        except Exception as e:
            err_msg = f"Error opening or processing CSV file {file_path}: {e}"
            log.error(err_msg, exc_info=True)
            used_params["error"] = err_msg
            return f"[{err_msg}]", used_params

        if not representation_lines:  # If somehow it's still empty
            return (
                "[No representation generated, file might be empty or unreadable]",
                used_params,
            )

        return "\\n".join(representation_lines), used_params

    def as_base64_image(self, file_path: str | Path, **kwargs: Any) -> str:
        return "Unsupported operation."

    def as_text(self, file_path: str | Path, **kwargs: Any) -> str:
        return self.represent(file_path, **kwargs)[0]
