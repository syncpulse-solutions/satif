import logging
from pathlib import Path
from typing import Any, List, Union

try:
    import pandas as pd
except ImportError:
    pd = None

from satif_core.representers.base import Representer

log = logging.getLogger(__name__)


class XlsxRepresenter(Representer):
    """Generates representation for XLSX files using pandas."""

    def represent(
        self, file_path: Union[str, Path], num_rows: int = 10, **kwargs: Any
    ) -> str:
        """
        Generates a string representation of an XLSX file by showing
        the header and the first N data rows for each sheet.

        Kwargs Options:
            engine (str): Pandas engine for reading (default: 'openpyxl').
        """
        file_path = Path(file_path)

        log.debug(f"Reading XLSX representation for: {file_path}")

        if pd is None:
            raise ImportError(
                "The 'pandas' library is required to read XLSX files. Please install it (`pip install pandas openpyxl`)."
            )

        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        engine = kwargs.get("engine", "openpyxl")
        representation_lines: List[str] = []

        try:
            excel_data = pd.read_excel(
                file_path, sheet_name=None, engine=engine, dtype=str
            )

            if not excel_data:
                return "[Excel file contains no readable sheets or is empty]"

            for sheet_name, df in excel_data.items():
                # Add sheet separator
                if len(excel_data) > 1:
                    representation_lines.append(f"--- Sheet: {sheet_name} ---")
                # Add sheet name even for single sheet if not default 'Sheet1' (or 0)
                elif (
                    list(excel_data.keys())[0] != 0
                    and list(excel_data.keys())[0] != "Sheet1"
                ):
                    representation_lines.append(f"--- Sheet: {sheet_name} ---")

                if df.empty:
                    if not df.columns.empty and all(
                        str(c).startswith("Unnamed:") for c in df.columns
                    ):
                        representation_lines.append(
                            "[Sheet appears empty or header could not be identified properly]"
                        )
                    elif not df.columns.empty:
                        representation_lines.append(
                            ",".join(df.columns.astype(str).tolist())
                        )
                        representation_lines.append(
                            "[Sheet has header but no data rows]"
                        )
                    else:
                        representation_lines.append("[Sheet is empty]")
                    continue

                # Add header
                header = df.columns.astype(str).tolist()
                representation_lines.append(",".join(header))

                # Add sample rows
                sample_df = df.iloc[:num_rows]
                for row_tuple in sample_df.itertuples(index=False, name=None):
                    formatted_row = [
                        str(item) if pd.notna(item) else "" for item in row_tuple
                    ]
                    representation_lines.append(",".join(formatted_row))

                if len(sample_df) < len(df) and len(sample_df) < num_rows:
                    log.debug(
                        f"Read {len(sample_df)} data rows from sheet '{sheet_name}' in {file_path} (less than requested {num_rows})."
                    )

        except ImportError:
            # Should be caught by the check at the start, but defensive
            raise ImportError("Missing pandas/openpyxl library for XLSX.")
        except Exception as e:
            log.error(f"Error reading Excel file {file_path}: {e}")
            return f"[Error reading Excel file {file_path}: {e}]"

        if not representation_lines:
            # This case should be covered by excel_data check, but defensive
            return "[Could not generate representation from Excel file]"

        return "\n".join(representation_lines)
