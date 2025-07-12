import csv
import decimal
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from typing import Counter as TypingCounter

from satif_core.comparators.base import Comparator

log = logging.getLogger(__name__)

# Helper type hint remains the same
CsvData = Tuple[
    Optional[List[str]],
    Optional[TypingCounter[Tuple[Any, ...]]],
    Optional[str],  # Allow Any for mixed types (str, float)
]


class CSVComparator(Comparator):
    """
    Compares two CSV files for equivalence based on specified criteria.

    Provides a detailed report on differences found in headers and row content.
    Supports options like ignoring row order, header case sensitivity, etc.
    """

    def _read_data(
        self,
        file_path: Union[str, Path],
        delimiter: Optional[str] = None,
        strip_whitespace: bool = True,
        encoding: str = "utf-8",
        decimal_places: Optional[int] = None,
    ) -> CsvData:
        """Helper to read CSV header and row data into a Counter."""
        file_path = Path(file_path)
        header: Optional[List[str]] = None
        row_counts: TypingCounter[Tuple[Any, ...]] = (
            Counter()
        )  # Allow Any type in tuple
        actual_delimiter = delimiter

        try:
            with open(file_path, newline="", encoding=encoding, errors="replace") as f:
                if actual_delimiter is None:
                    try:
                        sample_lines = [line for _, line in zip(range(10), f)]
                        sample = "".join(sample_lines)
                        if not sample:
                            log.debug(
                                f"File {file_path} appears empty during sniffing."
                            )
                            return None, Counter(), None
                        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                        actual_delimiter = dialect.delimiter
                        log.debug(
                            f"Detected delimiter '{actual_delimiter}' for {file_path}"
                        )
                        f.seek(0)
                    except (csv.Error, Exception) as sniff_err:
                        log.warning(
                            f"Could not sniff delimiter for {file_path}, defaulting to ','. Error: {sniff_err}"
                        )
                        actual_delimiter = ","
                        f.seek(0)

                reader = csv.reader(f, delimiter=actual_delimiter)
                try:
                    raw_header = next(reader)
                    header = [h.strip() if strip_whitespace else h for h in raw_header]
                    num_columns = len(header)

                    for i, row in enumerate(reader):
                        if len(row) != num_columns:
                            log.warning(
                                f"Row {i + 2} in {file_path} has {len(row)} columns, expected {num_columns}. Adapting row."
                            )
                            if len(row) > num_columns:
                                row = row[:num_columns]
                            else:
                                row.extend([""] * (num_columns - len(row)))

                        processed_row_values = []
                        for cell in row:
                            value: Any = cell.strip() if strip_whitespace else cell
                            if decimal_places is not None:
                                try:
                                    # Use Decimal for precise rounding
                                    d_value = decimal.Decimal(value)
                                    # Round to specified decimal places
                                    quantizer = decimal.Decimal(
                                        "1e-" + str(decimal_places)
                                    )
                                    value = d_value.quantize(
                                        quantizer, rounding=decimal.ROUND_HALF_UP
                                    )
                                    # Convert back to float for storage if needed, or keep as Decimal
                                    # Keeping as Decimal might be more precise but requires consumers to handle it
                                    # Let's convert back to float for broader compatibility, though precision issues might reappear
                                    value = float(value)
                                except (decimal.InvalidOperation, ValueError):
                                    # Keep as string if conversion fails
                                    pass
                            processed_row_values.append(value)

                        processed_row = tuple(processed_row_values)
                        row_counts[processed_row] += 1

                except StopIteration:
                    log.debug(f"File {file_path} is empty or header-only.")
                    return header, Counter(), None  # Return header if found, else None
                except Exception as read_err:
                    log.error(
                        f"Error reading CSV content from {file_path} after header: {read_err}"
                    )
                    return header, None, f"Error reading content: {read_err}"

            return header, row_counts, None

        except FileNotFoundError:
            log.error(f"File not found: {file_path}")
            return None, None, "File not found"
        except Exception as e:
            log.error(f"Failed to open or process file {file_path}: {e}")
            return None, None, f"Error opening/processing file: {e}"

    def _compare_headers(
        self,
        header1: Optional[List[str]],
        header2: Optional[List[str]],
        check_header_order: bool,
        check_header_case: bool,
    ) -> Dict[str, Any]:
        """Compares two sets of headers and returns comparison results."""
        diffs: List[str] = []
        result_text = "Identical"
        are_structurally_equivalent = True  # Assume true initially

        if header1 is None and header2 is None:
            result_text = "Both files have no header (or are empty)."
            # This is considered structurally equivalent
        elif header1 is None:
            result_text = "Different structure"
            diffs.append(f"File 1 has no header, File 2 header: {header2}")
            are_structurally_equivalent = False
        elif header2 is None:
            result_text = "Different structure"
            diffs.append(f"File 2 has no header, File 1 header: {header1}")
            are_structurally_equivalent = False
        else:
            h1_compare = header1 if check_header_case else [h.lower() for h in header1]
            h2_compare = header2 if check_header_case else [h.lower() for h in header2]

            if len(h1_compare) != len(h2_compare):
                result_text = "Different column count"
                diffs.append(
                    f"File 1 has {len(header1)} columns, File 2 has {len(header2)} columns."
                )
                diffs.append(f"File 1 Header: {header1}")
                diffs.append(f"File 2 Header: {header2}")
                are_structurally_equivalent = False
            elif check_header_order:
                if h1_compare != h2_compare:
                    result_text = "Different names or order"
                    are_structurally_equivalent = False
                    for i, (h1_val, h2_val) in enumerate(zip(h1_compare, h2_compare)):
                        if h1_val != h2_val:
                            orig_h1 = header1[i]
                            orig_h2 = header2[i]
                            case_note = (
                                ""
                                if check_header_case
                                or orig_h1.lower() != orig_h2.lower()
                                else " (differs only by case)"
                            )
                            diffs.append(
                                f"Column {i + 1}: File 1 '{orig_h1}' != File 2 '{orig_h2}'{case_note}"
                            )
                elif not check_header_case and header1 != header2:
                    # Same names and order, but different case
                    result_text = "Identical names/order (differs only by case)"
                    for i, (h1_val, h2_val) in enumerate(zip(header1, header2)):
                        if h1_val != h2_val:
                            diffs.append(
                                f"Column {i + 1} case difference: File 1 '{h1_val}', File 2 '{h2_val}'"
                            )
                    # This is considered structurally equivalent
            else:  # Ignore order
                if set(h1_compare) != set(h2_compare):
                    result_text = "Different names"
                    are_structurally_equivalent = False
                    only_h1 = set(h1_compare) - set(h2_compare)
                    only_h2 = set(h2_compare) - set(h1_compare)
                    if only_h1:
                        diffs.append(f"Headers only in File 1: {list(only_h1)}")
                    if only_h2:
                        diffs.append(f"Headers only in File 2: {list(only_h2)}")
                elif h1_compare != h2_compare:  # Same names, different order
                    result_text = "Identical names (different order)"
                    diffs.append(f"File 1 Header Order: {header1}")
                    diffs.append(f"File 2 Header Order: {header2}")
                    # This is considered structurally equivalent

        return {
            "result_text": result_text,
            "diffs": diffs,
            "are_structurally_equivalent": are_structurally_equivalent,
        }

    def _compare_rows(
        self,
        rows1_counter: TypingCounter[Tuple[Any, ...]],
        rows2_counter: TypingCounter[Tuple[Any, ...]],
        ignore_row_order: bool,
        decimal_places: Optional[int],
        max_examples: int,
        file_path1_name: str,
        file_path2_name: str,
        # Assuming headers are compatible if this function is called
    ) -> Dict[str, Any]:
        """Compares row content and returns comparison results."""
        row_count1 = sum(rows1_counter.values())
        row_count2 = sum(rows2_counter.values())

        details: Dict[str, Any] = {
            "result": "Comparing...",
            "row_count1": row_count1,
            "row_count2": row_count2,
            "unique_rows1": [],
            "unique_rows2": [],
            "count_diffs": [],
        }
        summary_messages: List[str] = []
        are_content_equivalent = True  # Assume true initially

        precision_text = (
            f" (within {decimal_places} decimal places)"
            if decimal_places is not None
            else ""
        )

        if ignore_row_order:
            details["result"] = f"Comparing content (order ignored){precision_text}..."

            if rows1_counter == rows2_counter:
                details["result"] = f"Identical content{precision_text}"
                summary_messages.append(
                    f"Row content is identical{precision_text} ({row_count1} rows)."
                )
            else:
                are_content_equivalent = False
                details["result"] = f"Different content{precision_text}"
                summary_messages.append(f"Row content differs{precision_text}.")

                unique_keys1 = list((rows1_counter - rows2_counter).keys())
                unique_keys2 = list((rows2_counter - rows1_counter).keys())

                details["unique_rows1"] = [
                    list(row) for row in unique_keys1[:max_examples]
                ]
                if len(unique_keys1) > 0:
                    details["result"] += " (unique rows found)"
                    summary_messages.append(
                        f"Found {len(unique_keys1)} unique row(s) in {file_path1_name}."
                    )

                details["unique_rows2"] = [
                    list(row) for row in unique_keys2[:max_examples]
                ]
                if len(unique_keys2) > 0:
                    details["result"] += (
                        " (unique rows found)"  # Potentially redundant if already added
                    )
                    summary_messages.append(
                        f"Found {len(unique_keys2)} unique row(s) in {file_path2_name}."
                    )

                # Iterate over all keys present in either counter to find count differences
                all_keys = rows1_counter.keys() | rows2_counter.keys()
                for key in all_keys:
                    c1 = rows1_counter.get(key, 0)
                    c2 = rows2_counter.get(key, 0)
                    if (
                        c1 != c2 and key not in unique_keys1 and key not in unique_keys2
                    ):  # Only rows that are not already unique
                        # This condition for key not in unique_keys might be tricky
                        # Let's simplify: report diff if counts are different, after unique rows are handled
                        pass  # This part is complex with unique_keys, simplify for now

                # Simpler approach for count_diffs:
                # Report rows present in both but with different counts
                # This was already part of the original logic.
                common_keys_with_diff_counts = []
                # Iterate over the smaller set of keys for efficiency
                iter_keys = (
                    rows1_counter.keys()
                    if len(rows1_counter) < len(rows2_counter)
                    else rows2_counter.keys()
                )
                for key in iter_keys:
                    if (
                        key in rows1_counter
                        and key in rows2_counter
                        and rows1_counter[key] != rows2_counter[key]
                    ):
                        common_keys_with_diff_counts.append(
                            {
                                "row": list(key),
                                "count1": rows1_counter[key],
                                "count2": rows2_counter[key],
                            }
                        )

                details["count_diffs"] = common_keys_with_diff_counts[:max_examples]
                if len(common_keys_with_diff_counts) > 0:
                    details["result"] += " (count differences found)"
                    summary_messages.append(
                        f"Found {len(common_keys_with_diff_counts)} row(s) with different occurrence counts{precision_text}."
                    )

                if (
                    row_count1 != row_count2
                ):  # This summary is useful if overall counts differ
                    summary_messages.append(
                        f"Total row counts differ: {file_path1_name} has {row_count1}, {file_path2_name} has {row_count2}."
                    )
        else:  # Compare row order
            details["result"] = "Comparing content (order matters)..."
            if row_count1 != row_count2:
                are_content_equivalent = False
                details["result"] = "Different row counts"
                summary_messages.append(
                    f"Row counts differ (order matters): File 1 has {row_count1}, File 2 has {row_count2}."
                )
            else:
                # This part requires reading files into lists of tuples, not just counters
                # The current _read_data only returns counters.
                # For a true ordered comparison, _read_data would need to change or a new reader implemented.
                # For now, if counts match and order matters, we can't confirm full equivalence with current data.
                details["result"] = (
                    "Ordered comparison beyond count matching not fully implemented with current data structure (counts match)"
                )
                summary_messages.append(
                    f"Files have the same number of rows{precision_text}. "
                    "Detailed ordered comparison of content is not fully supported by the current counter-based approach. "
                    "If content identity with order is critical, manual review or an alternative comparison method might be needed."
                )
                # We cannot definitively say they are equivalent or not without row-by-row check.
                # For now, if counts match, let's assume not different unless proven otherwise (conservative).
                # However, the spirit of this `else` branch means order matters.
                # If we cannot compare ordered content, we should state that.
                # Let's assume if counts match, and we can't do full ordered check, they are *not* proven different by this check.
                # But for the purpose of `are_content_equivalent`, if we didn't find a difference, it's true.
                # This is tricky. The original code had a similar placeholder.
                # For `check_structure_only=False`, this path means the user WANTS ordered comparison.
                # If we can't do it, we can't say they are equivalent.
                # Let's mark are_content_equivalent = False if full ordered check is not done.
                # Or rather, state that the comparison is incomplete.
                # For now, let's assume if counts match and order matters, we cannot confirm equivalence with current structure.
                # Let's make are_content_equivalent depend on a full ordered comparison if it were implemented.
                # Since it's not, we can't say true. Let's assume not equivalent if order matters and we can't verify.
                # This is a policy decision. Let's keep it simple: if counts match, it does not *prove* difference.
                # The original code didn't set results["are_equivalent"] = False here.
                # Let's align with that: if counts match, no difference found *by this specific check*.
                raise NotImplementedError(
                    "Ordered comparison beyond count matching not fully implemented with current data structure (counts match)"
                )

        return {
            "are_content_equivalent": are_content_equivalent,
            "details": details,
            "summary_messages": summary_messages,
        }

    def compare(
        self,
        file_path1: Union[str, Path],
        file_path2: Union[str, Path],
        file_config: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Compares two CSV files using specified options.

        Kwargs Options:
            ignore_row_order (bool): Compare row content regardless of order (default: True).
            check_header_order (bool): Require header columns in the same order (default: True).
            check_header_case (bool): Ignore case when comparing header names (default: True).
            strip_whitespace (bool): Strip leading/trailing whitespace from headers/cells (default: True).
            delimiter (Optional[str]): Delimiter for both files (default: auto-detect).
            encoding (str): Text encoding for reading files (default: 'utf-8').
            decimal_places (Optional[int]): Number of decimal places to consider for float comparison (default: 2 - 0.01 precision).
            max_examples (int): Max number of differing row examples (default: 5).
            check_structure_only (bool): If True, only compare headers. Row data is ignored for equivalence (default: False).
        """
        # --- Extract parameters with defaults ---
        file_path1 = Path(file_path1)
        file_path2 = Path(file_path2)
        ignore_row_order: bool = kwargs.get("ignore_row_order", True)
        check_header_order: bool = kwargs.get("check_header_order", True)
        check_header_case: bool = kwargs.get("check_header_case", True)
        strip_whitespace: bool = kwargs.get("strip_whitespace", True)
        delimiter: Optional[str] = kwargs.get("delimiter", None)
        encoding: str = kwargs.get("encoding", "utf-8")
        max_examples: int = kwargs.get("max_examples", 5)
        decimal_places: Optional[int] = kwargs.get("decimal_places", 2)
        check_structure_only: bool = kwargs.get("check_structure_only", False)

        # --- Initialize results structure ---
        results: Dict[str, Any] = {
            "files": {"file1": str(file_path1), "file2": str(file_path2)},
            "comparison_params": {
                "ignore_row_order": ignore_row_order,
                "check_header_order": check_header_order,
                "check_header_case": check_header_case,
                "strip_whitespace": strip_whitespace,
                "delimiter": delimiter,
                "encoding": encoding,
                "decimal_places": decimal_places,
                "max_examples": max_examples,
                "check_structure_only": check_structure_only,
            },
            "are_equivalent": True,
            "summary": [],
            "details": {
                "errors": [],
                "header_comparison": {"result": "Not compared", "diff": []},
                "row_comparison": {"result": "Not compared"},
            },
        }

        # --- Read Data ---
        header1, rows1_counter, error1 = self._read_data(
            file_path1,
            delimiter,
            strip_whitespace,
            encoding,
            decimal_places,
        )
        header2, rows2_counter, error2 = self._read_data(
            file_path2,
            delimiter,
            strip_whitespace,
            encoding,
            decimal_places,
        )

        if error1:
            results["details"]["errors"].append(f"File 1 ({file_path1.name}): {error1}")
            results["are_equivalent"] = False
        if error2:
            results["details"]["errors"].append(f"File 2 ({file_path2.name}): {error2}")
            results["are_equivalent"] = False

        if error1 or error2 or rows1_counter is None or rows2_counter is None:
            # This check handles cases where _read_data returns None for row_counts due to errors
            results["summary"].append(
                "Comparison aborted due to errors reading file(s)."
            )
            results["details"]["row_comparison"]["row_count1"] = (
                sum(rows1_counter.values()) if rows1_counter else -1
            )
            results["details"]["row_comparison"]["row_count2"] = (
                sum(rows2_counter.values()) if rows2_counter else -1
            )
            if not results["details"][
                "errors"
            ]:  # Ensure some error reported if not already
                results["summary"].insert(
                    0, "Files are considered different due to read errors."
                )
            else:
                results["summary"].insert(
                    0,
                    f"Files are considered different: {', '.join(results['details']['errors'])}",
                )
            return results

        # --- Compare Headers ---
        header_comp_result = self._compare_headers(
            header1, header2, check_header_order, check_header_case
        )
        results["details"]["header_comparison"]["result"] = header_comp_result[
            "result_text"
        ]
        results["details"]["header_comparison"]["diff"] = header_comp_result["diffs"]

        # Update summary based on header comparison result
        if not header_comp_result["are_structurally_equivalent"]:
            results["summary"].append(
                f"Headers differ: {header_comp_result['result_text']}."
            )
            results["are_equivalent"] = False
        elif (
            header_comp_result["result_text"] != "Identical"
        ):  # Structurally equivalent but with minor diffs (case, order)
            results["summary"].append(
                f"Headers are equivalent but differ slightly: {header_comp_result['result_text']}."
            )
        else:  # Identical
            results["summary"].append("Headers are identical.")

        # --- Handle check_structure_only ---
        if check_structure_only:
            # Equivalence is solely based on header structural equivalence
            results["are_equivalent"] = header_comp_result[
                "are_structurally_equivalent"
            ]
            results["summary"].append(
                "Comparison focused on structure only. Row content was not compared."
            )
            results["details"]["row_comparison"] = {
                "result": "Skipped (check_structure_only enabled)",
                "row_count1": sum(
                    rows1_counter.values()
                ),  # rows1_counter is not None here
                "row_count2": sum(
                    rows2_counter.values()
                ),  # rows2_counter is not None here
            }
            # Final summary message will be set based on results["are_equivalent"] later
        else:
            # --- Compare Rows (if headers allow and not structure_only) ---
            # Rows can only be meaningfully compared if headers are structurally sound for content comparison
            # (e.g. same number of columns). This is covered by are_structurally_equivalent.
            if not header_comp_result["are_structurally_equivalent"]:
                results["summary"].append(
                    "Row content not compared due to significant header differences."
                )
                results["details"]["row_comparison"] = {
                    "result": "Not compared (header mismatch)",
                    "row_count1": sum(rows1_counter.values()),
                    "row_count2": sum(rows2_counter.values()),
                }
            else:
                # Headers are structurally equivalent, proceed with row comparison
                row_comp_output = self._compare_rows(
                    rows1_counter,
                    rows2_counter,
                    ignore_row_order,
                    decimal_places,
                    max_examples,
                    file_path1.name,
                    file_path2.name,
                )
                results["details"]["row_comparison"] = row_comp_output["details"]
                results["summary"].extend(row_comp_output["summary_messages"])

                # If headers were equivalent, overall equivalence now also depends on row content
                if results[
                    "are_equivalent"
                ]:  # True if headers were perfectly identical or equivalent with minor diffs
                    results["are_equivalent"] = row_comp_output[
                        "are_content_equivalent"
                    ]

        # --- Final Summary ---
        if results["are_equivalent"]:
            results["summary"].insert(
                0, "Files are considered equivalent based on the specified parameters."
            )
        else:
            results["summary"].insert(
                0, "Files are considered different based on the specified parameters."
            )

        return results
