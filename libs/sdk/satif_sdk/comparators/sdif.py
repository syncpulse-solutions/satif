import decimal
import logging
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import numpy as np
import pandas as pd
from satif_core.comparators.base import Comparator
from satif_core.sdif_db import SDIFDatabase

log = logging.getLogger(__name__)

# Type hints
DbConnection = sqlite3.Connection
QueryResult = List[Tuple[Any, ...]]
MetadataDict = Dict[str, Any]
SourceMap = Dict[int, int]  # Maps source_id from file1 to source_id in file2
TableMap = Dict[
    str, Optional[str]
]  # Maps table_name from file1 to table_name in file2 (or None if no match)
ColumnMap = Dict[
    str, Optional[str]
]  # Maps col_name from table1 to col_name in table2 (or None if no match)
NameMap = Dict[
    str, Optional[str]
]  # Maps object/media name from file1 to file2 (or None if no match)


class SDIFComparator(Comparator):
    """
    Compares two SDIF (SQLite Data Interoperable Format) files for equivalence
    using the SDIFDatabase helper class.

    Focuses on comparing the structure and content of user data tables,
    JSON objects, and media data, based on the SDIF specification (v1.0).
    Allows configuration to ignore certain names or metadata aspects.
    """

    # --- Helper Functions ---

    def _normalize_value(self, value: Any, decimal_places: Optional[int]) -> Any:
        """Normalizes numeric values for comparison."""
        if decimal_places is not None and isinstance(value, (float, decimal.Decimal)):
            try:
                # Use Decimal for precise rounding
                d_value = decimal.Decimal(
                    str(value)
                )  # Ensure Decimal from string rep if float
                quantizer = decimal.Decimal("1e-" + str(decimal_places))
                rounded_value = d_value.quantize(
                    quantizer, rounding=decimal.ROUND_HALF_UP
                )
                # Convert back to float for comparison consistency, accepting potential minor precision loss after rounding
                return float(rounded_value)
            except (decimal.InvalidOperation, ValueError, TypeError):
                return value  # Keep original if conversion/rounding fails
        elif isinstance(value, float) and np.isnan(value):
            return None  # Treat NaN as None for comparison consistency
        # TODO: Consider specific handling for date/time types if needed
        return value

    def _compare_json_objects(self, obj1: Any, obj2: Any) -> bool:
        """
        Compares two Python objects derived from JSON for equivalence.
        Basic comparison for now, assumes order matters in lists.
        """
        # TODO: Implement more robust JSON comparison (e.g., ignore dict key order) if needed
        return obj1 == obj2

    # --- Comparison Logic using SDIFDatabase Schema ---

    def _compare_properties(
        self, props1: Optional[Dict], props2: Optional[Dict]
    ) -> Tuple[bool, List[str]]:
        """Compares sdif_properties."""
        diffs = []
        equivalent = True
        if not props1 or not props2:
            diffs.append(
                "sdif_properties missing or could not be read in one or both files."
            )
            return False, diffs
        # Only compare sdif_version as per RFC focus
        version1 = props1.get("sdif_version")
        version2 = props2.get("sdif_version")
        if version1 != version2:
            diffs.append(
                f"SDIF versions differ: File1='{version1}', File2='{version2}'"
            )
            equivalent = False
        if version1 != "1.0" or version2 != "1.0":
            diffs.append(
                f"Warning: Comparison logic assumes SDIF version 1.0. Found: File1='{version1}', File2='{version2}'"
            )
            # Don't mark as different solely based on non-1.0 version if they match, but warn.
        return equivalent, diffs

    def _compare_sources(
        self,
        sources1: List[Dict],
        sources2: List[Dict],
        ignore_original_file_name: bool,
    ) -> Tuple[bool, List[str], Optional[SourceMap]]:
        """Compares sdif_sources from schema and creates a mapping."""
        diffs = []
        source_map: SourceMap = {}
        matched2_indices: Set[int] = set()
        equivalent = True

        # Key function based on original_file_type and optionally original_file_name
        # Using tuple for dictionary key
        key_func = (  # noqa: E731
            lambda s: (s["original_file_type"],)
            if ignore_original_file_name
            else (s.get("original_file_name", None), s["original_file_type"])
        )

        try:
            # Index sources for easier lookup: {key: source_dict}
            sources1_dict = {key_func(s): s for s in sources1}
            # Index sources from file2: {key: (list_index, source_dict)}
            sources2_dict = {key_func(s): (idx, s) for idx, s in enumerate(sources2)}
        except TypeError as e:
            diffs.append(f"Error creating source keys (possibly unhashable types): {e}")
            return False, diffs, None

        for key1, source1_row in sources1_dict.items():
            source1_id = source1_row["source_id"]
            if key1 in sources2_dict:
                idx2, source2_row = sources2_dict[key1]
                if idx2 not in matched2_indices:
                    # Check for other potentially relevant attributes if needed in future
                    # For now, matching key is sufficient for mapping
                    source_map[source1_id] = source2_row["source_id"]  # Map id1 -> id2
                    matched2_indices.add(idx2)
                    log.debug(
                        f"Mapped Source ID {source1_id} -> {source2_row['source_id']} based on key {key1}"
                    )
                else:
                    # Duplicate key match in file2 - indicates ambiguity
                    diffs.append(
                        f"Source key {key1} (from File1 ID {source1_id}) matches multiple sources in File 2."
                    )
                    equivalent = False
                    # Invalidate the specific mapping attempt? For now, just report difference.
            else:
                diffs.append(
                    f"Source present only in File 1: ID={source1_id}, Key={key1}"
                )
                equivalent = False

        # Check for sources present only in file2
        for key2, (idx2, source2_row) in sources2_dict.items():
            if idx2 not in matched2_indices:
                diffs.append(
                    f"Source present only in File 2: ID={source2_row['source_id']}, Key={key2}"
                )
                equivalent = False

        if not equivalent:
            log.warning(f"Source comparison failed: {diffs}")
            return False, diffs, None  # Return None map if not equivalent

        log.debug(f"Source comparison successful. Map: {source_map}")
        return True, diffs, source_map

    def _compare_all_user_tables(
        self,
        db1: SDIFDatabase,
        db2: SDIFDatabase,
        schema1: Dict,
        schema2: Dict,
        source_map: SourceMap,
        **kwargs: Any,
    ) -> Tuple[bool, List[str], TableMap]:
        """Compares all user tables based on schema and data."""
        overall_equivalent = True
        all_diffs: List[str] = []
        table_map: TableMap = {}  # Maps table1 name to table2 name or None

        tables1_schema = schema1.get("tables", {})
        tables2_schema = schema2.get("tables", {})
        table_names1 = set(tables1_schema.keys())
        table_names2 = set(tables2_schema.keys())

        ignore_table_names = kwargs.get("ignore_user_table_names", False)
        # TODO: Implement mapping via original_identifier if ignore_table_names is True
        if ignore_table_names:
            all_diffs.append(
                "WARNING: ignore_user_table_names=True mapping by original_identifier not yet implemented. Comparing by name."
            )

        # Find common tables (by name for now) and unique tables
        common_tables = table_names1.intersection(table_names2)
        unique_tables1 = table_names1 - table_names2
        unique_tables2 = table_names2 - table_names1

        if unique_tables1:
            all_diffs.append(
                f"Tables only in File 1: {', '.join(sorted(list(unique_tables1)))}"
            )
            overall_equivalent = False
            for t_name in unique_tables1:
                table_map[t_name] = None
        if unique_tables2:
            all_diffs.append(
                f"Tables only in File 2: {', '.join(sorted(list(unique_tables2)))}"
            )
            overall_equivalent = False
            # Cannot map from file1 for tables only in file2

        # Compare common tables
        for table_name in common_tables:
            log.debug(f"Comparing table: {table_name}")
            table_map[table_name] = table_name  # Direct mapping by name
            table1_schema = tables1_schema[table_name]
            table2_schema = tables2_schema[table_name]

            # 1. Compare Source ID using source_map
            source_id1 = table1_schema.get("metadata", {}).get("source_id")
            source_id2 = table2_schema.get("metadata", {}).get("source_id")
            mapped_source_id2 = source_map.get(
                cast(int, source_id1)
            )  # source_id1 should be int

            if source_id1 is None or source_id2 is None:
                all_diffs.append(
                    f"Table '{table_name}': Missing source_id metadata in one or both files."
                )
                overall_equivalent = False
            elif mapped_source_id2 != source_id2:
                all_diffs.append(
                    f"Table '{table_name}': Source ID mismatch (File1 Source ID {source_id1} maps to {mapped_source_id2}, but File2 has Source ID {source_id2})."
                )
                overall_equivalent = False

            # 2. Compare Schema (Columns, FKs) & Data
            is_equiv, diffs = self._compare_single_table(
                db1, db2, table_name, table_name, table1_schema, table2_schema, **kwargs
            )
            if not is_equiv:
                overall_equivalent = False
                all_diffs.extend([f"Table '{table_name}': {d}" for d in diffs])
            else:
                all_diffs.append(
                    f"Table '{table_name}': Structure and data are equivalent."
                )

        return overall_equivalent, all_diffs, table_map

    def _compare_single_table(
        self,
        db1: SDIFDatabase,
        db2: SDIFDatabase,
        table_name1: str,
        table_name2: str,
        schema1: Dict,
        schema2: Dict,
        **kwargs: Any,
    ) -> Tuple[bool, List[str]]:
        """Compares schema and data of a single mapped table."""
        equivalent = True
        diffs: List[str] = []

        cols1 = schema1.get("columns", [])
        cols2 = schema2.get("columns", [])
        fks1 = schema1.get("foreign_keys", [])
        fks2 = schema2.get("foreign_keys", [])

        ignore_col_names = kwargs.get("ignore_user_column_names", False)
        compare_row_order = kwargs.get("compare_user_table_row_order", False)
        decimal_places = kwargs.get("decimal_places", None)
        max_examples = kwargs.get("max_examples", 5)

        # --- 1. Compare Column Schema ---
        col_map: ColumnMap = {}  # Map col1_name -> col2_name or None
        col_diffs, schema_equivalent, col_map = self._compare_column_schemas(
            cols1, cols2, ignore_col_names
        )
        if not schema_equivalent:
            diffs.extend(col_diffs)
            equivalent = False
            # Proceed to data comparison? Maybe not if schema differs significantly?
            # For now, let's stop if column schemas don't align well enough to map.
            diffs.append("Aborting data comparison due to schema differences.")
            return False, diffs

        # --- 2. Compare Foreign Keys ---
        # Basic comparison: check if sets of FK definitions are the same
        # Need a canonical representation for comparison (e.g., tuple of sorted dict items)
        def fk_to_tuple(fk: Dict) -> Tuple:
            # Select key fields, sort by key, convert to tuple for hashing/set comparison
            key_fields = {
                "from_column",
                "target_table",
                "target_column",
                "on_update",
                "on_delete",
                "match",
            }
            return tuple(sorted([(k, fk[k]) for k in key_fields if k in fk]))

        fk_set1 = {fk_to_tuple(fk) for fk in fks1}
        fk_set2 = {fk_to_tuple(fk) for fk in fks2}

        if fk_set1 != fk_set2:
            diffs.append("Foreign key definitions differ.")
            # List differences (more detailed reporting can be added)
            if fk_set1 - fk_set2:
                diffs.append(f"  FKs only in File 1: {fk_set1 - fk_set2}")
            if fk_set2 - fk_set1:
                diffs.append(f"  FKs only in File 2: {fk_set2 - fk_set1}")
            equivalent = False
            # Decide if FK diff prevents data comparison? Probably not, data could still match.

        # --- 3. Compare Table Data ---
        try:
            df1 = db1.read_table(table_name1)
            df2 = db2.read_table(table_name2)

            # Rename columns in df2 according to col_map for alignment
            rename_dict = {v: k for k, v in col_map.items() if v is not None}
            try:
                # Ensure all target columns exist in df2 before renaming
                missing_cols = [
                    col for col in rename_dict.keys() if col not in df2.columns
                ]
                if missing_cols:
                    raise ValueError(
                        f"Mapped columns missing in DataFrame for table '{table_name2}': {missing_cols}"
                    )
                df2_renamed = df2.rename(columns=rename_dict)
                # Select only the columns that are present and mapped in df1
                df2_aligned = df2_renamed[list(col_map.keys())]
            except Exception as e:
                diffs.append(f"Error aligning columns for data comparison: {e}")
                return False, diffs

            # Normalize data (especially numerics)
            for col in df1.columns:
                # Apply normalization based on target type or general rules
                df1[col] = df1[col].apply(
                    lambda x: self._normalize_value(x, decimal_places)
                )
                if col in df2_aligned.columns:
                    df2_aligned[col] = df2_aligned[col].apply(
                        lambda x: self._normalize_value(x, decimal_places)
                    )

            # Perform comparison based on row order preference
            if not compare_row_order:
                # Use Counter method (similar to CSVComparator)
                try:
                    # Convert rows to tuples of primitives for hashing
                    rows1 = [
                        tuple(row)
                        for row in df1.replace({np.nan: None}).to_records(index=False)
                    ]
                    rows2 = [
                        tuple(row)
                        for row in df2_aligned.replace({np.nan: None}).to_records(
                            index=False
                        )
                    ]
                    counter1 = Counter(rows1)
                    counter2 = Counter(rows2)

                    if counter1 == counter2:
                        diffs.append("Row content is identical (order ignored).")
                    else:
                        equivalent = False
                        diffs.append("Row content differs (order ignored).")
                        unique_rows1 = list((counter1 - counter2).keys())
                        unique_rows2 = list((counter2 - counter1).keys())
                        count_diffs_dict = {
                            k: (counter1[k], counter2[k])
                            for k in counter1 & counter2
                            if counter1[k] != counter2[k]
                        }

                        if unique_rows1:
                            diffs.append(
                                f"  Unique rows in File 1 ({min(len(unique_rows1), max_examples)} examples):"
                            )
                            diffs.extend(
                                [f"    - {row}" for row in unique_rows1[:max_examples]]
                            )
                        if unique_rows2:
                            diffs.append(
                                f"  Unique rows in File 2 ({min(len(unique_rows2), max_examples)} examples):"
                            )
                            diffs.extend(
                                [f"    - {row}" for row in unique_rows2[:max_examples]]
                            )
                        if count_diffs_dict:
                            diffs.append(
                                f"  Rows with different counts ({min(len(count_diffs_dict), max_examples)} examples):"
                            )
                            count_diff_examples = list(count_diffs_dict.items())
                            diffs.extend(
                                [
                                    f"    - Row: {row}, Counts: (File1: {c1}, File2: {c2})"
                                    for row, (c1, c2) in count_diff_examples[
                                        :max_examples
                                    ]
                                ]
                            )

                except TypeError as e:
                    # Handle potential unhashable types within rows
                    diffs.append(
                        f"Could not compare rows using Counter (unhashable type detected): {e}. Trying ordered comparison."
                    )
                    compare_row_order = True  # Fallback to ordered comparison attempt
                    # TODO: Implement the ordered comparison fallback here
                    diffs.append(
                        "Ordered comparison fallback not fully implemented after Counter error."
                    )
                    equivalent = False

            if compare_row_order:  # Handles explicit request or fallback
                # Use pandas comparison capabilities
                # Ensure indices are aligned/reset if they matter
                df1_reset = df1.reset_index(drop=True)
                df2_aligned_reset = df2_aligned.reset_index(drop=True)

                try:
                    # compare() is good for detailed diffs, equals() for quick check
                    if df1_reset.equals(df2_aligned_reset):
                        diffs.append("Row content and order are identical.")
                    else:
                        equivalent = False
                        diffs.append("Row content or order differs.")
                        # Find the first differing row/cell for an example
                        comparison_df = df1_reset.compare(df2_aligned_reset)
                        if not comparison_df.empty:
                            first_diff_index = comparison_df.index[0]
                            first_diff_details = comparison_df.iloc[0].to_dict()
                            diffs.append(
                                f"  First difference at index {first_diff_index}: {first_diff_details}"
                            )
                        else:
                            # Differences might be subtle (e.g., types) if compare is empty but equals is false
                            diffs.append(
                                "  Differences detected, but compare() returned empty (possibly type or subtle value diffs)."
                            )
                        # Provide row counts if they differ
                        if len(df1_reset) != len(df2_aligned_reset):
                            diffs.append(
                                f"  Row counts differ: File1={len(df1_reset)}, File2={len(df2_aligned_reset)}"
                            )

                except Exception as e:
                    diffs.append(f"Error during ordered data comparison: {e}")
                    equivalent = False

        except (ValueError, sqlite3.Error, pd.errors.DatabaseError) as e:
            diffs.append(f"Error reading or processing table data: {e}")
            equivalent = False
        except Exception as e:  # Catch unexpected errors during data comparison
            log.exception(
                f"Unexpected error comparing data for table '{table_name1}': {e}"
            )
            diffs.append(f"Unexpected error during data comparison: {e}")
            equivalent = False

        return equivalent, diffs

    def _compare_column_schemas(
        self, cols1: List[Dict], cols2: List[Dict], ignore_col_names: bool
    ) -> Tuple[List[str], bool, ColumnMap]:
        """Compares the column definitions of two tables."""
        diffs = []
        equivalent = True
        col_map: ColumnMap = {}  # map name1 -> name2
        cols2_dict_by_name = {c["name"]: c for c in cols2}
        # TODO: Add mapping by original_column_name if ignore_col_names is True

        if ignore_col_names:
            diffs.append(
                "WARNING: ignore_user_column_names mapping by original_column_name not yet implemented. Comparing by name."
            )
            # Fallback to comparing by name for now

        # Check counts first
        if len(cols1) != len(cols2):
            diffs.append(
                f"Column count differs: File1 has {len(cols1)}, File2 has {len(cols2)}."
            )
            equivalent = False
            # Try to map common columns anyway? For now, consider count difference major.
            # Create partial map based on names
            names1 = {c["name"] for c in cols1}
            names2 = {c["name"] for c in cols2}
            common_names = names1.intersection(names2)
            for c1 in cols1:
                col_map[c1["name"]] = c1["name"] if c1["name"] in common_names else None
            return (
                diffs,
                equivalent,
                col_map,
            )  # Return early if counts differ significantly

        # Compare individual columns (assuming name mapping for now)
        matched_cols2 = set()
        for col1 in cols1:
            col_name1 = col1["name"]
            col2 = cols2_dict_by_name.get(col_name1)

            if col2:
                col_map[col_name1] = col_name1  # Map found column
                matched_cols2.add(col_name1)
                # Compare properties (type, nullability, PK - ignore default value?)
                type1 = col1.get("sqlite_type", "").upper()
                type2 = col2.get("sqlite_type", "").upper()
                # Basic type comparison (might need refinement for affinity like INTEGER/INT)
                if type1 != type2:
                    diffs.append(
                        f"Column '{col_name1}': Type mismatch ('{type1}' vs '{type2}')"
                    )
                    equivalent = False
                if col1.get("not_null") != col2.get("not_null"):
                    diffs.append(f"Column '{col_name1}': Nullability mismatch")
                    equivalent = False
                if col1.get("primary_key") != col2.get("primary_key"):
                    diffs.append(f"Column '{col_name1}': Primary key mismatch")
                    equivalent = False
                # Compare SDIF metadata (description, original_format) - SHOULD BE IGNORED per request
                # if col1.get('description') != col2.get('description'): ...
                # if col1.get('original_data_format') != col2.get('original_data_format'): ...
            else:
                diffs.append(f"Column '{col_name1}' present only in File 1.")
                col_map[col_name1] = None
                equivalent = False

        # Check for columns only in File 2
        for col_name2 in cols2_dict_by_name:
            if col_name2 not in matched_cols2:
                diffs.append(f"Column '{col_name2}' present only in File 2.")
                equivalent = False
                # Cannot map from file1

        return diffs, equivalent, col_map

    def _compare_all_objects(
        self,
        db1: SDIFDatabase,
        db2: SDIFDatabase,
        schema1: Dict,
        schema2: Dict,
        source_map: SourceMap,
        **kwargs: Any,
    ) -> Tuple[bool, List[str]]:
        """Compares all objects based on schema and data."""
        overall_equivalent = True
        all_diffs: List[str] = []
        obj_map: NameMap = {}  # Maps obj1 name to obj2 name or None

        objs1_schema = schema1.get("objects", {})
        objs2_schema = schema2.get("objects", {})
        obj_names1 = set(objs1_schema.keys())
        obj_names2 = set(objs2_schema.keys())

        ignore_obj_names = kwargs.get("ignore_object_names", False)
        if ignore_obj_names:
            all_diffs.append(
                "WARNING: ignore_object_names=True mapping by content hash not yet implemented. Comparing by name."
            )

        # Find common and unique objects by name
        common_objs = obj_names1.intersection(obj_names2)
        unique_objs1 = obj_names1 - obj_names2
        unique_objs2 = obj_names2 - obj_names1

        if unique_objs1:
            all_diffs.append(
                f"Objects only in File 1: {', '.join(sorted(list(unique_objs1)))}"
            )
            overall_equivalent = False
            for name in unique_objs1:
                obj_map[name] = None
        if unique_objs2:
            all_diffs.append(
                f"Objects only in File 2: {', '.join(sorted(list(unique_objs2)))}"
            )
            overall_equivalent = False

        # Compare common objects
        for name in common_objs:
            log.debug(f"Comparing object: {name}")
            obj_map[name] = name  # Direct mapping by name
            obj1_meta = objs1_schema[name]
            obj2_meta = objs2_schema[name]

            # 1. Compare Source ID
            source_id1 = obj1_meta.get("source_id")
            source_id2 = obj2_meta.get("source_id")
            mapped_source_id2 = source_map.get(cast(int, source_id1))

            if source_id1 is None or source_id2 is None:
                all_diffs.append(f"Object '{name}': Missing source_id metadata.")
                overall_equivalent = False
            elif mapped_source_id2 != source_id2:
                all_diffs.append(
                    f"Object '{name}': Source ID mismatch (File1 Source ID {source_id1} maps to {mapped_source_id2}, but File2 has Source ID {source_id2})."
                )
                overall_equivalent = False

            # 2. Compare JSON Data
            try:
                # Retrieve full object data (already parsed by get_object if schema was reliable)
                # Fallback to reading directly if needed
                obj1_data = db1.get_object(name, parse_json=True)
                obj2_data = db2.get_object(name, parse_json=True)

                if not obj1_data or not obj2_data:
                    all_diffs.append(
                        f"Object '{name}': Could not retrieve data from one or both files."
                    )
                    overall_equivalent = False
                    continue  # Skip content comparison if data missing

                json1 = obj1_data.get("json_data")
                json2 = obj2_data.get("json_data")

                if not self._compare_json_objects(json1, json2):
                    all_diffs.append(f"Object '{name}': JSON content differs.")
                    overall_equivalent = False
                    # Optionally add diff details here if _compare_json_objects provides them

                # Ignore description, schema_hint as per requirements

            except (ValueError, sqlite3.Error) as e:
                all_diffs.append(
                    f"Object '{name}': Error reading or parsing object data: {e}"
                )
                overall_equivalent = False
            except Exception as e:
                log.exception(f"Unexpected error comparing object '{name}': {e}")
                all_diffs.append(
                    f"Object '{name}': Unexpected error during comparison: {e}"
                )
                overall_equivalent = False

        if overall_equivalent and not unique_objs1 and not unique_objs2:
            all_diffs.append("All compared objects are equivalent.")

        return overall_equivalent, all_diffs

    def _compare_all_media(
        self,
        db1: SDIFDatabase,
        db2: SDIFDatabase,
        schema1: Dict,
        schema2: Dict,
        source_map: SourceMap,
        **kwargs: Any,
    ) -> Tuple[bool, List[str]]:
        """Compares all media items based on schema and data."""
        overall_equivalent = True
        all_diffs: List[str] = []
        media_map: NameMap = {}  # Maps media1 name to media2 name or None

        media1_schema = schema1.get("media", {})
        media2_schema = schema2.get("media", {})
        media_names1 = set(media1_schema.keys())
        media_names2 = set(media2_schema.keys())

        ignore_media_names = kwargs.get("ignore_media_names", False)
        if ignore_media_names:
            all_diffs.append(
                "WARNING: ignore_media_names=True mapping by content hash not yet implemented. Comparing by name."
            )

        # Find common and unique media by name
        common_media = media_names1.intersection(media_names2)
        unique_media1 = media_names1 - media_names2
        unique_media2 = media_names2 - media_names1

        if unique_media1:
            all_diffs.append(
                f"Media only in File 1: {', '.join(sorted(list(unique_media1)))}"
            )
            overall_equivalent = False
            for name in unique_media1:
                media_map[name] = None
        if unique_media2:
            all_diffs.append(
                f"Media only in File 2: {', '.join(sorted(list(unique_media2)))}"
            )
            overall_equivalent = False

        # Compare common media items
        for name in common_media:
            log.debug(f"Comparing media: {name}")
            media_map[name] = name  # Direct mapping by name
            media1_meta = media1_schema[name]
            media2_meta = media2_schema[name]

            # 1. Compare Source ID
            source_id1 = media1_meta.get("source_id")
            source_id2 = media2_meta.get("source_id")
            mapped_source_id2 = source_map.get(cast(int, source_id1))

            if source_id1 is None or source_id2 is None:
                all_diffs.append(f"Media '{name}': Missing source_id metadata.")
                overall_equivalent = False
            elif mapped_source_id2 != source_id2:
                all_diffs.append(
                    f"Media '{name}': Source ID mismatch (File1 Source ID {source_id1} maps to {mapped_source_id2}, but File2 has Source ID {source_id2})."
                )
                overall_equivalent = False

            # 2. Compare Key Metadata (media_type, original_format)
            type1 = media1_meta.get("media_type")
            type2 = media2_meta.get("media_type")
            fmt1 = media1_meta.get("original_format")
            fmt2 = media2_meta.get("original_format")

            if type1 != type2:
                all_diffs.append(
                    f"Media '{name}': media_type differs ('{type1}' vs '{type2}')"
                )
                overall_equivalent = False
            if fmt1 != fmt2:
                all_diffs.append(
                    f"Media '{name}': original_format differs ('{fmt1}' vs '{fmt2}')"
                )
                overall_equivalent = False
            # Ignore description, technical_metadata

            # 3. Compare Media Data (BLOB)
            try:
                media1_data = db1.get_media(
                    name, parse_json=False
                )  # Get raw tech meta if needed later
                media2_data = db2.get_media(name, parse_json=False)

                if not media1_data or not media2_data:
                    all_diffs.append(
                        f"Media '{name}': Could not retrieve data from one or both files."
                    )
                    overall_equivalent = False
                    continue

                blob1 = media1_data.get("media_data")
                blob2 = media2_data.get("media_data")

                if not isinstance(blob1, bytes) or not isinstance(blob2, bytes):
                    all_diffs.append(
                        f"Media '{name}': Retrieved media_data is not bytes type."
                    )
                    overall_equivalent = False
                elif blob1 != blob2:
                    all_diffs.append(
                        f"Media '{name}': Binary content (media_data) differs (Size1: {len(blob1)} bytes, Size2: {len(blob2)} bytes)."
                    )
                    overall_equivalent = False

            except (ValueError, sqlite3.Error) as e:
                all_diffs.append(f"Media '{name}': Error reading media data: {e}")
                overall_equivalent = False
            except Exception as e:
                log.exception(f"Unexpected error comparing media '{name}': {e}")
                all_diffs.append(
                    f"Media '{name}': Unexpected error during comparison: {e}"
                )
                overall_equivalent = False

        if overall_equivalent and not unique_media1 and not unique_media2:
            all_diffs.append("All compared media items are equivalent.")

        return overall_equivalent, all_diffs

    # --- Main Comparison Method ---

    def compare(
        self, file_path1: Union[str, Path], file_path2: Union[str, Path], **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Compares two SDIF files using SDIFDatabase and specified options.

        Kwargs Options:
            ignore_user_table_names (bool): Map tables by name only (mapping by original_identifier not implemented). Default: False.
            ignore_user_column_names (bool): Map columns by name only (mapping by original_column_name not implemented). Default: False.
            compare_user_table_row_order (bool): Compare table row content respecting order. Default: False.
            ignore_source_original_file_name (bool): Ignore original_file_name in sdif_sources mapping. Default: False.
            ignore_object_names (bool): Map objects by name only (mapping by hash not implemented). Default: False.
            ignore_media_names (bool): Map media by name only (mapping by hash not implemented). Default: False.
            decimal_places (Optional[int]): Decimal places for comparing REAL/float numbers in tables. Default: None (exact comparison).
            max_examples (int): Max number of differing row/item examples. Default: 5.
        """
        file_path1 = Path(file_path1)
        file_path2 = Path(file_path2)

        # Store passed parameters
        comparison_params = {
            "ignore_user_table_names": kwargs.get("ignore_user_table_names", False),
            "ignore_user_column_names": kwargs.get("ignore_user_column_names", False),
            "compare_user_table_row_order": kwargs.get(
                "compare_user_table_row_order", False
            ),
            "ignore_source_original_file_name": kwargs.get(
                "ignore_source_original_file_name", False
            ),
            "ignore_object_names": kwargs.get("ignore_object_names", False),
            "ignore_media_names": kwargs.get("ignore_media_names", False),
            "decimal_places": kwargs.get("decimal_places", None),
            "max_examples": kwargs.get("max_examples", 5),
        }

        # Initialize results structure
        results: Dict[str, Any] = {
            "files": {"file1": str(file_path1), "file2": str(file_path2)},
            "comparison_params": comparison_params,
            "are_equivalent": True,  # Assume true initially
            "summary": [],
            "details": {
                "errors": [],
                "sdif_version_check": {"result": "Not compared", "diff": []},
                "source_comparison": {
                    "result": "Not compared",
                    "diff": [],
                    "map": None,
                },
                "user_table_comparison": {
                    "result": "Not compared",
                    "diff": [],
                    "map": None,
                },
                "object_comparison": {"result": "Not compared", "diff": []},
                "media_comparison": {"result": "Not compared", "diff": []},
                # Semantic links and annotations are ignored by default per requirements
            },
        }

        db1: Optional[SDIFDatabase] = None
        db2: Optional[SDIFDatabase] = None
        schema1: Optional[Dict] = None
        schema2: Optional[Dict] = None
        source_map: Optional[SourceMap] = None

        try:
            # --- Connect and Get Schemas ---
            try:
                log.info(f"Opening SDIF file 1: {file_path1}")
                db1 = SDIFDatabase(file_path1, read_only=True)
                log.info("Retrieving schema for file 1...")
                schema1 = db1.get_schema()
                log.info("Successfully retrieved schema for file 1.")
            except (FileNotFoundError, sqlite3.Error, ValueError) as e:
                results["details"]["errors"].append(
                    f"Error accessing File 1 ({file_path1.name}): {e}"
                )
                results["are_equivalent"] = False

            try:
                log.info(f"Opening SDIF file 2: {file_path2}")
                db2 = SDIFDatabase(file_path2, read_only=True)
                log.info("Retrieving schema for file 2...")
                schema2 = db2.get_schema()
                log.info("Successfully retrieved schema for file 2.")
            except (FileNotFoundError, sqlite3.Error, ValueError) as e:
                results["details"]["errors"].append(
                    f"Error accessing File 2 ({file_path2.name}): {e}"
                )
                results["are_equivalent"] = False

            if not db1 or not db2 or not schema1 or not schema2:
                results["summary"].append(
                    "Comparison aborted due to errors accessing files or schemas."
                )
                # are_equivalent already set to False if needed
                return results  # Exit early

            # --- Compare SDIF Properties (Version) ---
            props1 = schema1.get("sdif_properties")
            props2 = schema2.get("sdif_properties")
            prop_eq, prop_diffs = self._compare_properties(props1, props2)
            results["details"]["sdif_version_check"]["result"] = (
                "Equivalent" if prop_eq else "Different"
            )
            results["details"]["sdif_version_check"]["diff"] = prop_diffs
            if not prop_eq:
                results["are_equivalent"] = False
                results["summary"].append("SDIF properties (version) differ.")
            else:
                results["summary"].append("SDIF properties (version) are equivalent.")

            # --- Compare Sources ---
            sources1 = schema1.get("sources", [])
            sources2 = schema2.get("sources", [])
            source_eq, source_diffs, source_map = self._compare_sources(
                sources1,
                sources2,
                comparison_params["ignore_source_original_file_name"],
            )
            results["details"]["source_comparison"]["result"] = (
                "Equivalent" if source_eq else "Different"
            )
            results["details"]["source_comparison"]["diff"] = source_diffs
            results["details"]["source_comparison"]["map"] = (
                source_map  # Store the map even if diffs exist
            )
            if not source_eq:
                results["are_equivalent"] = False
                results["summary"].append("Source information differs.")
                # Abort detailed comparison if sources don't map cleanly?
                # For now, continue comparing other elements if possible, but flag overall difference.
                results["summary"].append(
                    "Continuing comparison despite source mismatch..."
                )

            else:
                results["summary"].append("Source information is equivalent.")

            # Ensure source_map is not None if sources were equivalent
            if source_eq and source_map is None:
                log.error(
                    "Source comparison reported equivalent but map is None. Inconsistency."
                )
                results["details"]["errors"].append(
                    "Internal error: Source map missing after successful comparison."
                )
                results["are_equivalent"] = False
                source_map = {}  # Provide empty map to prevent downstream errors

            # --- Compare User Tables ---
            if source_map is not None:  # Only proceed if source mapping was possible
                table_eq, table_diffs, table_map = self._compare_all_user_tables(
                    db1, db2, schema1, schema2, source_map, **kwargs
                )
                results["details"]["user_table_comparison"]["result"] = (
                    "Equivalent" if table_eq else "Different"
                )
                results["details"]["user_table_comparison"]["diff"] = table_diffs
                results["details"]["user_table_comparison"]["map"] = table_map
                if not table_eq:
                    results["are_equivalent"] = False
                    results["summary"].append(
                        "User table structure or content differs."
                    )
                else:
                    results["summary"].append("User tables are equivalent.")
            else:
                results["details"]["user_table_comparison"]["result"] = (
                    "Not compared (source mismatch)"
                )
                results["summary"].append(
                    "User table comparison skipped due to source mismatch."
                )

            # --- Compare Objects ---
            if source_map is not None:
                obj_eq, obj_diffs = self._compare_all_objects(
                    db1, db2, schema1, schema2, source_map, **kwargs
                )
                results["details"]["object_comparison"]["result"] = (
                    "Equivalent" if obj_eq else "Different"
                )
                results["details"]["object_comparison"]["diff"] = obj_diffs
                if not obj_eq:
                    results["are_equivalent"] = False
                    results["summary"].append("SDIF object content differs.")
                else:
                    results["summary"].append("SDIF objects are equivalent.")
            else:
                results["details"]["object_comparison"]["result"] = (
                    "Not compared (source mismatch)"
                )
                results["summary"].append(
                    "Object comparison skipped due to source mismatch."
                )

            # --- Compare Media ---
            if source_map is not None:
                media_eq, media_diffs = self._compare_all_media(
                    db1, db2, schema1, schema2, source_map, **kwargs
                )
                results["details"]["media_comparison"]["result"] = (
                    "Equivalent" if media_eq else "Different"
                )
                results["details"]["media_comparison"]["diff"] = media_diffs
                if not media_eq:
                    results["are_equivalent"] = False
                    results["summary"].append("SDIF media content differs.")
                else:
                    results["summary"].append("SDIF media are equivalent.")
            else:
                results["details"]["media_comparison"]["result"] = (
                    "Not compared (source mismatch)"
                )
                results["summary"].append(
                    "Media comparison skipped due to source mismatch."
                )

            # --- Final Summary ---
            final_summary_message = (
                "Files are considered equivalent based on the specified parameters."
                if results["are_equivalent"]
                else "Files are considered different based on the specified parameters."
            )
            results["summary"].insert(0, final_summary_message)

        except Exception as e:
            log.exception(f"An unexpected error occurred during SDIF comparison: {e}")
            results["details"]["errors"].append(f"Unexpected comparison error: {e}")
            results["are_equivalent"] = False
            results["summary"].append("Comparison failed due to an unexpected error.")
        finally:
            # --- Close Connections via SDIFDatabase context/close ---
            if db1:
                db1.close()
                log.debug(f"Closed connection to {file_path1}")
            if db2:
                db2.close()
                log.debug(f"Closed connection to {file_path2}")

        return results
