from typing import Any, Dict

import pytest
from sdif_db.schema import SDIFSchemaConfig

from satif_sdk.comparators.sdif_schema import SDIFSchemaComparator

# --- Fixtures and Helper Data ---


@pytest.fixture
def default_config() -> SDIFSchemaConfig:
    return SDIFSchemaConfig()


@pytest.fixture
def comparator_default_config(default_config: SDIFSchemaConfig) -> SDIFSchemaComparator:
    return SDIFSchemaComparator(config=default_config)


@pytest.fixture
def basic_schema_1() -> Dict[str, Any]:
    return {
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "table1": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER", "not_null": True, "pk": 1},
                    {
                        "name": "value",
                        "sqlite_type": "TEXT",
                        "not_null": False,
                        "pk": 0,
                    },
                ],
                "primary_key_columns": ("id",),
                "foreign_keys": [],
            }
        },
        "objects": {},
        "media": {},
        "semantic_links": [],
    }


@pytest.fixture
def basic_schema_1_copy() -> Dict[str, Any]:  # Identical to basic_schema_1
    return {
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "table1": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER", "not_null": True, "pk": 1},
                    {
                        "name": "value",
                        "sqlite_type": "TEXT",
                        "not_null": False,
                        "pk": 0,
                    },
                ],
                "primary_key_columns": ("id",),
                "foreign_keys": [],
            }
        },
        "objects": {},
        "media": {},
        "semantic_links": [],
    }


@pytest.fixture
def basic_schema_2_diff_col_name() -> Dict[str, Any]:
    return {
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "table1": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER", "not_null": True, "pk": 1},
                    {
                        "name": "data",
                        "sqlite_type": "TEXT",
                        "not_null": False,
                        "pk": 0,
                    },  # Changed name: value -> data
                ],
                "primary_key_columns": ("id",),
                "foreign_keys": [],
            }
        },
        "objects": {},
        "media": {},
        "semantic_links": [],
    }


@pytest.fixture
def basic_schema_3_diff_col_order() -> Dict[str, Any]:
    return {
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "table1": {
                "columns": [
                    {
                        "name": "value",
                        "sqlite_type": "TEXT",
                        "not_null": False,
                        "pk": 0,
                    },  # Order changed
                    {"name": "id", "sqlite_type": "INTEGER", "not_null": True, "pk": 1},
                ],
                "primary_key_columns": ("id",),
                "foreign_keys": [],
            }
        },
        "objects": {},
        "media": {},
        "semantic_links": [],
    }


@pytest.fixture
def basic_schema_4_diff_table_name() -> Dict[str, Any]:
    return {
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "table_renamed": {  # Changed table name
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER", "not_null": True, "pk": 1},
                    {
                        "name": "value",
                        "sqlite_type": "TEXT",
                        "not_null": False,
                        "pk": 0,
                    },
                ],
                "primary_key_columns": ("id",),
                "foreign_keys": [],
            }
        },
        "objects": {},
        "media": {},
        "semantic_links": [],
    }


# --- Test Cases for __init__ ---


def test_sdif_schema_comparator_init_default_config():
    comparator = SDIFSchemaComparator()
    assert isinstance(comparator.config, SDIFSchemaConfig)
    assert comparator.config.enforce_table_names is True  # Check a default value


def test_sdif_schema_comparator_init_custom_config():
    custom_config = SDIFSchemaConfig(enforce_table_names=False)
    comparator = SDIFSchemaComparator(config=custom_config)
    assert comparator.config is custom_config
    assert comparator.config.enforce_table_names is False


# --- Test Cases for compare method ---


def test_compare_equivalent_schemas_default_config(
    comparator_default_config: SDIFSchemaComparator,
    basic_schema_1: Dict[str, Any],
    basic_schema_1_copy: Dict[str, Any],
):
    are_equivalent, diff = comparator_default_config.compare(
        basic_schema_1, basic_schema_1_copy
    )
    assert are_equivalent is True
    assert "Schemas are equivalent" in diff[0]


def test_compare_nonequivalent_schemas_col_name_default_config(
    comparator_default_config: SDIFSchemaComparator,
    basic_schema_1: Dict[str, Any],
    basic_schema_2_diff_col_name: Dict[str, Any],
):
    are_equivalent, diff = comparator_default_config.compare(
        basic_schema_1, basic_schema_2_diff_col_name
    )
    assert are_equivalent is False
    assert "Schema differences found" in diff[0]

    # Check for set_item_added summary for the new column name
    found_added = False
    for idx, line in enumerate(diff):
        if "* Other difference type 'set_item_added':" in line:
            if (
                idx + 1 < len(diff)
                and "[('name', 'data')]" in diff[idx + 1]
                and "columns'][1]" in diff[idx + 1]
            ):  # ensure it's about the second column
                found_added = True
                break
    assert found_added, (
        "Did not find summary for set_item_added with correct column name data"
    )

    # Check for set_item_removed summary for the old column name
    found_removed = False
    for idx, line in enumerate(diff):
        if "* Other difference type 'set_item_removed':" in line:
            if (
                idx + 1 < len(diff)
                and "[('name', 'value')]" in diff[idx + 1]
                and "columns'][1]" in diff[idx + 1]
            ):  # ensure it's about the second column
                found_removed = True
                break
    assert found_removed, (
        "Did not find summary for set_item_removed with correct column name data"
    )


def test_compare_nonequivalent_schemas_col_order_default_config(
    comparator_default_config: SDIFSchemaComparator,
    basic_schema_1: Dict[str, Any],
    basic_schema_3_diff_col_order: Dict[str, Any],
):
    # Default config has enforce_column_order = True
    are_equivalent, diff = comparator_default_config.compare(
        basic_schema_1, basic_schema_3_diff_col_order
    )
    assert are_equivalent is False
    assert "Schema differences found" in diff[0]

    # Look for any indication of changes to the columns
    found_col0_change = False
    found_col1_change = False
    for line in diff:
        if "columns" in line and ("0" in line or "[0]" in line):
            found_col0_change = True
        if "columns" in line and ("1" in line or "[1]" in line):
            found_col1_change = True

    assert found_col0_change, "No mentions of changes to column 0 found"
    assert found_col1_change, "No mentions of changes to column 1 found"


def test_compare_nonequivalent_schemas_table_name_default_config(
    comparator_default_config: SDIFSchemaComparator,
    basic_schema_1: Dict[str, Any],
    basic_schema_4_diff_table_name: Dict[str, Any],
):
    # Default config has enforce_table_names = True
    are_equivalent, diff = comparator_default_config.compare(
        basic_schema_1, basic_schema_4_diff_table_name
    )
    assert are_equivalent is False
    assert "Schema differences found" in diff[0]

    # Look for any indication of table1 removal and table_renamed addition
    found_table1_reference = False
    found_table_renamed_reference = False
    for line in diff:
        if "table1" in line:
            found_table1_reference = True
        if "table_renamed" in line:
            found_table_renamed_reference = True

    assert found_table1_reference, "No mentions of table1 found in the diff"
    assert found_table_renamed_reference, (
        "No mentions of table_renamed found in the diff"
    )


# Test compare with verbose_diff_level
def test_compare_verbose_level_1(
    comparator_default_config: SDIFSchemaComparator,
    basic_schema_1: Dict[str, Any],
    basic_schema_2_diff_col_name: Dict[str, Any],
):
    are_equivalent, diff = comparator_default_config.compare(
        basic_schema_1, basic_schema_2_diff_col_name, verbose_diff_level=1
    )
    assert are_equivalent is False
    assert isinstance(diff, dict)  # DeepDiff dict output
    # Based on test failure logs, DeepDiff reports these as set changes within the frozenset
    assert "set_item_added" in diff
    assert "set_item_removed" in diff
    assert (
        "root['tables']['table1']['columns'][1][('name', 'data')]"
        in diff["set_item_added"]
    )
    assert (
        "root['tables']['table1']['columns'][1][('name', 'value')]"
        in diff["set_item_removed"]
    )


def test_compare_verbose_level_2(
    comparator_default_config: SDIFSchemaComparator,
    basic_schema_1: Dict[str, Any],
    basic_schema_2_diff_col_name: Dict[str, Any],
):
    are_equivalent, diff_obj = comparator_default_config.compare(
        basic_schema_1, basic_schema_2_diff_col_name, verbose_diff_level=2
    )
    assert are_equivalent is False
    assert hasattr(diff_obj, "to_dict")  # Check if it's a DeepDiff object
    diff_dict = diff_obj.to_dict()
    # Based on test failure logs, DeepDiff reports these as set changes
    assert "set_item_added" in diff_dict
    assert "set_item_removed" in diff_dict
    assert (
        "root['tables']['table1']['columns'][1][('name', 'data')]"
        in diff_dict["set_item_added"]
    )
    assert (
        "root['tables']['table1']['columns'][1][('name', 'value')]"
        in diff_dict["set_item_removed"]
    )


# Test compare with custom configs
def test_compare_equivalent_with_config_ignore_col_order(
    basic_schema_1: Dict[str, Any],
    basic_schema_3_diff_col_order: Dict[str, Any],
):
    config_ignore_col_order = SDIFSchemaConfig(enforce_column_order=False)
    comparator = SDIFSchemaComparator(config=config_ignore_col_order)
    are_equivalent, diff = comparator.compare(
        basic_schema_1, basic_schema_3_diff_col_order
    )
    assert are_equivalent is True
    assert "Schemas are equivalent" in diff[0]


def test_compare_equivalent_with_config_ignore_table_names(
    basic_schema_1: Dict[str, Any],
    basic_schema_4_diff_table_name: Dict[str, Any],
):
    config_ignore_table_names = SDIFSchemaConfig(enforce_table_names=False)
    comparator = SDIFSchemaComparator(config=config_ignore_table_names)
    # With enforce_table_names=False, the 'tables' key becomes 'tables_set'
    # The schemas basic_schema_1 and basic_schema_4_diff_table_name will have different
    # table names under the 'tables' key. When enforce_table_names=False, these are canonicalized
    # into a 'tables_set'. If the structures are otherwise identical, they should match.
    are_equivalent, diff = comparator.compare(
        basic_schema_1, basic_schema_4_diff_table_name
    )
    assert are_equivalent is True
    assert "Schemas are equivalent" in diff[0]


def test_compare_nonequivalent_with_config_enforce_sdif_version(
    basic_schema_1: Dict[str, Any],
):
    schema_alt_version = basic_schema_1.copy()
    schema_alt_version["sdif_properties"] = {"sdif_version": "1.1"}  # Different version

    config_enforce_version = SDIFSchemaConfig(
        enforce_sdif_version=True
    )  # Default is True
    comparator = SDIFSchemaComparator(config=config_enforce_version)
    are_equivalent, diff = comparator.compare(basic_schema_1, schema_alt_version)
    assert are_equivalent is False
    assert any("Changed at 'root['sdif_version']'" in item for item in diff)

    config_ignore_version = SDIFSchemaConfig(enforce_sdif_version=False)
    comparator_ignore = SDIFSchemaComparator(config=config_ignore_version)
    are_equivalent_ignored, diff_ignored = comparator_ignore.compare(
        basic_schema_1, schema_alt_version
    )
    assert are_equivalent_ignored is True
    assert "Schemas are equivalent" in diff_ignored[0]


# --- Fixtures for is_compatible_with tests ---


@pytest.fixture
def consumer_schema_basic() -> Dict[str, Any]:
    """A basic consumer schema requiring one table with specific columns."""
    return {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},
                    {"name": "email", "sqlite_type": "TEXT"},
                ],
                "primary_key_columns": ("id",),
            }
        }
    }


@pytest.fixture
def producer_schema_compatible(consumer_schema_basic: Dict[str, Any]) -> Dict[str, Any]:
    """A producer schema that is compatible with consumer_schema_basic."""
    return consumer_schema_basic  # Exact match is compatible


@pytest.fixture
def producer_schema_compatible_extra_col() -> Dict[str, Any]:
    """Producer has an extra column, still compatible."""
    return {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},
                    {"name": "email", "sqlite_type": "TEXT"},
                    {"name": "status", "sqlite_type": "TEXT"},  # Extra column
                ],
                "primary_key_columns": ("id",),
            }
        }
    }


@pytest.fixture
def producer_schema_compatible_extra_table() -> Dict[str, Any]:
    """Producer has an extra table, still compatible."""
    return {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},
                    {"name": "email", "sqlite_type": "TEXT"},
                ],
                "primary_key_columns": ("id",),
            },
            "products": {  # Extra table
                "columns": [{"name": "sku", "sqlite_type": "TEXT"}],
                "primary_key_columns": ("sku",),
            },
        }
    }


@pytest.fixture
def producer_schema_incompatible_missing_col() -> Dict[str, Any]:
    """Producer is missing a column required by consumer."""
    return {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},  # email column missing
                ],
                "primary_key_columns": ("id",),
            }
        }
    }


@pytest.fixture
def producer_schema_incompatible_diff_col_type() -> Dict[str, Any]:
    """Producer has a different column type for a required column."""
    return {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},
                    {"name": "email", "sqlite_type": "BLOB"},  # Type TEXT vs BLOB
                ],
                "primary_key_columns": ("id",),
            }
        }
    }


@pytest.fixture
def producer_schema_incompatible_missing_table() -> Dict[str, Any]:
    """Producer is missing a table required by consumer."""
    return {
        "tables": {
            "items": {  # Different table name, effectively missing 'users'
                "columns": [{"name": "id", "sqlite_type": "INTEGER"}],
                "primary_key_columns": ("id",),
            }
        }
    }


# --- Test Cases for is_compatible_with method ---


def test_is_compatible_with_identical_schemas(
    comparator_default_config: SDIFSchemaComparator,
    consumer_schema_basic: Dict[str, Any],
    producer_schema_compatible: Dict[str, Any],
):
    assert (
        comparator_default_config.is_compatible_with(
            consumer_schema_basic, producer_schema_compatible
        )
        is True
    )


def test_is_compatible_with_producer_has_extra_column(
    comparator_default_config: SDIFSchemaComparator,
    consumer_schema_basic: Dict[str, Any],
    producer_schema_compatible_extra_col: Dict[str, Any],
):
    assert (
        comparator_default_config.is_compatible_with(
            consumer_schema_basic, producer_schema_compatible_extra_col
        )
        is True
    )


def test_is_compatible_with_producer_has_extra_table(
    comparator_default_config: SDIFSchemaComparator,
    consumer_schema_basic: Dict[str, Any],
    producer_schema_compatible_extra_table: Dict[str, Any],
):
    # With default config (enforce_table_names=True), the producer's extra table is ignored
    # as long as the required table ('users') is present and compatible.
    assert (
        comparator_default_config.is_compatible_with(
            consumer_schema_basic, producer_schema_compatible_extra_table
        )
        is True
    )


def test_is_incompatible_producer_missing_column(
    comparator_default_config: SDIFSchemaComparator,
    consumer_schema_basic: Dict[str, Any],
    producer_schema_incompatible_missing_col: Dict[str, Any],
):
    assert (
        comparator_default_config.is_compatible_with(
            consumer_schema_basic, producer_schema_incompatible_missing_col
        )
        is False
    )


def test_is_incompatible_producer_diff_column_type(
    comparator_default_config: SDIFSchemaComparator,
    consumer_schema_basic: Dict[str, Any],
    producer_schema_incompatible_diff_col_type: Dict[str, Any],
):
    assert (
        comparator_default_config.is_compatible_with(
            consumer_schema_basic, producer_schema_incompatible_diff_col_type
        )
        is False
    )


def test_is_incompatible_producer_missing_table(
    comparator_default_config: SDIFSchemaComparator,
    consumer_schema_basic: Dict[str, Any],
    producer_schema_incompatible_missing_table: Dict[str, Any],
):
    assert (
        comparator_default_config.is_compatible_with(
            consumer_schema_basic, producer_schema_incompatible_missing_table
        )
        is False
    )


# Test compatibility with custom config
def test_is_compatible_with_config_ignore_col_names(
    consumer_schema_basic: Dict[str, Any],
    producer_schema_incompatible_missing_col: Dict[
        str, Any
    ],  # This producer normally fails
):
    # Consumer: id (INT), email (TEXT)
    # Producer: id (INT)
    # If we ignore column names and order, and only look at types by position:
    # Consumer[0] is id (INT), Producer[0] is id (INT) -> Match
    # Consumer[1] is email (TEXT), Producer has no second column -> Incompatible

    config_ignore_names = SDIFSchemaConfig(
        enforce_column_names=False, enforce_column_order=True
    )
    comparator = SDIFSchemaComparator(config=config_ignore_names)

    # Even if we ignore names, producer is still missing a column by position/count
    assert (
        comparator.is_compatible_with(
            consumer_schema_basic, producer_schema_incompatible_missing_col
        )
        is False
    )

    # Let's try a case where ignoring names makes it compatible
    # Consumer requires: col_A (INT), col_B (TEXT)
    consumer_mod = {
        "tables": {
            "users": {
                "columns": [
                    {"name": "col_A", "sqlite_type": "INTEGER"},
                    {"name": "col_B", "sqlite_type": "TEXT"},
                ],
            }
        }
    }
    # Producer provides: id (INT), email (TEXT) (same types, different names)
    producer_mod = {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},
                    {"name": "email", "sqlite_type": "TEXT"},
                ],
            }
        }
    }
    assert comparator.is_compatible_with(consumer_mod, producer_mod) is True


def test_is_compatible_with_config_ignore_table_names(
    consumer_schema_basic: Dict[str, Any],  # Requires table 'users'
):
    # Producer has the right structure but under table 'customers'
    producer_diff_table_name = {
        "tables": {
            "customers": {  # Name differs from 'users'
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},
                    {"name": "email", "sqlite_type": "TEXT"},
                ],
                "primary_key_columns": ("id",),
            }
        }
    }
    config_ignore_names = SDIFSchemaConfig(enforce_table_names=False)
    comparator = SDIFSchemaComparator(config=config_ignore_names)

    # If table names are ignored, it looks for a compatible structure in the set of tables.
    assert (
        comparator.is_compatible_with(consumer_schema_basic, producer_diff_table_name)
        is True
    )

    # Ensure it's still False if structure is also incompatible with ignored table names
    producer_diff_table_name_and_col = {
        "tables": {
            "customers": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER"},
                    # email column missing
                ],
                "primary_key_columns": ("id",),
            }
        }
    }
    assert (
        comparator.is_compatible_with(
            consumer_schema_basic, producer_diff_table_name_and_col
        )
        is False
    )


def test_compatibility_consumer_part_none():
    comparator = SDIFSchemaComparator()
    consumer = None
    producer = {"key": "value"}
    assert comparator._check_compatibility_recursive(consumer, producer) is True


def test_compatibility_producer_part_none_consumer_not_none():
    comparator = SDIFSchemaComparator()
    consumer = {"key": "value"}
    producer = None
    assert comparator._check_compatibility_recursive(consumer, producer) is False


def test_compatibility_type_mismatch():
    comparator = SDIFSchemaComparator()
    consumer = {"key": "value"}
    producer = ["list_item"]
    assert comparator._check_compatibility_recursive(consumer, producer) is False


def test_compatibility_basic_types_match():
    comparator = SDIFSchemaComparator()
    assert comparator._check_compatibility_recursive("string", "string") is True
    assert comparator._check_compatibility_recursive(123, 123) is True
    assert comparator._check_compatibility_recursive(True, True) is True


def test_compatibility_basic_types_mismatch():
    comparator = SDIFSchemaComparator()
    assert (
        comparator._check_compatibility_recursive("string", "another_string") is False
    )
    assert comparator._check_compatibility_recursive(123, 456) is False
    assert comparator._check_compatibility_recursive(True, False) is False


# More detailed tests for _check_compatibility_recursive for dicts and lists/tuples
def test_compatibility_dict_subset():
    comparator = SDIFSchemaComparator()
    consumer = {"a": 1, "b": {"c": 2}}
    producer_exact = {"a": 1, "b": {"c": 2}}
    producer_superset = {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}
    assert comparator._check_compatibility_recursive(consumer, producer_exact) is True
    assert (
        comparator._check_compatibility_recursive(consumer, producer_superset) is True
    )


def test_compatibility_dict_missing_key():
    comparator = SDIFSchemaComparator()
    consumer = {"a": 1, "b": 2}
    producer = {"a": 1}  # Missing 'b'
    assert comparator._check_compatibility_recursive(consumer, producer) is False


def test_compatibility_dict_incompatible_value():
    comparator = SDIFSchemaComparator()
    consumer = {"a": 1, "b": {"c": 2}}
    producer = {"a": 1, "b": {"c": 99}}  # Value of c differs
    assert comparator._check_compatibility_recursive(consumer, producer) is False


def test_compatibility_tuple_frozenset_subset():
    # These are canonicalized forms from apply_rules_to_schema
    comparator = SDIFSchemaComparator()
    # Tuples represent ordered lists (e.g. primary_key_columns, or columns if order enforced)
    consumer_tuple = ("col1", "col2")
    producer_tuple_exact = ("col1", "col2")
    producer_tuple_superset = (
        "col1",
        "col2",
        "col3",
    )  # For tuples, this specific check means subset
    producer_tuple_diff_order = ("col2", "col1")

    assert (
        comparator._check_compatibility_recursive(consumer_tuple, producer_tuple_exact)
        is True
    )
    # _check_compatibility_recursive for tuples currently uses set subset. This is an interesting point.
    # If strict order and exact match is needed for some tuple elements, that's handled by how the
    # canonical form (which feeds into this recursive check) is built by apply_rules_to_schema.
    # For example, primary_key_columns are tuples. If consumer requires (id, name) and producer has (id, name, date),
    # this specific check should be true. The `_check_compatibility_recursive` is checking if all required items are present.
    assert (
        comparator._check_compatibility_recursive(
            consumer_tuple, producer_tuple_superset
        )
        is True
    )
    assert (
        comparator._check_compatibility_recursive(
            consumer_tuple, producer_tuple_diff_order
        )
        is True
    )  # Due to set conversion

    # If tuple comparison should be strict order and length, it would need a different logic inside _check_compatibility_recursive
    # However, the current logic in `SDIFSchemaComparator` for tuples/lists relies on set operations, implying order is not strictly checked *at this stage*.
    # The order is canonicalized earlier if `enforce_column_order` etc. is true.

    # Frozensets represent unordered sets (e.g. tables_set if enforce_table_names=False)
    consumer_fset = frozenset(["item1", "item2"])
    producer_fset_exact = frozenset(["item1", "item2"])
    producer_fset_superset = frozenset(["item1", "item2", "item3"])
    assert (
        comparator._check_compatibility_recursive(consumer_fset, producer_fset_exact)
        is True
    )
    assert (
        comparator._check_compatibility_recursive(consumer_fset, producer_fset_superset)
        is True
    )


def test_compatibility_tuple_frozenset_missing_item():
    comparator = SDIFSchemaComparator()
    consumer_tuple = ("col1", "col2")
    producer_tuple_missing = ("col1",)
    assert (
        comparator._check_compatibility_recursive(
            consumer_tuple, producer_tuple_missing
        )
        is False
    )

    consumer_fset = frozenset(["item1", "item2"])
    producer_fset_missing = frozenset(["item1"])
    assert (
        comparator._check_compatibility_recursive(consumer_fset, producer_fset_missing)
        is False
    )
