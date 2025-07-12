from typing import Any, Dict

import pytest

from sdif_db.schema import (
    SDIFSchemaConfig,
    _canonicalize_value,
    _get_sort_key_for_list_of_dicts,
    apply_rules_to_schema,
)

# --- Tests for SDIFSchemaConfig ---


def test_sdif_schema_config_default_values():
    config = SDIFSchemaConfig()
    assert config.enforce_sdif_version is True
    assert config.enforce_table_names is True
    assert config.enforce_column_order is True
    assert config.enforce_column_names is True
    assert config.enforce_column_types is True
    assert config.enforce_column_not_null_constraints is True
    assert config.enforce_column_default_values is True
    assert config.enforce_primary_keys is True
    assert config.enforce_foreign_keys is True
    assert config.enforce_foreign_key_referential_actions is True
    assert config.objects_mode == "names_and_schema_hint"
    assert config.media_mode == "names_type_and_original_format"
    assert config.media_technical_metadata_mode == "ignore"
    assert config.semantic_links_mode == "full_structure"


def test_sdif_schema_config_custom_values():
    config = SDIFSchemaConfig(
        enforce_sdif_version=False,
        enforce_table_names=False,
        objects_mode="names_only",
        media_technical_metadata_mode="content_comparison",
        semantic_links_mode="link_types_only",
    )
    assert config.enforce_sdif_version is False
    assert config.enforce_table_names is False
    assert config.objects_mode == "names_only"
    assert config.media_technical_metadata_mode == "content_comparison"
    assert config.semantic_links_mode == "link_types_only"


def test_sdif_schema_config_invalid_modes():
    with pytest.raises(ValueError, match="Invalid objects_mode"):
        SDIFSchemaConfig(objects_mode="invalid_mode")
    with pytest.raises(ValueError, match="Invalid media_mode"):
        SDIFSchemaConfig(media_mode="invalid_mode")
    with pytest.raises(ValueError, match="Invalid media_technical_metadata_mode"):
        SDIFSchemaConfig(media_technical_metadata_mode="invalid_mode")
    with pytest.raises(ValueError, match="Invalid semantic_links_mode"):
        SDIFSchemaConfig(semantic_links_mode="invalid_mode")


# --- Tests for Helper Functions ---


def test_canonicalize_value():
    assert _canonicalize_value(1) == 1
    assert _canonicalize_value("a") == "a"
    assert _canonicalize_value(None) is None
    assert _canonicalize_value([3, 1, 2]) == (3, 1, 2)
    assert _canonicalize_value({"b": 2, "a": 1}) == frozenset([("a", 1), ("b", 2)])
    nested_list = [1, {"c": 3, "b": [4, 2]}, 5]
    expected_nested = (1, frozenset([("b", (4, 2)), ("c", 3)]), 5)
    assert _canonicalize_value(nested_list) == expected_nested
    assert _canonicalize_value({"key": []}) == frozenset([("key", tuple())])
    assert _canonicalize_value([]) == tuple()
    assert _canonicalize_value({}) == frozenset()


def test_get_sort_key_for_list_of_dicts():
    # Test with canonicalized dicts (frozensets)
    item1_fs = frozenset([("name", "apple"), ("color", "red")])
    item2_fs = frozenset([("name", "banana"), ("color", "yellow")])
    item3_fs = frozenset([("name", "apple"), ("color", "green")])

    # Sorting should be based on the sorted tuple of (key, value) pairs
    # For item1_fs: (('color', 'red'), ('name', 'apple'))
    # For item2_fs: (('color', 'yellow'), ('name', 'banana'))
    # For item3_fs: (('color', 'green'), ('name', 'apple'))

    # Expected order after sorting: item3_fs, item1_fs, item2_fs
    # (('color', 'green'), ('name', 'apple'))
    # (('color', 'red'), ('name', 'apple'))
    # (('color', 'yellow'), ('name', 'banana'))

    list_of_frozensets = [item2_fs, item1_fs, item3_fs]
    list_of_frozensets.sort(key=_get_sort_key_for_list_of_dicts)
    assert list_of_frozensets == [item3_fs, item1_fs, item2_fs]

    # Test with actual dicts (should also work, but canonicalization is preferred before)
    item1_dict = {"name": "apple", "color": "red"}
    item2_dict = {"name": "banana", "color": "yellow"}
    item3_dict = {"name": "apple", "color": "green"}

    list_of_dicts = [item2_dict, item1_dict, item3_dict]
    list_of_dicts.sort(key=_get_sort_key_for_list_of_dicts)
    assert list_of_dicts == [item3_dict, item1_dict, item2_dict]

    # Test with other types
    assert _get_sort_key_for_list_of_dicts(123) == 123
    assert _get_sort_key_for_list_of_dicts("string") == "string"


# --- Base Schemas and Configs for apply_rules_to_schema ---

EMPTY_SCHEMA = {}
BASIC_SCHEMA_V1 = {
    "sdif_properties": {"sdif_version": "1.0.0"},
    "tables": {
        "table1": {
            "columns": [
                {
                    "name": "id",
                    "sqlite_type": "INTEGER",
                    "not_null": True,
                    "default_value": None,
                    "pk": 1,
                },
                {
                    "name": "data",
                    "sqlite_type": "TEXT",
                    "not_null": False,
                    "default_value": "N/A",
                    "pk": 0,
                },
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {
                    "id": 0,
                    "seq": 0,
                    "target_table": "table2",
                    "from_column": "id",
                    "target_column": "t1_id",
                    "on_update": "CASCADE",
                    "on_delete": "SET NULL",
                }
            ],
        },
        "table2": {
            "columns": [
                {
                    "name": "t1_id",
                    "sqlite_type": "INTEGER",
                    "not_null": False,
                    "default_value": None,
                    "pk": 0,
                },
            ],
            "primary_key": [],
        },
    },
    "objects": {
        "obj1": {"schema_hint": {"type": "string"}},
        "obj2": {"schema_hint": {"error": "invalid json"}},
        "obj3": {},
    },
    "media": {
        "media1": {
            "media_type": "image/png",
            "original_format": "png",
            "technical_metadata": {"dpi": 300},
        },
        "media2": {
            "media_type": "application/json",
            "technical_metadata": {"error": "parsing failed"},
        },
        "media3": {"media_type": "video/mp4"},
    },
    "semantic_links": [
        {
            "link_id": "l1",
            "link_type": "related",
            "from_element_type": "table",
            "from_element_spec": "table1",
            "to_element_type": "object",
            "to_element_spec": "obj1",
            "description": "link 1",
        },
        {
            "link_id": "l2",
            "link_type": "parent_of",
            "from_element_type": "object",
            "from_element_spec": "obj1",
            "to_element_type": "object",
            "to_element_spec": "obj2",
        },
    ],
}

DEFAULT_CONFIG = SDIFSchemaConfig()
FULLY_RELAXED_CONFIG = SDIFSchemaConfig(
    enforce_sdif_version=False,
    enforce_table_names=False,
    enforce_column_order=False,
    enforce_column_names=False,
    enforce_column_types=False,
    enforce_column_not_null_constraints=False,
    enforce_column_default_values=False,
    enforce_primary_keys=False,
    enforce_foreign_keys=False,
    enforce_foreign_key_referential_actions=False,
    objects_mode="ignore",
    media_mode="ignore",
    media_technical_metadata_mode="ignore",
    semantic_links_mode="ignore",
)

# --- Tests for apply_rules_to_schema ---


def test_apply_rules_empty_schema_default_config():
    minimal_schema = apply_rules_to_schema(EMPTY_SCHEMA, DEFAULT_CONFIG)
    # With default config, it expects sdif_version, and potentially empty structures for tables, objects, media, links
    # if enforcement for those is on by default for structure (e.g., 'tables_set' if no tables but PK/FK enforced)
    expected = {
        "sdif_version": None,  # No sdif_properties in empty schema
        "tables": {},  # No tables, so empty dict
        "objects": {},  # No objects
        "media": {},  # No media
        "semantic_links": tuple(),  # No links, full_structure is default, expects tuple
    }
    # Adjusting expectation based on current apply_rules_to_schema for empty inputs:
    # If no tables, "tables" key might be missing or be an empty dict if enforce_table_names=True.
    # If FK/PK enforced, it might create 'tables_set': frozenset()
    # Default config has enforce_table_names=True.
    # - sdif_version: None (correct)
    # - tables: if raw_tables_schema is empty, and enforce_table_names=True, it becomes minimal_schema["tables"] = {}.
    #           If enforce_table_names=False, and PK/FK on, it becomes tables_set=frozenset()
    # - objects: if raw_objects_schema empty, objects_mode != "ignore", minimal_schema["objects"] = {}
    # - media: if raw_media_schema empty, media_mode != "ignore", minimal_schema["media"] = {}
    # - semantic_links: if raw_links empty, mode = "full_structure", minimal_schema["semantic_links"] = tuple()
    assert minimal_schema == expected


def test_apply_rules_empty_schema_relaxed_config():
    minimal_schema = apply_rules_to_schema(EMPTY_SCHEMA, FULLY_RELAXED_CONFIG)
    assert (
        minimal_schema == {}
    )  # Fully relaxed should result in an empty schema if input is empty


def test_apply_rules_sdif_version():
    config_enf = SDIFSchemaConfig(
        enforce_sdif_version=True,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    config_no_enf = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )

    schema_with_version = {"sdif_properties": {"sdif_version": "1.0"}}
    schema_without_version_prop = {"sdif_properties": {}}
    schema_without_sdif_prop = {}

    res = apply_rules_to_schema(schema_with_version, config_enf)
    assert "sdif_version" in res
    assert res["sdif_version"] == "1.0"

    res = apply_rules_to_schema(schema_without_version_prop, config_enf)
    assert "sdif_version" in res
    assert res["sdif_version"] is None

    res = apply_rules_to_schema(schema_without_sdif_prop, config_enf)
    assert "sdif_version" in res
    assert res["sdif_version"] is None

    res = apply_rules_to_schema(schema_with_version, config_no_enf)
    assert "sdif_version" not in res
    assert res == {}  # Since other things are ignored/False


def test_apply_rules_table_names():
    schema = {"tables": {"t1": {"columns": []}}}
    config_enf_names = SDIFSchemaConfig(
        enforce_table_names=True,
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_column_order=False,
        enforce_column_names=False,
        enforce_column_types=False,
        enforce_column_not_null_constraints=False,
        enforce_column_default_values=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    config_no_enf_names = SDIFSchemaConfig(
        enforce_table_names=False,
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_column_order=False,
        enforce_column_names=False,
        enforce_column_types=False,
        enforce_column_not_null_constraints=False,
        enforce_column_default_values=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,  # Added this for tables_set
    )

    res_enf = apply_rules_to_schema(schema, config_enf_names)
    assert "tables" in res_enf
    assert "t1" in res_enf["tables"]
    assert "tables_set" not in res_enf

    res_no_enf = apply_rules_to_schema(schema, config_no_enf_names)
    assert "tables_set" in res_no_enf
    assert "tables" not in res_no_enf
    # The content of table (columns, pk, fk) with minimal config would be:
    # min_table_data = {"columns": tuple(), "primary_key_columns": tuple(), "foreign_keys": tuple()}
    # but primary_key and foreign_keys parts only added if their enforce flags are True.
    # With those flags False as above, it's simpler:
    # config_no_enf_names_simplified_table = SDIFSchemaConfig(enforce_table_names=False, ..., enforce_primary_keys=False, enforce_foreign_keys=False)
    # For this case: min_table_data becomes {'columns': ()} because other aspects are off.
    # Then this is canonicalized and added to tables_set.
    # In our config_no_enf_names, PK and FK are False, so min_table_data is just {'columns': ()}
    expected_table_structure = _canonicalize_value({"columns": tuple()})
    assert res_no_enf["tables_set"] == frozenset([expected_table_structure])


def test_apply_rules_column_structure_all_enforced():
    config = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=True,
        enforce_column_order=True,
        enforce_column_names=True,
        enforce_column_types=True,
        enforce_column_not_null_constraints=True,
        enforce_column_default_values=True,
        enforce_primary_keys=True,  # to include pk info if present
        enforce_foreign_keys=False,  # to simplify column checks
    )
    schema = {
        "tables": {
            "test_table": {
                "columns": [
                    {
                        "name": "col1",
                        "sqlite_type": "INT",
                        "not_null": True,
                        "default_value": 0,
                        "pk": 1,
                    },
                    {
                        "name": "col2",
                        "sqlite_type": "TEXT",
                        "not_null": False,
                        "default_value": "abc",
                        "pk": 0,
                    },
                ]
            }
        }
    }
    minimal_schema = apply_rules_to_schema(schema, config)
    table_data = minimal_schema["tables"]["test_table"]

    expected_cols = (
        _canonicalize_value(
            {"name": "col1", "sqlite_type": "INT", "not_null": True, "default_value": 0}
        ),
        _canonicalize_value(
            {
                "name": "col2",
                "sqlite_type": "TEXT",
                "not_null": False,
                "default_value": "abc",
            }
        ),
    )
    assert table_data["columns"] == expected_cols
    assert table_data["primary_key_columns"] == ("col1",)


def test_apply_rules_column_structure_partially_enforced():
    config = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=True,
        enforce_column_order=True,
        enforce_column_names=True,  # Keep names
        enforce_column_types=False,  # Ignore types
        enforce_column_not_null_constraints=True,  # Keep not_null
        enforce_column_default_values=False,  # Ignore defaults
        enforce_primary_keys=False,
    )
    schema = {
        "tables": {
            "test_table": {
                "columns": [
                    {
                        "name": "col1",
                        "sqlite_type": "INT",
                        "not_null": True,
                        "default_value": 0,
                        "pk": 1,
                    },
                    {
                        "name": "col2",
                        "sqlite_type": "TEXT",
                        "not_null": False,
                        "default_value": "abc",
                        "pk": 0,
                    },
                ]
            }
        }
    }
    minimal_schema = apply_rules_to_schema(schema, config)
    table_data = minimal_schema["tables"]["test_table"]

    expected_cols = (
        _canonicalize_value({"name": "col1", "not_null": True}),
        _canonicalize_value({"name": "col2", "not_null": False}),
    )
    assert table_data["columns"] == expected_cols
    assert "primary_key_columns" not in table_data


def test_apply_rules_column_order_ignored():
    config = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=True,
        enforce_column_order=False,  # Key change
        enforce_column_names=True,
        enforce_column_types=True,
        enforce_column_not_null_constraints=False,
        enforce_column_default_values=False,
        enforce_primary_keys=False,
    )
    schema = {  # Columns are intentionally "out of order" if sorted by name
        "tables": {
            "test_table": {
                "columns": [
                    {"name": "col_z", "sqlite_type": "TEXT", "pk": 0},
                    {"name": "col_a", "sqlite_type": "INT", "pk": 0},
                ]
            }
        }
    }
    minimal_schema = apply_rules_to_schema(schema, config)
    table_data = minimal_schema["tables"]["test_table"]

    # Columns should be sorted (default sort key for dicts will sort by keys of dicts, then values)
    # Here, canonicalized dicts become frozensets of items. Sorting is based on these frozensets.
    col_a_def = _canonicalize_value({"name": "col_a", "sqlite_type": "INT"})
    col_z_def = _canonicalize_value({"name": "col_z", "sqlite_type": "TEXT"})

    expected_cols_sorted = tuple(
        sorted([col_a_def, col_z_def], key=_get_sort_key_for_list_of_dicts)
    )
    assert table_data["columns"] == expected_cols_sorted


def test_apply_rules_column_names_ignored_order_enforced():
    config = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=True,
        enforce_column_order=True,
        enforce_column_names=False,  # Key change
        enforce_column_types=True,
        enforce_column_not_null_constraints=False,
        enforce_column_default_values=False,
        enforce_primary_keys=False,
    )
    schema = {
        "tables": {
            "test_table": {
                "columns": [
                    {"name": "col_z", "sqlite_type": "TEXT", "pk": 0},
                    {"name": "col_a", "sqlite_type": "INT", "pk": 0},
                ]
            }
        }
    }
    minimal_schema = apply_rules_to_schema(schema, config)
    table_data = minimal_schema["tables"]["test_table"]

    # Names are ignored, so only type is present in min_col_def. Order is preserved.
    expected_cols = (
        _canonicalize_value({"sqlite_type": "TEXT"}),
        _canonicalize_value({"sqlite_type": "INT"}),
    )
    assert table_data["columns"] == expected_cols


def test_apply_rules_primary_keys():
    config = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=True,
        enforce_column_order=False,
        enforce_column_names=False,
        enforce_column_types=False,  # Simplify columns
        enforce_column_not_null_constraints=False,
        enforce_column_default_values=False,
        enforce_primary_keys=True,
        enforce_foreign_keys=False,
    )
    schema = {
        "tables": {
            "pk_table": {  # Composite PK, order matters in definition
                "columns": [{"name": "id2", "pk": 2}, {"name": "id1", "pk": 1}]
            },
            "no_pk_table": {"columns": [{"name": "data", "pk": 0}]},
        }
    }
    minimal_schema = apply_rules_to_schema(schema, config)
    assert minimal_schema["tables"]["pk_table"]["primary_key_columns"] == (
        "id1",
        "id2",
    )  # Sorted by pk index
    assert minimal_schema["tables"]["no_pk_table"]["primary_key_columns"] == tuple()


def test_apply_rules_foreign_keys():
    config_enf_actions = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=True,
        enforce_column_order=False,
        enforce_column_names=False,
        enforce_column_types=False,
        enforce_column_not_null_constraints=False,
        enforce_column_default_values=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=True,
        enforce_foreign_key_referential_actions=True,
    )
    config_no_actions = SDIFSchemaConfig(
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=True,
        enforce_column_order=False,
        enforce_column_names=False,
        enforce_column_types=False,
        enforce_column_not_null_constraints=False,
        enforce_column_default_values=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=True,
        enforce_foreign_key_referential_actions=False,  # Key change
    )
    schema = {
        "tables": {
            "fk_table": {
                "columns": [{"name": "f1", "pk": 0}, {"name": "f2", "pk": 0}],
                "foreign_keys": [  # Composite FK, seq matters for grouping
                    {
                        "id": 0,
                        "seq": 1,
                        "target_table": "t2",
                        "from_column": "f2",
                        "target_column": "c2",
                        "on_update": "CASCADE",
                        "on_delete": "NO ACTION",
                    },
                    {
                        "id": 0,
                        "seq": 0,
                        "target_table": "t2",
                        "from_column": "f1",
                        "target_column": "c1",
                        "on_update": "CASCADE",
                        "on_delete": "NO ACTION",
                    },
                    {
                        "id": 1,
                        "seq": 0,
                        "target_table": "t3",
                        "from_column": "f1",
                        "target_column": "c3",
                        "on_update": "RESTRICT",
                        "on_delete": "SET NULL",
                    },
                ],
            }
        }
    }

    # With referential actions
    minimal_enf = apply_rules_to_schema(schema, config_enf_actions)
    fks_enf = minimal_enf["tables"]["fk_table"]["foreign_keys"]

    expected_fk1_enf = _canonicalize_value(
        {
            "from_columns": ("f1", "f2"),
            "target_table": "t2",
            "target_columns": ("c1", "c2"),
            "on_update": "CASCADE",
            "on_delete": "NO ACTION",
        }
    )
    expected_fk2_enf = _canonicalize_value(
        {
            "from_columns": ("f1",),
            "target_table": "t3",
            "target_columns": ("c3",),
            "on_update": "RESTRICT",
            "on_delete": "SET NULL",
        }
    )
    # FKs list is sorted
    assert fks_enf == tuple(
        sorted(
            [expected_fk1_enf, expected_fk2_enf], key=_get_sort_key_for_list_of_dicts
        )
    )

    # Without referential actions
    minimal_no_actions = apply_rules_to_schema(schema, config_no_actions)
    fks_no_actions = minimal_no_actions["tables"]["fk_table"]["foreign_keys"]
    expected_fk1_no_act = _canonicalize_value(
        {
            "from_columns": ("f1", "f2"),
            "target_table": "t2",
            "target_columns": ("c1", "c2"),
        }
    )
    expected_fk2_no_act = _canonicalize_value(
        {"from_columns": ("f1",), "target_table": "t3", "target_columns": ("c3",)}
    )
    assert fks_no_actions == tuple(
        sorted(
            [expected_fk1_no_act, expected_fk2_no_act],
            key=_get_sort_key_for_list_of_dicts,
        )
    )

    # Test no FKs
    schema_no_fk = {"tables": {"no_fk_table": {"columns": [], "foreign_keys": []}}}
    minimal_no_fk = apply_rules_to_schema(schema_no_fk, config_enf_actions)
    assert minimal_no_fk["tables"]["no_fk_table"]["foreign_keys"] == tuple()


def test_apply_rules_objects():
    schema = BASIC_SCHEMA_V1  # Uses obj1, obj2 (error hint), obj3 (no hint)

    # Mode: names_only
    config_names = SDIFSchemaConfig(
        objects_mode="names_only",
        enforce_sdif_version=False,
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_names = apply_rules_to_schema(schema, config_names)
    assert "objects" in res_names
    assert (
        "obj1" in res_names["objects"] and res_names["objects"]["obj1"] == frozenset()
    )
    assert (
        "obj2" in res_names["objects"] and res_names["objects"]["obj2"] == frozenset()
    )
    assert (
        "obj3" in res_names["objects"] and res_names["objects"]["obj3"] == frozenset()
    )

    # Mode: names_and_schema_hint
    config_hint = SDIFSchemaConfig(
        objects_mode="names_and_schema_hint",
        enforce_sdif_version=False,
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_hint = apply_rules_to_schema(schema, config_hint)
    assert res_hint["objects"]["obj1"] == _canonicalize_value(
        {"schema_hint_exists_and_valid": True, "schema_hint": {"type": "string"}}
    )
    assert res_hint["objects"]["obj2"] == _canonicalize_value(
        {"schema_hint_exists_and_valid": False}
    )  # Error hint
    assert res_hint["objects"]["obj3"] == _canonicalize_value(
        {"schema_hint_exists_and_valid": False}
    )  # No hint

    # Mode: ignore
    config_ignore = SDIFSchemaConfig(
        objects_mode="ignore",
        enforce_sdif_version=False,
        media_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_ignore = apply_rules_to_schema(schema, config_ignore)
    assert "objects" not in res_ignore


def test_apply_rules_media():
    schema = BASIC_SCHEMA_V1  # Uses media1, media2 (error tech_meta), media3 (no original_format/tech_meta)

    # Mode: names_only
    config_names = SDIFSchemaConfig(
        media_mode="names_only",
        media_technical_metadata_mode="ignore",
        enforce_sdif_version=False,
        objects_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_names = apply_rules_to_schema(schema, config_names)
    assert "media" in res_names
    assert res_names["media"]["media1"] == frozenset()
    assert res_names["media"]["media2"] == frozenset()
    assert res_names["media"]["media3"] == frozenset()

    # Mode: names_and_type
    config_type = SDIFSchemaConfig(
        media_mode="names_and_type",
        media_technical_metadata_mode="ignore",
        enforce_sdif_version=False,
        objects_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_type = apply_rules_to_schema(schema, config_type)
    assert res_type["media"]["media1"] == _canonicalize_value(
        {"media_type": "image/png"}
    )
    assert res_type["media"]["media2"] == _canonicalize_value(
        {"media_type": "application/json"}
    )
    assert res_type["media"]["media3"] == _canonicalize_value(
        {"media_type": "video/mp4"}
    )

    # Mode: names_type_and_original_format
    config_format = SDIFSchemaConfig(
        media_mode="names_type_and_original_format",
        media_technical_metadata_mode="ignore",
        enforce_sdif_version=False,
        objects_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_format = apply_rules_to_schema(schema, config_format)
    assert res_format["media"]["media1"] == _canonicalize_value(
        {"media_type": "image/png", "original_format": "png"}
    )
    assert res_format["media"]["media2"] == _canonicalize_value(
        {"media_type": "application/json", "original_format": None}
    )
    assert res_format["media"]["media3"] == _canonicalize_value(
        {"media_type": "video/mp4", "original_format": None}
    )

    # Technical Metadata: content_comparison
    config_tech_meta = SDIFSchemaConfig(
        media_mode="names_only",
        media_technical_metadata_mode="content_comparison",
        enforce_sdif_version=False,
        objects_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_tech_meta = apply_rules_to_schema(schema, config_tech_meta)
    assert res_tech_meta["media"]["media1"] == _canonicalize_value(
        {
            "technical_metadata_exists_and_valid": True,
            "technical_metadata": {"dpi": 300},
        }
    )
    assert res_tech_meta["media"]["media2"] == _canonicalize_value(
        {"technical_metadata_exists_and_valid": False}
    )  # Error in tech_meta
    assert res_tech_meta["media"]["media3"] == _canonicalize_value(
        {"technical_metadata_exists_and_valid": False}
    )  # No tech_meta

    # Mode: ignore
    config_ignore = SDIFSchemaConfig(
        media_mode="ignore",
        media_technical_metadata_mode="ignore",
        enforce_sdif_version=False,
        objects_mode="ignore",
        semantic_links_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_ignore = apply_rules_to_schema(schema, config_ignore)
    assert "media" not in res_ignore


def test_apply_rules_semantic_links():
    schema = BASIC_SCHEMA_V1  # Has two links
    schema_no_links = {
        k: v for k, v in BASIC_SCHEMA_V1.items() if k != "semantic_links"
    }

    # Mode: link_types_only
    config_types = SDIFSchemaConfig(
        semantic_links_mode="link_types_only",
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_types = apply_rules_to_schema(schema, config_types)
    assert res_types["semantic_link_types_present"] == frozenset(
        ["related", "parent_of"]
    )

    res_types_no_links = apply_rules_to_schema(schema_no_links, config_types)
    assert res_types_no_links["semantic_link_types_present"] == frozenset()

    # Mode: full_structure
    config_full = SDIFSchemaConfig(
        semantic_links_mode="full_structure",
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_full = apply_rules_to_schema(schema, config_full)

    link1_min = _canonicalize_value(
        {
            "link_type": "related",
            "from_element_type": "table",
            "from_element_spec": "table1",
            "to_element_type": "object",
            "to_element_spec": "obj1",
        }
    )
    link2_min = _canonicalize_value(
        {
            "link_type": "parent_of",
            "from_element_type": "object",
            "from_element_spec": "obj1",
            "to_element_type": "object",
            "to_element_spec": "obj2",
        }
    )
    expected_links = tuple(
        sorted([link1_min, link2_min], key=_get_sort_key_for_list_of_dicts)
    )
    assert res_full["semantic_links"] == expected_links

    res_full_no_links = apply_rules_to_schema(schema_no_links, config_full)
    assert res_full_no_links["semantic_links"] == tuple()

    # Mode: ignore
    config_ignore = SDIFSchemaConfig(
        semantic_links_mode="ignore",
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
    )
    res_ignore = apply_rules_to_schema(schema, config_ignore)
    assert "semantic_links" not in res_ignore
    assert "semantic_link_types_present" not in res_ignore


def test_apply_rules_tables_set_created_when_no_tables_but_pk_fk_enforced():
    config = SDIFSchemaConfig(
        enforce_sdif_version=False,
        enforce_table_names=False,  # Key for tables_set
        enforce_primary_keys=True,  # or enforce_foreign_keys=True
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
    )
    minimal_schema = apply_rules_to_schema(EMPTY_SCHEMA, config)
    assert "tables_set" in minimal_schema
    assert minimal_schema["tables_set"] == frozenset()

    config_fk = SDIFSchemaConfig(
        enforce_sdif_version=False,
        enforce_table_names=False,
        enforce_foreign_keys=True,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
    )
    minimal_schema_fk = apply_rules_to_schema(EMPTY_SCHEMA, config_fk)
    assert "tables_set" in minimal_schema_fk
    assert minimal_schema_fk["tables_set"] == frozenset()

    config_neither_pk_fk = SDIFSchemaConfig(
        enforce_sdif_version=False,
        enforce_table_names=False,
        enforce_primary_keys=False,
        enforce_foreign_keys=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
    )
    # If no tables AND (pk or fk enforced is False) AND enforce_table_names=False,
    # "tables_set" is NOT added if raw_tables_schema is empty.
    minimal_schema_neither = apply_rules_to_schema(EMPTY_SCHEMA, config_neither_pk_fk)
    assert "tables_set" not in minimal_schema_neither
    assert "tables" not in minimal_schema_neither  # Because table_names=False too


# --- Minimal Schema Comparator for testing apply_rules_to_schema indirectly ---
# This helps ensure that transformations are consistent for comparison purposes.


def schemas_are_equivalent(
    schema1: Dict[str, Any], schema2: Dict[str, Any], config: SDIFSchemaConfig
) -> bool:
    """
    A simplified comparison based on the output of apply_rules_to_schema.
    This is NOT a replacement for SDIFSchemaComparator but a test utility.
    """
    min_s1 = apply_rules_to_schema(schema1, config)
    min_s2 = apply_rules_to_schema(schema2, config)
    # A basic check; DeepDiff would be more robust for complex structures / debugging.
    return min_s1 == min_s2


def test_schema_equivalency_with_apply_rules():
    schema_a = {
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER", "pk": 1},
                    {"name": "name", "sqlite_type": "TEXT", "pk": 0},
                ],
                "primary_key": ["id"],
            }
        },
    }
    schema_b = {  # Same as A but different column order definition in source
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "users": {
                "columns": [
                    {"name": "name", "sqlite_type": "TEXT", "pk": 0},
                    {"name": "id", "sqlite_type": "INTEGER", "pk": 1},
                ],
                "primary_key": ["id"],
            }
        },
    }
    schema_c = {  # Different data
        "sdif_properties": {"sdif_version": "1.1"},  # Different version
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER", "pk": 1},
                    {"name": "email", "sqlite_type": "TEXT", "pk": 0},
                ],  # Different col
                "primary_key": ["id"],
            }
        },
    }
    schema_d = {  # Different table name
        "sdif_properties": {"sdif_version": "1.0"},
        "tables": {
            "accounts": {  # Different table name
                "columns": [
                    {"name": "id", "sqlite_type": "INTEGER", "pk": 1},
                    {"name": "name", "sqlite_type": "TEXT", "pk": 0},
                ],
                "primary_key": ["id"],
            }
        },
    }

    default_config = SDIFSchemaConfig(  # Enforces column order
        objects_mode="ignore", media_mode="ignore", semantic_links_mode="ignore"
    )
    assert schemas_are_equivalent(schema_a, schema_a, default_config)
    assert not schemas_are_equivalent(
        schema_a, schema_b, default_config
    )  # Col order matters
    assert not schemas_are_equivalent(schema_a, schema_c, default_config)
    assert not schemas_are_equivalent(schema_a, schema_d, default_config)

    config_ignore_col_order = SDIFSchemaConfig(
        enforce_column_order=False,  # Key change
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
    )
    assert schemas_are_equivalent(schema_a, schema_a, config_ignore_col_order)
    assert schemas_are_equivalent(
        schema_a, schema_b, config_ignore_col_order
    )  # Col order ignored, now equivalent
    assert not schemas_are_equivalent(schema_a, schema_c, config_ignore_col_order)

    config_ignore_table_names = SDIFSchemaConfig(
        enforce_table_names=False,  # Key change
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
    )
    # A and D should be equivalent if table names are ignored (structure is the same)
    # For this to work, all other aspects of the table must be compared,
    # so the default settings for columns, PKs etc. in config_ignore_table_names are important.
    assert schemas_are_equivalent(schema_a, schema_d, config_ignore_table_names)
    assert not schemas_are_equivalent(
        schema_a, schema_c, config_ignore_table_names
    )  # Still different version / column content

    schema_e_fk_order1 = {
        "tables": {
            "t1": {
                "columns": [{"name": "f1", "pk": 0}],
                "foreign_keys": [
                    {
                        "id": 0,
                        "seq": 0,
                        "target_table": "t2",
                        "from_column": "f1",
                        "target_column": "id",
                        "on_update": "CASCADE",
                        "on_delete": "CASCADE",
                    }
                ],
            }
        }
    }
    schema_f_fk_order2 = {  # same FKs, just different definition order in source list
        "tables": {
            "t1": {
                "columns": [{"name": "f1", "pk": 0}],
                "foreign_keys": [
                    {
                        "id": 1,
                        "seq": 0,
                        "target_table": "t3",
                        "from_column": "f1",
                        "target_column": "id",
                        "on_update": "CASCADE",
                        "on_delete": "CASCADE",
                    },  # This FK is different
                    {
                        "id": 0,
                        "seq": 0,
                        "target_table": "t2",
                        "from_column": "f1",
                        "target_column": "id",
                        "on_update": "CASCADE",
                        "on_delete": "CASCADE",
                    },
                ],
            }
        }
    }
    # The canonicalization process for foreign_keys sorts them, so their original order in the list doesn't matter.
    # schema_f_fk_order2 actually has a DIFFERENT set of FKs compared to schema_e_fk_order1
    # Let's make one with same FKs but different order in the list
    schema_g_fk_same_but_reordered_list = {
        "tables": {
            "t1": {
                "columns": [{"name": "f1", "pk": 0}, {"name": "f2", "pk": 0}],
                "foreign_keys": [
                    {
                        "id": 1,
                        "seq": 0,
                        "target_table": "t3",
                        "from_column": "f2",
                        "target_column": "id",
                        "on_update": "RESTRICT",
                        "on_delete": "SET NULL",
                    },
                    {
                        "id": 0,
                        "seq": 0,
                        "target_table": "t2",
                        "from_column": "f1",
                        "target_column": "id",
                        "on_update": "CASCADE",
                        "on_delete": "CASCADE",
                    },
                ],
            }
        }
    }
    schema_h_fk_same_as_g_different_list_order = {
        "tables": {
            "t1": {
                "columns": [{"name": "f1", "pk": 0}, {"name": "f2", "pk": 0}],
                "foreign_keys": [
                    {
                        "id": 0,
                        "seq": 0,
                        "target_table": "t2",
                        "from_column": "f1",
                        "target_column": "id",
                        "on_update": "CASCADE",
                        "on_delete": "CASCADE",
                    },
                    {
                        "id": 1,
                        "seq": 0,
                        "target_table": "t3",
                        "from_column": "f2",
                        "target_column": "id",
                        "on_update": "RESTRICT",
                        "on_delete": "SET NULL",
                    },
                ],
            }
        }
    }
    fk_config = SDIFSchemaConfig(
        enforce_foreign_keys=True,
        enforce_foreign_key_referential_actions=True,
        enforce_sdif_version=False,
        objects_mode="ignore",
        media_mode="ignore",
        semantic_links_mode="ignore",
    )
    assert not schemas_are_equivalent(schema_e_fk_order1, schema_f_fk_order2, fk_config)
    assert schemas_are_equivalent(
        schema_g_fk_same_but_reordered_list,
        schema_h_fk_same_as_g_different_list_order,
        fk_config,
    )
