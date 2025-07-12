import json
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from sdif_db.database import SDIFDatabase


# Initialization & Connection Management
def test_create_new_db(tmp_db_path: Path):
    """Verify SDIFDatabase(path) creates a file and basic metadata tables."""
    assert not tmp_db_path.exists()
    db = SDIFDatabase(tmp_db_path)
    assert tmp_db_path.exists()
    assert tmp_db_path.is_file()

    # Check for sdif_properties table existence
    try:
        props = db.get_properties()
        assert props is not None
        assert "sdif_version" in props
    finally:
        db.close()


def test_open_existing_db(tmp_db_path: Path):
    """Create a DB, close it, then reopen. Verify it's usable."""
    db1 = SDIFDatabase(tmp_db_path)
    source_id_1 = db1.add_source("file1.txt", "txt")
    db1.close()

    db2 = SDIFDatabase(tmp_db_path)
    sources = db2.list_sources()
    assert len(sources) == 1
    assert sources[0]["source_id"] == source_id_1
    assert sources[0]["original_file_name"] == "file1.txt"
    db2.close()


def test_read_only_mode_success(tmp_db_path: Path):
    """Successfully open an existing DB in read-only."""
    # Create a dummy DB file first
    SDIFDatabase(tmp_db_path).close()

    db_ro = SDIFDatabase(tmp_db_path, read_only=True)
    assert db_ro.read_only
    # Perform a read operation
    assert db_ro.get_properties() is not None
    db_ro.close()


def test_read_only_mode_file_not_found(tmp_db_path: Path):
    """Raise FileNotFoundError if DB file doesn't exist for read-only."""
    assert not tmp_db_path.exists()  # Ensure file does not exist
    with pytest.raises(FileNotFoundError):
        SDIFDatabase(tmp_db_path, read_only=True)


def test_read_only_mode_disallows_writes(readonly_db: SDIFDatabase):
    """Verify write operations raise PermissionError in read-only mode."""
    assert readonly_db.read_only
    with pytest.raises(PermissionError):
        readonly_db.add_source("test.txt", "txt")

    # Test another write operation (e.g., create_table)
    with pytest.raises(PermissionError):
        readonly_db.create_table(
            "test_table", {"id": {"type": "INTEGER"}}, source_id=1
        )  # Dummy source_id for test


def test_overwrite_mode(tmp_db_path: Path):
    """Create a DB, add data. Re-initialize with overwrite=True. Verify old data gone."""
    db1 = SDIFDatabase(tmp_db_path)
    db1.add_source("initial_source.txt", "txt")
    assert len(db1.list_sources()) == 1
    db1.close()

    db2 = SDIFDatabase(tmp_db_path, overwrite=True)
    assert len(db2.list_sources()) == 0  # Should be a fresh DB
    props = db2.get_properties()
    assert props is not None
    assert "sdif_version" in props
    db2.close()


def test_context_manager(tmp_db_path: Path):
    """Use with SDIFDatabase(path) as db: ... and ensure connection state."""
    db_instance_outside = None
    with SDIFDatabase(tmp_db_path) as db:
        assert db.conn is not None
        db.add_source("context_source.txt", "txt")
        db_instance_outside = db

    assert db_instance_outside is not None
    # Connection should be closed after exiting context
    assert db_instance_outside.conn is None
    with pytest.raises(sqlite3.ProgrammingError):
        db_instance_outside.list_sources()

    # Verify data was written and can be read by a new connection
    with SDIFDatabase(tmp_db_path) as db_reopened:
        sources = db_reopened.list_sources()
        assert len(sources) == 1
        assert sources[0]["original_file_name"] == "context_source.txt"


def test_close_idempotency(empty_db: SDIFDatabase):
    """Call db.close() multiple times; should not error."""
    empty_db.close()
    empty_db.close()  # Second call
    # No exception should be raised


# Source Operations
def test_add_list_sources(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source  # Uses the fixture that already added a source

    sources = db.list_sources()
    assert len(sources) == 1
    added_source = sources[0]
    assert added_source["source_id"] == source_id
    assert added_source["original_file_name"] == "test_file.csv"
    assert added_source["original_file_type"] == "csv"
    assert added_source["source_description"] == "Test source"
    assert "processing_timestamp" in added_source

    # Verify direct table content
    db.query(
        f"SELECT * FROM sdif_sources WHERE source_id = {source_id}",
        return_format="dict",
    )
    # The query method as implemented seems to execute the plain_sql and not take params.
    # We'll adjust this if query() is changed or use a more direct execute for verification.
    cursor = db.conn.execute(
        "SELECT * FROM sdif_sources WHERE source_id = ?", (source_id,)
    )
    row = cursor.fetchone()
    assert row is not None
    assert dict(row)["original_file_name"] == "test_file.csv"


def test_add_multiple_sources(empty_db: SDIFDatabase):
    source_id_1 = empty_db.add_source("file1.csv", "csv", "First source")
    source_id_2 = empty_db.add_source("doc.pdf", "pdf", "Second source")

    sources = empty_db.list_sources()
    assert len(sources) == 2

    source_map = {s["source_id"]: s for s in sources}
    assert source_id_1 in source_map
    assert source_map[source_id_1]["original_file_name"] == "file1.csv"
    assert source_id_2 in source_map
    assert source_map[source_id_2]["original_file_name"] == "doc.pdf"


# Table Creation & Basic Metadata
def test_create_simple_table(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    table_name = "test_table_simple"
    columns = {"id": {"type": "INTEGER", "primary_key": True}, "name": {"type": "TEXT"}}
    created_table_name = db.create_table(
        table_name, columns, source_id, description="A test table"
    )
    assert created_table_name == table_name

    assert table_name in db.list_tables()

    metadata = db.get_table_metadata(table_name)
    assert metadata is not None
    assert metadata["table_name"] == table_name
    assert metadata["source_id"] == source_id
    assert metadata["description"] == "A test table"
    assert metadata["row_count"] == 0
    assert len(metadata["columns"]) == 2
    col_names = {col["column_name"] for col in metadata["columns"]}
    assert col_names == {"id", "name"}

    # Check physical table in sqlite_master
    cursor = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    assert cursor.fetchone() is not None


def test_create_table_if_exists_fail(
    db_with_simple_table: tuple[SDIFDatabase, int, str],
):
    db, _, table_name = db_with_simple_table  # table_name is already created by fixture

    with pytest.raises(ValueError, match=f"Table '{table_name}' already exists."):
        db.create_table(
            table_name, {"col1": {"type": "TEXT"}}, source_id=1
        )  # source_id can be dummy here


def test_create_table_if_exists_replace(
    db_with_simple_table: tuple[SDIFDatabase, int, str],
):
    db, source_id, table_name = db_with_simple_table

    # Insert some data into the original table
    db.insert_data(table_name, [{"id": 1, "name": "Original Name", "value": 10.5}])
    assert db.read_table(table_name).shape[0] == 1

    new_columns = {
        "key": {"type": "TEXT", "primary_key": True},
        "data": {"type": "BLOB"},
    }
    replaced_table_name = db.create_table(
        table_name,
        new_columns,
        source_id,
        description="Replaced table",
        if_exists="replace",
    )
    assert replaced_table_name == table_name

    metadata = db.get_table_metadata(table_name)
    assert metadata is not None
    assert metadata["description"] == "Replaced table"
    assert len(metadata["columns"]) == 2
    new_col_names = {col["column_name"] for col in metadata["columns"]}
    assert new_col_names == {"key", "data"}

    # Verify old data is gone (table should be empty or raise error if columns don't match for read_table)
    # Since columns changed, reading directly might fail or return empty for old columns.
    # Best to check row_count in metadata or query for new columns.
    assert metadata["row_count"] == 0
    # Ensure it's readable with new structure (will be empty)
    df_new = db.read_table(table_name)
    assert df_new.empty
    assert list(df_new.columns) == ["key", "data"]


def test_create_table_if_exists_add(
    db_with_simple_table: tuple[SDIFDatabase, int, str],
):
    db, source_id, original_table_name = db_with_simple_table

    new_columns = {"info": {"type": "TEXT"}}
    # Try to create with the same original_table_name, expecting a new name
    added_table_name = db.create_table(
        original_table_name,
        new_columns,
        source_id,
        description="Added table",
        if_exists="add",
    )

    assert added_table_name != original_table_name
    assert added_table_name.startswith(f"{original_table_name}_")
    assert added_table_name in db.list_tables()

    original_metadata = db.get_table_metadata(original_table_name)
    assert original_metadata is not None  # Original table should still exist
    assert len(original_metadata["columns"]) == 3  # From db_with_simple_table fixture

    added_metadata = db.get_table_metadata(added_table_name)
    assert added_metadata is not None
    assert added_metadata["description"] == "Added table"
    assert len(added_metadata["columns"]) == 1
    assert added_metadata["columns"][0]["column_name"] == "info"


def test_create_table_with_pk_fk(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    parent_table_name = "parent_pk_fk"
    child_table_name = "child_pk_fk"

    parent_cols = {"id": {"type": "INTEGER", "primary_key": True}}
    db.create_table(parent_table_name, parent_cols, source_id)

    child_cols = {
        "child_id": {"type": "INTEGER", "primary_key": True},
        "parent_ref": {
            "type": "INTEGER",
            "foreign_key": {"table": parent_table_name, "column": "id"},
        },
    }
    db.create_table(child_table_name, child_cols, source_id)

    # Verify FK via get_schema (or direct PRAGMA)
    schema = db.get_schema()
    assert child_table_name in schema["tables"]
    child_table_schema = schema["tables"][child_table_name]
    assert len(child_table_schema["foreign_keys"]) == 1
    fk = child_table_schema["foreign_keys"][0]
    assert fk["from_column"] == "parent_ref"
    assert fk["target_table"] == parent_table_name
    assert fk["target_column"] == "id"


def test_invalid_table_creation(empty_db: SDIFDatabase):
    db = empty_db
    source_id = db.add_source("dummy.src", "src")  # Need a valid source_id

    with pytest.raises(ValueError, match="must not start with 'sdif_'"):
        db.create_table("sdif_my_table", {"id": {"type": "INT"}}, source_id)

    with pytest.raises(ValueError, match="Cannot create a table with no columns"):
        db.create_table("no_cols_table", {}, source_id)

    with pytest.raises(ValueError, match="Invalid source_id: 9999 does not exist"):
        db.create_table("bad_source_table", {"id": {"type": "INT"}}, source_id=9999)


# Data Insertion & Reading
def test_insert_read_table(db_with_simple_table: tuple[SDIFDatabase, int, str]):
    db, _, table_name = (
        db_with_simple_table  # Fixture provides db, source_id, table_name
    )

    data_to_insert = [
        {"id": 1, "name": "Alice", "value": 10.5},
        {"id": 2, "name": "Bob", "value": 22.3},
        {"id": 3, "name": "Charlie", "value": None},  # Test None for REAL type
    ]
    db.insert_data(table_name, data_to_insert)

    # Verify row_count in metadata
    metadata = db.get_table_metadata(table_name)
    assert metadata is not None
    assert metadata["row_count"] == len(data_to_insert)

    # Read back and verify content
    df_read = db.read_table(table_name)
    assert isinstance(df_read, pd.DataFrame)
    assert df_read.shape[0] == len(data_to_insert)
    assert list(df_read.columns) == ["id", "name", "value"]

    # Convert to list of dicts for easier comparison, handling NaN for None value
    expected_df = pd.DataFrame(data_to_insert)
    pd.testing.assert_frame_equal(
        df_read, expected_df, check_dtype=False
    )  # check_dtype False due to potential int/float nuances with None


def test_insert_empty_data(db_with_simple_table: tuple[SDIFDatabase, int, str]):
    db, _, table_name = db_with_simple_table
    db.insert_data(table_name, [])  # Insert empty list
    metadata = db.get_table_metadata(table_name)
    assert metadata["row_count"] == 0
    df_read = db.read_table(table_name)
    assert df_read.empty


def test_insert_data_constraints(db_with_simple_table: tuple[SDIFDatabase, int, str]):
    db, _, table_name = (
        db_with_simple_table  # table 'simple_test_table' has id PK, name NOT NULL
    )

    # Violate NOT NULL (name)
    with pytest.raises(sqlite3.IntegrityError):
        db.insert_data(table_name, [{"id": 1, "name": None, "value": 1.0}])

    # Violate PRIMARY KEY (id)
    db.insert_data(table_name, [{"id": 10, "name": "Unique Name", "value": 1.0}])
    with pytest.raises(sqlite3.IntegrityError):
        db.insert_data(table_name, [{"id": 10, "name": "Another Name", "value": 2.0}])


def test_read_non_existent_table(empty_db: SDIFDatabase):
    with pytest.raises(
        ValueError, match="Table 'non_existent_table' not found in the database file."
    ):
        empty_db.read_table("non_existent_table")


# DataFrame Operations
def test_write_read_dataframe(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    table_name = "df_test_table"

    data = {
        "col_int": [1, 2, 3],
        "col_str": ["a", "b", "c"],
        "col_float": [1.1, 2.2, np.nan],  # Test NaN
        "col_bool": [True, False, True],
        "col_dt": [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2, 10, 30),
            pd.NaT,
        ],  # Test NaT
    }
    df_original = pd.DataFrame(data)

    # Test if_exists='fail' (default)
    db.write_dataframe(
        df_original.copy(), table_name, source_id, description="DF table"
    )

    df_read = db.read_table(table_name)

    # Prepare expected DataFrame (SQLite might convert bools to 0/1, NaT/NaN to None, datetimes to ISO strings)
    df_expected = df_original.copy()
    df_expected["col_bool"] = df_expected["col_bool"].astype(int)
    df_expected["col_dt"] = (
        df_expected["col_dt"].astype(object).where(df_expected["col_dt"].notna(), None)
    )
    # Convert datetimes to string if they are not None
    df_expected["col_dt"] = df_expected["col_dt"].apply(
        lambda x: x.isoformat() if isinstance(x, datetime) else x
    )
    df_expected = df_expected.replace(
        {np.nan: None}
    )  # General NaN to None for float comparison

    pd.testing.assert_frame_equal(df_read, df_expected, check_dtype=False)

    metadata = db.get_table_metadata(table_name)
    assert metadata is not None
    assert metadata["row_count"] == len(df_original)
    assert metadata["description"] == "DF table"
    assert len(metadata["columns"]) == len(df_original.columns)

    # Test if_exists='replace'
    df_new_data = pd.DataFrame({"new_col": ["x", "y"]})
    db.write_dataframe(
        df_new_data.copy(),
        table_name,
        source_id,
        description="Replaced DF",
        if_exists="replace",
    )
    df_read_replaced = db.read_table(table_name)
    pd.testing.assert_frame_equal(df_read_replaced, df_new_data, check_dtype=False)
    metadata_replaced = db.get_table_metadata(table_name)
    assert metadata_replaced["description"] == "Replaced DF"
    assert len(metadata_replaced["columns"]) == 1

    # Test if_exists='fail' when table exists
    with pytest.raises(ValueError, match=f"Table '{table_name}' already exists."):
        db.write_dataframe(df_original, table_name, source_id, if_exists="fail")

    # Test write_dataframe with an empty dataframe
    empty_df_table_name = "empty_df_table"
    empty_df = pd.DataFrame({"A": pd.Series(dtype="int"), "B": pd.Series(dtype="str")})
    db.write_dataframe(
        empty_df, empty_df_table_name, source_id, description="Empty DF Table"
    )
    meta_empty = db.get_table_metadata(empty_df_table_name)
    assert meta_empty is not None
    assert meta_empty["row_count"] == 0
    assert len(meta_empty["columns"]) == 2
    read_empty_df = db.read_table(empty_df_table_name)
    assert read_empty_df.empty
    assert list(read_empty_df.columns) == ["A", "B"]


def test_write_dataframe_with_column_metadata(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    table_name = "df_col_meta_table"
    df = pd.DataFrame({"finalName": [1, 2], "anotherFinalName": ["a", "b"]})

    cols_meta = {
        "finalName": {
            "description": "This is an ID",
            "original_column_name": "originalID",
        },
        "anotherFinalName": {
            "description": "Some text",
            "original_column_name": "originalText",
        },
    }
    db.write_dataframe(df, table_name, source_id, columns_metadata=cols_meta)

    table_metadata = db.get_table_metadata(table_name)
    assert table_metadata is not None
    found_cols_meta = {c["column_name"]: c for c in table_metadata["columns"]}

    assert found_cols_meta["finalName"]["description"] == "This is an ID"
    assert found_cols_meta["finalName"]["original_column_name"] == "originalID"
    assert found_cols_meta["anotherFinalName"]["description"] == "Some text"
    assert found_cols_meta["anotherFinalName"]["original_column_name"] == "originalText"


# Object Storage
def test_add_get_list_object(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    object_name = "test_object_1"
    json_data = {"key": "value", "number": 123, "nested": {"a": True}}
    schema_hint = {"type": "object", "properties": {"key": {"type": "string"}}}

    db.add_object(
        object_name,
        json_data,
        source_id,
        description="My test object",
        schema_hint=schema_hint,
    )

    assert object_name in db.list_objects()

    # Test get_object with parse_json=True (default)
    retrieved_obj_parsed = db.get_object(object_name)
    assert retrieved_obj_parsed is not None
    assert retrieved_obj_parsed["object_name"] == object_name
    assert retrieved_obj_parsed["source_id"] == source_id
    assert retrieved_obj_parsed["description"] == "My test object"
    assert retrieved_obj_parsed["json_data"] == json_data
    assert retrieved_obj_parsed["schema_hint"] == schema_hint

    # Test get_object with parse_json=False
    retrieved_obj_raw = db.get_object(object_name, parse_json=False)
    assert retrieved_obj_raw is not None
    assert retrieved_obj_raw["json_data"] == json.dumps(json_data)
    assert retrieved_obj_raw["schema_hint"] == json.dumps(schema_hint)

    # Test get non-existent object
    assert db.get_object("non_existent_object") is None


def test_add_object_duplicate_name(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    object_name = "duplicate_obj_test"
    db.add_object(object_name, {"data": 1}, source_id)

    with pytest.raises(
        (ValueError, sqlite3.IntegrityError),
        match=f"Object with name '{object_name}' may already exist.",
    ):
        db.add_object(object_name, {"data": 2}, source_id)


def test_add_object_invalid_source_id(empty_db: SDIFDatabase):
    with pytest.raises(ValueError, match="Invalid source_id: 9999 does not exist"):
        empty_db.add_object("obj_bad_source", {"data": 1}, source_id=9999)


def test_add_object_non_serializable_data(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    # Path objects are not directly JSON serializable
    with pytest.raises(TypeError, match="is not JSON serializable"):
        db.add_object("obj_non_serializable", Path("."), source_id)
    with pytest.raises(TypeError, match="is not JSON serializable"):
        db.add_object(
            "obj_non_serializable_hint", {"data": 1}, source_id, schema_hint=Path(".")
        )


# Media Storage
def test_add_get_list_media(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    media_name = "test_media_1"
    media_data = b"\x00\x01\x02\x03Hello Media\x04\x05"
    media_type = "binary/custom"
    original_format = "custom"
    tech_meta = {"encoding": "custom", "details": "some binary data"}

    db.add_media(
        media_name,
        media_data,
        media_type,
        source_id,
        description="My test media",
        original_format=original_format,
        technical_metadata=tech_meta,
    )

    assert media_name in db.list_media()

    # Test get_media with parse_json=True (default)
    retrieved_media_parsed = db.get_media(media_name)
    assert retrieved_media_parsed is not None
    assert retrieved_media_parsed["media_name"] == media_name
    assert retrieved_media_parsed["media_data"] == media_data
    assert retrieved_media_parsed["media_type"] == media_type
    assert retrieved_media_parsed["source_id"] == source_id
    assert retrieved_media_parsed["description"] == "My test media"
    assert retrieved_media_parsed["original_format"] == original_format
    assert retrieved_media_parsed["technical_metadata"] == tech_meta

    # Test get_media with parse_json=False
    retrieved_media_raw = db.get_media(media_name, parse_json=False)
    assert retrieved_media_raw is not None
    assert retrieved_media_raw["technical_metadata"] == json.dumps(tech_meta)

    # Test get non-existent media
    assert db.get_media("non_existent_media") is None


def test_add_media_duplicate_name(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    media_name = "duplicate_media_test"
    db.add_media(media_name, b"data1", "bin", source_id)

    with pytest.raises(
        (ValueError, sqlite3.IntegrityError),
        match=f"Media with name '{media_name}' may already exist.",
    ):
        db.add_media(media_name, b"data2", "bin", source_id)


def test_add_media_invalid_source_id(empty_db: SDIFDatabase):
    with pytest.raises(ValueError, match="Invalid source_id: 8888 does not exist"):
        empty_db.add_media("media_bad_source", b"data", "bin", source_id=8888)


def test_add_media_data_not_bytes(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    with pytest.raises(TypeError, match="media_data must be of type bytes"):
        db.add_media("media_bad_data", "not bytes", "txt", source_id)


def test_add_media_non_serializable_tech_meta(db_with_source: tuple[SDIFDatabase, int]):
    db, source_id = db_with_source
    with pytest.raises(TypeError, match="is not JSON serializable"):
        db.add_media(
            "media_bad_meta", b"data", "bin", source_id, technical_metadata=Path(".")
        )


# Semantic Links
def test_add_list_semantic_links(db_with_simple_table: tuple[SDIFDatabase, int, str]):
    db, source_id, table_name = db_with_simple_table
    object_name = "linked_object"
    db.add_object(object_name, {"obj_data": True}, source_id)

    link1_from_spec = {"table_name": table_name, "column_name": "name"}
    link1_to_spec = {"object_name": object_name}
    db.add_semantic_link(
        link_type="annotation",
        from_element_type="column",
        from_element_spec=link1_from_spec,
        to_element_type="object",
        to_element_spec=link1_to_spec,
        description="Name column is annotated by object",
    )

    link2_from_spec = {"source_id": source_id}
    link2_to_spec = {"table_name": table_name}
    db.add_semantic_link(
        link_type="reference",
        from_element_type="table",
        from_element_spec=link2_to_spec,  # Swapped for logical sense based on type
        to_element_type="source",
        to_element_spec=link2_from_spec,
    )

    links_parsed = db.list_semantic_links()
    assert len(links_parsed) == 2

    link_types = {link["link_type"] for link in links_parsed}
    assert link_types == {"annotation", "reference"}

    # Check one link in detail (parsed)
    l1_found = False
    for link in links_parsed:
        if link["link_type"] == "annotation":
            assert link["from_element_type"] == "column"
            assert link["from_element_spec"] == link1_from_spec
            assert link["to_element_type"] == "object"
            assert link["to_element_spec"] == link1_to_spec
            assert link["description"] == "Name column is annotated by object"
            l1_found = True
    assert l1_found

    # Test list_semantic_links with parse_json=False
    links_raw = db.list_semantic_links(parse_json=False)
    assert len(links_raw) == 2
    l1_raw_found = False
    for link in links_raw:
        if link["link_type"] == "annotation":
            assert link["from_element_spec"] == json.dumps(link1_from_spec)
            assert link["to_element_spec"] == json.dumps(link1_to_spec)
            l1_raw_found = True
    assert l1_raw_found


def test_add_semantic_link_invalid_types(empty_db: SDIFDatabase):
    from_spec = {"name": "a"}
    to_spec = {"name": "b"}
    with pytest.raises(ValueError, match="Invalid from_element_type: 'wrong_type'"):
        empty_db.add_semantic_link("link", "wrong_type", from_spec, "table", to_spec)
    with pytest.raises(ValueError, match="Invalid to_element_type: 'bad_type'"):
        empty_db.add_semantic_link("link", "object", from_spec, "bad_type", to_spec)


def test_add_semantic_link_non_serializable_spec(empty_db: SDIFDatabase):
    with pytest.raises(TypeError, match="not JSON serializable"):
        empty_db.add_semantic_link("link", "table", Path("."), "object", {"name": "a"})


# Schema Inspection
@pytest.fixture
def populated_db_for_schema(
    db_with_simple_table: tuple[SDIFDatabase, int, str],
) -> tuple[SDIFDatabase, dict]:
    db, source_id, table_name = db_with_simple_table

    # Add more elements for a comprehensive schema test
    # Another table with FK to simple_test_table
    fk_table_name = "fk_test_table"
    fk_columns = {
        "fk_id": {"type": "INTEGER", "primary_key": True},
        "ref_to_simple": {
            "type": "INTEGER",
            "foreign_key": {"table": table_name, "column": "id"},
        },
        "data": {"type": "TEXT"},
    }
    db.create_table(fk_table_name, fk_columns, source_id, description="FK table")

    # Object
    obj_name = "schema_test_object"
    obj_data = {"attr": "val", "num_attr": 100}
    obj_hint = {"type": "object", "properties": {"attr": {"type": "string"}}}
    db.add_object(obj_name, obj_data, source_id, schema_hint=obj_hint)

    # Media
    media_name = "schema_test_media.png"
    media_data = b"somedummyimagedata"
    db.add_media(media_name, media_data, "image/png", source_id, original_format="png")

    # Semantic Link
    link_from = {"table_name": table_name, "column_name": "name"}
    link_to = {"object_name": obj_name}
    db.add_semantic_link("relates_to", "column", link_from, "object", link_to)

    expected_elements = {
        "source_id": source_id,
        "table_name": table_name,
        "fk_table_name": fk_table_name,
        "obj_name": obj_name,
        "media_name": media_name,
    }
    return db, expected_elements


def test_get_schema_structure_populated(
    populated_db_for_schema: tuple[SDIFDatabase, dict],
):
    db, expected_elements = populated_db_for_schema
    schema = db.get_schema()

    # 1. sdif_properties
    assert "sdif_properties" in schema
    assert schema["sdif_properties"] is not None
    assert "sdif_version" in schema["sdif_properties"]

    # 2. sources
    assert "sources" in schema
    assert isinstance(schema["sources"], list)
    assert len(schema["sources"]) >= 1
    assert any(
        s["source_id"] == expected_elements["source_id"] for s in schema["sources"]
    )

    # 3. tables
    assert "tables" in schema
    assert isinstance(schema["tables"], dict)

    # Check simple_test_table (from fixture)
    simple_table_name = expected_elements["table_name"]
    assert simple_table_name in schema["tables"]
    simple_table_schema = schema["tables"][simple_table_name]
    assert "metadata" in simple_table_schema
    assert simple_table_schema["metadata"]["table_name"] == simple_table_name
    assert "columns" in simple_table_schema
    assert len(simple_table_schema["columns"]) == 3  # id, name, value
    col_names_simple = {c["name"] for c in simple_table_schema["columns"]}
    assert col_names_simple == {"id", "name", "value"}
    pk_cols_simple = [
        c["name"] for c in simple_table_schema["columns"] if c["primary_key"]
    ]
    assert pk_cols_simple == ["id"]
    assert "foreign_keys" in simple_table_schema  # Should be empty list
    assert len(simple_table_schema["foreign_keys"]) == 0

    # Check fk_test_table
    fk_table_name = expected_elements["fk_table_name"]
    assert fk_table_name in schema["tables"]
    fk_table_schema = schema["tables"][fk_table_name]
    assert "metadata" in fk_table_schema
    assert "columns" in fk_table_schema
    assert len(fk_table_schema["columns"]) == 3  # fk_id, ref_to_simple, data
    assert "foreign_keys" in fk_table_schema
    assert len(fk_table_schema["foreign_keys"]) == 1
    fk_detail = fk_table_schema["foreign_keys"][0]
    assert fk_detail["from_column"] == "ref_to_simple"
    assert fk_detail["target_table"] == simple_table_name
    assert fk_detail["target_column"] == "id"

    # 4. objects
    assert "objects" in schema
    assert isinstance(schema["objects"], dict)
    obj_name = expected_elements["obj_name"]
    assert obj_name in schema["objects"]
    obj_schema = schema["objects"][obj_name]
    assert obj_schema["source_id"] == expected_elements["source_id"]
    assert obj_schema["schema_hint"] is not None  # From populated_db_for_schema
    assert obj_schema["schema_hint"]["type"] == "object"

    # 5. media
    assert "media" in schema
    assert isinstance(schema["media"], dict)
    media_name = expected_elements["media_name"]
    assert media_name in schema["media"]
    media_schema = schema["media"][media_name]
    assert media_schema["source_id"] == expected_elements["source_id"]
    assert media_schema["media_type"] == "image/png"
    assert media_schema["original_format"] == "png"

    # 6. semantic_links
    assert "semantic_links" in schema
    assert isinstance(schema["semantic_links"], list)
    assert len(schema["semantic_links"]) >= 1
    found_link = False
    for link in schema["semantic_links"]:
        if link["link_type"] == "relates_to":
            assert link["from_element_type"] == "column"
            assert link["to_element_type"] == "object"
            found_link = True
            break
    assert found_link


def test_get_schema_empty_db(empty_db: SDIFDatabase):
    schema = empty_db.get_schema()
    assert "sdif_properties" in schema
    assert schema["sdif_properties"] is not None
    assert "sources" in schema and len(schema["sources"]) == 0
    assert "tables" in schema and len(schema["tables"]) == 0
    assert "objects" in schema and len(schema["objects"]) == 0
    assert "media" in schema and len(schema["media"]) == 0
    assert "semantic_links" in schema and len(schema["semantic_links"]) == 0


@pytest.fixture
def db_for_query_tests(
    db_with_simple_table: tuple[SDIFDatabase, int, str],
) -> SDIFDatabase:
    db, source_id, table_name = db_with_simple_table
    # simple_test_table (id INT PK, name TEXT, value REAL)
    data = [
        {"id": 1, "name": "Query Test User 1", "value": 100.0},
        {"id": 2, "name": "Query Test User 2", "value": 200.0},
        {"id": 3, "name": "Query Test User 3", "value": 100.0},
    ]
    db.insert_data(table_name, data)
    return db


def test_query_select_dataframe(db_for_query_tests: SDIFDatabase):
    """Test successful SELECT query returning a DataFrame."""
    db = db_for_query_tests
    table_name = "simple_test_table"  # from db_with_simple_table fixture
    df_result = db.query(
        f'SELECT id, name FROM "{table_name}" WHERE value = 100.0 ORDER BY id'
    )
    assert isinstance(df_result, pd.DataFrame)
    assert len(df_result) == 2
    assert list(df_result.columns) == ["id", "name"]
    assert df_result.iloc[0]["id"] == 1
    assert df_result.iloc[1]["id"] == 3


def test_query_select_dict(db_for_query_tests: SDIFDatabase):
    """Test successful SELECT query returning a list of dicts."""
    db = db_for_query_tests
    table_name = "simple_test_table"
    dict_result = db.query(
        f'SELECT name FROM "{table_name}" WHERE id = 2', return_format="dict"
    )
    assert isinstance(dict_result, list)
    assert len(dict_result) == 1
    assert isinstance(dict_result[0], dict)
    assert dict_result[0]["name"] == "Query Test User 2"


def test_query_with_cte(db_for_query_tests: SDIFDatabase):
    """Test SELECT query with a Common Table Expression (CTE)."""
    db = db_for_query_tests
    table_name = "simple_test_table"
    query_sql = f"""
    WITH HighValues AS (
        SELECT id, name FROM "{table_name}\" WHERE value > 150.0
    )
    SELECT * FROM HighValues;
    """
    df_result = db.query(query_sql)
    assert len(df_result) == 1
    assert df_result.iloc[0]["name"] == "Query Test User 2"


def test_query_explain_allowed(db_for_query_tests: SDIFDatabase):
    """Test that EXPLAIN queries are allowed."""
    db = db_for_query_tests
    table_name = "simple_test_table"
    # EXPLAIN queries return different structures depending on SQLite version,
    # so we just check that it doesn't raise PermissionError and returns something.
    # The return type might be list of dicts if row_factory is still active.
    result = db.query(
        f'EXPLAIN QUERY PLAN SELECT * FROM "{table_name}"', return_format="dict"
    )
    assert isinstance(result, list)
    assert len(result) > 0
    # Example check for one of the typical columns in EXPLAIN QUERY PLAN output
    assert (
        "detail" in result[0] or "explain" in result[0]
    )  # Older vs Newer SQLite versions


@pytest.mark.parametrize(
    "disallowed_query",
    [
        "INSERT INTO simple_test_table (id, name, value) VALUES (4, 'Bad', 1.0)",
        "UPDATE simple_test_table SET name = 'Bad' WHERE id = 1",
        "DELETE FROM simple_test_table WHERE id = 1",
        "DROP TABLE simple_test_table",
        "CREATE TABLE new_bad_table (col TEXT)",
        "ALTER TABLE simple_test_table ADD COLUMN new_col TEXT",
        "ATTACH DATABASE './another.db' AS another",
        "PRAGMA writable_schema = ON",
        "REPLACE INTO simple_test_table (id, name, value) VALUES (1, 'Replaced', 1.0)",  # Tests REPLACE
        "VACUUM;",
        "REINDEX simple_test_table;",
    ],
)
def test_query_disallowed_statements(
    db_for_query_tests: SDIFDatabase, disallowed_query: str
):
    """Test that disallowed SQL statements raise PermissionError."""
    db = db_for_query_tests
    # Substitute table name if needed for some queries, though most are general
    final_query = disallowed_query.replace("simple_test_table", "simple_test_table")
    with pytest.raises(PermissionError):
        db.query(final_query)


def test_query_non_select_prefix_disallowed(db_for_query_tests: SDIFDatabase):
    """Test that queries not starting with SELECT, WITH, or EXPLAIN are disallowed."""
    db = db_for_query_tests
    with pytest.raises(
        PermissionError,
        match="Only SELECT, WITH...SELECT, or EXPLAIN queries are allowed",
    ):
        db.query("; SELECT * FROM simple_test_table")  # Starts with semicolon


def test_query_non_existent_table(db_for_query_tests: SDIFDatabase):
    """Test querying a non-existent table raises sqlite3.Error (OperationalError)."""
    db = db_for_query_tests
    with pytest.raises(
        sqlite3.OperationalError, match="no such table: non_existent_table"
    ):
        db.query("SELECT * FROM non_existent_table")


def test_query_returns_no_rows(db_for_query_tests: SDIFDatabase):
    """Test a query that correctly returns no rows."""
    db = db_for_query_tests
    table_name = "simple_test_table"
    df_result = db.query(f'SELECT * FROM "{table_name}" WHERE id = 999')
    assert isinstance(df_result, pd.DataFrame)
    assert df_result.empty
    assert list(df_result.columns) == [
        "id",
        "name",
        "value",
    ]  # Columns should still be there

    dict_result = db.query(
        f'SELECT * FROM "{table_name}" WHERE id = 999', return_format="dict"
    )
    assert isinstance(dict_result, list)
    assert not dict_result


def test_drop_table_successfully(db_with_simple_table: tuple[SDIFDatabase, int, str]):
    """Test successfully dropping an existing table."""
    db, source_id, table_name = db_with_simple_table  # 'simple_test_table'

    # Add a semantic link to check if it gets affected (currently, it won't be deleted by drop_table)
    db.add_semantic_link(
        link_type="test_link",
        from_element_type="table",
        from_element_spec={"table_name": table_name},
        to_element_type="source",
        to_element_spec={"source_id": source_id},
    )
    initial_links = db.list_semantic_links()
    assert len(initial_links) > 0

    assert table_name in db.list_tables()
    assert db.get_table_metadata(table_name) is not None
    # Verify physical existence
    cursor = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    assert cursor.fetchone() is not None

    db.drop_table(table_name)

    assert table_name not in db.list_tables()
    assert db.get_table_metadata(table_name) is None
    # Verify physical removal
    cursor = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    assert cursor.fetchone() is None

    # Verify column metadata is also gone
    col_meta_cursor = db.conn.execute(
        "SELECT * FROM sdif_columns_metadata WHERE table_name = ?", (table_name,)
    )
    assert col_meta_cursor.fetchone() is None

    # Verify semantic link still exists (based on current drop_table implementation)
    # If drop_table is updated to remove links, this assertion needs to change.
    remaining_links = db.list_semantic_links()
    assert len(remaining_links) == len(initial_links)
    assert any(
        link["from_element_spec"].get("table_name") == table_name
        for link in remaining_links
    )


def test_drop_non_existent_table(empty_db: SDIFDatabase):
    """Test dropping a non-existent table is handled gracefully."""
    # drop_table uses 'DROP TABLE IF EXISTS' and deletes from metadata where table_name matches.
    # So, no error should be raised.
    try:
        empty_db.drop_table("i_do_not_exist_table")
    except Exception as e:
        pytest.fail(f"Dropping non-existent table raised an unexpected error: {e}")

    assert "i_do_not_exist_table" not in empty_db.list_tables()


def test_drop_table_read_only_mode(readonly_db: SDIFDatabase):
    """Test dropping a table in read-only mode raises PermissionError."""
    # readonly_db fixture has 'simple_test_table' by virtue of its setup.
    # First, ensure the table exists to make the test meaningful.
    # We can't create it if it's read-only, so this relies on fixture setup.
    # Let's assume the fixture setup has a table, or this test logic needs adjustment.
    # For this test, we'll attempt to add a table, which should fail, then try to drop.
    # A better way might be to create a db, add table, close, reopen as read-only.

    # Try to get metadata for a table that should exist from fixture setup
    # (db_with_simple_table is basis for readonly_db)
    if "simple_test_table" in readonly_db.list_tables():
        with pytest.raises(PermissionError, match="Database is open in read-only mode"):
            readonly_db.drop_table("simple_test_table")
    else:
        # If the table isn't there for some reason, we can still test the drop attempt
        # on a non-existent table in read-only mode.
        with pytest.raises(PermissionError, match="Database is open in read-only mode"):
            readonly_db.drop_table("any_table_name_really")
