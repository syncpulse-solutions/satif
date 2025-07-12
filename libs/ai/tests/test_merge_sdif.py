import logging
import sqlite3  # For specific error checking if needed
from pathlib import Path

import pytest
from sdif_db.database import SDIFDatabase

from satif_ai.utils.merge_sdif import merge_sdif_files

# Configure logging for tests (optional, but can be helpful for debugging)
logging.basicConfig(level=logging.INFO)  # Or logging.DEBUG for more verbosity
log = logging.getLogger(__name__)


@pytest.fixture
def temp_test_dir(tmp_path_factory) -> Path:
    """Create a temporary directory for test files that persists if a test fails."""
    # Using tmp_path_factory to create a base temp directory if needed,
    # but for individual test runs, tmp_path fixture (function-scoped) is often sufficient.
    # This creates a sub-directory that's easier to find.
    tdir = tmp_path_factory.mktemp("merge_sdif_tests_")
    log.info(f"Created temp test directory: {tdir}")
    return tdir


def _create_dummy_sdif(
    file_path: Path,
    db_index: int,
    include_fk: bool = False,
    include_conflict_names: bool = False,
    include_semantic_link: bool = False,
    custom_object_name: str = None,
    custom_media_name: str = None,
    custom_table_name: str = None,
) -> SDIFDatabase:
    """Helper to create a dummy SDIF file with some data."""
    with SDIFDatabase(file_path, overwrite=True) as db:
        # Source
        source_name = f"source_file_{db_index}.csv"
        source_id = db.add_source(source_name, "csv", f"Source for DB {db_index}")

        # Table 1
        table1_name = custom_table_name or (
            "conflicting_table"
            if include_conflict_names and db_index > 0
            else f"table_a_db{db_index}"
        )

        table1_cols = {
            "id": {"type": "INTEGER", "primary_key": True},
            "data_col": {
                "type": "TEXT",
                "description": f"Data for table A DB {db_index}",
            },
        }
        db.create_table(
            table1_name, table1_cols, source_id, f"Table A for DB {db_index}"
        )
        db.insert_data(
            table1_name,
            [
                {"id": 1, "data_col": f"row1_db{db_index}_tA"},
                {"id": 2, "data_col": f"row2_db{db_index}_tA"},
            ],
        )

        if include_fk:
            table2_name = f"table_b_fk_db{db_index}"
            table2_cols = {
                "fk_id": {"type": "INTEGER", "primary_key": True},
                "ref_id": {
                    "type": "INTEGER",
                    "foreign_key": {
                        "table": table1_name,
                        "column": "id",
                        "on_delete": "CASCADE",
                    },
                },
                "fk_data_col": {"type": "TEXT"},
            }
            db.create_table(
                table2_name,
                table2_cols,
                source_id,
                f"Table B with FK for DB {db_index}",
            )
            db.insert_data(
                table2_name,
                [{"fk_id": 101, "ref_id": 1, "fk_data_col": f"fk_row1_db{db_index}"}],
            )

        # Object
        obj_name = custom_object_name or (
            "conflicting_object"
            if include_conflict_names and db_index > 0
            else f"object_db{db_index}"
        )
        db.add_object(
            obj_name,
            {"key": f"value_db{db_index}", "index": db_index},
            source_id,
            f"Object for DB {db_index}",
        )

        # Media
        media_name = custom_media_name or (
            "conflicting_media"
            if include_conflict_names and db_index > 0
            else f"media_db{db_index}"
        )
        db.add_media(
            media_name,
            f"dummy_bytes_db{db_index}".encode(),
            "text/plain",
            source_id,
            f"Media for DB {db_index}",
        )

        if include_semantic_link:
            db.add_semantic_link(
                link_type="test_relation",
                from_element_type="table",
                from_element_spec={"table_name": table1_name},  # No column, whole table
                to_element_type="object",
                to_element_spec={"object_name": obj_name},
                description=f"Link from {table1_name} to {obj_name} in DB {db_index}",
            )
    return SDIFDatabase(file_path, read_only=True)  # Return a new instance for safety


@pytest.fixture
def sdif_file_1(temp_test_dir: Path) -> Path:
    file_path = temp_test_dir / "dummy_1.sdif"
    _create_dummy_sdif(file_path, 1, include_fk=True, include_semantic_link=True)
    return file_path


@pytest.fixture
def sdif_file_2(temp_test_dir: Path) -> Path:
    file_path = temp_test_dir / "dummy_2.sdif"
    _create_dummy_sdif(file_path, 2, include_fk=False, include_semantic_link=False)
    return file_path


@pytest.fixture
def sdif_file_3_conflicts(temp_test_dir: Path) -> Path:
    file_path = temp_test_dir / "dummy_3_conflicts.sdif"
    # db_index=0 is intentional for _create_dummy_sdif's conflict logic
    _create_dummy_sdif(
        file_path,
        0,
        include_conflict_names=True,
        include_semantic_link=True,
        custom_table_name="conflicting_table",
        custom_object_name="conflicting_object",
        custom_media_name="conflicting_media",
    )
    return file_path


def test_merge_two_simple_sdifs(
    temp_test_dir: Path, sdif_file_1: Path, sdif_file_2: Path
):
    output_merged_path = temp_test_dir / "merged_1_2.sdif"

    merged_path = merge_sdif_files([sdif_file_1, sdif_file_2], output_merged_path)
    assert merged_path.exists()
    assert merged_path == output_merged_path

    with SDIFDatabase(merged_path, read_only=True) as db:
        # Properties
        props = db.get_properties()
        assert props is not None
        assert props["sdif_version"] == "1.0"
        assert "creation_timestamp" in props  # Should be set by merge_all

        # Sources
        sources = db.list_sources()
        assert len(sources) == 2
        assert sources[0]["original_file_name"] == "source_file_1.csv"
        assert sources[1]["original_file_name"] == "source_file_2.csv"
        new_source_id_1 = sources[0][
            "source_id"
        ]  # actual new ID for source from sdif_file_1
        new_source_id_2 = sources[1][
            "source_id"
        ]  # actual new ID for source from sdif_file_2

        # Tables
        tables = db.list_tables()
        assert "table_a_db1" in tables
        assert "table_b_fk_db1" in tables  # from sdif_file_1
        assert "table_a_db2" in tables  # from sdif_file_2

        # Check data from table_a_db1
        meta_t1_db1 = db.get_table_metadata("table_a_db1")
        assert meta_t1_db1["source_id"] == new_source_id_1
        data_t1_db1 = db.read_table("table_a_db1")
        assert len(data_t1_db1) == 2
        assert data_t1_db1["data_col"].iloc[0] == "row1_db1_tA"

        # Check FK in table_b_fk_db1
        schema_info = db.get_schema()
        fk_table_schema = schema_info["tables"]["table_b_fk_db1"]
        assert len(fk_table_schema["foreign_keys"]) == 1
        fk_detail = fk_table_schema["foreign_keys"][0]
        assert fk_detail["from_column"] == "ref_id"
        assert (
            fk_detail["target_table"] == "table_a_db1"
        )  # Name should be preserved if no conflict
        assert fk_detail["target_column"] == "id"

        # Objects
        objects = db.list_objects()
        assert "object_db1" in objects
        assert "object_db2" in objects
        obj1 = db.get_object("object_db1")
        assert obj1["source_id"] == new_source_id_1
        assert obj1["json_data"]["key"] == "value_db1"

        # Media
        media_items = db.list_media()
        assert "media_db1" in media_items
        assert "media_db2" in media_items
        med1 = db.get_media("media_db1")
        assert med1["source_id"] == new_source_id_1
        assert med1["media_data"] == b"dummy_bytes_db1"

        # Semantic Links (only from sdif_file_1 in this setup)
        links = db.list_semantic_links()
        assert len(links) == 1
        link1 = links[0]
        assert link1["description"] == "Link from table_a_db1 to object_db1 in DB 1"
        assert link1["from_element_spec"]["table_name"] == "table_a_db1"
        assert link1["to_element_spec"]["object_name"] == "object_db1"


def test_merge_with_name_conflicts(
    temp_test_dir: Path, sdif_file_1: Path, sdif_file_3_conflicts: Path
):
    output_merged_path = temp_test_dir / "merged_conflicts.sdif"

    # sdif_file_1 has: table_a_db1, object_db1, media_db1
    # sdif_file_3_conflicts has: conflicting_table, conflicting_object, conflicting_media
    # The _create_dummy_sdif for sdif_file_3_conflicts (db_index=0) and include_conflict_names=True
    # sets names to "conflicting_table", etc.
    # Now, let's make sdif_file_1 (db_index=1) also try to use these names if we modify its creation

    # Recreate sdif_file_1 to have conflicting names for this test
    sdif_file_1_conflicting_path = temp_test_dir / "dummy_1_conflicting.sdif"
    _create_dummy_sdif(
        sdif_file_1_conflicting_path,
        1,
        custom_table_name="conflicting_table",
        custom_object_name="conflicting_object",
        custom_media_name="conflicting_media",
        include_semantic_link=True,
    )  # Link to its own conflicting items

    merged_path = merge_sdif_files(
        [sdif_file_3_conflicts, sdif_file_1_conflicting_path], output_merged_path
    )
    assert merged_path.exists()

    with SDIFDatabase(merged_path, read_only=True) as db:
        tables = db.list_tables()
        log.info(f"Tables in merged conflicting DB: {tables}")
        assert "conflicting_table" in tables  # From first file (sdif_file_3_conflicts)
        assert (
            "conflicting_table_1" in tables
        )  # From second file (sdif_file_1_conflicting)

        objects = db.list_objects()
        log.info(f"Objects in merged conflicting DB: {objects}")
        assert "conflicting_object" in objects
        assert "conflicting_object_1" in objects

        media_items = db.list_media()
        log.info(f"Media in merged conflicting DB: {media_items}")
        assert "conflicting_media" in media_items
        assert "conflicting_media_1" in media_items

        # Verify data origin for a renamed table
        data_renamed_table = db.read_table("conflicting_table_1")
        assert (
            data_renamed_table["data_col"].iloc[0] == "row1_db1_tA"
        )  # Belongs to original sdif_file_1_conflicting

        sources = db.list_sources()
        new_source_id_for_file1_conflicting = sources[1][
            "source_id"
        ]  # second file processed

        meta_renamed_table = db.get_table_metadata("conflicting_table_1")
        assert meta_renamed_table["source_id"] == new_source_id_for_file1_conflicting

        # Check semantic link from the second file (sdif_file_1_conflicting_path)
        # Its table, object were renamed.
        links = db.list_semantic_links()
        assert len(links) == 2  # One from sdif_file_3, one from sdif_file_1_conflicting

        found_renamed_link = False
        for link in links:
            log.info(f"Checking link: {link}")
            # The link from sdif_file_1_conflicting should now point to renamed elements
            if (
                link["from_element_spec"].get("table_name") == "conflicting_table_1"
                and link["to_element_spec"].get("object_name") == "conflicting_object_1"
            ):
                found_renamed_link = True
                assert (
                    link["description"]
                    == "Link from conflicting_table to conflicting_object in DB 1"
                )  # Original desc
                break
        assert found_renamed_link, (
            "Semantic link for renamed elements not found or not correctly remapped."
        )


def test_merge_single_file(temp_test_dir: Path, sdif_file_1: Path):
    output_path = temp_test_dir / "single_merged.sdif"

    merged_path = merge_sdif_files([sdif_file_1], output_path)
    assert merged_path.exists()
    assert merged_path == output_path

    # Verify it's a copy by checking some content
    with SDIFDatabase(merged_path, read_only=True) as db:
        assert "table_a_db1" in db.list_tables()
        data_t1_db1 = db.read_table("table_a_db1")
        assert len(data_t1_db1) == 2


def test_merge_single_file_to_self(temp_test_dir: Path, sdif_file_1: Path):
    # This tests the optimization: if output_path is same as single input, no copy needed
    # sdif_file_1 is already created by its fixture within temp_test_dir.
    # We use sdif_file_1 directly as the target output path to test this scenario.
    target_output_path = sdif_file_1

    merged_path = merge_sdif_files([target_output_path], target_output_path)
    assert merged_path.exists()


def test_merge_no_input_files(temp_test_dir: Path):
    with pytest.raises(ValueError, match="No SDIF files provided for merging."):
        merge_sdif_files([], temp_test_dir / "empty_merge.sdif")


def test_merge_source_file_not_found(temp_test_dir: Path):
    non_existent_file = temp_test_dir / "i_do_not_exist.sdif"
    with pytest.raises(
        FileNotFoundError, match=f"Source SDIF file not found: {non_existent_file}"
    ):
        merge_sdif_files([non_existent_file], temp_test_dir / "merge_fail.sdif")


def test_merge_output_path_is_directory(temp_test_dir: Path, sdif_file_1: Path):
    # temp_test_dir itself is a directory
    with pytest.raises(
        ValueError, match="is an existing directory. Please provide a full file path."
    ):
        merge_sdif_files([sdif_file_1], temp_test_dir)


def test_merge_properties_update(
    temp_test_dir: Path, sdif_file_1: Path, sdif_file_2: Path
):
    output_merged_path = temp_test_dir / "merged_props.sdif"

    # Get original creation time of sdif_file_1's properties to compare
    # Note: this is tricky as the _create_metadata_tables in SDIFDatabase sets it on init.
    # The merge will set its own creation_timestamp.
    # We are mostly checking that the sdif_version is from the first file.

    merge_sdif_files([sdif_file_1, sdif_file_2], output_merged_path)

    with SDIFDatabase(sdif_file_1, read_only=True) as db1:
        props1_version = db1.get_properties()["sdif_version"]

    with SDIFDatabase(output_merged_path, read_only=True) as merged_db:
        merged_props = merged_db.get_properties()
        assert (
            merged_props["sdif_version"] == props1_version
        )  # Should be from the first file
        assert "creation_timestamp" in merged_props
        # The merged_props["creation_timestamp"] should be newer than either original file's timestamp
        # This is hard to assert precisely without controlling time, but presence is key.


def test_foreign_key_remapping_within_source(temp_test_dir: Path):
    db1_path = temp_test_dir / "fk_source1.sdif"
    db2_path = (
        temp_test_dir / "fk_source2.sdif"
    )  # Does not have FKs, just to make it a merge

    # Create DB1 with two tables, one FK to another
    with SDIFDatabase(db1_path, overwrite=True) as db:
        s_id = db.add_source("s1.csv", "csv")
        db.create_table(
            "parent_tab", {"id": {"type": "INTEGER", "primary_key": True}}, s_id
        )
        db.create_table(
            "child_tab",
            {
                "cid": {"type": "INTEGER", "primary_key": True},
                "pid": {
                    "type": "INTEGER",
                    "foreign_key": {"table": "parent_tab", "column": "id"},
                },
            },
            s_id,
        )
        db.insert_data("parent_tab", [{"id": 1}, {"id": 2}])
        db.insert_data("child_tab", [{"cid": 10, "pid": 1}, {"cid": 11, "pid": 2}])

    # Create DB2 (simple, no FKs)
    _create_dummy_sdif(db2_path, 22)  # db_index 22 for distinct names

    output_merged_path = temp_test_dir / "merged_fks.sdif"
    merge_sdif_files([db1_path, db2_path], output_merged_path)

    with SDIFDatabase(output_merged_path, read_only=True) as merged_db:
        # Check if tables from db1 exist (they shouldn't conflict with db2's dummy names)
        assert "parent_tab" in merged_db.list_tables()
        assert "child_tab" in merged_db.list_tables()

        schema = merged_db.get_schema()
        child_table_schema = schema["tables"]["child_tab"]
        assert len(child_table_schema["foreign_keys"]) == 1
        fk = child_table_schema["foreign_keys"][0]
        assert fk["from_column"] == "pid"
        assert (
            fk["target_table"] == "parent_tab"
        )  # Should still point to parent_tab (no rename)
        assert fk["target_column"] == "id"

        # Verify data integrity / FK constraint by inserting valid/invalid
        # Need to open writeable to test insert
    with SDIFDatabase(output_merged_path, read_only=False) as merged_db_rw:
        try:
            merged_db_rw.conn.execute(
                "INSERT INTO child_tab (cid, pid) VALUES (12, 1)"
            )  # Valid
            merged_db_rw.conn.commit()
        except sqlite3.Error as e:
            pytest.fail(f"Valid FK insert failed in merged DB: {e}")

        with pytest.raises(sqlite3.IntegrityError):  # Foreign key constraint failed
            merged_db_rw.conn.execute(
                "INSERT INTO child_tab (cid, pid) VALUES (13, 99)"
            )  # Invalid pid
            merged_db_rw.conn.commit()
