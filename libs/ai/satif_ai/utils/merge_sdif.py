import json
import logging
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from satif_core.types import SDIFPath
from sdif_db.database import (
    SDIFDatabase,  # Assuming this is the conventional import path
)

log = logging.getLogger(__name__)


class _SDIFMerger:
    def __init__(self, target_sdif_path: Path):
        self.target_db = SDIFDatabase(target_sdif_path, overwrite=True)
        # Mappings per source_db_idx:
        self.source_id_map: Dict[int, Dict[int, int]] = {}
        self.table_name_map: Dict[int, Dict[str, str]] = {}
        self.object_name_map: Dict[int, Dict[str, str]] = {}
        self.media_name_map: Dict[int, Dict[str, str]] = {}

    def _get_new_source_id(self, source_db_idx: int, old_source_id: int) -> int:
        return self.source_id_map[source_db_idx][old_source_id]

    def _get_new_table_name(self, source_db_idx: int, old_table_name: str) -> str:
        return self.table_name_map[source_db_idx].get(old_table_name, old_table_name)

    def _get_new_object_name(self, source_db_idx: int, old_object_name: str) -> str:
        return self.object_name_map[source_db_idx].get(old_object_name, old_object_name)

    def _get_new_media_name(self, source_db_idx: int, old_media_name: str) -> str:
        return self.media_name_map[source_db_idx].get(old_media_name, old_media_name)

    def _generate_unique_name_in_target(self, base_name: str, list_func) -> str:
        """Generates a unique name for the target DB by appending a suffix if base_name exists."""
        if base_name not in list_func():
            return base_name
        i = 1
        while True:
            new_name = f"{base_name}_{i}"
            if new_name not in list_func():
                return new_name
            i += 1
            if i > 1000:  # Safety break
                raise RuntimeError(
                    f"Could not generate a unique name for base '{base_name}' after 1000 attempts."
                )

    def _merge_properties(self, source_db: SDIFDatabase, source_db_idx: int):
        source_props = source_db.get_properties()
        if not source_props:
            log.warning(
                f"Source database {source_db.path} has no properties. Skipping properties merge for this source."
            )
            return

        if source_props.get("sdif_version") != "1.0":
            # Or allow a configurable expected version
            raise ValueError(
                f"Source database {source_db.path} has unsupported SDIF version: {source_props.get('sdif_version')}. Expected '1.0'."
            )

        if source_db_idx == 0:  # First database sets the version for the target
            try:
                self.target_db.conn.execute(
                    "UPDATE sdif_properties SET sdif_version = ?",
                    (
                        source_props.get("sdif_version", "1.0"),
                    ),  # Default to 1.0 if somehow missing
                )
                self.target_db.conn.commit()
            except sqlite3.Error as e:
                log.error(
                    f"Failed to set sdif_version in target DB from {source_db.path}: {e}"
                )
                raise
        # creation_timestamp will be set at the end of the entire merge process.

    def _merge_sources(self, source_db: SDIFDatabase, source_db_idx: int):
        self.source_id_map[source_db_idx] = {}
        source_sources = source_db.list_sources()
        for old_source_entry in source_sources:
            old_source_id = old_source_entry["source_id"]
            new_source_id = self.target_db.add_source(
                file_name=old_source_entry["original_file_name"],
                file_type=old_source_entry["original_file_type"],
                description=old_source_entry.get("source_description"),
            )
            # original processing_timestamp is not directly carried over, new one is set by add_source
            self.source_id_map[source_db_idx][old_source_id] = new_source_id

    def _merge_tables(self, source_db: SDIFDatabase, source_db_idx: int):
        self.table_name_map[source_db_idx] = {}
        source_schema = source_db.get_schema()
        source_tables_schema = source_schema.get("tables", {})

        # Pass 1: Determine new table names for all tables from this source DB
        # This is to ensure FKs can be remapped correctly to tables from the *same* source db.
        temp_name_map_for_this_source = {}
        for old_table_name in source_tables_schema.keys():
            # Use create_table with if_exists='add' to get a unique name, but only for name generation.
            # This is a bit of a workaround. A dedicated _generate_unique_target_table_name might be cleaner.
            # The SDIFDatabase.create_table(if_exists='add') will actually create metadata entries.
            # This might be acceptable if we're careful.
            # Let's use the simpler approach of generating unique name first.
            effective_new_name = self._generate_unique_name_in_target(
                old_table_name, self.target_db.list_tables
            )
            temp_name_map_for_this_source[old_table_name] = effective_new_name
        self.table_name_map[source_db_idx] = temp_name_map_for_this_source

        # Pass 2: Create tables with remapped FKs and copy data
        for old_table_name, table_detail_from_schema in source_tables_schema.items():
            new_table_name = self.table_name_map[source_db_idx][old_table_name]

            columns_for_create: Dict[str, Dict[str, Any]] = {}
            original_columns_detail = table_detail_from_schema.get("columns", [])

            for col_detail in original_columns_detail:
                col_name = col_detail["name"]
                col_props = {
                    "type": col_detail["sqlite_type"],
                    "not_null": col_detail["not_null"],
                    "primary_key": col_detail[
                        "primary_key"
                    ],  # Assumes single col PK flag
                    "description": col_detail.get("description"),
                    "original_column_name": col_detail.get("original_column_name"),
                    # 'unique' constraint not in get_schema output, assumed not used or handled by primary_key
                }

                # Remap foreign keys defined for this column
                table_fks_detail = table_detail_from_schema.get("foreign_keys", [])
                for fk_info in table_fks_detail:
                    if fk_info["from_column"] == col_name:
                        original_fk_target_table = fk_info["target_table"]
                        # FKs are assumed to target tables within the same source SDIF file.
                        remapped_fk_target_table = self.table_name_map[
                            source_db_idx
                        ].get(original_fk_target_table)
                        if not remapped_fk_target_table:
                            log.warning(
                                f"Could not remap FK target table '{original_fk_target_table}' for column '{col_name}' in table '{old_table_name}'. FK might be dropped or invalid."
                            )
                            # Decide: skip FK, or raise error, or create FK pointing to original name (which might conflict or be wrong)
                            # For now, we'll proceed without this FK if target not found in map (shouldn't happen if all tables from source are processed)
                            continue

                        col_props["foreign_key"] = {
                            "table": remapped_fk_target_table,
                            "column": fk_info["target_column"],
                            "on_delete": fk_info[
                                "on_delete"
                            ].upper(),  # Ensure standard casing
                            "on_update": fk_info[
                                "on_update"
                            ].upper(),  # Ensure standard casing
                        }
                        break  # Assuming one FK per 'from_column' for this col_props structure
                columns_for_create[col_name] = col_props

            source_table_metadata = table_detail_from_schema.get("metadata", {})
            old_source_id_for_table = source_table_metadata.get("source_id")
            if old_source_id_for_table is None:
                raise ValueError(
                    f"Table '{old_table_name}' from {source_db.path} is missing source_id in its metadata."
                )

            new_source_id_for_table = self._get_new_source_id(
                source_db_idx, old_source_id_for_table
            )

            # Create the table structure in the target database
            # Using if_exists="fail" because new_table_name should already be unique.
            # SDIFDatabase.create_table handles complex PKs via table_constraints reconstruction.
            actual_created_name = self.target_db.create_table(
                table_name=new_table_name,
                columns=columns_for_create,
                source_id=new_source_id_for_table,
                description=source_table_metadata.get("description"),
                original_identifier=source_table_metadata.get("original_identifier"),
                if_exists="fail",
            )
            if actual_created_name != new_table_name:
                # This case should ideally not happen if _generate_unique_target_table_name was correct
                # and create_table used if_exists='fail'. If create_table internally changes name even with 'fail',
                # this is an issue. For now, assume 'fail' means it uses the name or errors.
                log.warning(
                    f"Table name discrepancy: expected {new_table_name}, created as {actual_created_name}. Using created name."
                )
                self.table_name_map[source_db_idx][old_table_name] = (
                    actual_created_name  # Update map
                )

            # Copy data
            try:
                data_df = source_db.read_table(old_table_name)
                if not data_df.empty:
                    # SDIFDatabase.insert_data expects List[Dict].
                    # SDIFDatabase.write_dataframe is higher level but might re-create table.
                    # Let's use insert_data.

                    # Handle data type conversions that pandas might do, to align with SQLite expectations
                    # For example, pandas bools to int 0/1, datetimes to ISO strings.
                    # The SDIFDatabase.write_dataframe has logic for this.
                    # We can replicate parts or simplify if read_table and insert_data are robust.
                    # For now, assume read_table gives compatible data for insert_data
                    # or insert_data can handle common pandas types.
                    # A quick check: SDIFDatabase.insert_data does not do type conversion.
                    # SDIFDatabase.write_dataframe does. So it's safer to go df -> records -> insert
                    # after manual conversion like in write_dataframe.

                    df_copy = data_df.copy()
                    for col_name_str in df_copy.columns:
                        col_name = str(col_name_str)  # Ensure string
                        if pd.api.types.is_bool_dtype(df_copy[col_name].dtype):
                            df_copy[col_name] = df_copy[col_name].astype(int)
                        elif pd.api.types.is_datetime64_any_dtype(
                            df_copy[col_name].dtype
                        ):
                            df_copy[col_name] = df_copy[col_name].apply(
                                lambda x: x.isoformat() if pd.notnull(x) else None
                            )
                        elif pd.api.types.is_timedelta64_dtype(df_copy[col_name].dtype):
                            df_copy[col_name] = df_copy[col_name].astype(str)
                        # Handle potential np.nan to None for JSON compatibility if objects were stored as text
                        if df_copy[col_name].dtype == object:
                            df_copy[col_name] = df_copy[col_name].replace(
                                {np.nan: None}
                            )

                    data_records = df_copy.to_dict("records")
                    if data_records:  # Ensure there are records to insert
                        self.target_db.insert_data(actual_created_name, data_records)
            except Exception as e:
                log.error(
                    f"Failed to copy data for table {old_table_name} to {actual_created_name}: {e}"
                )
                # Decide: continue with other tables or raise? For robustness, log and continue.
                # Or add a strict mode flag. For now, log and continue.

    def _merge_objects(self, source_db: SDIFDatabase, source_db_idx: int):
        self.object_name_map[source_db_idx] = {}
        for old_object_name in source_db.list_objects():
            obj_data = source_db.get_object(
                old_object_name, parse_json=False
            )  # Get raw JSON strings
            if not obj_data:
                log.warning(
                    f"Could not retrieve object '{old_object_name}' from {source_db.path}. Skipping."
                )
                continue

            new_object_name = self._generate_unique_name_in_target(
                old_object_name, self.target_db.list_objects
            )
            self.object_name_map[source_db_idx][old_object_name] = new_object_name

            new_source_id = self._get_new_source_id(
                source_db_idx, obj_data["source_id"]
            )

            # Data is already string from parse_json=False. Schema hint also string.
            # SDIFDatabase.add_object expects data to be Any (serializable) and schema_hint Dict.
            # So we need to parse them back if they are strings.
            parsed_json_data = json.loads(obj_data["json_data"])
            parsed_schema_hint = (
                json.loads(obj_data["schema_hint"])
                if obj_data.get("schema_hint")
                else None
            )

            self.target_db.add_object(
                object_name=new_object_name,
                json_data=parsed_json_data,
                source_id=new_source_id,
                description=obj_data.get("description"),
                schema_hint=parsed_schema_hint,
            )

    def _merge_media(self, source_db: SDIFDatabase, source_db_idx: int):
        self.media_name_map[source_db_idx] = {}
        for old_media_name in source_db.list_media():
            media_entry = source_db.get_media(
                old_media_name, parse_json=False
            )  # Get raw JSON for tech_metadata
            if not media_entry:
                log.warning(
                    f"Could not retrieve media '{old_media_name}' from {source_db.path}. Skipping."
                )
                continue

            new_media_name = self._generate_unique_name_in_target(
                old_media_name, self.target_db.list_media
            )
            self.media_name_map[source_db_idx][old_media_name] = new_media_name

            new_source_id = self._get_new_source_id(
                source_db_idx, media_entry["source_id"]
            )

            parsed_tech_metadata = (
                json.loads(media_entry["technical_metadata"])
                if media_entry.get("technical_metadata")
                else None
            )

            self.target_db.add_media(
                media_name=new_media_name,
                media_data=media_entry["media_data"],  # Should be bytes
                media_type=media_entry["media_type"],
                source_id=new_source_id,
                description=media_entry.get("description"),
                original_format=media_entry.get("original_format"),
                technical_metadata=parsed_tech_metadata,
            )

    def _remap_element_spec(
        self, element_type: str, element_spec_json: str, source_db_idx: int
    ) -> str:
        if not element_spec_json:
            return element_spec_json

        try:
            spec_dict = json.loads(element_spec_json)
        except json.JSONDecodeError:
            log.warning(
                f"Invalid JSON in element_spec: {element_spec_json}. Returning as is."
            )
            return element_spec_json

        new_spec_dict = spec_dict.copy()

        # Remap source_id if present (relevant for 'source' element_type in annotations, not directly in semantic_links spec)
        # Semantic links link to other entities which carry their own source_id.
        # But if spec itself contains a source_id key (e.g. for target_element_type='source' in annotations)
        if "source_id" in new_spec_dict and isinstance(new_spec_dict["source_id"], int):
            new_spec_dict["source_id"] = self._get_new_source_id(
                source_db_idx, new_spec_dict["source_id"]
            )

        # Remap names based on element_type
        if element_type in ["table", "column"]:
            if "table_name" in new_spec_dict:
                new_spec_dict["table_name"] = self._get_new_table_name(
                    source_db_idx, new_spec_dict["table_name"]
                )
        elif element_type == "object":  # Direct object reference
            if "object_name" in new_spec_dict:
                new_spec_dict["object_name"] = self._get_new_object_name(
                    source_db_idx, new_spec_dict["object_name"]
                )
        elif element_type == "json_path":  # JSONPath typically refers to an object
            if (
                "object_name" in new_spec_dict
            ):  # If the spec identifies the object container
                new_spec_dict["object_name"] = self._get_new_object_name(
                    source_db_idx, new_spec_dict["object_name"]
                )
        elif element_type == "media":
            if "media_name" in new_spec_dict:
                new_spec_dict["media_name"] = self._get_new_media_name(
                    source_db_idx, new_spec_dict["media_name"]
                )
        # 'file' type needs no remapping of spec content.
        # 'source' type: primary key is 'source_id', remapped above.

        return json.dumps(new_spec_dict)

    def _merge_semantic_links(self, source_db: SDIFDatabase, source_db_idx: int):
        # SDIFDatabase.list_semantic_links default parses JSON spec. We need this.
        source_links = source_db.list_semantic_links(parse_json=True)

        for link in source_links:
            # The specs are already dicts because parse_json=True was used.
            try:
                from_spec_dict = link["from_element_spec"]
                to_spec_dict = link["to_element_spec"]

                # Remap the dicts directly
                new_from_spec_dict = self._remap_element_spec_dict(
                    link["from_element_type"], from_spec_dict, source_db_idx
                )
                new_to_spec_dict = self._remap_element_spec_dict(
                    link["to_element_type"], to_spec_dict, source_db_idx
                )

                self.target_db.add_semantic_link(
                    link_type=link["link_type"],
                    from_element_type=link["from_element_type"],
                    from_element_spec=new_from_spec_dict,  # add_semantic_link takes dict
                    to_element_type=link["to_element_type"],
                    to_element_spec=new_to_spec_dict,  # add_semantic_link takes dict
                    description=link.get("description"),
                )
            except Exception as e:
                link_id = link.get("link_id", "Unknown")
                log.error(
                    f"Failed to merge semantic link ID {link_id} from {source_db.path}: {e}. Skipping link."
                )

    def _remap_element_spec_dict(
        self, element_type: str, spec_dict: Dict, source_db_idx: int
    ) -> Dict:
        # Helper for _merge_semantic_links that works with dicts directly
        new_spec_dict = spec_dict.copy()

        if "source_id" in new_spec_dict and isinstance(new_spec_dict["source_id"], int):
            new_spec_dict["source_id"] = self._get_new_source_id(
                source_db_idx, new_spec_dict["source_id"]
            )

        if element_type in ["table", "column"]:
            if "table_name" in new_spec_dict:
                new_spec_dict["table_name"] = self._get_new_table_name(
                    source_db_idx, new_spec_dict["table_name"]
                )
        elif element_type == "object" or (
            element_type == "json_path" and "object_name" in new_spec_dict
        ):
            if "object_name" in new_spec_dict:
                new_spec_dict["object_name"] = self._get_new_object_name(
                    source_db_idx, new_spec_dict["object_name"]
                )
        elif element_type == "media":
            if "media_name" in new_spec_dict:
                new_spec_dict["media_name"] = self._get_new_media_name(
                    source_db_idx, new_spec_dict["media_name"]
                )
        return new_spec_dict

    def merge_all(self, source_sdif_paths: List[SDIFPath]):
        # Import pandas and numpy here to avoid making them a hard dependency of the module if not used.
        # However, SDIFDatabase itself uses them. So they are effectively dependencies.
        global pd, np
        import numpy as np
        import pandas as pd

        for idx, source_path_item in enumerate(source_sdif_paths):
            source_path = Path(source_path_item)  # Ensure Path object
            log.info(
                f"Processing source SDIF ({idx + 1}/{len(source_sdif_paths)}): {source_path}"
            )
            source_db = SDIFDatabase(source_path, read_only=True)
            try:  # Ensure source_db is closed
                self._merge_properties(source_db, idx)
                self._merge_sources(source_db, idx)
                self._merge_tables(source_db, idx)  # This needs pandas for data reading
                self._merge_objects(source_db, idx)
                self._merge_media(source_db, idx)
                self._merge_semantic_links(source_db, idx)
                # Not merging sdif_annotations in this version.
            finally:
                source_db.close()

        # Finalize target DB properties
        try:
            current_timestamp_utc_z = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            self.target_db.conn.execute(
                "UPDATE sdif_properties SET creation_timestamp = ?",
                (current_timestamp_utc_z,),
            )
            self.target_db.conn.commit()
        except sqlite3.Error as e:
            log.error(f"Failed to update final creation_timestamp in target DB: {e}")
            # Non-fatal, proceed.

        self.target_db.close()
        log.info(
            f"Successfully merged {len(source_sdif_paths)} SDIF files into {self.target_db.path}"
        )
        return self.target_db.path


def merge_sdif_files(sdif_paths: List[SDIFPath], output_path: Path) -> Path:
    """
    Merges multiple SDIF files into a single new SDIF file.

    Args:
        sdif_paths: A list of paths to the SDIF files to merge.
        output_path: The full path where the merged SDIF file should be saved.
                     Its parent directory will be created if it doesn't exist.
                     If output_path is an existing file, it will be overwritten.
                     If output_path is an existing directory, a ValueError is raised.

    Returns:
        Path to the newly created merged SDIF file (same as output_path).

    Raises:
        ValueError: If no SDIF files are provided, or output_path is invalid (e.g., an existing directory).
        FileNotFoundError: If a source SDIF file does not exist.
        sqlite3.Error: For database-related errors during merging.
        RuntimeError: For critical errors like inability to generate unique names.
    """
    if not sdif_paths:
        raise ValueError("No SDIF files provided for merging.")

    output_path = Path(output_path).resolve()

    if output_path.is_dir():
        raise ValueError(
            f"Output path '{output_path}' is an existing directory. Please provide a full file path."
        )

    # Ensure parent directory of output_path exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure all source paths are Path objects and exist
    processed_sdif_paths: List[Path] = []
    for p in sdif_paths:
        path_obj = Path(p).resolve()
        if not path_obj.exists():
            raise FileNotFoundError(f"Source SDIF file not found: {path_obj}")
        if not path_obj.is_file():
            raise ValueError(f"Source SDIF path is not a file: {path_obj}")
        processed_sdif_paths.append(path_obj)

    if len(processed_sdif_paths) == 1:
        source_file = processed_sdif_paths[0]

        # If the source and target are the same file, no copy is needed.
        if source_file == output_path:
            return source_file

        shutil.copy(source_file, output_path)
        log.info(f"Copied single SDIF file to '{output_path}' as no merge was needed.")
        return output_path

    # For multiple files, merge them into the output_path
    merger = _SDIFMerger(output_path)
    return merger.merge_all(processed_sdif_paths)
