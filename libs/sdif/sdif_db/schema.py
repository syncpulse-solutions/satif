import logging
from typing import Any, Dict, List, Literal, Tuple

log = logging.getLogger(__name__)


class SDIFSchemaConfig:
    """
    Configuration for comparing SDIF structural schemas. Defines which aspects of the
    schema to enforce during comparison.

    Attributes:
        enforce_sdif_version: If True, compares the 'sdif_version' from properties.
        enforce_table_names: If True, tables are matched by name. If False, the set of
            table structures is compared, ignoring original names.
        enforce_column_order: If True, the order of columns within a table must match.
        enforce_column_names: If True, columns are matched by name. If False (and
            enforce_column_order is True), columns are compared by their position.
        enforce_column_types: If True, SQLite data types of columns must match.
        enforce_column_not_null_constraints: If True, NOT NULL constraints must match.
        enforce_column_default_values: If True, column default values must match.
        enforce_primary_keys: If True, compares the ordered list of column names
            forming each table's primary key.
        enforce_foreign_keys: If True, compares foreign key definitions (target table,
            ordered source/target columns).
        enforce_foreign_key_referential_actions: If True (and enforce_foreign_keys is
            True), 'ON UPDATE' and 'ON DELETE' referential actions must match.
        objects_mode: Defines how JSON objects in 'sdif_objects' are compared.
            'ignore': Objects are not compared.
            'names_only': Only the set of object names is compared.
            'names_and_schema_hint': Object names and the content of their
                                       'schema_hint' (if present and valid) are compared.
        media_mode: Defines how media items in 'sdif_media' are compared.
            'ignore': Media items are not compared.
            'names_only': Only the set of media names is compared.
            'names_and_type': Media names and 'media_type' are compared.
            'names_type_and_original_format': Media names, 'media_type', and
                                               'original_format' are compared.
        media_technical_metadata_mode: Defines how 'technical_metadata' for media
            items is compared.
            'ignore': Technical metadata is not compared.
            'content_comparison': The content of 'technical_metadata' (if present
                                    and valid JSON) is compared.
        semantic_links_mode: Defines how links in 'sdif_semantic_links' are compared.
            'ignore': Semantic links are not compared.
            'link_types_only': Only the set of unique 'link_type' values is compared.
            'full_structure': All structural aspects of links (type, from/to element
                              type and spec, excluding 'link_id' and 'description')
                              are compared.

    Note:
        Comparison of non-primary-key UNIQUE constraints and CHECK constraints on tables
        is currently NOT SUPPORTED, as SDIFDatabase.get_schema() does not extract them.
    """

    def __init__(
        self,
        # General
        enforce_sdif_version: bool = True,
        # Tables - Overall Structure
        enforce_table_names: bool = True,
        # Tables - Column Definitions
        enforce_column_order: bool = True,
        enforce_column_names: bool = True,
        enforce_column_types: bool = True,
        enforce_column_not_null_constraints: bool = True,
        enforce_column_default_values: bool = True,
        # Tables - Key Constraints
        enforce_primary_keys: bool = True,
        enforce_foreign_keys: bool = True,
        enforce_foreign_key_referential_actions: bool = True,
        # Objects
        objects_mode: Literal[
            "ignore",
            "names_only",
            "names_and_schema_hint",
        ] = "names_and_schema_hint",
        # Media
        media_mode: Literal[
            "ignore",
            "names_only",
            "names_and_type",
            "names_type_and_original_format",
        ] = "names_type_and_original_format",
        media_technical_metadata_mode: Literal[
            "ignore",
            "content_comparison",
        ] = "ignore",
        # Semantic Links
        semantic_links_mode: Literal[
            "ignore",
            "link_types_only",
            "full_structure",
        ] = "full_structure",
    ):
        self.enforce_sdif_version = enforce_sdif_version
        self.enforce_table_names = enforce_table_names
        self.enforce_column_order = enforce_column_order
        self.enforce_column_names = enforce_column_names
        self.enforce_column_types = enforce_column_types
        self.enforce_column_not_null_constraints = enforce_column_not_null_constraints
        self.enforce_column_default_values = enforce_column_default_values
        self.enforce_primary_keys = enforce_primary_keys
        self.enforce_foreign_keys = enforce_foreign_keys
        self.enforce_foreign_key_referential_actions = (
            enforce_foreign_key_referential_actions
        )
        self.objects_mode = objects_mode
        self.media_mode = media_mode
        self.media_technical_metadata_mode = media_technical_metadata_mode
        self.semantic_links_mode = semantic_links_mode
        self._validate_modes()

    def _validate_modes(self):
        if self.objects_mode not in [
            "ignore",
            "names_only",
            "names_and_schema_hint",
        ]:
            raise ValueError(f"Invalid objects_mode: {self.objects_mode}")
        if self.media_mode not in [
            "ignore",
            "names_only",
            "names_and_type",
            "names_type_and_original_format",
        ]:
            raise ValueError(f"Invalid media_mode: {self.media_mode}")
        if self.media_technical_metadata_mode not in [
            "ignore",
            "content_comparison",
        ]:
            raise ValueError(
                f"Invalid media_technical_metadata_mode: {self.media_technical_metadata_mode}"
            )
        if self.semantic_links_mode not in [
            "ignore",
            "link_types_only",
            "full_structure",
        ]:
            raise ValueError(f"Invalid semantic_links_mode: {self.semantic_links_mode}")


# --- Helper for Canonicalization ---


def _canonicalize_value(value: Any) -> Any:
    """Recursively converts mutable collections to immutable, canonical forms
    (frozensets for dicts, tuples for lists) for stable hashing and comparison.
    Sorts dictionary items by key before creating frozenset.
    """
    if isinstance(value, dict):
        return frozenset((k, _canonicalize_value(v)) for k, v in sorted(value.items()))
    if isinstance(value, list):
        return tuple(_canonicalize_value(item) for item in value)
    return value


def _get_sort_key_for_list_of_dicts(
    item: Any,  # Expected to be a canonicalized dict (frozenset of items)
    primary_key_field: str = "name",  # Not directly used if item is already a frozenset
) -> Any:
    """Provides a sort key for items that were originally dictionaries and have been
    canonicalized (e.g., to frozensets), to allow sorting lists of such items.
    It sorts based on the canonical representation itself.
    """
    if isinstance(item, frozenset):  # Item is a canonicalized dictionary
        return tuple(
            sorted(list(item))
        )  # Sort items of the frozenset for stable sort key
    if isinstance(item, dict):  # Should ideally be canonicalized first
        return tuple(sorted((k, v) for k, v in item.items()))
    return item  # Fallback for other types


# --- Apply Rules to Schema ---


def apply_rules_to_schema(
    full_schema: Dict[str, Any], config: SDIFSchemaConfig
) -> Dict[str, Any]:
    """
    Transforms a full structural schema (from SDIFDatabase.get_schema())
    into a minimal, canonical schema based on the provided configuration.

    Args:
        full_schema: The schema dictionary from SDIFDatabase.get_schema().
        config: An SDIFSchemaConfig instance defining the comparison rules.

    Returns:
        A minimal, canonical schema dictionary, ready for direct comparison.
    """
    minimal_schema: Dict[str, Any] = {}

    if config.enforce_sdif_version:
        minimal_schema["sdif_version"] = full_schema.get("sdif_properties", {}).get(
            "sdif_version"
        )

    # Tables
    raw_tables_schema = full_schema.get("tables", {})
    if (
        raw_tables_schema
        or config.enforce_table_names
        or config.enforce_primary_keys
        or config.enforce_foreign_keys
    ):
        canonical_table_structures = []
        processed_tables_dict: Dict[str, Any] = {}

        for table_name, table_data in raw_tables_schema.items():
            min_table_data: Dict[str, Any] = {}
            raw_columns = table_data.get(
                "columns", []
            )  # Output from get_schema, includes 'pk'

            min_columns_list = []
            pk_columns_ordered_temp: List[Tuple[int, str]] = []

            for col_info in raw_columns:
                min_col_def: Dict[str, Any] = {}
                col_name = col_info.get("name")

                if config.enforce_column_names:
                    min_col_def["name"] = col_name
                if config.enforce_column_types:
                    min_col_def["sqlite_type"] = col_info.get("sqlite_type")
                if config.enforce_column_not_null_constraints:
                    min_col_def["not_null"] = col_info.get("not_null")
                if config.enforce_column_default_values:
                    min_col_def["default_value"] = _canonicalize_value(
                        col_info.get("default_value")
                    )

                # Primary key information is handled separately if enforce_primary_keys is True
                # No per-column 'primary_key_member' flag is added to min_col_def.
                if config.enforce_primary_keys:
                    pk_order_index = col_info.get(
                        "pk"
                    )  # 1-based index from PRAGMA, via get_schema
                    if col_name and pk_order_index is not None and pk_order_index > 0:
                        pk_columns_ordered_temp.append((pk_order_index, col_name))

                if (
                    min_col_def or not config.enforce_column_names
                ):  # Add if captures info, or if names ignored (to keep position)
                    min_columns_list.append(_canonicalize_value(min_col_def))

            if not config.enforce_column_order:
                min_columns_list.sort(key=_get_sort_key_for_list_of_dicts)
            min_table_data["columns"] = tuple(min_columns_list)

            if config.enforce_primary_keys:
                pk_columns_ordered_temp.sort(key=lambda x: x[0])
                min_table_data["primary_key_columns"] = tuple(
                    name for _, name in pk_columns_ordered_temp
                )

            if config.enforce_foreign_keys:
                raw_fks = table_data.get("foreign_keys", [])
                grouped_fks: Dict[int, List[Dict]] = {}
                for fk_item in raw_fks:  # fk_item is from PRAGMA foreign_key_list
                    grouped_fks.setdefault(fk_item["id"], []).append(fk_item)

                canonical_fks_list = []
                for _, fk_items_group in grouped_fks.items():
                    fk_items_group.sort(key=lambda x: x["seq"])
                    fk_def: Dict[str, Any] = {
                        "from_columns": tuple(
                            item["from_column"] for item in fk_items_group
                        ),
                        "target_table": fk_items_group[0]["target_table"],
                        "target_columns": tuple(
                            item["target_column"] for item in fk_items_group
                        ),
                    }
                    if config.enforce_foreign_key_referential_actions:
                        fk_def["on_update"] = fk_items_group[0]["on_update"].upper()
                        fk_def["on_delete"] = fk_items_group[0]["on_delete"].upper()
                    canonical_fks_list.append(_canonicalize_value(fk_def))

                canonical_fks_list.sort(key=_get_sort_key_for_list_of_dicts)
                min_table_data["foreign_keys"] = tuple(canonical_fks_list)

            if config.enforce_table_names:
                processed_tables_dict[table_name] = min_table_data
            else:
                canonical_table_structures.append(_canonicalize_value(min_table_data))

        if config.enforce_table_names:
            minimal_schema["tables"] = processed_tables_dict
        elif raw_tables_schema:  # Only add tables_set if there were tables to process
            minimal_schema["tables_set"] = frozenset(canonical_table_structures)
        elif (
            config.enforce_primary_keys or config.enforce_foreign_keys
        ):  # If rules active but no tables, represent as empty
            minimal_schema["tables_set"] = frozenset()

    # Objects
    if config.objects_mode != "ignore":
        raw_objects_schema = full_schema.get("objects", {})
        min_objects: Dict[str, Any] = {}
        for obj_name, obj_data in raw_objects_schema.items():
            obj_entry: Dict[str, Any] = {}
            if config.objects_mode == "names_only":
                pass  # Presence is indicated by the key obj_name
            elif config.objects_mode == "names_and_schema_hint":
                schema_hint = obj_data.get("schema_hint")
                is_error_hint = isinstance(schema_hint, dict) and "error" in schema_hint
                schema_hint_actually_provided_and_valid = (
                    schema_hint is not None and not is_error_hint
                )
                obj_entry["schema_hint_exists_and_valid"] = (
                    schema_hint_actually_provided_and_valid
                )
                if schema_hint_actually_provided_and_valid:
                    obj_entry["schema_hint"] = _canonicalize_value(schema_hint)
            min_objects[obj_name] = _canonicalize_value(obj_entry)
        minimal_schema["objects"] = min_objects

    # Media
    if config.media_mode != "ignore":
        raw_media_schema = full_schema.get("media", {})
        min_media: Dict[str, Any] = {}
        for media_name, media_data in raw_media_schema.items():
            media_entry: Dict[str, Any] = {}
            if config.media_mode == "names_only":
                pass
            elif config.media_mode == "names_and_type":
                media_entry["media_type"] = media_data.get("media_type")
            elif config.media_mode == "names_type_and_original_format":
                media_entry["media_type"] = media_data.get("media_type")
                media_entry["original_format"] = media_data.get("original_format")

            if config.media_technical_metadata_mode != "ignore":
                tech_meta = media_data.get("technical_metadata")
                is_error_meta = isinstance(tech_meta, dict) and "error" in tech_meta
                tech_meta_exists_and_valid = tech_meta is not None and not is_error_meta

                media_entry["technical_metadata_exists_and_valid"] = (
                    tech_meta_exists_and_valid
                )
                if config.media_technical_metadata_mode == "content_comparison":
                    if tech_meta_exists_and_valid:
                        media_entry["technical_metadata"] = _canonicalize_value(
                            tech_meta
                        )

            min_media[media_name] = _canonicalize_value(media_entry)
        minimal_schema["media"] = min_media

    # Semantic Links
    if config.semantic_links_mode != "ignore":
        raw_links = full_schema.get("semantic_links", [])
        if raw_links:
            if config.semantic_links_mode == "link_types_only":
                link_types = frozenset(
                    link.get("link_type") for link in raw_links if link.get("link_type")
                )
                minimal_schema["semantic_link_types_present"] = link_types
            elif config.semantic_links_mode == "full_structure":
                canonical_links_list = []
                for link in raw_links:
                    min_link_def = {
                        k: _canonicalize_value(v)
                        for k, v in link.items()
                        if k
                        not in [
                            "link_id",
                            "description",
                        ]  # Exclude instance-specific/semantic fields
                    }
                    canonical_links_list.append(_canonicalize_value(min_link_def))
                canonical_links_list.sort(key=_get_sort_key_for_list_of_dicts)
                minimal_schema["semantic_links"] = tuple(canonical_links_list)
        elif (
            config.semantic_links_mode == "link_types_only"
        ):  # If no links but mode is active, show empty set
            minimal_schema["semantic_link_types_present"] = frozenset()
        elif (
            config.semantic_links_mode == "full_structure"
        ):  # If no links but mode is active, show empty tuple
            minimal_schema["semantic_links"] = tuple()

    return minimal_schema
