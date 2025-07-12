---
sidebar_label: schema
title: sdif_db.schema
---

## SDIFSchemaConfig Objects

```python
class SDIFSchemaConfig()
```

> Configuration for comparing SDIF structural schemas. Defines which aspects of the
> schema to enforce during comparison.
>
> **Attributes**:
>
> - `enforce_sdif_version` - If True, compares the &#x27;sdif_version&#x27; from properties.
> - `enforce_table_names` - If True, tables are matched by name. If False, the set of
>   table structures is compared, ignoring original names.
> - `enforce_column_order` - If True, the order of columns within a table must match.
> - `enforce_column_names` - If True, columns are matched by name. If False (and
>   enforce_column_order is True), columns are compared by their position.
> - `enforce_column_types` - If True, SQLite data types of columns must match.
> - `enforce_column_not_null_constraints` - If True, NOT NULL constraints must match.
> - `enforce_column_default_values` - If True, column default values must match.
> - `enforce_primary_keys` - If True, compares the ordered list of column names
>   forming each table&#x27;s primary key.
> - `enforce_foreign_keys` - If True, compares foreign key definitions (target table,
>   ordered source/target columns).
> - `enforce_foreign_key_referential_actions` - If True (and enforce_foreign_keys is
>   True), &#x27;ON UPDATE&#x27; and &#x27;ON DELETE&#x27; referential actions must match.
> - `enforce_table_names`0 - Defines how JSON objects in &#x27;sdif_objects&#x27; are compared.
> - `enforce_table_names`1 - Objects are not compared.
> - `enforce_table_names`2 - Only the set of object names is compared.
> - `enforce_table_names`3 - Object names and the content of their
>   &#x27;schema_hint&#x27; (if present and valid) are compared.
> - `enforce_table_names`4 - Defines how media items in &#x27;sdif_media&#x27; are compared.
> - `enforce_table_names`1 - Media items are not compared.
> - `enforce_table_names`2 - Only the set of media names is compared.
> - `enforce_table_names`7 - Media names and &#x27;media_type&#x27; are compared.
> - `enforce_table_names`8 - Media names, &#x27;media_type&#x27;, and
>   &#x27;original_format&#x27; are compared.
> - `enforce_table_names`9 - Defines how &#x27;technical_metadata&#x27; for media
>   items is compared.
> - `enforce_table_names`1 - Technical metadata is not compared.
> - `enforce_column_order`1 - The content of &#x27;technical_metadata&#x27; (if present
>   and valid JSON) is compared.
> - `enforce_column_order`2 - Defines how links in &#x27;sdif_semantic_links&#x27; are compared.
> - `enforce_table_names`1 - Semantic links are not compared.
> - `enforce_column_order`4 - Only the set of unique &#x27;link_type&#x27; values is compared.
> - `enforce_column_order`5 - All structural aspects of links (type, from/to element
>   type and spec, excluding &#x27;link_id&#x27; and &#x27;description&#x27;)
>   are compared.
>
>
> **Notes**:
>
>   Comparison of non-primary-key UNIQUE constraints and CHECK constraints on tables
>   is currently NOT SUPPORTED, as SDIFDatabase.get_schema() does not extract them.

#### apply\_rules\_to\_schema

```python
def apply_rules_to_schema(full_schema: Dict[str, Any],
                          config: SDIFSchemaConfig) -> Dict[str, Any]
```

> Transforms a full structural schema (from SDIFDatabase.get_schema())
> into a minimal, canonical schema based on the provided configuration.
>
> **Arguments**:
>
> - `full_schema` - The schema dictionary from SDIFDatabase.get_schema().
> - `config` - An SDIFSchemaConfig instance defining the comparison rules.
>
>
> **Returns**:
>
>   A minimal, canonical schema dictionary, ready for direct comparison.
