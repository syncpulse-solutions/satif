# SP-RFC-002: SDIF

**Standardized Data Interoperable Format (SDIF) Version 1.0**

**1. Summary**

SDIF is a standardized format using a single SQLite database file to represent heterogeneous data sources — including tabular data, JSON objects, and binary media files — as well as their structural metadata, semantic descriptions, and interrelationships. It aims to provide a structured and directly queryable input format for automated data processing systems, particularly AI agents involved in data analysis and transformation tasks.

**2. Motivation**

Organizations constantly handle data from diverse sources and varied formats (flat files like CSV, Excel spreadsheets, structured documents like JSON or XML, PDF, database exports, etc.). This intrinsic heterogeneity is a major obstacle to automating processing and integration workflows, particularly for AI systems that greatly benefit from a predictable, semantically rich, and easily queryable input representation. SDIF addresses this need by proposing a standardized intermediate format, encapsulated in a single SQLite database, that simplifies ingestion, structured analysis via SQL, and subsequent transformation of data by downstream systems.

**3. Objectives**

The main objectives of the SDIF version 1.0 specification are to:

* Use a single SQLite database file as a standard container.
* Define a specific and mandatory database schema within the SQLite file to fully store the data source and associated metadata.
* Leverage SQLite's native features to the maximum: data types (INTEGER, REAL, TEXT, BLOB), constraints (PRIMARY KEY, FOREIGN KEY, NOT NULL), and SQL querying capabilities.
* Clearly specify how tabular data, JSON objects, and media files are stored in dedicated tables.
* Include comprehensive metadata describing the data itself as well as its original source(s).
* Provide a clear mechanism to represent relationships between data, strongly favoring the use of SQLite's native foreign key (FOREIGN KEY) constraints when applicable.
* Establish a robust and extensible foundation for future versions or specializations.

**4. Compliance**

The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 [RFC2119] [RFC8174] when, and only when, they appear in all capitals. These keywords indicate the levels of requirement for implementations conforming to the SDIF Version 1.0 specification.

**5. SDIF Format Specification**

5.1. An SDIF file **MUST** be a valid SQLite database file, compatible with SQLite version 3 [SQLITE_FORMAT].

5.2. The recommended file extension for an SDIF file **SHOULD** be `.sdif`. An alternative extension `.sqlite` **MAY** be used to facilitate recognition by operating systems or standard SQLite tools.

5.3. Text Encoding: SQLite handles the internal encoding of the database. However, all textual data stored in TEXT type columns (including JSON strings and descriptions) **MUST** be encoded in UTF-8 [RFC3629] when inserted and interpreted as such when read.

5.4. Naming: The table and column names defined in this schema (Section 6) **MUST** be used exactly as specified (in `snake_case`). The names of tables containing actual user data (see Section 7.1) **SHOULD** also use `snake_case` and **MUST NOT** start with the `sdif_` prefix to avoid conflicts with metadata tables.

**6. SDIF Database Schema**

A valid SDIF file **MUST** contain at minimum the following metadata tables. Other tables containing user data will also be present (see Section 7).

**6.1. Table `sdif_properties`**

This table contains global properties about the SDIF file itself. It **MUST** contain exactly one row.

* `sdif_version` (TEXT, NOT NULL): The version of the SDIF specification to which this file conforms. For this specification, the value **MUST** be `"1.0"`.
* `creation_timestamp` (TEXT): The date and time of creation of the SDIF file, in ISO 8601 [ISO8601] format (e.g., `"2024-07-31T10:00:00Z"`). **RECOMMENDED**.

**6.2. Table `sdif_sources`**

This table describes the original source(s) of the data before integration into the SDIF file. Each distinct source (e.g., an Excel file, a CSV file, a database export) **SHOULD** have an entry in this table.

* `source_id` (INTEGER, PRIMARY KEY AUTOINCREMENT): A unique identifier for each source.
* `original_file_name` (TEXT, NOT NULL): The name of the original file or source.
* `original_file_type` (TEXT, NOT NULL): The type of the original file or source (e.g., "csv", "xlsx", "json", "xml", "pdf", "database_export").
* `source_description` (TEXT): A free-form textual description of the source. **RECOMMENDED**.
* `processing_timestamp` (TEXT): The date and time at which this specific source was processed and integrated into the SDIF file, in ISO 8601 format. **OPTIONAL**.

**6.3. Table `sdif_tables_metadata`**

This table stores metadata associated with each table containing user tabular data (see Section 7.1).

* `table_name` (TEXT, PRIMARY KEY): The name of the SQL table in the SDIF database containing user data (e.g., `"delivery_details"`). This name **MUST NOT** start with `sdif_`.
* `source_id` (INTEGER, NOT NULL): Reference to the original source of the data in this table.
  * FOREIGN KEY (`source_id`) REFERENCES `sdif_sources`(`source_id`)
* `description` (TEXT): A semantic description of what the table represents. **RECOMMENDED**.
* `original_identifier` (TEXT): An identifier of the original structure if relevant (e.g., sheet name in an Excel file). **OPTIONAL**.
* `row_count` (INTEGER): The number of rows in the user data table. Can be calculated (`SELECT COUNT(*)`) but can be stored for quick access. **OPTIONAL**.

**6.4. Table `sdif_columns_metadata`**

This table stores metadata associated with each column of the user data tables.

* `table_name` (TEXT, NOT NULL): The name of the user table containing the column.
* `column_name` (TEXT, NOT NULL): The name of the column in the user table.
* `description` (TEXT): A semantic description of what the column represents. **RECOMMENDED**.
* `original_column_name` (TEXT): The original name of the column as it appeared in the data source. **OPTIONAL**.
* PRIMARY KEY (`table_name`, `column_name`)
* FOREIGN KEY (`table_name`) REFERENCES `sdif_tables_metadata`(`table_name`) ON DELETE CASCADE

*Note:* Basic structural information such as SQLite data type (INTEGER, REAL, TEXT, BLOB) and nullability (NOT NULL constraint) are defined directly in the `CREATE TABLE` statement of the user table itself and are not duplicated here.

**6.5. Table `sdif_objects`**

This table stores non-tabular data represented as JSON objects.

* `object_name` (TEXT, PRIMARY KEY): A unique logical name identifying this object within the SDIF file.
* `source_id` (INTEGER, NOT NULL): Reference to the original source of this object.
  * FOREIGN KEY (`source_id`) REFERENCES `sdif_sources`(`source_id`)
* `json_data` (TEXT, NOT NULL): The complete representation of the object as a valid JSON string [RFC8259]. SQLite's JSON functions [SQLITE_JSON] can be used to query this content.
* `description` (TEXT): A semantic description of what the JSON object represents. **RECOMMENDED**.
* `schema_hint` (TEXT): A JSON schema [JSON-Schema] describing the expected structure of `json_data`, stored as a JSON string. **RECOMMENDED**.

**6.6. Table `sdif_media`**

This table stores media files or other binary data.

* `media_name` (TEXT, PRIMARY KEY): A unique logical name identifying this media resource.
* `source_id` (INTEGER, NOT NULL): Reference to the original source of this media.
  * FOREIGN KEY (`source_id`) REFERENCES `sdif_sources`(`source_id`)
* `media_type` (TEXT, NOT NULL): The general type of the media. **RECOMMENDED** values for v1.0 include: `"image"`, `"audio"`, `"video"`, `"binary"`.
* `media_data` (BLOB, NOT NULL): The raw binary content of the media file.
* `description` (TEXT): A semantic description of what the media represents. **RECOMMENDED**.
* `original_format` (TEXT): The specific format of the original file (e.g., "png", "jpeg", "mp3", "wav", "pdf"). **RECOMMENDED**.
* `technical_metadata` (TEXT): Technical metadata extracted or known about the media, stored as a JSON string (e.g., `{"width": 300, "height": 150}` for an image, `{"duration_seconds": 125}` for audio). **OPTIONAL**.

**6.7. Table `sdif_semantic_links`**

This table is **OPTIONAL** and serves to define logical or semantic relationships between different elements in the SDIF file that cannot be represented by native SQLite FOREIGN KEY constraints (see Section 8).

* `link_id` (INTEGER, PRIMARY KEY AUTOINCREMENT): A unique identifier for this semantic link.
* `link_type` (TEXT, NOT NULL): The type of the relationship. **RECOMMENDED** values include: `"annotation"` (links descriptive information to an element), `"reference"` (generic link), `"logical_foreign_key"` (e.g., to link a value in a JSON to a table column). Other types can be used but should be documented by the application.
* `description` (TEXT): A textual description of the relationship. **RECOMMENDED**.
* `from_element_type` (TEXT, NOT NULL): The type of the source element. Possible values: `"table"`, `"column"`, `"object"`, `"media"`, `"json_path"`.
* `from_element_spec` (TEXT, NOT NULL): A JSON string specifying the exact source element (e.g., `{"table_name": "deliveries", "column_name": "delivery_id"}`, `{"object_name": "notes", "path": "$.[*].delivery_ref"}`).
* `to_element_type` (TEXT, NOT NULL): The type of the target element. Same possible values as `from_element_type`.
* `to_element_spec` (TEXT, NOT NULL): A JSON string specifying the exact target element.

**6.8. Table `sdif_annotations` (OPTIONAL)**

This **OPTIONAL** table provides a centralized mechanism for attaching arbitrary metadata, defined by the user or the generation process, to various elements within the SDIF file.

* `annotation_id` (INTEGER, PRIMARY KEY AUTOINCREMENT): A unique identifier for each annotation record.
* `target_element_type` (TEXT, NOT NULL): Specifies the type of SDIF element to which this annotation is attached. Values **MUST** be one of the following:
  * `"file"`: The annotation applies to the SDIF file as a whole.
  * `"source"`: The annotation applies to a specific entry in `sdif_sources`.
  * `"table"`: The annotation applies to a specific user data table (referenced in `sdif_tables_metadata`).
  * `"column"`: The annotation applies to a specific column of a user data table (referenced in `sdif_columns_metadata`).
  * `"object"`: The annotation applies to a specific JSON object in `sdif_objects`.
  * `"media"`: The annotation applies to a specific media resource in `sdif_media`.
* `target_element_spec` (TEXT, NOT NULL): A valid JSON string [RFC8259] that precisely identifies the target element specified by `target_element_type`. The structure of this JSON depends on `target_element_type`:
  * If `target_element_type` is `"file"`: The JSON **SHOULD** be an empty object `{}` since there is only one file.
  * If `target_element_type` is `"source"`: The JSON **MUST** contain the key `"source_id"` with the corresponding ID in `sdif_sources`. E.g.: `{"source_id": 1}`.
  * If `target_element_type` is `"table"`: The JSON **MUST** contain the key `"table_name"` with the name of the user table. E.g.: `{"table_name": "delivery_details"}`.
  * If `target_element_type` is `"column"`: The JSON **MUST** contain the keys `"table_name"` and `"column_name"`. E.g.: `{"table_name": "delivery_details", "column_name": "date_time"}`.
  * If `target_element_type` is `"object"`: The JSON **MUST** contain the key `"object_name"`. E.g.: `{"object_name": "additional_notes"}`.
  * If `target_element_type` is `"media"`: The JSON **MUST** contain the key `"media_name"`. E.g.: `{"media_name": "company_logo"}`.
* `annotation_content` (TEXT, NOT NULL): The content of the annotation itself, stored as a valid JSON string [RFC8259]. The internal structure of this JSON object is entirely defined by the user or application generating the annotation and is not constrained by the SDIF specification.

**7. User Data Storage**

**7.1. Tabular Data**

* Each set of tabular data from a source (e.g., an Excel sheet, a CSV file) **SHOULD** be stored in its own SQL table within the SDIF file.
* The name of these tables **MUST NOT** start with `sdif_`.
* The definition of these tables (`CREATE TABLE`) **MUST** use appropriate SQLite data types (INTEGER, REAL, TEXT, BLOB) and **SHOULD** include relevant constraints (PRIMARY KEY, NOT NULL).
* Each user data table **MUST** have a corresponding entry in `sdif_tables_metadata`, and each column of these tables **MUST** have a corresponding entry in `sdif_columns_metadata`.

**7.2. Object Data (JSON)**

* Structured non-tabular data (from native JSON, converted XML, etc.) **MUST** be stored as valid JSON strings in the `json_data` column of the `sdif_objects` table.
* Each logical JSON object **MUST** have a unique entry in `sdif_objects`, identified by `object_name`.

**7.3. Media and Binary Data**

* Media files or other relevant binary data **MUST** be stored as BLOBs in the `media_data` column of the `sdif_media` table.
* Each logical media file **MUST** have a unique entry in `sdif_media`, identified by `media_name`.

**8. Relationships Between Data**

8.1. **Primary Keys:** Primary keys for user data tables **MUST** be defined using the standard SQL `PRIMARY KEY` syntax when creating the table.

8.2. **Foreign Keys Between Tables:** When relationships exist between different user data tables, they **MUST** be implemented using SQLite's native `FOREIGN KEY` constraints. This is the **REQUIRED** method for representing relational links between tables.

8.3. **Other Semantic Links:** For relationships that cannot be modeled by native foreign keys (e.g., linking a value extracted from JSON via JSONPath to a table column, annotating a table with a descriptive JSON object, linking a media to a specific table), the `sdif_semantic_links` table (Section 6.7) **MAY** be used. Its use **SHOULD** be reserved for cases where `FOREIGN KEY` is not applicable.

**9. Future Considerations**

Version 1.0 of SDIF mandates the use of SQLite as the file format and underlying database engine. Future versions of this specification **MAY** consider defining equivalent schemas for other file-oriented database engines (such as DuckDB [DUCKDB]), if clear use cases and significant advantages (e.g., analytical performance) justify it. However, ensuring compatibility or conversion between different database file formats represents a significant challenge that is not addressed in this version.

**Appendix A. Example Schema and Data (Non-Normative)**

*This appendix illustrates the structure of a simple SDIF file.*

**A.1. Data Sources**

Suppose the data comes from two files:

1. `deliveries_oct2024.xlsx` (containing a "Details" sheet)
2. `operator_notes.json`

**A.2. SDIF Metadata Tables Content**

* **`sdif_properties`** (1 row)
  | sdif_version | creation_timestamp     |
  |--------------|------------------------|
  | "1.0"        | "2024-11-15T14:30:00Z" |

* **`sdif_sources`** (2 rows)
  | source_id | original_file_name      | original_file_type | source_description              | processing_timestamp   |
  |-----------|-------------------------|--------------------|---------------------------------|------------------------|
  | 1         | deliveries_oct2024.xlsx | xlsx               | Monthly deliveries file         | "2024-11-15T14:25:10Z" |
  | 2         | operator_notes.json     | json               | Additional notes from operators | "2024-11-15T14:25:15Z" |

* **`sdif_tables_metadata`** (1 row)
  | table_name         | source_id | description               | original_identifier | row_count |
  |--------------------|-----------|---------------------------|---------------------|-----------|
  | `delivery_details` | 1         | Detail of each delivery.  | Details             | 850       |

* **`sdif_columns_metadata`** (Example for 2 columns)
  | table_name         | column_name   | description                       | original_column_name |
  |--------------------|---------------|-----------------------------------|----------------------|
  | `delivery_details` | `delivery_id` | Unique delivery identifier.       | NULL                 |
  | `delivery_details` | `date_time`   | Date/time of delivery completion. | "DD/MM/YYYY HH:MM"   |
  | ...                | ...           | ...                               | ...                  |

* **`delivery_details`** (5 rows)
    | delivery_id | customer_num | date_time           | product     | quantity_l | amount_eur |
    |-------------|--------------|---------------------|-------------|------------|------------|
    | DEL-001     | CUST-101     | 2024-10-15T10:30:00Z | Diesel      | 1500.5     | 2250.75    |
    | DEL-002     | CUST-203     | 2024-10-15T11:45:10Z | Heating Oil | 800.0      | 1120.00    |
    | DEL-003     | CUST-101     | 2024-10-16T09:15:00Z | Diesel      | 200.75     | 301.13     |
    | DEL-004     | CUST-502     | 2024-10-16T14:00:00Z | AdBlue      | 50.0       | 45.00      |
    | DEL-005     | CUST-203     | 2024-10-17T08:50:00Z | Diesel      | 1200.0     | 1800.00    |



* **`sdif_objects`** (1 row)
  | object_name        | source_id | json_data                                                                                      | description                   | schema_hint (JSON Schema as text) |
  |--------------------|-----------|------------------------------------------------------------------------------------------------|-------------------------------|-----------------------------------|
  | `additional_notes` | 2         | `[{"delivery_ref": "DEL-001", "note": "Customer absent"}, {"delivery_ref": "DEL-003", ...}]` | Specific notes per delivery.  | `{"type": "array", "items": {...}}` |

* **`sdif_media`** (0 rows in this simple example)

* **`sdif_semantic_links`** (1 row to link JSON to table)
  | link_id | link_type             | description                             | from_element_type | from_element_spec                                                  | to_element_type | to_element_spec                                                    |
  |---------|-----------------------|-----------------------------------------|-------------------|--------------------------------------------------------------------|-----------------|--------------------------------------------------------------------|
  | 1       | `logical_foreign_key` | Links delivery ref. in notes to ID.     | `json_path`       | `{"object_name": "additional_notes", "path": "$.[*].delivery_ref"}` | `column`        | `{"table_name": "delivery_details", "column_name": "delivery_id"}` |
