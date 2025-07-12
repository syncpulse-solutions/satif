---
sidebar_position: 1
---
# SDIF

Standardized Data Interoperable Format (SDIF) is the standardized intermediate representation at the core of SATIF, implemented as a SQLite database file.

## Core Design Principle

> **SDIF eliminates format styling from source files to focus exclusively on the substantive data and relationships.**

It aims to:

1. Preserve source information while imposing structure
2. Create a consistent interface for transformation logic

## Data Organization

SDIF organizes content into three fundamental data categories:

### 1. Tables

Tables represent normalized, schematized data like:

- Rows from CSV/Excel sheets
- Extracted tabular data from PDFs
- Normalized collections from JSON/XML

Every table includes full metadata about its origin and structure, with columns properly typed and constrained (PRIMARY KEY, FOREIGN KEY, etc.).

### 2. Objects (JSON)

Non-tabular structured content stored as queryable JSON:

- Document sections and paragraphs
- Headers, footers, and page metadata
- Sparse, irregular, or hierarchical data
- Configuration blocks and property collections

Objects maintain their internal structure while gaining rich queryability through SQLite's JSON functions.

### 3. Media Assets

Binary content preserved with contextual metadata:

- Images embedded in documents
- Audio/video content
- PDF page renderings
- Other binary resources

Each asset is stored alongside technical metadata (dimensions, format, etc.) and semantic descriptions.

## Metadata Layer

SDIF enriches raw data with comprehensive metadata:

- **Source Attribution**: Every data element tracks its original source file and location
- **Semantic Descriptions**: AI-generated explanations of data meaning
- **Technical Context**: Data types, formats and constraints
- **Annotations**: Additional insights attached to any data element

This metadata enables AI to understand both structure and meaning, facilitating more accurate transformations.

## Relationship Model

SDIF captures both explicit and implicit relationships between data elements:

- **SQL Foreign Keys**: Native database relationships between tables
- **Semantic Links**: Connections between heterogeneous elements (e.g., an object referencing a table)

## Technical Implementation

The format consists of system tables with a `sdif_` prefix (objects, metadata, sources, relationships) and data tables without this prefix. Please refer to the [RFC](rfc/sp-rfc-002.md) for more detailed information.

## Why SQLite ?

SDIF's SQLite foundation offers key advantages for the transformation pipeline:

- **LLM Fluent**: SQL is well-understood by LLMs, enabling them to efficiently query and transform data
- **Memory Efficiency**: SQLite reads only required data, it does not load the entire dataset into memory
- **Targeted Access**: SQL queries extract only needed data without parsing entire files
- **Transfer Efficiency**: Single-file design simplifies transmission between systems
- **Compression Ratio**: 2-4x reduction with standard compression tools (gzip, zstd)
- **Cross-File Joins**: Multiple SQLite files can be attached and queried together
- **Reliability**: Battle-tested, zero-configuration embedded database with no external dependencies
