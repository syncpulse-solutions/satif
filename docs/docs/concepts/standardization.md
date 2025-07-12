---
sidebar_position: 2
---
# Standardization

Standardization is the first layer in the SATIF pipeline, responsible for converting a heterogeneous **Datasource**—comprising various file formats (CSV, Excel, PDF, XML, etc.)—into a single, canonical [**SDIF**](concepts/sdif.md) file.

This layer systematically decouples the complexities of source file parsing and structure normalization from the subsequent Transformation Layer. It ensures that all downstream business logic operates on a consistent, predictable, and AI-interpretable data representation.

## Process Overview

The Standardization Layer, executes a multi-stage, agentic approach:

1. **AI-Driven Parsing & Initial SDIF Generation**:
   An `AI Standardizer` orchestrator routes each file from the `Datasource` to a specialized AI agent (e.g., `AI XLSXStandardizer`, `AI CSVStandardizer`, `AI PDFStandardizer`). These agents are responsible for:

   * Intelligently interpreting file content. This can involve sophisticated logic such as visual layout analysis for complex Excel files (potentially leveraging an `XLSX Representer` to understand structure from a visual rendering), dynamic parameter inference for CSVs, or OCR for PDFs.
   * Extracting all relevant data entities: structured tables, semi-structured objects (like JSON), and binary media (images, etc.).
   * Generating an initial [**SDIF**](concepts/sdif.md) file. file tailored to that individual source file.
2. **SDIF Merge**:
   If the `Datasource` contains multiple files, the individual SDIF outputs from each file-specific standardizer are consolidated into a single, comprehensive SDIF structure.
3. **Schema Enforcement**:
   If an `sdif_schema` (target schema definition) is provided, the merged SDIF undergoes schema validation and adaptation. The `Schema Adapter` component attempts to conform the data to this target schema. This iterative process (e.g., `max_iteration = 5`) may involve data type coercions, structural transformations, or flagging discrepancies. If conformity cannot be achieved, an error is raised.
4. **Data Tidying (Optional)**:
   A `Tidy Adapter` can perform further data cleaning, normalization, or restructuring. It transforms all tables into tidy data tables.

The final output is a single [**SDIF**](concepts/sdif.md) file, primed for the Transformation Layer.

## Inputs & Outputs

- **Inputs**:
  - `Datasource`: A collection of one or more source files (e.g., `*.xlsx`, `*.csv`, `*.pdf`).
  - `.SDIF Schema` (Optional): A JSON file defining the target schema for the output SDIF.
- **Output**:
  - `sdif_standardized` (e.g., `invoices.sdif`): A single SDIF file.

## Components

The Standardization Layer conceptually involves:

- **`AI Standardizer` (Orchestrator)**: Dispatches files to appropriate specialized standardizer agents.
- **Format-Specific AI Agents** (e.g., `AI XLSXStandardizer`): Perform advanced, format-aware data extraction.
- **`SDIF Merge` Utility**: Consolidates multiple intermediate SDIFs.
- **`Schema Adapter`**: Enforces conformity to a target SDIF schema.
- **`Tidy Adapter`**: Applies data cleaning and refinement rules.
