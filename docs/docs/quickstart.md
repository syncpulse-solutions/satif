---
sidebar_position: 2
---
# Quickstart

This guide will get you up and running by demonstrating the core workflow: standardizing a [datasource](terminology.md#datasource) into a **SDIF** and then transforming it into your desired output.

## 1. Installation

SATIF is designed as a Python SDK. You can install it into your project using pip (ensure you have Python 3.10+):

```bash
pip install satif-ai
```

Or, alternatively

```bash
pip install satif-sdk[ai]
```

## 2. Core Workflow: From Source to Output

SATIF processes data in two main stages:

1. **Standardization**: Converts your diverse source files (CSV, Excel, PDF, etc.) into a consistent, queryable SDIF database. SDIF is a SQLite-based format that preserves all your data (tables, objects, media) with rich metadata.
2. **Transformation**: Applies your business logic to the SDIF database to produce the final output files in the formats you need (e.g., JSON, XLSX, CSV). SATIF's AI Agent generates the transformation code based on your instructions.

Let's walk through an example.

### Prerequisites: Prepare Your Input

1. Create a project directory, for example, `my_satif_project`.
2. Inside it, create an `input_files` directory.
3. Add two CSV files into `input_files/`:

   a. `my_data.csv` with the following content:
   ```csv
   id,name,value
   1,Alice,100
   2,Bob,200
   3,Charlie,150
   ```

   b. `reference.csv` with the following content:
   ```csv
   name,category,multiplier
   Alice,Premium,1.5
   Bob,Standard,1.0
   Charlie,Premium,1.5
   ```

4. Your project structure should look like this:

   ```
   my_satif_project/
   ├── input_files/
   │   ├── my_data.csv
   │   └── reference.csv
   └── (your Python script will go here)
   ```

### Step 1: Standardization - Creating an SDIF File

First, we'll standardize both input files into a single SDIF database (e.g., `datasource.sdif`).

Create a Python script (e.g., `run_satif.py`) in your `my_satif_project` directory:

```python
import asyncio
from pathlib import Path
from satif_ai import astandardize

async def run_standardization():
    datasource = [
        "input_files/my_data.csv",
        "input_files/reference.csv"
    ]

    standardization_result = await astandardize(
        datasource=datasource,
        output_path="datasource.sdif",
        overwrite=True
    )
    return standardization_result.output_path
```

**What this does:**

* `astandardize` takes both your CSV files and processes them together.
* It creates an SDIF database at `datasource.sdif` containing your data in a structured, queryable format.
* The `overwrite=True` flag allows rerunning the script without manual cleanup of previous outputs.

### Step 2: Transformation - Generating Your Desired Output

Now, let's transform the data from our SDIF database into a CSV file with calculations applied based on the reference data. We'll instruct SATIF's AI to generate the transformation logic.

Add the following function to your `run_satif.py`:

```python
from satif_ai import atransform

async def run_transformation(sdif_input_path: Path):
    output_csv_file = "transformed_data.csv"

    instructions = (
        "Create a CSV file with columns: id, name, category, original_value, multiplier, adjusted_value. "
        "Join the data from my_data.csv with reference.csv based on the name field. "
        "Calculate adjusted_value as value * multiplier. "
        "Include all records from my_data.csv, sorted by id."
    )

    transformation_result = await atransform(
        sdif=sdif_input_path,
        output_target_files=output_csv_file,
        instructions=instructions,
        llm_model="o4-mini"
    )

    return transformation_result.output_path
```

**What this does:**

* `atransform` takes the path to your SDIF database.
* `output_target_files`: For a single output file scenario like this, you provide the full `Path` to where you want your final CSV file. This helps the AI understand the target (e.g., file type from extension) and tells the system where to save the result.
* `instructions`: You provide natural language instructions on how to transform the data. SATIF's AI uses this to generate Python code that performs the transformation.
* The result is `transformed_data.csv` containing the transformed data.

## 3. Putting It All Together

Now, let's combine these steps into a single executable script. Complete your `run_satif.py` like this:

```python
import asyncio
from pathlib import Path
from satif_ai import astandardize, atransform

async def run_standardization():
    datasource = [
        "input_files/my_data.csv",
        "input_files/reference.csv"
    ]

    standardization_result = await astandardize(
        datasource=datasource,
        output_path="datasource.sdif",
        overwrite=True
    )
    return standardization_result.output_path

async def run_transformation(sdif_input_path: Path):
    output_csv_file = "transformed_data.csv"

    instructions = (
        "Create a CSV file with columns: id, name, category, original_value, multiplier, adjusted_value. "
        "Join the data from my_data.csv with reference.csv based on the name field. "
        "Calculate adjusted_value as value * multiplier. "
        "Include all records from my_data.csv, sorted by id."
    )

    transformation_result = await atransform(
        sdif=sdif_input_path,
        output_target_files=output_csv_file,
        instructions=instructions,
        llm_model="o4-mini"
    )

    return transformation_result.output_path

async def main():
    # Step 1: Standardize both files
    standardized_sdif_path = await run_standardization()

    # Step 2: Transform
    if standardized_sdif_path:
        await run_transformation(standardized_sdif_path)

if __name__ == "__main__":
    asyncio.run(main())
```

**To Run:**

1. Save the script above as `run_satif.py` in `my_satif_project/`.
2. Ensure `input_files/my_data.csv` and `input_files/reference.csv` exist as described.
3. Open your terminal, navigate to the `my_satif_project` directory.
4. Run the script: `python run_satif.py`

After running the script, you'll find a `transformed_data.csv` file containing the joined and calculated data.

## 4. What Happened?

1. **`astandardize`** converted both CSV files into a single, structured SDIF database file (`datasource.sdif`). This SDIF file is a queryable SQLite database containing tables from both source files.

2. **`atransform`** took this SDIF database file and followed your instructions to:
   - Join the tables using the name field
   - Apply the mathematical transformation using the reference multipliers
   - Output the results in the specified CSV format

The power of SATIF lies in this decoupling:

* **Standardize once**: Handle complex, varied source formats into a single, reliable SDIF file.
* **Transform flexibly**: Apply different business rules or generate various outputs from the same standardized data, often with AI assistance for generating the transformation logic.

## 5. Next Steps

This quickstart covered a basic workflow with multiple input files and data transformation. SATIF offers much more:

* Explore different **Datasource** types (Excel, PDF, JSON, and more).
* Dive deeper into **SDIF** and its capabilities.
* Learn about providing `sdif_schema` for schema enforcement during standardization.
* Use `TidyAdapter` for advanced data cleaning.
* Provide existing `transformation_code` instead of relying on AI generation.
* Check out the detailed documentation for [Concepts](concepts/sdif.md) and the API reference.
