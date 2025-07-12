import json
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest
from sdif_db import SDIFDatabase

from satif_sdk.transformers.code import CodeTransformer, transformation


@pytest.fixture
def create_test_sdif(tmp_path: Path):
    """
    Fixture to create a test SDIF database with sample data.
    """

    def _create_test_sdif(name: str, tables: Dict[str, pd.DataFrame] = None) -> Path:
        db_path = tmp_path / f"{name}.sdif"

        with SDIFDatabase(db_path, overwrite=True) as db:
            # Add a test source
            source_id = db.add_source(
                file_name=f"{name}_source.csv",
                file_type="csv",
                description=f"Test source for {name}",
            )

            # Add tables if provided
            if tables:
                for table_name, df in tables.items():
                    db.write_dataframe(
                        df=df,
                        table_name=table_name,
                        source_id=source_id,
                        description=f"Test table {table_name}",
                    )

        return db_path

    return _create_test_sdif


@pytest.fixture
def create_test_code_file(tmp_path: Path):
    """
    Fixture to create a test Python file with transformation code.
    """

    def _create_test_code_file(code: str, filename: str = "transform.py") -> Path:
        file_path = tmp_path / filename
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(code)
        return file_path

    return _create_test_code_file


# Sample transformation functions for testing


@transformation
def simple_transform(conn):
    """A simple transformation that reads a table and returns a CSV."""
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)
    return {"users.csv": df}


@transformation
def transform_with_context(conn, context):
    """A transformation that uses the context parameter."""
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)
    if context.get("filter_age"):
        df = df[df["age"] >= context["filter_age"]]
    return {"filtered_users.csv": df}


@transformation(name="custom_name_transform")
def transform_with_custom_name(conn):
    """A transformation with a custom registered name."""
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)
    return {"custom_users.csv": df}


def test_transformer_init_with_callable():
    """Test initializing CodeTransformer with a callable function."""
    transformer = CodeTransformer(function=simple_transform)
    assert transformer.transform_function_obj is simple_transform
    assert transformer.function_name == "simple_transform"
    assert transformer.transform_code is None


def test_transformer_init_with_registered_name():
    """Test initializing CodeTransformer with a registered transformation name."""
    transformer = CodeTransformer(function="custom_name_transform")
    assert transformer.transform_function_obj is not None
    assert transformer.function_name == "custom_name_transform"
    assert transformer.transform_code is None


def test_transformer_init_with_code_string():
    """Test initializing CodeTransformer with a code string."""
    code = """
def transform(conn):
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)
    return {"users.csv": df}
"""
    transformer = CodeTransformer(function=code)
    assert transformer.transform_function_obj is None
    assert transformer.function_name == "transform"  # Default name
    assert transformer.transform_code == code
    assert transformer.code_executor is not None


def test_transformer_init_with_code_file(create_test_code_file):
    """Test initializing CodeTransformer with a code file path."""
    code = """
def custom_function(conn):
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)
    return {"users.csv": df}
"""
    file_path = create_test_code_file(code)
    transformer = CodeTransformer(function=file_path, function_name="custom_function")
    assert transformer.transform_function_obj is None
    assert transformer.function_name == "custom_function"
    assert transformer.transform_code is not None
    assert transformer.code_executor is not None


def test_transform_with_direct_callable(create_test_sdif):
    """Test transforming data with a direct callable function."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer with the simple_transform function
    transformer = CodeTransformer(function=simple_transform)

    # Transform data
    result = transformer.transform(sdif=sdif_path)

    # Verify result
    assert isinstance(result, dict)
    assert "users.csv" in result
    assert isinstance(result["users.csv"], pd.DataFrame)
    assert len(result["users.csv"]) == 3
    assert list(result["users.csv"]["name"]) == ["Alice", "Bob", "Charlie"]


def test_transform_with_context(create_test_sdif):
    """Test transforming data with context parameter."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer with the transform_with_context function
    transformer = CodeTransformer(
        function=transform_with_context, extra_context={"filter_age": 30}
    )

    # Transform data
    result = transformer.transform(sdif=sdif_path)

    # Verify result
    assert isinstance(result, dict)
    assert "filtered_users.csv" in result
    assert isinstance(result["filtered_users.csv"], pd.DataFrame)
    assert len(result["filtered_users.csv"]) == 2  # Only Bob and Charlie are >= 30
    assert list(result["filtered_users.csv"]["name"]) == ["Bob", "Charlie"]


def test_transform_with_code_string(create_test_sdif):
    """Test transforming data with a code string."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer with a code string
    code = """
def transform(conn):
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)
    return {"users_from_code.csv": df}
"""
    transformer = CodeTransformer(function=code)

    # Transform data
    result = transformer.transform(sdif=sdif_path)

    # Verify result
    assert isinstance(result, dict)
    assert "users_from_code.csv" in result
    assert isinstance(result["users_from_code.csv"], pd.DataFrame)
    assert len(result["users_from_code.csv"]) == 3


def test_transform_with_multiple_sdifs(create_test_sdif):
    """Test transforming data with multiple SDIF inputs."""
    # Create test data for two databases
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    products_df = pd.DataFrame(
        {"id": [101, 102], "name": ["Laptop", "Phone"], "price": [1200, 800]}
    )

    users_sdif = create_test_sdif("users_db", {"users": users_df})
    products_sdif = create_test_sdif("products_db", {"products": products_df})

    # Create transformer with a function that uses both databases
    code = """
def transform(conn):
    import pandas as pd
    users = pd.read_sql_query("SELECT * FROM db1.users", conn)
    products = pd.read_sql_query("SELECT * FROM db2.products", conn)

    # Create a combined report
    report = {
        "users_count": len(users),
        "products_count": len(products),
        "total_inventory_value": products["price"].sum()
    }

    return {
        "users.csv": users,
        "products.csv": products,
        "report.json": report
    }
"""
    transformer = CodeTransformer(function=code)

    # Transform data with a list of SDIFs
    result = transformer.transform(sdif=[users_sdif, products_sdif])

    # Verify result
    assert isinstance(result, dict)
    assert "users.csv" in result
    assert "products.csv" in result
    assert "report.json" in result
    assert len(result["users.csv"]) == 3
    assert len(result["products.csv"]) == 2
    assert result["report.json"]["total_inventory_value"] == 2000


def test_transform_with_custom_schema_names(create_test_sdif):
    """Test transforming data with custom schema names."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    products_df = pd.DataFrame(
        {"id": [101, 102], "name": ["Laptop", "Phone"], "price": [1200, 800]}
    )

    users_sdif = create_test_sdif("users_db", {"users": users_df})
    products_sdif = create_test_sdif("products_db", {"products": products_df})

    # Create transformer with a function that uses custom schema names
    code = """
def transform(conn):
    import pandas as pd
    users = pd.read_sql_query("SELECT * FROM users_schema.users", conn)
    products = pd.read_sql_query("SELECT * FROM products_schema.products", conn)

    return {
        "users.csv": users,
        "products.csv": products
    }
"""
    transformer = CodeTransformer(function=code)

    # Transform data with a dictionary mapping schema names to SDIF paths
    result = transformer.transform(
        sdif={"users_schema": users_sdif, "products_schema": products_sdif}
    )

    # Verify result
    assert isinstance(result, dict)
    assert "users.csv" in result
    assert "products.csv" in result
    assert len(result["users.csv"]) == 3
    assert len(result["products.csv"]) == 2


def test_export_to_files(create_test_sdif, tmp_path):
    """Test exporting transformation results to files."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer
    transformer = CodeTransformer(function=simple_transform)

    # Export data to files - specify the output directory and ensure it exists first
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = transformer.export(sdif=sdif_path, output_path=output_dir)

    # Verify result
    assert result_path == output_dir
    assert result_path.exists()
    assert (result_path / "users.csv").exists()

    # Check the contents
    exported_df = pd.read_csv(result_path / "users.csv")
    assert len(exported_df) == 3
    assert list(exported_df["name"]) == ["Alice", "Bob", "Charlie"]


def test_export_to_zip(create_test_sdif, tmp_path):
    """Test exporting transformation results to a zip archive."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    products_df = pd.DataFrame(
        {"id": [101, 102], "name": ["Laptop", "Phone"], "price": [1200, 800]}
    )

    sdif_path = create_test_sdif(
        "test_db", {"users": users_df, "products": products_df}
    )

    # Create transformer with a function that returns multiple files
    code = """
def transform(conn):
    import pandas as pd
    users = pd.read_sql_query("SELECT * FROM db1.users", conn)
    products = pd.read_sql_query("SELECT * FROM db1.products", conn)

    # Create a report
    report = {
        "users_count": len(users),
        "products_count": len(products)
    }

    return {
        "data/users.csv": users,
        "data/products.csv": products,
        "report.json": report
    }
"""
    transformer = CodeTransformer(function=code)

    # Export data to zip
    zip_path = tmp_path / "output.zip"
    result_path = transformer.export(
        sdif=sdif_path, output_path=zip_path, zip_archive=True
    )

    # Verify result
    assert result_path == zip_path
    assert result_path.exists()

    # Check zip contents
    import zipfile

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        file_list = zip_ref.namelist()
        assert "data/users.csv" in file_list
        assert "data/products.csv" in file_list
        assert "report.json" in file_list

        # Extract and check data
        zip_ref.extractall(tmp_path / "extracted")

    extracted_users = pd.read_csv(tmp_path / "extracted" / "data" / "users.csv")
    assert len(extracted_users) == 3

    with open(tmp_path / "extracted" / "report.json", "r") as f:
        report = json.load(f)
        assert report["users_count"] == 3
        assert report["products_count"] == 2


def test_error_handling_invalid_function():
    """Test error handling with invalid function type."""
    with pytest.raises(TypeError):
        CodeTransformer(function=123)  # type: ignore


def test_error_handling_missing_file(tmp_path):
    """Test error handling with non-existent file path."""
    non_existent_path = tmp_path / "does_not_exist.py"
    with pytest.raises(ValueError):
        CodeTransformer(function=non_existent_path)


def test_error_handling_invalid_sdif_path(tmp_path):
    """Test error handling with invalid SDIF path."""
    non_existent_sdif = tmp_path / "does_not_exist.sdif"
    transformer = CodeTransformer(function=simple_transform)

    with pytest.raises(FileNotFoundError):
        transformer.transform(sdif=non_existent_sdif)


def test_error_handling_invalid_transform_result(create_test_sdif):
    """Test error handling when transformation returns invalid result."""
    users_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create a transformation function that returns a non-dict
    @transformation
    def invalid_transform(conn):
        return "This is not a dict"

    transformer = CodeTransformer(function=invalid_transform)

    with pytest.raises(Exception):  # Should be ExportError in actual execution
        transformer.transform(sdif=sdif_path)


def test_transform_with_sdifdatabase_object(create_test_sdif):
    """Test using an SDIFDatabase object directly."""
    users_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Open database directly
    db = SDIFDatabase(sdif_path)

    # Simple transform function
    @transformation
    def transform_db_object(conn):
        df = pd.read_sql_query("SELECT * FROM users", conn)
        return {"direct_users.csv": df}

    transformer = CodeTransformer(function=transform_db_object)

    try:
        # Transform using the database object
        result = transformer.transform(sdif=db)

        # Verify result
        assert "direct_users.csv" in result
        assert len(result["direct_users.csv"]) == 3
    finally:
        db.close()


def test_transform_with_nested_output_directories(create_test_sdif, tmp_path):
    """Test creating transformation outputs with nested directory structure."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer with a function that returns nested directory structure
    code = """
def transform(conn):
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)

    # Create different output formats for the same data
    csv_df = df.copy()
    json_data = df.to_dict(orient="records")
    summary = {
        "total_users": len(df),
        "age_stats": {
            "min": int(df["age"].min()),
            "max": int(df["age"].max()),
            "avg": float(df["age"].mean())
        }
    }

    return {
        "data/csv/users.csv": csv_df,
        "data/json/users.json": json_data,
        "reports/summary.json": summary,
    }
"""
    transformer = CodeTransformer(function=code)

    # Export data to files
    output_dir = tmp_path / "nested_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = transformer.export(sdif=sdif_path, output_path=output_dir)

    # Verify result
    assert (result_path / "data" / "csv" / "users.csv").exists()
    assert (result_path / "data" / "json" / "users.json").exists()
    assert (result_path / "reports" / "summary.json").exists()

    # Check the contents
    exported_df = pd.read_csv(result_path / "data" / "csv" / "users.csv")
    assert len(exported_df) == 3

    with open(result_path / "reports" / "summary.json", "r") as f:
        summary = json.load(f)
        assert summary["total_users"] == 3
        assert summary["age_stats"]["min"] == 25
        assert summary["age_stats"]["max"] == 35
        assert summary["age_stats"]["avg"] == 30.0


def test_transform_with_multiple_output_formats(create_test_sdif, tmp_path):
    """Test transformation with multiple output formats (CSV, JSON, Excel)."""
    # Create test data
    users_df = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer with a function that returns multiple formats
    code = """
def transform(conn):
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)

    return {
        "users.csv": df,
        "users.json": df,
        "users.xlsx": df
    }
"""
    transformer = CodeTransformer(function=code)

    # Export data to files
    output_dir = tmp_path / "multi_format"
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = transformer.export(sdif=sdif_path, output_path=output_dir)

    # Verify CSV exists and is correct
    assert (result_path / "users.csv").exists()
    csv_df = pd.read_csv(result_path / "users.csv")
    assert len(csv_df) == 3

    # Verify JSON exists and is correct
    assert (result_path / "users.json").exists()
    with open(result_path / "users.json", "r") as f:
        json_data = json.load(f)
        assert len(json_data) == 3
        assert json_data[0]["name"] == "Alice"

    # Verify Excel exists - we'll just check existence since we'd need openpyxl for reading
    assert (result_path / "users.xlsx").exists()


def test_transform_with_in_memory_db(tmp_path):
    """Test transformation with an in-memory database (not using fixtures)."""
    # Create an in-memory SDIF database directly
    with SDIFDatabase(":memory:") as db:
        # Add a source
        source_id = db.add_source(
            file_name="manual_source.csv",
            file_type="csv",
            description="Manually created source",
        )

        # Create and add a table with a simple, safe name
        table_name = "test_inmemory_table"  # Use a simple alphanumeric name
        columns = {
            "id": {"type": "INTEGER"},
            "value": {"type": "REAL"},
            "tag": {"type": "TEXT"},
        }

        db.create_table(table_name, columns, source_id, if_exists="replace")

        # Insert some data
        data = [
            {"id": 1, "value": 10.5, "tag": "A"},
            {"id": 2, "value": 20.3, "tag": "B"},
            {"id": 3, "value": 15.7, "tag": "C"},
        ]
        db.insert_data(table_name, data)

        # Define a transformation function that uses the table name
        def process_data(conn):
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            processed_df = df.copy()
            processed_df["value_squared"] = processed_df["value"] ** 2
            return {"processed.csv": processed_df}

        # Create and use the transformer
        transformer = CodeTransformer(function=process_data)
        result = transformer.transform(sdif=db)

        # Verify the result
        assert "processed.csv" in result
        assert len(result["processed.csv"]) == 3
        assert "value_squared" in result["processed.csv"].columns
        assert result["processed.csv"]["value_squared"].iloc[0] == 10.5**2


def test_db_schema_prefix_customization(create_test_sdif):
    """Test customizing the database schema prefix."""
    # Create test data
    users_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer with custom db_schema_prefix
    code = """
def transform(conn):
    import pandas as pd
    # Use custom prefix in the query
    df = pd.read_sql_query("SELECT * FROM custom_prefix1.users", conn)
    return {"users.csv": df}
"""
    transformer = CodeTransformer(function=code, db_schema_prefix="custom_prefix")

    # Transform data
    result = transformer.transform(sdif=sdif_path)

    # Verify result
    assert "users.csv" in result
    assert len(result["users.csv"]) == 3


def test_transform_with_binary_output(create_test_sdif, tmp_path):
    """Test transformation that returns binary data (e.g., images, PDFs)."""
    # Create test data
    users_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

    sdif_path = create_test_sdif("test_db", {"users": users_df})

    # Create transformer with a function that returns binary data
    code = """
def transform(conn):
    import pandas as pd
    import io
    import base64

    # Get the data
    df = pd.read_sql_query("SELECT * FROM db1.users", conn)

    # Create a simple binary file (a fake image)
    binary_data = b'\\x89PNG\\r\\n\\x1a\\n' + b'\\x00' * 100  # Fake PNG header + some bytes

    # Create CSV data as bytes
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    return {
        "users.bin": binary_data,
        "users_bytes.csv": csv_bytes,
        "text_as_bytes.txt": "This is plain text stored as bytes".encode('utf-8')
    }
"""
    transformer = CodeTransformer(function=code)

    # Export data to files
    output_dir = tmp_path / "binary_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = transformer.export(sdif=sdif_path, output_path=output_dir)

    # Verify binary file exists and has content
    binary_file_path = result_path / "users.bin"
    assert binary_file_path.exists()
    assert binary_file_path.stat().st_size > 0

    # Check the binary content
    with open(binary_file_path, "rb") as f:
        content = f.read()
        # Check PNG signature (first 8 bytes)
        assert content.startswith(b"\x89PNG\r\n\x1a\n")

    # Verify CSV bytes were correctly saved
    csv_file_path = result_path / "users_bytes.csv"
    assert csv_file_path.exists()

    # Can be read as normal CSV despite being written from bytes
    csv_df = pd.read_csv(csv_file_path)
    assert len(csv_df) == 3

    # Verify text file written from bytes
    text_file_path = result_path / "text_as_bytes.txt"
    assert text_file_path.exists()
    with open(text_file_path, "r", encoding="utf-8") as f:
        text = f.read()
        assert text == "This is plain text stored as bytes"
