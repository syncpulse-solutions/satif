import asyncio

import pytest

from satif_core.transformation_builders.base import (
    AsyncTransformationBuilder,
    TransformationBuilder,
)


class SimpleTransformationBuilder(TransformationBuilder):
    """Simple concrete implementation of the TransformationBuilder abstract class."""

    def build(self, **kwargs) -> str:
        """
        Simple implementation that returns a mock transformation code.
        """
        # Access kwargs to make sure they're passed through
        input_file = kwargs.get("input_file", "default_input.sdif")
        output_format = kwargs.get("output_format", "csv")

        # Return a template transformation function
        return f"""
def transform(conn, context=None):
    \"\"\"
    Transforms data from {input_file} to {output_format} format.
    \"\"\"
    import pandas as pd

    # Read data from the input
    df = pd.read_sql_query("SELECT * FROM db1.table", conn)

    # Simple transformation
    df['transformed'] = df['column'].apply(lambda x: x.upper())

    # Return the transformed data
    return {{"output.{output_format}": df}}
"""


class SimpleAsyncTransformationBuilder(AsyncTransformationBuilder):
    """Simple concrete implementation of the AsyncTransformationBuilder abstract class."""

    async def build(self, **kwargs) -> str:
        """
        Simple implementation that returns a mock transformation code asynchronously.
        """
        # Simulate async operation
        await asyncio.sleep(0.01)

        # Access kwargs to make sure they're passed through
        input_file = kwargs.get("input_file", "default_input.sdif")
        output_format = kwargs.get("output_format", "csv")

        # Return a template transformation function
        return f"""
def transform(conn, context=None):
    \"\"\"
    Transforms data from {input_file} to {output_format} format.
    \"\"\"
    import pandas as pd

    # Read data from the input
    df = pd.read_sql_query("SELECT * FROM db1.table", conn)

    # Simple transformation
    df['transformed'] = df['column'].apply(lambda x: x.upper())

    # Return the transformed data
    return {{"output.{output_format}": df}}
"""


@pytest.fixture
def simple_transformation_builder():
    """Fixture providing a SimpleTransformationBuilder instance."""
    return SimpleTransformationBuilder()


@pytest.fixture
def simple_async_transformation_builder():
    """Fixture providing a SimpleAsyncTransformationBuilder instance."""
    return SimpleAsyncTransformationBuilder()


def test_transformation_builder_interface(simple_transformation_builder):
    """Test that the TransformationBuilder interface works as expected."""
    # Test with no kwargs
    code = simple_transformation_builder.build()

    # Check result type and content
    assert isinstance(code, str)
    assert "def transform(conn, context=None):" in code
    assert "default_input.sdif" in code

    # Test with kwargs
    code = simple_transformation_builder.build(
        input_file="customers.sdif", output_format="json"
    )

    assert "customers.sdif" in code
    assert "output.json" in code


@pytest.mark.asyncio
async def test_async_transformation_builder_interface(
    simple_async_transformation_builder,
):
    """Test that the AsyncTransformationBuilder interface works as expected."""
    # Test with no kwargs
    code = await simple_async_transformation_builder.build()

    # Check result type and content
    assert isinstance(code, str)
    assert "def transform(conn, context=None):" in code
    assert "default_input.sdif" in code

    # Test with kwargs
    code = await simple_async_transformation_builder.build(
        input_file="customers.sdif", output_format="json"
    )

    assert "customers.sdif" in code
    assert "output.json" in code


def test_transformation_builder_subclass_abstractness():
    """Test that TransformationBuilder can't be instantiated directly."""
    with pytest.raises(TypeError):
        TransformationBuilder()


def test_async_transformation_builder_subclass_abstractness():
    """Test that AsyncTransformationBuilder can't be instantiated directly."""
    with pytest.raises(TypeError):
        AsyncTransformationBuilder()
