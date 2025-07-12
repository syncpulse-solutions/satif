---
sidebar_position: 2
---
# SDIF Schema Comparator

The `SDIFSchemaComparator` compares SDIF database schemas for equivalence and compatibility based on configurable rules.

## Basic Usage

```python
from satif_sdk.comparators import SDIFSchemaComparator
from satif_sdk import SDIFDatabase

# Get schemas from SDIF databases
db1 = SDIFDatabase("data1.sdif")
db2 = SDIFDatabase("data2.sdif")
schema1 = db1.get_schema()
schema2 = db2.get_schema()

# Compare schemas
comparator = SDIFSchemaComparator()
are_equivalent, differences = comparator.compare(schema1, schema2)

print(f"Schemas equivalent: {are_equivalent}")
for diff in differences:
    print(f"- {diff}")
```

## Configuration

Use `SDIFSchemaConfig` to customize comparison rules:

```python
from sdif_db.schema import SDIFSchemaConfig

config = SDIFSchemaConfig(
    ignore_table_order=True,
    ignore_column_order=False,
    ignore_metadata=True
)

comparator = SDIFSchemaComparator(config=config)
```

## Methods

### `compare(schema1, schema2, verbose_diff_level=0)`

Compares two schemas for equivalence.

**Parameters:**
- **`schema1`**, **`schema2`**: Schemas from `SDIFDatabase.get_schema()`
- **`verbose_diff_level`**: Detail level of differences
  - `0`: Human-readable summary (default)
  - `1`: DeepDiff dictionary
  - `2+`: Full DeepDiff object

**Returns:** `(bool, differences)` where differences format depends on `verbose_diff_level`

### `is_compatible_with(consumer_schema, producer_schema)`

Checks if producer schema satisfies consumer requirements (subset relationship).

**Returns:** `bool` - True if producer is compatible with consumer needs

## Usage Examples

**Basic equivalence check:**
```python
are_equal, diffs = comparator.compare(schema1, schema2)
```

**Detailed differences:**
```python
are_equal, diff_dict = comparator.compare(schema1, schema2, verbose_diff_level=1)
```

**Compatibility check:**
```python
# Can producer satisfy consumer's requirements?
is_compatible = comparator.is_compatible_with(
    consumer_schema=required_schema,
    producer_schema=actual_schema
)
```

**Custom configuration:**
```python
from sdif_db.schema import SDIFSchemaConfig

config = SDIFSchemaConfig(
    ignore_metadata=True,
    ignore_table_order=True
)
comparator = SDIFSchemaComparator(config)
```
