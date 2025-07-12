---
sidebar_label: base
title: satif_core.transformers.base
---

## Transformer Objects

```python
class Transformer(ABC)
```

> Abstract Base Class for data transformation.
>
> This class defines the interface for all transformer implementations.
> Transformers are responsible for taking SDIF (Standardized Data Interchange Format)
> data as input, performing some transformation logic, and producing output data,
> which can then be exported to various file formats.
>
> Concrete implementations of this class should provide logic for the `transform`
> and `_export_data` methods.

#### transform

```python
@abstractmethod
def transform(
    sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str, SDIFPath]]
) -> Dict[str, Any]
```

> Transforms input SDIF data into an in-memory representation.
>
> This method should be implemented by subclasses to define the core
> transformation logic. It takes one or more SDIF sources, processes them,
> and returns a dictionary where keys are intended output filenames and
> values are the data to be written to these files (e.g., pandas DataFrames,
> dictionaries, lists, strings, or bytes).
>
> **Arguments**:
>
> - `sdif` - The SDIF data source(s) to transform. This can be:
>   - A single SDIF file path (str or Path).
>   - A list of SDIF file paths.
>   - An `SDIFDatabase` instance.
>   - A dictionary mapping custom schema names (str) to SDIF file paths.
>
>
> **Returns**:
>
>   A dictionary where keys are relative output filenames (e.g., &quot;data.csv&quot;)
>   and values are the corresponding transformed data objects.
>
>
> **Raises**:
>
> - `NotImplementedError` - If the method is not implemented by a subclass.
> - `FileNotFoundError` - If any input SDIF file path does not exist.
> - `ValueError` - If input arguments are invalid or incompatible.
> - `Exception` - Subclasses may raise specific exceptions related to
>   transformation errors (e.g., database errors, data processing issues).

#### export

```python
def export(sdif: Union[SDIFPath, List[SDIFPath], SDIFDatabase, Dict[str,
                                                                    SDIFPath]],
           output_path: Union[str, Path] = Path("."),
           zip_archive: bool = False) -> Path
```

> Transforms SDIF data and exports the results to files.
>
> This is a convenience method that orchestrates the transformation and
> export process. It first calls the `transform` method to get the
> in-memory transformed data, and then calls the `_export_data` method
> to write this data to the specified output path.
>
> **Arguments**:
>
> - `sdif` - The SDIF data source(s) to transform. Passed directly to the
>   `transform` method. See `transform` method docstring for details.
> - `output_path` - The base path for output. Passed directly to the
>   `_export_data` method. See `_export_data` method
>   docstring for details. Defaults to the current directory.
> - `zip_archive` - If True, package all output files into a single ZIP archive.
>   Passed directly to the `_export_data` method.
>   Defaults to False.
>
>
> **Returns**:
>
>   The absolute path to the created output file or directory.
>   See `_export_data` method return value for more details.
>
>
> **Raises**:
>
>   This method can raise any exceptions thrown by `transform` or
>   `_export_data` methods (e.g., FileNotFoundError, ValueError, IOError).
