from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, TypeAlias, TypedDict, Union

# Basic Types
FilePath = Union[Path, str]

# Input/Output Specifications
Datasource: TypeAlias = Union[FilePath, List[FilePath]]  # Path(s) to input file(s)
OutputData = Union[FilePath, Dict[str, FilePath]]  # Path(s) to output file(s)
WriteOutputFiles = List[Path]

SDIFPath: TypeAlias = FilePath


class FileConfig(TypedDict, total=False):
    pass


@dataclass
class StandardizationResult:
    """
    Represents the result of a standardization process.

    Attributes:
        output_path: The path to the generated SDIF file.
        file_configs: An optional dictionary where keys are string representations
                       of input file paths and values are `FileConfig` dictionaries
                       containing the configuration used during the standardization for that file.
                       The order of items will reflect the order of datasources processed.
                       Will be None if no such configuration is returned by the standardizer.
    """

    output_path: Path
    file_configs: Dict[str, FileConfig] | None = None


@dataclass
class TransformationResult:
    """
    Represents the result of a transformation process.

    Attributes:
        output_path: The path to the generated output (file or directory) from the transformation.
        function_code: The source code of the transformation function that was executed.
    """

    output_path: Path
    function_code: str
