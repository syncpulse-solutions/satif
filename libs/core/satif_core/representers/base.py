import abc
from pathlib import Path
from typing import Any, Dict, Tuple, Union


class Representer(abc.ABC):
    """
    Abstract base class for generating file representations.

    Defines the common interface for creating a concise string summary
    (e.g., header + first N rows) of a file's content.
    """

    @abc.abstractmethod
    def represent(
        self,
        file_path: Union[str, Path],
        **kwargs: Any,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Generates a representation of the file content.

        Args:
            file_path: Path to the input file.
            **kwargs: Representer-specific options.

        Returns:
            A tuple containing:
                - str: The representation of the file content, or an error message string
                       if the file cannot be read properly.
                - dict: A dictionary of parameters used to generate the representation
                        (e.g., encoding, delimiter).

        Raises:
            FileNotFoundError: If the file_path does not exist (should ideally be
                               checked before calling or handled within).
            ImportError: If required libraries for a specific type are missing.
            # Other specific exceptions might be raised depending on implementation
        """
        pass

    @abc.abstractmethod
    def as_base64_image(self, file_path: Union[str, Path], **kwargs: Any) -> str:
        """Generates a base64 representation of the file content."""
        pass

    @abc.abstractmethod
    def as_text(self, file_path: Union[str, Path], **kwargs: Any) -> str:
        """Generates a text representation of the file content."""
        pass
