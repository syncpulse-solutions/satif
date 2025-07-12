from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Protocol,
    TypeAlias,
    Union,
    runtime_checkable,
)

if TYPE_CHECKING:
    import pandas as pd

    DataFrameLike: TypeAlias = "pd.DataFrame"


@runtime_checkable
class SDIFDatabase(Protocol):
    # Lifecycle
    def close(self) -> None: ...
    def __enter__(self) -> SDIFDatabase: ...  # noqa: D105
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...  # noqa: D105

    # Metadata & Structure
    def get_properties(self) -> Optional[Dict[str, Any]]: ...
    def list_sources(self) -> List[Dict[str, Any]]: ...
    def list_tables(self) -> List[str]: ...
    def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]: ...
    def list_objects(self) -> List[str]: ...
    def get_object(
        self, object_name: str, parse_json: bool = True
    ) -> Optional[Dict[str, Any]]: ...
    def list_media(self) -> List[str]: ...
    def get_media(
        self, media_name: str, parse_json: bool = True
    ) -> Optional[Dict[str, Any]]: ...
    def list_semantic_links(self, parse_json: bool = True) -> List[Dict[str, Any]]: ...
    def get_schema(self) -> Dict[str, Any]: ...

    # Data Reading
    def read_table(self, table_name: str) -> DataFrameLike: ...
    def query(
        self, plain_sql: str, return_format: str = "dataframe"
    ) -> Union[DataFrameLike, List[Dict[str, Any]]]: ...
    def get_sample_analysis(
        self,
        num_sample_rows: int = 5,
        top_n_common_values: int = 10,
        include_objects: bool = False,
        include_media: bool = False,
    ) -> Dict[str, Any]: ...

    # Data Writing
    def add_source(
        self, file_name: str, file_type: str, description: Optional[str] = None
    ) -> int: ...
    def create_table(
        self,
        table_name: str,
        columns: Dict[str, Dict[str, Any]],
        source_id: int,
        description: Optional[str] = None,
        original_identifier: Optional[str] = None,
    ) -> None: ...
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> None: ...
    def add_object(
        self,
        object_name: str,
        json_data: Any,
        source_id: int,
        description: Optional[str] = None,
        schema_hint: Optional[Dict] = None,
    ) -> None: ...
    def add_media(
        self,
        media_name: str,
        media_data: bytes,
        media_type: str,
        source_id: int,
        description: Optional[str] = None,
        original_format: Optional[str] = None,
        technical_metadata: Optional[Dict] = None,
    ) -> None: ...
    def add_semantic_link(
        self,
        link_type: str,
        from_element_type: str,
        from_element_spec: Dict,
        to_element_type: str,
        to_element_spec: Dict,
        description: Optional[str] = None,
    ) -> None: ...
    def drop_table(self, table_name: str) -> None: ...
    def write_dataframe(
        self,
        df: DataFrameLike,
        table_name: str,
        source_id: int,
        description: Optional[str] = None,
        original_identifier: Optional[str] = None,
        if_exists: str = "fail",
    ) -> None: ...
