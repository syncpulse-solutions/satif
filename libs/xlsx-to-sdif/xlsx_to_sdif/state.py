from typing import (
    Annotated,
    Optional,
    Sequence,
    TypedDict,
)

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field


class Table(BaseModel):
    title: str
    range: str
    metadata: dict = Field(
        description="Any relevant information not included in the final table's rectangular data block. This includes descriptions, notes found in nearby cells, removed header rows/columns, summary information, etc. The goal is to ensure no information is lost from the original spreadsheet context.",
    )


class ActiveSheet(TypedDict):
    title: str
    range_to_display: str
    range_displayed: str


class Sheet(TypedDict):
    title: str
    max_row: Optional[int]
    max_col: Optional[int]


class SpreadsheetState(TypedDict):
    title: str
    sheets: list[Sheet]


class State(TypedDict):
    """The state of the xlsx_to_sdif."""

    messages: Annotated[Sequence[BaseMessage], add_messages]
    spreadsheet_path: str
    spreadsheet_state: Optional[SpreadsheetState]
    active_sheet: Optional[ActiveSheet]
    remaining_steps: Optional[int]
    extracted_tables: Optional[list[Table]]
    output_sdif_path: Optional[str]
