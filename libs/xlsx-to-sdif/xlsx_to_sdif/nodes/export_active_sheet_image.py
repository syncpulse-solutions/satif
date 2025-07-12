from typing import Optional

from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig

from xlsx_to_sdif.services.aspose_cells2png import export_excel_area_to_png_bytes
from xlsx_to_sdif.spreadsheet.aspose_cells import AsposeCellsManager, get_workbook
from xlsx_to_sdif.state import State
from xlsx_to_sdif.utils import image_to_base64


def get_sheet_info_by_title(title: str, sheets: list[dict]):
    sheet_info = next(
        (sheet for sheet in sheets if sheet["title"] == title),
        None,
    )
    return sheet_info


class ExportActiveSheetImageNode:
    def __init__(self):
        """Initialize the node with a specific prefix."""
        pass

    def __call__(self, state: State, config: Optional[RunnableConfig] = None):
        """This method is executed when the node is called by LangGraph."""
        sheet_name = state["active_sheet"]["title"]
        active_sheet_info = get_sheet_info_by_title(
            sheet_name, state["spreadsheet_state"]["sheets"]
        )

        active_sheet_thumbnail_base64_image = image_to_base64(
            export_excel_area_to_png_bytes(
                workbook=get_workbook(state["spreadsheet_path"]),
                sheet=sheet_name,
            )
        )
        active_sheet_view_base64_image = image_to_base64(
            export_excel_area_to_png_bytes(
                workbook=get_workbook(state["spreadsheet_path"]),
                sheet=sheet_name,
                export_range=state["active_sheet"]["range_to_display"].split("!")[1],
            )
        )

        spreadsheet_manager = AsposeCellsManager(
            workbook=get_workbook(state["spreadsheet_path"]),
        )

        sheet_info = {
            "title": state["active_sheet"]["title"],
            "range_displayed": state["active_sheet"]["range_displayed"],
            "max_row": active_sheet_info["max_row"],
            "max_col": spreadsheet_manager.col_index_to_letter(
                active_sheet_info["max_col"]
            ),
        }
        return {
            "messages": [
                HumanMessage(
                    content=[
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{active_sheet_thumbnail_base64_image}"
                            },
                        },
                    ]
                ),
                HumanMessage(
                    content=[
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{active_sheet_view_base64_image}"
                            },
                        },
                    ]
                ),
                HumanMessage(
                    content=f"Analyze, Plan, and Execute. Here is the active sheet info: <active_sheet>{str(sheet_info)}</active_sheet>"
                ),
            ]
        }
