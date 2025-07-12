from langchain_core.messages import SystemMessage

from xlsx_to_sdif.prompt import prompt
from xlsx_to_sdif.spreadsheet.aspose_cells import AsposeCellsManager, get_workbook
from xlsx_to_sdif.state import State


class BuildSpreadsheetStateNode:
    def __init__(self):
        pass

    def __call__(self, state: State):
        spreadsheet_workbook = get_workbook(state["spreadsheet_path"])

        spreadsheet_state = state.get("spreadsheet_state", {})

        spreadsheet_manager = AsposeCellsManager(spreadsheet_workbook)

        sheets_metadata = spreadsheet_manager.get_sheets_metadata()

        sheets_data = sheets_metadata["sheets"]

        for sheet in sheets_data:
            sheet.setdefault("max_row", None)
            sheet.setdefault("max_col", None)

        first_sheet_name = sheets_data[0]["title"] if sheets_data else ""

        default_range_displayed = f"{first_sheet_name}!A1:H25"

        spreadsheet_state["title"] = sheets_metadata["title"]
        spreadsheet_state["sheets"] = sheets_data

        # TODO: not the best place for this
        system_message = SystemMessage(content=prompt)

        return {
            "messages": [system_message],
            "spreadsheet_state": spreadsheet_state,
            "active_sheet": {
                "title": first_sheet_name,
                "range_to_display": default_range_displayed,
                "range_displayed": default_range_displayed,
            },
            "remaining_steps": 100,
            "extracted_tables": [],
            "output_sdif_path": None,
        }
