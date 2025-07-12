from xlsx_to_sdif.nodes.export_active_sheet_image import get_sheet_info_by_title
from xlsx_to_sdif.spreadsheet.aspose_cells import AsposeCellsManager, get_workbook
from xlsx_to_sdif.state import State


class EnsureActiveSheetDisplayedRangeNode:
    def __init__(self):
        pass

    def __call__(self, state: State):
        range_to_display = state["active_sheet"]["range_to_display"]
        range_displayed = state["active_sheet"]["range_displayed"]
        sheet_name = state["active_sheet"]["title"]
        new_sheet_name = None
        if range_to_display != range_displayed:
            new_sheet_name = range_to_display.split("!")[0]
            new_sheet_info = get_sheet_info_by_title(
                new_sheet_name, state["spreadsheet_state"]["sheets"]
            )
            max_row = new_sheet_info["max_row"]
            max_col = new_sheet_info["max_col"]
            spreadsheet_manager = AsposeCellsManager(
                workbook=get_workbook(state["spreadsheet_path"]),
            )
            spreadsheet_manager.display_only_range(range_to_display, max_row, max_col)

        return {
            "active_sheet": {
                "title": new_sheet_name or sheet_name,
                "range_to_display": range_to_display,
                "range_displayed": range_to_display,
            },
        }
