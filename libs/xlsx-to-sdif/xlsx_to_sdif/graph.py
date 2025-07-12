"""Define a simple chatbot xlsx_to_sdif.

This agent returns a predefined response without using an actual LLM.
"""

import os

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph
from langgraph.prebuilt import tools_condition

from xlsx_to_sdif.configuration import Configuration
from xlsx_to_sdif.nodes.build_spreadsheet_state import BuildSpreadsheetStateNode
from xlsx_to_sdif.nodes.cleanup_workbook import cleanup_workbook_node
from xlsx_to_sdif.nodes.ensure_active_sheet_displayed_range import (
    EnsureActiveSheetDisplayedRangeNode,
)
from xlsx_to_sdif.nodes.export_active_sheet_image import ExportActiveSheetImageNode
from xlsx_to_sdif.nodes.llm_call import LLMCallNode
from xlsx_to_sdif.nodes.tools_execution import SequentialToolNode
from xlsx_to_sdif.nodes.transform_to_sdif import transform_to_sdif
from xlsx_to_sdif.state import State
from xlsx_to_sdif.tools import (
    add_values,
    auto_fill,
    copy_paste,
    delete_columns,
    delete_rows,
    delete_values,
    insert_columns,
    insert_rows,
    merge_cells,
    navigate,
    read_cells,
    unmerge_cells,
    update_values,
)

memory = MemorySaver()

pro = "gemini-2.5-pro"
# flash = "gemini-2.5-flash-preview-04-17"


def create_graph():
    tools = [
        navigate,
        add_values,
        delete_values,
        update_values,
        insert_rows,
        delete_rows,
        insert_columns,
        delete_columns,
        merge_cells,
        unmerge_cells,
        copy_paste,
        auto_fill,
        read_cells,
    ]

    model = ChatGoogleGenerativeAI(
        model=pro,
        google_api_key=os.environ.get("GEMINI_API_KEY"),
    )

    llm_with_tools = model.bind_tools(tools)

    # Define a new graph
    workflow = StateGraph(State, config_schema=Configuration)

    workflow.add_node("llm_call", LLMCallNode(llm_with_tools))
    workflow.add_node("tools", SequentialToolNode(tools=tools))
    workflow.add_node("build_spreadsheet_state", BuildSpreadsheetStateNode())
    workflow.add_node("export_active_sheet_image", ExportActiveSheetImageNode())
    workflow.add_node(
        "ensure_active_sheet_display_range",
        EnsureActiveSheetDisplayedRangeNode(),
    )
    workflow.add_node("transform_to_sdif", transform_to_sdif)
    workflow.add_node("cleanup_workbook", cleanup_workbook_node)

    workflow.add_edge("__start__", "build_spreadsheet_state")
    workflow.add_edge("build_spreadsheet_state", "ensure_active_sheet_display_range")
    workflow.add_edge("ensure_active_sheet_display_range", "export_active_sheet_image")
    workflow.add_edge("export_active_sheet_image", "llm_call")

    workflow.add_conditional_edges(
        "llm_call", tools_condition, {"tools": "tools", "__end__": "transform_to_sdif"}
    )

    workflow.add_edge("tools", "ensure_active_sheet_display_range")

    workflow.add_edge("transform_to_sdif", "cleanup_workbook")
    workflow.add_edge("cleanup_workbook", "__end__")

    # Compile the workflow into an executable graph
    graph = workflow.compile(checkpointer=memory, name="Spreadsheet Standardizer")

    return graph


graph = create_graph()
