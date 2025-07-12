from langchain_core.runnables import Runnable

from xlsx_to_sdif.state import State


class LLMCallNode:
    def __init__(self, llm_with_tools: Runnable):
        self.llm_with_tools = llm_with_tools

    def __call__(self, state: State):
        return {"messages": [self.llm_with_tools.invoke(state["messages"])]}
