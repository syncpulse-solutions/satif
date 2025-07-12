import logging
from typing import Any

from agents.mcp.server import CallToolResult, MCPServer, MCPTool
from fastmcp import FastMCP

logger = logging.getLogger(__name__)


class OpenAICompatibleMCP(MCPServer):
    def __init__(self, mcp: FastMCP):
        self.mcp = mcp
        self._is_connected = False  # Track connection state

    async def connect(self):
        """Connect to the server.
        For FastMCP, connection is managed externally when the server is run.
        This method marks the wrapper as connected.
        """
        # Assuming FastMCP instance is already running and configured.
        # No specific connect action required for the FastMCP instance itself here,
        # as its lifecycle (run, stop) is managed outside this wrapper.
        logger.info(
            f"OpenAICompatibleMCP: Simulating connection to FastMCP server '{self.mcp.name}'."
        )
        self._is_connected = True

    @property
    def name(self) -> str:
        """A readable name for the server."""
        return self.mcp.name

    async def cleanup(self):
        """Cleanup the server.
        For FastMCP, cleanup is managed externally. This method marks the wrapper as disconnected.
        """
        # Similar to connect, actual server cleanup is external.
        logger.info(
            f"OpenAICompatibleMCP: Simulating cleanup for FastMCP server '{self.mcp.name}'."
        )
        self._is_connected = False

    async def list_tools(self) -> list[MCPTool]:
        """List the tools available on the server."""
        if not self._is_connected:
            # Or raise an error, depending on desired behavior for disconnected state
            raise RuntimeError(
                "OpenAICompatibleMCP.list_tools called while not connected."
            )

        # FastMCP's get_tools() returns a dict[str, fastmcp.tools.tool.Tool]
        # Each fastmcp.tools.tool.Tool has a to_mcp_tool(name=key) method
        # MCPTool is an alias for mcp.types.Tool
        try:
            fastmcp_tools = await self.mcp.get_tools()
            mcp_tools_list = [
                tool.to_mcp_tool(name=key) for key, tool in fastmcp_tools.items()
            ]
            return mcp_tools_list
        except Exception as e:
            logger.error(
                f"Error listing tools from FastMCP server '{self.mcp.name}': {e}",
                exc_info=True,
            )
            raise e

    async def call_tool(
        self, tool_name: str, arguments: dict[str, Any] | None
    ) -> CallToolResult:
        """Invoke a tool on the server."""
        if not self._is_connected:
            logger.warning(
                f"OpenAICompatibleMCP.call_tool '{tool_name}' called while not connected."
            )
            # Return an error CallToolResult
            return CallToolResult(
                content=[{"type": "text", "text": "Server not connected"}], isError=True
            )

        try:
            # FastMCP's _mcp_call_tool is a protected member, but seems to be what we need.
            # It returns: list[TextContent | ImageContent | EmbeddedResource]
            # This matches the 'content' part of CallToolResult.
            # We need to handle potential errors and wrap the result.
            content = await self.mcp._mcp_call_tool(tool_name, arguments or {})
            return CallToolResult(content=content, isError=False)
        except Exception as e:
            logger.error(
                f"Error calling tool '{tool_name}' on FastMCP server '{self.mcp.name}': {e}",
                exc_info=True,
            )
            error_message = f"Error calling tool '{tool_name}': {type(e).__name__}: {e}"
            # Ensure content is a list of valid MCP content items, even for errors.
            # A TextContent is a safe choice.
            return CallToolResult(
                content=[{"type": "text", "text": error_message}], isError=True
            )
