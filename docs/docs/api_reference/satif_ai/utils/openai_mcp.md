---
sidebar_label: openai_mcp
title: satif_ai.utils.openai_mcp
---

## OpenAICompatibleMCP Objects

```python
class OpenAICompatibleMCP(MCPServer)
```

#### connect

```python
async def connect()
```

> Connect to the server.
> For FastMCP, connection is managed externally when the server is run.
> This method marks the wrapper as connected.

#### name

```python
@property
def name() -> str
```

> A readable name for the server.

#### cleanup

```python
async def cleanup()
```

> Cleanup the server.
> For FastMCP, cleanup is managed externally. This method marks the wrapper as disconnected.

#### list\_tools

```python
async def list_tools() -> list[MCPTool]
```

> List the tools available on the server.

#### call\_tool

```python
async def call_tool(tool_name: str,
                    arguments: dict[str, Any] | None) -> CallToolResult
```

> Invoke a tool on the server.
