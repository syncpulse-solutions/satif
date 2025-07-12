---
sidebar_label: tidy
title: satif_ai.adapters.tidy
---

#### execute\_tidy\_adaptation

```python
@function_tool
async def execute_tidy_adaptation(code: str) -> str
```

> Tool implementation for the agent to execute the tidying adaptation code.
> Runs the code against a *copy* of the input SDIF, creating a *new* output SDIF,
> and returns a sample analysis of the modified output.

## TidyAdapter Objects

```python
class TidyAdapter(Adapter)
```

> Uses an AI agent (via agents library) to generate and execute Python code
> that transforms tables in an SDIF file into a tidy format using CodeAdapter.

#### \_\_init\_\_

```python
def __init__(mcp_server: MCPServer,
             mcp_session: ClientSession,
             llm_model: str = "o4-mini",
             max_iterations: int = 5)
```

> Initialize the TidyAdapter.
>
> **Arguments**:
>
> - `mcp_server` - An instance of MCPServer for agent communication.
> - `mcp_session` - An instance of ClientSession for resource/prompt fetching.
> - `llm_model` - Name of the language model to use for the agent.
> - `max_iterations` - Maximum number of attempts the agent gets to refine the code.

#### parse\_code

```python
def parse_code(code_text: str) -> Optional[str]
```

> Extracts Python code from markdown code blocks.

#### adapt

```python
async def adapt(sdif: Union[SDIFPath, SDIFDatabase]) -> Datasource
```

> Transforms the data in the input SDIF to be tidy using an AI agent.
>
> **Arguments**:
>
> - `sdif` - The input SDIF database instance. Connection will be closed.
>
>
> **Returns**:
>
>   Path to the new SDIF file containing the tidied data.
>
>
> **Raises**:
>
> - `FileNotFoundError` - If the input SDIF path doesn&#x27;t exist.
> - `RuntimeError` - If the agent fails to produce valid tidy code.
> - `Exception` - For unexpected errors during the process.
