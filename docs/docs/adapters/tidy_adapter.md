---
sidebar_position: 2
---
# Tidy Adapter


The `TidyAdapter` transforms SDIF database tables into tidy format using AI-generated code. It follows tidy data principles (each variable forms a column, each observation forms a row, and each observational unit forms a table) to restructure data for better analysis.

## 1. Basic Usage

```python
from satif_ai.adapters import TidyAdapter
from pathlib import Path

# ... initialize mcp_server and mcp_session

# Create the adapter
adapter = TidyAdapter(
    mcp_server=mcp_server,
    mcp_session=mcp_session,
    llm_model="o4-mini"  # Default model
)

# Path to input SDIF file
input_sdif_path = "input_data.sdif"

# Transform the data - returns path to new SDIF with tidied data
tidied_sdif_path = adapter.adapt(input_sdif_path)
```

## 2. How It Works

The `TidyAdapter` operates through a multi-step process:

1. **Analysis:** Extracts schema and sample data from the input SDIF database
2. **AI Code Generation:** Prompts an LLM to generate Python code that will transform the data into tidy format
3. **Code Execution:** Runs the generated code using a `CodeAdapter` to transform the SDIF database in place
4. **Output:** Creates a new SDIF file containing the tidied data structure

The adapter uses a specialized prompt that instructs the AI to follow tidy data principles when restructuring tables. Common operations in the generated code typically include:
- Melting wide-format tables into long format
- Splitting columns with multiple values
- Creating new tables for different observation types
- Standardizing variable names and formats

## 3. Constructor Parameters

```python
TidyAdapter(
    mcp_server: MCPServer,
    mcp_session: ClientSession,
    llm_model: str = "o4-mini",
    max_iterations: int = 5
)
```

- **`mcp_server`:** An instance of `MCPServer` for agent communication
- **`mcp_session`:** An instance of `ClientSession` for resource/prompt fetching
- **`llm_model`:** Name of the language model to use for the AI agent (default: "o4-mini")
- **`max_iterations`:** Maximum number of attempts the agent gets to refine the code (default: 5)

## 4. Methods

### `adapt(sdif)`

Transforms the data in the input SDIF to tidy format using an AI agent.

```python
adapt(sdif: Union[SDIFPath, SDIFDatabase]) -> Datasource
```

**Parameters:**
- **`sdif`:** The input SDIF database path or instance. If an instance is provided, its connection will be closed.

**Returns:**
- Path to the new SDIF file containing the tidied data.

**Raises:**
- **`FileNotFoundError`:** If the input SDIF path doesn't exist
- **`RuntimeError`:** If the agent fails to produce valid tidy code
- **`Exception`:** For unexpected errors during the process

## 5. Error Handling

The `TidyAdapter` provides detailed error messages when issues occur:

```python
try:
    tidied_path = adapter.adapt(input_sdif_path)
except FileNotFoundError as e:
    print(f"Input file error: {e}")
except RuntimeError as e:
    print(f"AI code generation error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```
