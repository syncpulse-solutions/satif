---
sidebar_position: 2
---

# Remote Standardizer

> **🚧 WORK IN PROGRESS: The RemoteStandardizer is currently under development and the cloud API service is not yet available. This documentation serves as a reference for the intended functionality.**

The `RemoteStandardizer` is an asynchronous component designed to interact with a remote Satif-compliant standardization API. It enables offloading standardization tasks to a cloud service, providing benefits such as avoiding local AI dependencies and running standardization as background tasks with progress tracking.

## Benefits

- **Reduced Dependencies**: Avoid installing heavy AI dependencies locally
- **Background Processing**: Run standardization as asynchronous background tasks
- **Progress Tracking**: Monitor standardization progress in real-time via Server-Sent Events
- **Scalability**: Leverage cloud infrastructure for processing large datasets
- **Run History**: Track and manage previous standardization runs

## How It Works

The RemoteStandardizer follows a workflow pattern:

1. **File Upload**: Packages input files (compressing multiple files into ZIP archives)
2. **Run Creation**: Initiates a standardization run on the remote API
3. **Progress Monitoring**: Tracks progress via Server-Sent Events (SSE) streaming
4. **Result Download**: Retrieves the resulting SDIF file once processing completes

## Configuration

The standardizer requires configuration of the remote API endpoints:

```python
from satif_sdk.standardizers import RemoteStandardizer

# Using environment variables (recommended)
standardizer = RemoteStandardizer()

# Or explicit configuration
standardizer = RemoteStandardizer(
    base_url="https://api.satif.example.com",
    api_key="your-api-key",
    timeout=600.0
)
```

### Environment Variables

- `SATIF_REMOTE_BASE_URL`: Base URL of the remote API
- `SATIF_REMOTE_API_KEY`: API key for authentication

### Custom HTTP Client

For advanced configurations, provide a custom `httpx.AsyncClient`:

```python
import httpx
from satif_sdk.standardizers import RemoteStandardizer

custom_client = httpx.AsyncClient(
    timeout=httpx.Timeout(900.0),
    limits=httpx.Limits(max_connections=10)
)

standardizer = RemoteStandardizer(client=custom_client)
```

## Basic Usage

```python
import asyncio
from satif_sdk.standardizers import RemoteStandardizer

async def main():
    standardizer = RemoteStandardizer()

    result = await standardizer.standardize(
        datasource="data.csv",
        output_path="output.sdif",
        options={"custom_config": "value"},
        log_sse_events=True
    )

    print(f"Standardization completed: {result.output_path}")

asyncio.run(main())
```

## Parameters

### `standardize()` Method

- **`datasource`**: Path or list of paths to input files
- **`output_path`**: Path where the resulting SDIF file will be saved
- **`options`**: Optional processing configuration sent to the remote API
- **`log_sse_events`**: Whether to log Server-Sent Events messages (default: `False`)
- **`overwrite`**: Whether to overwrite existing output files (default: `False`)

## File Handling

### Single Files
Individual files are uploaded directly with their original filename and MIME type.

### Multiple Files
Multiple input files are automatically compressed into a ZIP archive before upload, reducing transfer time and simplifying remote processing.

### ZIP Archives
ZIP files in the datasource are handled as single entities and processed by the remote service.

## Error Handling

The standardizer provides comprehensive error handling for:

- **Network Issues**: HTTP timeouts, connection failures
- **Authentication**: Invalid API keys or authorization failures
- **File Operations**: Missing files, permission issues, disk space
- **Remote Processing**: Failed standardization runs, invalid responses
- **Download Issues**: Corrupted or incomplete result files

Common exceptions:

- `ValueError`: Invalid datasource or configuration
- `FileNotFoundError`: Input files don't exist
- `FileExistsError`: Output exists and overwrite is False
- `RuntimeError`: Remote API or processing failures
- `httpx.HTTPStatusError`: HTTP-level errors

## Current Status

**The RemoteStandardizer is not yet production-ready:**

- Cloud API service infrastructure is under development
- API endpoints and authentication methods may change
- Documentation reflects intended functionality

For immediate standardization needs, use the local standardizers:
- [`AIStandardizer`](./ai_standardizer.md) for AI-powered processing
