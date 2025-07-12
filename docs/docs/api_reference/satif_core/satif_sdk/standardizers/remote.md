---
sidebar_label: remote
title: satif_sdk.standardizers.remote
---

#### DEFAULT\_TIMEOUT

> Default timeout 10 minutes (httpx uses float)

## RemoteStandardizer Objects

```python
class RemoteStandardizer(AsyncStandardizer)
```

> A standardizer that interacts with a remote Satif-compliant standardization API.
> It handles file uploads, monitors progress via Server-Sent Events (SSE),
> and downloads the resulting SDIF file.
>
> Allows providing a custom `httpx.AsyncClient` instance for advanced configuration,
> otherwise creates a default client based on environment variables or parameters.
> Compresses multiple input files into a single zip archive before uploading.
>
> Requires configuration of the remote API base URL and potentially an API key.
> The remote API is expected to follow a specific pattern:
> - POST to `runs_path_prefix` to create a run.
> - SSE stream at `events_url` (from create run response).
> - GET from `runs_path_prefix/{run_id}/result` to download the output.

#### \_\_init\_\_

```python
def __init__(base_url: Optional[str] = None,
             api_key: Optional[str] = None,
             runs_path_prefix: Optional[str] = None,
             timeout: Optional[float] = DEFAULT_TIMEOUT,
             client: Optional[httpx.AsyncClient] = None,
             **kwargs: Any)
```

> Initializes the remote standardizer.
>
> **Arguments**:
>
> - `base_url` - The base URL of the remote standardization API.
>   Defaults to env {ENV_REMOTE_BASE_URL}. Used only if &#x27;client&#x27; is not provided.
> - `api_key` - The API key for authentication.
>   Defaults to env {ENV_REMOTE_API_KEY}. Used as Bearer token if &#x27;client&#x27; is not provided.
> - `runs_path_prefix` - Base path for standardization runs on the remote API.
>   Defaults to env {ENV_REMOTE_RUNS_PATH_PREFIX} or &#x27;{DEFAULT_RUNS_PATH_PREFIX}&#x27;.
> - `timeout` - Default request timeout in seconds. Used only if &#x27;client&#x27; is not provided.
>   Defaults to {DEFAULT_TIMEOUT} seconds.
> - `client` - An optional pre-configured `httpx.AsyncClient` instance. If provided,
>   `base_url`, `api_key`, and `timeout` args are ignored for client creation,
>   but `runs_path_prefix` is still used.
> - `api_key`0 - Additional keyword arguments passed to the default `httpx.AsyncClient` constructor
>   if `client` is not provided.

#### standardize

```python
async def standardize(datasource: Datasource,
                      output_path: SDIFPath,
                      *,
                      options: Optional[Dict[str, Any]] = None,
                      log_sse_events: bool = False,
                      overwrite: bool = False) -> StandardizationResult
```

> Performs standardization by interacting with the remote Satif API.
>
> This involves:
> 1. Validating inputs and preparing file paths.
> 2. Packaging datasource file(s) for upload (zipping if multiple).
> 3. Uploading the datasource and options to initiate a run.
> 4. Monitoring the run&#x27;s progress via Server-Sent Events (SSE).
> 5. Fetching final run details (including file_configs and result_url).
> 6. Downloading the resulting SDIF file using the result_url.
> 7. Saving the downloaded SDIF file.
>
> **Arguments**:
>
> - `datasource` - Path or list of paths to the input file(s).
> - `output_path` - The path where the resulting SDIF file should be saved.
> - `options` - Optional dictionary of processing options for the standardization run.
>   These are serialized to JSON and sent as a form field.
> - `log_sse_events` - If True, SSE messages from the server will be logged.
> - `overwrite` - If True, overwrite the output file if it exists.
>
>
> **Returns**:
>
>   A StandardizationResult object containing the path to the created SDIF
>   database file and the file-specific configurations.
>
>
> **Raises**:
>
> - `FileNotFoundError` - If an input file doesn&#x27;t exist.
> - `FileExistsError` - If output_path exists and overwrite is False.
> - `IOError` - If file reading/writing fails (now primarily OSError).
> - `httpx.HTTPStatusError` - For unsuccessful API responses (4xx, 5xx).
> - `RuntimeError` - For other operational errors, including failed standardization runs.
> - `output_path`0 - If zip creation fails.
> - `output_path`1 - If datasource is invalid.
