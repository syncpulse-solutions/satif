import json
import logging
import mimetypes
import os
import tempfile
import zipfile
from pathlib import Path
from typing import IO, Any, Dict, List, Optional, Tuple

import aiofiles
import httpx
from satif_core import AsyncStandardizer
from satif_core.types import Datasource, SDIFPath, StandardizationResult

log = logging.getLogger(__name__)

# ENV VARIABLES NAMES
ENV_REMOTE_BASE_URL = "SATIF_REMOTE_BASE_URL"
ENV_REMOTE_API_KEY = "SATIF_REMOTE_API_KEY"
ENV_REMOTE_RUNS_PATH_PREFIX = "SATIF_REMOTE_RUNS_PATH_PREFIX"
# DEFAULT VALUES
DEFAULT_RUNS_PATH_PREFIX = "/standardization/runs"
DEFAULT_TIMEOUT = 600.0  # Default timeout 10 minutes (httpx uses float)


class RemoteStandardizer(AsyncStandardizer):
    """
    A standardizer that interacts with a remote Satif-compliant standardization API.
    It handles file uploads, monitors progress via Server-Sent Events (SSE),
    and downloads the resulting SDIF file.

    Allows providing a custom `httpx.AsyncClient` instance for advanced configuration,
    otherwise creates a default client based on environment variables or parameters.
    Compresses multiple input files into a single zip archive before uploading.

    Requires configuration of the remote API base URL and potentially an API key.
    The remote API is expected to follow a specific pattern:
    - POST to `runs_path_prefix` to create a run.
    - SSE stream at `events_url` (from create run response).
    - GET from `runs_path_prefix/{run_id}/result` to download the output.
    """

    _client: httpx.AsyncClient
    _runs_path_prefix: str

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        runs_path_prefix: Optional[str] = None,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        client: Optional[httpx.AsyncClient] = None,
        **kwargs: Any,
    ):
        """
        Initializes the remote standardizer.

        Args:
            base_url: The base URL of the remote standardization API.
                      Defaults to env {ENV_REMOTE_BASE_URL}. Used only if 'client' is not provided.
            api_key: The API key for authentication.
                     Defaults to env {ENV_REMOTE_API_KEY}. Used as Bearer token if 'client' is not provided.
            runs_path_prefix: Base path for standardization runs on the remote API.
                              Defaults to env {ENV_REMOTE_RUNS_PATH_PREFIX} or '{DEFAULT_RUNS_PATH_PREFIX}'.
            timeout: Default request timeout in seconds. Used only if 'client' is not provided.
                     Defaults to {DEFAULT_TIMEOUT} seconds.
            client: An optional pre-configured `httpx.AsyncClient` instance. If provided,
                    `base_url`, `api_key`, and `timeout` args are ignored for client creation,
                    but `runs_path_prefix` is still used.
            **kwargs: Additional keyword arguments passed to the default `httpx.AsyncClient` constructor
                      if `client` is not provided.
        """
        config_base_url = base_url or os.environ.get(ENV_REMOTE_BASE_URL)
        config_api_key = api_key or os.environ.get(ENV_REMOTE_API_KEY)
        self._runs_path_prefix = (
            runs_path_prefix
            or os.environ.get(ENV_REMOTE_RUNS_PATH_PREFIX)
            or DEFAULT_RUNS_PATH_PREFIX
        )

        if client:
            if not isinstance(client, httpx.AsyncClient):
                raise TypeError(
                    f"Expected client to be an instance of httpx.AsyncClient, got {type(client)}"
                )
            self._client = client
            log.debug(
                "Using provided httpx.AsyncClient instance. Base URL/Auth/Timeout args ignored for client creation."
            )
        else:
            if not config_base_url:
                raise ValueError(
                    f"RemoteStandardizer requires a base_url if no client is provided, "
                    f"either via constructor argument or environment variable {ENV_REMOTE_BASE_URL}"
                )

            default_headers = self._prepare_default_headers(config_api_key)
            effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT

            try:
                self._client = httpx.AsyncClient(
                    base_url=config_base_url,
                    headers=default_headers,
                    timeout=effective_timeout,
                    follow_redirects=True,
                    **kwargs,
                )
                log.debug(
                    f"Created default httpx.AsyncClient: base_url='{config_base_url}', timeout={effective_timeout}s"
                )
            except Exception as e:
                log.error(
                    f"Failed to create default httpx.AsyncClient: {e}", exc_info=True
                )
                raise RuntimeError(
                    f"Failed to initialize default httpx AsyncClient: {e}"
                ) from e

    def _prepare_default_headers(self, api_key: Optional[str]) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def _get_mimetype(self, file_path: Path) -> str:
        mime_type, _ = mimetypes.guess_type(file_path.name)
        return mime_type or "application/octet-stream"

    def _validate_and_prepare_paths(
        self, datasource: Datasource, output_path_str: SDIFPath, overwrite: bool
    ) -> Tuple[List[Path], Path]:
        output_path = Path(output_path_str)
        if output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Output file exists and overwrite is False: {output_path}"
            )

        if isinstance(datasource, (str, Path)):
            input_paths = [Path(datasource)]
        elif isinstance(datasource, list) and datasource:
            input_paths = [Path(p) for p in datasource]
        else:
            raise ValueError("Invalid or empty datasource provided.")

        for p in input_paths:
            if not p.exists() or not p.is_file():
                raise FileNotFoundError(f"Input file not found or is not a file: {p}")

        return input_paths, output_path

    def _package_files_for_upload(
        self, input_paths: List[Path]
    ) -> Tuple[Dict[str, Any], Optional[Path], Optional[IO[bytes]]]:
        files_to_upload: Dict[str, Any] = {}
        temp_zip_path: Optional[Path] = None
        file_handle_to_close_explicitly: Optional[IO[bytes]] = None

        if not input_paths:
            raise ValueError("No input files to process.")

        if len(input_paths) == 1:
            upload_file_path = input_paths[0]
            upload_filename = upload_file_path.name
            upload_mimetype = self._get_mimetype(upload_file_path)
            log.debug(f"Preparing single file upload: {upload_filename}")
            file_handle_to_close_explicitly = open(upload_file_path, "rb")
            files_to_upload["file"] = (
                upload_filename,
                file_handle_to_close_explicitly,
                upload_mimetype,
            )
        elif len(input_paths) > 1:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
                temp_zip_path = Path(tmp_zip.name)
            log.info(
                f"Compressing {len(input_paths)} files into temporary zip: {temp_zip_path}"
            )
            try:
                with zipfile.ZipFile(temp_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for file_path in input_paths:
                        zf.write(file_path, arcname=file_path.name)

                file_handle_to_close_explicitly = open(temp_zip_path, "rb")
                files_to_upload["file"] = (
                    "datasource.zip",
                    file_handle_to_close_explicitly,
                    "application/zip",
                )
            except (OSError, zipfile.BadZipFile) as e:
                # Clean up temp_zip_path if zip creation failed before returning/raising
                if temp_zip_path and temp_zip_path.exists():
                    try:
                        os.unlink(temp_zip_path)
                    except OSError:
                        log.warning(
                            f"Failed to remove partially created temp zip: {temp_zip_path}"
                        )
                raise RuntimeError(
                    f"Failed to create zip archive for upload: {e}"
                ) from e

        return files_to_upload, temp_zip_path, file_handle_to_close_explicitly

    async def _create_remote_run(
        self, files_to_upload: Dict[str, Any], options: Optional[Dict[str, Any]]
    ) -> Tuple[str, str]:
        create_run_url = self._runs_path_prefix.lstrip("/")
        form_data: Dict[str, str] = {}
        if options:
            form_data["options"] = json.dumps(options)

        log.info(
            f"Creating standardization run via POST to {self._client.base_url}{create_run_url}..."
        )

        try:
            create_run_response = await self._client.post(
                create_run_url, files=files_to_upload, data=form_data
            )
            create_run_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            error_detail = ""
            try:
                content = e.response.text[:1024]  # Limit error message size
                error_detail = f" - Response Body: {content}"
            except Exception:
                pass  # Ignore if response body cannot be read
            log.error(
                f"Failed to create run: {e.response.status_code}{error_detail}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to create standardization run: {e.response.status_code}{error_detail}"
            ) from e

        run_details = create_run_response.json()
        run_id = run_details.get("run_id")
        events_url = run_details.get("events_url")

        if not run_id or not events_url:
            raise RuntimeError(
                f"Invalid response from create run API: missing run_id or events_url. Response: {run_details}"
            )
        log.info(
            f"Run created successfully. Run ID: {run_id}, Events URL: {events_url}"
        )
        return run_id, events_url

    async def _monitor_run_progress_sse(
        self, events_url: str, run_id: str, log_sse_events: bool
    ) -> None:
        log.info(f"Connecting to SSE stream: {events_url}")
        try:
            async with self._client.stream("GET", events_url) as sse_response:
                sse_response.raise_for_status()
                current_event_type = None
                current_event_data = ""
                async for line_bytes in sse_response.aiter_lines():
                    line = line_bytes.strip()  # aiter_lines provides bytes
                    if not line:  # Empty line signifies end of an event
                        if current_event_type and current_event_data:
                            try:
                                data_dict = json.loads(current_event_data)
                                if log_sse_events:
                                    log.info(
                                        f"SSE Event [{current_event_type}]: {data_dict}"
                                    )

                                if current_event_type == "end" or (
                                    current_event_type == "status"
                                    and "status"
                                    in data_dict  # Ensure status key exists
                                ):
                                    final_status = data_dict.get("status")
                                    final_message = data_dict.get(
                                        "message", "No message."
                                    )
                                    if final_status == "completed":
                                        log.info(
                                            f"Run {run_id} completed successfully. Message: {final_message}"
                                        )
                                        return  # Exit monitoring
                                    elif final_status == "failed":
                                        log.error(
                                            f"Run {run_id} failed. Message: {final_message}"
                                        )
                                        raise RuntimeError(
                                            f"Standardization run {run_id} failed: {final_message}"
                                        )
                            except json.JSONDecodeError:
                                log.warning(
                                    f"Failed to parse SSE data: {current_event_data}"
                                )
                            except (
                                RuntimeError
                            ):  # Propagate RuntimeError from failed status
                                raise
                            except Exception as e_parse:
                                log.error(
                                    f"Error processing SSE event: {e_parse}",
                                    exc_info=True,
                                )
                        # Reset for next event
                        current_event_type = None
                        current_event_data = ""
                        continue

                    # Process line
                    if line.startswith("event:"):
                        current_event_type = line[len("event:") :].strip()
                    elif line.startswith("data:"):
                        current_event_data += line[len("data:") :].strip()
                    # Ignore other lines like comments (starting with ':') or id:

        except httpx.HTTPStatusError as e:
            log.error(
                f"Failed to connect to SSE stream {events_url}: {e.response.status_code}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to connect to SSE stream: {e.response.status_code}"
            ) from e
        except httpx.RequestError as e:
            log.error(
                f"Error during SSE streaming for {events_url}: {e}", exc_info=True
            )
            raise RuntimeError(f"Error during SSE streaming: {e}") from e

    async def _fetch_final_run_details(self, run_id: str) -> Dict[str, Any]:
        """Fetches the final run details (status, result_url, file_configs) from the API."""
        run_status_url = f"{self._runs_path_prefix.lstrip('/')}/{run_id}"
        log.info(
            f"Fetching final run status from {self._client.base_url}{run_status_url}"
        )
        try:
            status_response = await self._client.get(run_status_url)
            status_response.raise_for_status()
            return status_response.json()
        except httpx.HTTPStatusError as e_status:
            error_detail = ""
            try:
                content = e_status.response.text[:1024]
                error_detail = f" - Response Body: {content}"
            except Exception:
                pass
            log.error(
                f"Failed to fetch final run status for {run_id}: {e_status.response.status_code}{error_detail}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to fetch final run status for {run_id}: {e_status.response.status_code}{error_detail}"
            ) from e_status
        except json.JSONDecodeError as e_json:
            log.error(
                f"Failed to parse JSON from final run status response for {run_id}: {e_json}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Invalid JSON response when fetching final run status for {run_id}."
            ) from e_json

    async def _download_run_result_from_url(
        self, download_url: str, output_path: Path
    ) -> None:
        """Downloads a run result from a full URL."""
        log.info(f"Downloading result from {download_url}...")
        try:
            # Use a new request, not necessarily based on self._client.base_url
            # if download_url is absolute. If it's relative, it will use base_url.
            async with (
                self._client.stream(
                    "GET",
                    download_url,
                    headers={"Accept": "application/octet-stream"},
                    follow_redirects=True,  # Important if result_url could be a presigned S3, etc.
                ) as result_response
            ):
                result_response.raise_for_status()

                async with aiofiles.open(output_path, "wb") as f_out:
                    async for chunk in result_response.aiter_bytes():
                        await f_out.write(chunk)
                log.info(f"Successfully saved standardized SDIF file to {output_path}")
        except httpx.HTTPStatusError as e:
            error_detail = ""
            try:
                content = e.response.text[:1024]
                error_detail = f" - Response Body: {content}"
            except Exception:
                pass
            log.error(
                f"Failed to download result from {download_url}: {e.response.status_code}{error_detail}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to download result file from {download_url}: {e.response.status_code}{error_detail}"
            ) from e
        except OSError as e:
            log.error(
                f"Failed to write result file to {output_path}: {e}", exc_info=True
            )
            raise OSError(
                f"Failed to write received SDIF file to {output_path}: {e}"
            ) from e

    async def standardize(
        self,
        datasource: Datasource,
        output_path: SDIFPath,
        *,
        options: Optional[Dict[str, Any]] = None,
        log_sse_events: bool = False,
        overwrite: bool = False,
    ) -> StandardizationResult:
        """
        Performs standardization by interacting with the remote Satif API.

        This involves:
        1. Validating inputs and preparing file paths.
        2. Packaging datasource file(s) for upload (zipping if multiple).
        3. Uploading the datasource and options to initiate a run.
        4. Monitoring the run's progress via Server-Sent Events (SSE).
        5. Fetching final run details (including file_configs and result_url).
        6. Downloading the resulting SDIF file using the result_url.
        7. Saving the downloaded SDIF file.

        Args:
            datasource: Path or list of paths to the input file(s).
            output_path: The path where the resulting SDIF file should be saved.
            options: Optional dictionary of processing options for the standardization run.
                     These are serialized to JSON and sent as a form field.
            log_sse_events: If True, SSE messages from the server will be logged.
            overwrite: If True, overwrite the output file if it exists.

        Returns:
            A StandardizationResult object containing the path to the created SDIF
            database file and the file-specific configurations.

        Raises:
            FileNotFoundError: If an input file doesn't exist.
            FileExistsError: If output_path exists and overwrite is False.
            IOError: If file reading/writing fails (now primarily OSError).
            httpx.HTTPStatusError: For unsuccessful API responses (4xx, 5xx).
            RuntimeError: For other operational errors, including failed standardization runs.
            zipfile.BadZipFile: If zip creation fails.
            ValueError: If datasource is invalid.
        """
        input_paths, output_path_obj = self._validate_and_prepare_paths(
            datasource, output_path, overwrite
        )

        files_to_upload: Dict[str, Any] = {}
        temp_zip_path: Optional[Path] = None
        file_handle_to_close_explicitly: Optional[IO[bytes]] = None

        try:
            files_to_upload, temp_zip_path, file_handle_to_close_explicitly = (
                self._package_files_for_upload(input_paths)
            )

            run_id, events_url = await self._create_remote_run(files_to_upload, options)

            await self._monitor_run_progress_sse(events_url, run_id, log_sse_events)

            run_details = await self._fetch_final_run_details(run_id)

            if run_details.get("status") != "completed":
                raise RuntimeError(
                    f"Remote standardization run {run_id} did not complete successfully. Status: {run_details.get('status')}, Message: {run_details.get('message')}"
                )

            result_download_url = run_details.get("result_url")
            if not result_download_url:
                raise RuntimeError(
                    f"No result_url found in completed run details for run {run_id}."
                )

            await self._download_run_result_from_url(
                result_download_url, output_path_obj
            )

            file_configs = run_details.get("file_configs")

        except Exception as e:
            log.error(
                f"Error during remote standardization process: {e}", exc_info=True
            )
            # Ensure consistent error types are raised or re-raised
            if not isinstance(
                e,
                (
                    FileNotFoundError,
                    FileExistsError,
                    ValueError,
                    RuntimeError,
                    zipfile.BadZipFile,
                    OSError,  # Changed from IOError to be more specific
                    httpx.HTTPError,  # Catching base httpx error
                ),
            ):
                # Wrap unexpected errors in a RuntimeError for clarity
                raise RuntimeError(
                    f"An unexpected error occurred during standardization: {e}"
                ) from e
            raise
        finally:
            if file_handle_to_close_explicitly:
                try:
                    file_handle_to_close_explicitly.close()
                except Exception as close_err:
                    log.warning(f"Error closing file handle: {close_err}")

            if temp_zip_path and temp_zip_path.exists():
                try:
                    os.unlink(temp_zip_path)
                    log.debug(f"Removed temporary zip file: {temp_zip_path}")
                except OSError as e:
                    log.warning(
                        f"Failed to remove temporary zip file {temp_zip_path}: {e}"
                    )

        return StandardizationResult(
            output_path=output_path_obj.resolve(), file_configs=file_configs
        )
