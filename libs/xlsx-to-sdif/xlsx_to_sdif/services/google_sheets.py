import base64
import os
from typing import Any

import requests
from google.auth import default
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from xlsx_to_sdif.services.html2png import excel_sheet_to_png_selenium
from xlsx_to_sdif.utils import image_to_base64

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_google_sheets_service_from_client_secrets(secrets_file_path: str):
    """Dependency that creates and returns a Google Sheets API service object
    using client secrets file for OAuth 2.0.

    Returns:
        googleapiclient.discovery.Resource: The Google Sheets API service object.

    Raises:
        HTTPException: If there is an error creating the service.
    """
    flow = InstalledAppFlow.from_client_secrets_file(
        secrets_file_path,
        scopes=SCOPES,
    )
    creds = flow.run_local_server(port=0)
    service = build("sheets", "v4", credentials=creds)
    return service


def get_google_sheets_service_from_default_credentials():
    credentials, project = default(scopes=SCOPES)
    service = build("sheets", "v4", credentials=credentials)
    return service


def export_spreadsheet_as_zip(spreadsheet_id: str, google_sheets_service: Any) -> bytes:
    """Export a Google Spreadsheet as a zip file containing HTML.

    Args:
        google_sheets_service: The Google Sheets service object
        spreadsheet_id: ID of the spreadsheet to export

    Returns:
        bytes: The content of the zip file
    """
    # URL to export as HTML (zip file)
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=zip"
    )

    headers = {
        "Authorization": f"Bearer {google_sheets_service._http.credentials.token}"
    }

    response = requests.get(export_url, headers=headers)
    response.raise_for_status()
    return response.content


def export_sheet_as_csv(
    spreadsheet_id: str, sheet_gid: str, google_sheets_service: Any
) -> bytes:
    """Export a specific sheet from a Google Spreadsheet as CSV.

    Args:
        spreadsheet_id: ID of the spreadsheet to export
        sheet_gid: Sheet ID (gid parameter)
        google_sheets_service: The Google Sheets service object

    Returns:
        bytes: The content of the CSV file
    """
    # Directly construct the export URL with parameters
    export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={sheet_gid}"

    # Build headers with authorization token
    headers = {
        "Authorization": f"Bearer {google_sheets_service._http.credentials.token}"
    }

    # Make the request
    response = requests.get(export_url, headers=headers)
    response.raise_for_status()

    return response.content


def export_sheet_as_csv_by_name(
    spreadsheet_id: str, sheet_name: str, google_sheets_service: Any
) -> bytes:
    """Export a specific sheet by name from a Google Spreadsheet as CSV.

    Args:
        spreadsheet_id: ID of the spreadsheet to export
        sheet_name: Name of the sheet to export
        google_sheets_service: The Google Sheets service object

    Returns:
        bytes: The content of the CSV file
    """
    # Get the spreadsheet info
    spreadsheet_info = (
        google_sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    )

    # Find the sheet ID by name
    sheet_gid = None
    for sheet in spreadsheet_info.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            sheet_gid = sheet["properties"]["sheetId"]
            break

    if sheet_gid is None:
        raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet")

    # Use the export_sheet_as_csv function with the found sheet_gid
    return export_sheet_as_csv(spreadsheet_id, sheet_gid, google_sheets_service)


def get_dimensions_from_csv_bytes(csv_bytes: bytes) -> tuple[int, int]:
    """Get the maximum dimensions (rows and columns) from CSV content in bytes format.

    Args:
        csv_bytes: The CSV content as bytes

    Returns:
        tuple: (max_rows, max_cols)
    """
    # Decode bytes to string
    content = csv_bytes.decode("utf-8")

    # Parse CSV to find dimensions
    rows = content.strip().split("\n")
    max_rows = len(rows)
    max_cols = max(len(row.split(",")) for row in rows) if rows else 0

    return max_rows, max_cols


def export_spreadsheet_as_base64_image(
    spreadsheet_id: str, sheet_name: str, google_sheets_service: Any
) -> str:
    spreadsheet_html_zip = export_spreadsheet_as_zip(
        spreadsheet_id, google_sheets_service
    )
    image_selenium = excel_sheet_to_png_selenium(spreadsheet_html_zip, sheet_name)

    return image_to_base64(image_selenium)


def save_spreadsheet_as_png(
    spreadsheet_id: str, sheet_name: str, path: str, google_sheets_service: Any
) -> None:
    image_base64 = export_spreadsheet_as_base64_image(
        spreadsheet_id, sheet_name, google_sheets_service
    )
    image_bytes = base64.b64decode(image_base64)
    with open(path, "wb") as f:
        f.write(image_bytes)


current_dir = os.path.dirname(os.path.abspath(__file__))
client_secret_path = os.path.join(
    current_dir,
    "client_secret_807632543924-u9208qrjip1675fohh6msr3gp5vlkdgi.apps.googleusercontent.com.json",
)
google_sheets_service = get_google_sheets_service_from_client_secrets(
    client_secret_path
)
