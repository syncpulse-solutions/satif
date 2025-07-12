import io
import zipfile
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def excel_sheet_to_png_selenium(zip_bytes: bytes, sheet_name: str) -> bytes:
    """Convert an Excel sheet from zip bytes to PNG image bytes.

    Args:
        zip_bytes (bytes): The zip file contents as bytes
        sheet_name (str): Name of the sheet to convert

    Returns:
        bytes: PNG image bytes
    """
    # Create a temporary zip file in memory
    zip_buffer = io.BytesIO(zip_bytes)

    with zipfile.ZipFile(zip_buffer, "r") as zip_ref:
        # Find the HTML file for the requested sheet
        html_file = f"{sheet_name}.html"
        if html_file not in zip_ref.namelist():
            raise ValueError(f"Sheet '{sheet_name}' not found in zip file")

        # Extract the HTML content and CSS resources
        html_content = zip_ref.read(html_file).decode("utf-8")

        # Create a temporary directory for resources
        temp_dir = Path("temp_resources")
        temp_dir.mkdir(exist_ok=True)

    # Set up headless Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        # Create temporary HTML file with updated CSS paths
        temp_html = temp_dir / "temp.html"
        modified_html = html_content.replace("resources/", "temp_resources/")
        temp_html.write_text(modified_html)

        # Load the HTML file
        driver.get(f"file://{temp_html.absolute()}")

        # Wait for any dynamic content to load
        driver.implicitly_wait(2)

        # Get page dimensions
        width = driver.execute_script("return document.documentElement.scrollWidth")
        height = driver.execute_script("return document.documentElement.scrollHeight")
        driver.set_window_size(width + 200, height + 200)

        # Take screenshot
        screenshot = driver.get_screenshot_as_png()

        return screenshot

    finally:
        driver.quit()
        # Cleanup temporary files
        import shutil

        shutil.rmtree(temp_dir)
