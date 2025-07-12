import io
import logging
from typing import Union  # For type hinting int | str

from aspose.cells import GridlineType, Workbook
from aspose.cells.drawing import ImageType
from aspose.cells.rendering import ImageOrPrintOptions, SheetRender
from PIL import Image  # Import Pillow

logger = logging.getLogger(__name__)


def get_sheet_index_by_name(workbook: Workbook, sheet_name: str) -> int | None:
    """Finds the zero-based index of a worksheet by its name (case-insensitive).

    Args:
        workbook: The Aspose.Cells Workbook object.
        sheet_name: The name of the worksheet to find.

    Returns:
        The zero-based index of the sheet if found, otherwise None.
    """
    for i, sheet in enumerate(workbook.worksheets):
        # Excel sheet names are typically case-insensitive in lookups
        if sheet.name.lower() == sheet_name.lower():
            return i  # Return the index
    return None  # Not found


def export_excel_area_to_png_bytes(
    workbook: Workbook,
    sheet: Union[int, str],  # Accept index or name
    export_range: str | None = None,
    show_gridlines: bool = True,
    show_headers: bool = True,
    gridline_style: GridlineType = GridlineType.HAIR,
    render_all_content_on_one_page: bool = True,  # Only applies if export_range is None
    crop_top_pixels: int = 0,
    crop_right_pixels: int = 0,
    crop_bottom_pixels: int = 0,
    crop_left_pixels: int = 0,
) -> bytes | None:
    """Exports a specified sheet or a range within it from a Workbook object
    to PNG image bytes, optionally including gridlines, headers, and cropping.

    Args:
        workbook: The Aspose.Cells Workbook object to export from.
        sheet: The zero-based index (int) or the name (str)
                          of the worksheet to export from.
        export_range: The specific range to export (e.g., "A1:F10").
                      If None, the entire sheet content (as determined by
                      render_all_content_on_one_page) will be exported.
        show_gridlines: Whether to include gridlines in the output image.
        show_headers: Whether to include row and column headers (A,B,C..., 1,2,3...)
                      in the output image.
        gridline_style: The style of the gridlines if shown (e.g., GridlineType.HAIR).
        render_all_content_on_one_page: If export_range is None, setting this to True
                                       will render the entire sheet content onto a
                                       single image page, ignoring print page breaks.
                                       If False, only the first print page is rendered.
                                       This parameter is ignored if export_range is specified.
        crop_top_pixels: Number of pixels to crop from the top.
        crop_right_pixels: Number of pixels to crop from the right.
        crop_bottom_pixels: Number of pixels to crop from the bottom.
        crop_left_pixels: Number of pixels to crop from the left.

    Returns:
        A bytes object containing the potentially cropped PNG image data,
        or None if an error occurred.
    """
    try:
        worksheet = None
        sheet_name_for_log = ""  # For logging purposes

        # --- Determine Worksheet based on identifier type ---
        if isinstance(sheet, int):
            sheet_index = sheet
            if sheet_index < 0 or sheet_index >= len(workbook.worksheets):
                logger.error(f"Error: Sheet index {sheet_index} is out of bounds.")
                return None
            worksheet = workbook.worksheets[sheet_index]
            sheet_name_for_log = worksheet.name  # Get name for logging
        elif isinstance(sheet, str):
            sheet_name = sheet
            sheet_index = get_sheet_index_by_name(workbook, sheet_name)
            if sheet_index is None:  # Check if lookup failed
                logger.error(f"Error: Worksheet with name '{sheet_name}' not found.")
                return None
            worksheet = workbook.worksheets[sheet_index]
            sheet_name_for_log = sheet_name
        else:
            logger.error(
                f"Error: Invalid sheet type. Expected int or str, got {type(sheet)}."
            )
            return None

        # --- Configure Page Setup for Rendering ---
        page_setup = worksheet.page_setup
        page_setup.print_gridlines = show_gridlines
        page_setup.print_headings = show_headers

        # --- Configure Rendering Options ---
        options = ImageOrPrintOptions()
        options.image_type = ImageType.PNG
        options.gridline_type = gridline_style

        # --- Handle Range vs. Full Sheet Export ---
        if export_range:
            logger.info(
                f"Configuring print area to '{export_range}' for sheet '{sheet_name_for_log}'..."
            )
            # Set the print area to the specified range
            page_setup.print_area = export_range
            # When exporting a specific range via print area,
            # usually you want it rendered as one image.
            options.one_page_per_sheet = True
        else:
            logger.info(f"Exporting entire sheet '{sheet_name_for_log}'...")
            # Ensure no specific print area is set if exporting the whole sheet
            page_setup.print_area = None  # Clear any existing print area
            options.one_page_per_sheet = render_all_content_on_one_page

        # --- Render the Worksheet to Memory Stream ---
        sheet_render = SheetRender(worksheet, options)
        memory_stream = io.BytesIO()

        # Render the relevant page (page 0 covers the range or the single combined page)
        if sheet_render.page_count > 0:
            sheet_render.to_image(0, memory_stream)
            logger.info(
                f"Successfully generated PNG bytes for sheet '{sheet_name_for_log}'"
                f"{f' range {export_range}' if export_range else ''}."
            )
        else:
            logger.info(
                f"Warning: No content found to render for sheet '{sheet_name_for_log}' with the given options."
            )
            memory_stream.close()
            return None  # Return None if nothing was rendered

        # Get the bytes from the stream
        image_bytes = memory_stream.getvalue()
        memory_stream.close()  # Good practice to close the stream

        # --- Apply Cropping if needed ---
        should_crop = (
            crop_top_pixels > 0
            or crop_right_pixels > 0
            or crop_bottom_pixels > 0
            or crop_left_pixels > 0
        )

        if should_crop and image_bytes:
            try:
                img = Image.open(io.BytesIO(image_bytes))
                width, height = img.size

                # Calculate crop box coordinates
                left = max(0, crop_left_pixels)
                top = max(0, crop_top_pixels)
                right = max(left, width - crop_right_pixels)  # Ensure right >= left
                bottom = max(top, height - crop_bottom_pixels)  # Ensure bottom >= top

                # Validate crop box dimensions
                if left >= right or top >= bottom:
                    logger.warning(
                        f"Warning: Invalid crop dimensions for sheet '{sheet_name_for_log}'. "
                        f"Calculated box ({left},{top},{right},{bottom}) from original "
                        f"size ({width}x{height}) with crops ({crop_top_pixels}, "
                        f"{crop_right_pixels}, {crop_bottom_pixels}, {crop_left_pixels}). "
                        "Returning uncropped image."
                    )
                    # Optionally, could return None or raise an error here
                else:
                    logger.info(
                        f"Cropping image for sheet '{sheet_name_for_log}' from {width}x{height} "
                        f"to box ({left},{top},{right},{bottom})."
                    )
                    cropped_img = img.crop((left, top, right, bottom))

                    # Save cropped image back to bytes
                    cropped_stream = io.BytesIO()
                    cropped_img.save(cropped_stream, format="PNG")
                    image_bytes = cropped_stream.getvalue()
                    cropped_stream.close()
                    cropped_img.close()  # Close the cropped image object

                img.close()  # Close the original image object
            except Exception as crop_error:
                logger.error(
                    f"Error during image cropping for sheet '{sheet_name_for_log}': {crop_error}"
                )
                # Decide how to handle crop errors: return original bytes, None, or re-raise
                # Returning original bytes for now.
                pass  # Keep original image_bytes

        return image_bytes

    except Exception as e:
        logger.error(f"An error occurred during export: {e}")
        import traceback

        traceback.print_exc()
        return None
