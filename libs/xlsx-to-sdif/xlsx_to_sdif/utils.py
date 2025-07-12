import base64
import io
import json
import re
from typing import Any, Dict, List, Union, cast

from langchain_core.messages import AIMessage, AnyMessage
from PIL import Image
from pydantic import BaseModel


def resize_base64_image(base64_string, size=(128, 128)):
    """Resize an image encoded as a Base64 string.

    :param base64_string: A Base64 encoded string of the image to be resized.
    :param size: A tuple representing the new size (width, height) for the image.
    :return: A Base64 encoded string of the resized image.
    """
    img_data = base64.b64decode(base64_string)
    img = Image.open(io.BytesIO(img_data))
    resized_img = img.resize(size, Image.LANCZOS)
    buffered = io.BytesIO()
    resized_img.save(buffered, format=img.format)
    return base64.b64encode(buffered.getvalue()).decode("utf-8")


def image_to_base64(image_bytes: bytes) -> str:
    """Convert image bytes to base64 encoded string.

    Args:
        image_bytes (bytes): The image file contents as bytes

    Returns:
        str: Base64 encoded string representation of the image
    """
    return base64.b64encode(image_bytes).decode("utf-8")


# Define possible state types based on LangGraph examples
StateType = Union[List[AnyMessage], Dict[str, Any], BaseModel]


class ParseError(ValueError):
    """Custom exception for parsing errors."""

    pass


def parse_json_from_last_message(
    state: StateType,
    messages_key: str = "messages",
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """Parses a JSON object embedded in triple backticks (```json ... ```)
    from the content of the last AI message in the state.

    Args:
        state: The state object, which can be a list of messages, a dictionary
               containing a list of messages under `messages_key`, or a Pydantic
               model with a messages attribute.
        messages_key: The key to access the list of messages if state is a dictionary.

    Returns:
        The parsed JSON object (typically a list of table dictionaries or a single dictionary).

    Raises:
        ParseError: If messages are not found, the last message is not an AI message,
                      no JSON block is found, or the JSON is invalid.
    """  # noqa: D205
    last_message: AnyMessage | None = None

    # 1. Access the last message based on state type
    if isinstance(state, list):
        if not state:
            raise ParseError("State is an empty list, cannot find messages.")
        last_message = state[-1]
    elif isinstance(state, dict):
        messages = state.get(messages_key)
        if not messages or not isinstance(messages, list):
            raise ParseError(
                f"Messages not found or not a list under key '{messages_key}' in state dict."
            )
        if not messages:
            raise ParseError(f"Messages list under key '{messages_key}' is empty.")
        last_message = messages[-1]
    elif isinstance(state, BaseModel) and hasattr(state, messages_key):
        messages = getattr(state, messages_key)
        if not messages or not isinstance(messages, list):
            raise ParseError(
                f"Messages attribute '{messages_key}' not found or not a list in state model."
            )
        if not messages:
            raise ParseError(
                f"Messages list under attribute '{messages_key}' is empty."
            )
        last_message = messages[-1]
    else:
        raise ParseError(f"Unsupported state type or missing messages: {type(state)}")

    # 2. Ensure it's an AI message with content
    if not isinstance(last_message, AIMessage):
        raise ParseError(f"Last message is not an AIMessage: {type(last_message)}")

    content = last_message.content
    if not isinstance(content, str):
        raise ParseError(f"Last message content is not a string: {type(content)}")

    # 3. Find the JSON block using regex
    # Matches ```json ... ``` or ``` ... ``` allowing for optional whitespace
    match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", content, re.DOTALL)

    json_string = None
    if match:
        json_string = match.group(1).strip()
    else:
        # If no backticks found, try parsing the entire content as JSON
        # Strip whitespace just in case
        potential_json = content.strip()
        if potential_json.startswith(("{", "[")) and potential_json.endswith(
            ("}", "]")
        ):
            json_string = potential_json

    if json_string is None:
        raise ParseError(
            "No JSON block found in backticks, and the message content does not appear to be raw JSON."
        )

    # 4. Parse the JSON string
    try:
        parsed_json = json.loads(json_string)
        # Basic validation based on expected structure (list of dicts or a single dict)
        if not isinstance(parsed_json, (list, dict)):
            raise ParseError(f"Parsed JSON is not a list or dict: {type(parsed_json)}")
        if isinstance(parsed_json, list) and not all(
            isinstance(item, dict) for item in parsed_json
        ):
            raise ParseError(
                "Parsed JSON is a list, but not all items are dictionaries."
            )

        # Cast the typehint for the return value
        return cast(Union[List[Dict[str, Any]], Dict[str, Any]], parsed_json)
    except json.JSONDecodeError as e:
        raise ParseError(
            f"Invalid JSON encountered: {e}\nJSON string was: {json_string}"
        )
    except Exception as e:
        # Catch unexpected errors during parsing/validation
        raise ParseError(f"An unexpected error occurred during JSON processing: {e}")
