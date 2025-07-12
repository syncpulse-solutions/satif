from copy import deepcopy
from dataclasses import replace
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    ToolCall,
    ToolMessage,
    convert_to_messages,
)
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import (
    get_config_list,
)
from langchain_core.tools import BaseTool
from langchain_core.tools import tool as create_tool
from langgraph.errors import GraphBubbleUp
from langgraph.prebuilt.tool_node import (
    _get_state_args,
    _get_store_arg,
    _handle_tool_error,
    _infer_handled_types,
    msg_content_output,
)
from langgraph.store.base import BaseStore
from langgraph.types import Command, Send
from langgraph.utils.runnable import RunnableCallable
from pydantic import BaseModel

INVALID_TOOL_NAME_ERROR_TEMPLATE = (
    "Error: {requested_tool} is not a valid tool, try one of [{available_tools}]."
)
TOOL_CALL_ERROR_TEMPLATE = "Error: {error}\n Please fix your mistakes."


class SequentialToolNode(RunnableCallable):
    """A node that runs the tools called in the last AIMessage sequentially.

    It can be used either in StateGraph with a "messages" state key (or a custom key passed via 'messages_key').
    If multiple tool calls are requested, they will be run sequentially in the order they appear
    in the tool_calls list. The output will be a list of ToolMessages, one for each tool call.

    Tool calls can also be passed directly as a list of `ToolCall` dicts.

    Args:
        tools: A sequence of tools that can be invoked by the ToolNode.
        name: The name of the ToolNode in the graph. Defaults to "tools".
        tags: Optional tags to associate with the node. Defaults to None.
        handle_tool_errors: How to handle tool errors raised by tools inside the node. Defaults to True.
            (See original ToolNode docstring for options)
        messages_key: The state key in the input that contains the list of messages.
            The same key will be used for the output from the ToolNode.
            Defaults to "messages".

    Important:
        - The input state can be one of the following:
            - A dict with a messages key containing a list of messages.
            - A list of messages.
            - A list of tool calls.
        - If operating on a message list, the last message must be an `AIMessage` with
            `tool_calls` populated.
    """

    name: str = "ToolNode"

    def __init__(
        self,
        tools: Sequence[Union[BaseTool, Callable]],
        *,
        name: str = "tools",
        tags: Optional[list[str]] = None,
        handle_tool_errors: Union[
            bool, str, Callable[..., str], tuple[type[Exception], ...]
        ] = True,
        messages_key: str = "messages",
    ) -> None:
        # Initialize RunnableCallable with modified _func and _afunc
        super().__init__(self._func, self._afunc, name=name, tags=tags, trace=False)
        self.tools_by_name: dict[str, BaseTool] = {}
        self.tool_to_state_args: dict[str, dict[str, Optional[str]]] = {}
        self.tool_to_store_arg: dict[str, Optional[str]] = {}
        self.handle_tool_errors = handle_tool_errors
        self.messages_key = messages_key
        for tool_ in tools:
            if not isinstance(tool_, BaseTool):
                # Assuming create_tool handles function wrapping
                tool_ = create_tool(tool_)
            self.tools_by_name[tool_.name] = tool_
            # Assuming helper functions exist
            self.tool_to_state_args[tool_.name] = _get_state_args(tool_)
            self.tool_to_store_arg[tool_.name] = _get_store_arg(tool_)

    def _func(
        self,
        input: Union[
            list[AnyMessage],
            dict[str, Any],
            BaseModel,
        ],
        config: RunnableConfig,
        *,
        store: Optional[BaseStore],
    ) -> Any:
        tool_calls, input_type = self._parse_input(input, store)
        config_list = get_config_list(config, len(tool_calls))
        outputs = []
        # Execute tools sequentially
        for i, call in enumerate(tool_calls):
            output = self._run_one(call, input_type, config_list[i])
            outputs.append(output)
            # Optional: Add logic here to stop if a tool fails,
            # or if a Command is returned that should halt execution.

        return self._combine_tool_outputs(outputs, input_type)

    async def _afunc(
        self,
        input: Union[
            list[AnyMessage],
            dict[str, Any],
            BaseModel,
        ],
        config: RunnableConfig,
        *,
        store: Optional[BaseStore],
    ) -> Any:
        tool_calls, input_type = self._parse_input(input, store)
        outputs = []
        # Execute tools sequentially
        for call in tool_calls:
            # Note: We might want to pass unique configs if needed, using get_config_list
            # For simplicity here, we pass the original config to each async call.
            output = await self._arun_one(call, input_type, config)
            outputs.append(output)
            # Optional: Add logic here to stop if a tool fails,
            # or if a Command is returned that should halt execution.

        return self._combine_tool_outputs(outputs, input_type)

    # _combine_tool_outputs remains the same as in the original ToolNode
    def _combine_tool_outputs(
        self,
        outputs: list[Union[ToolMessage, Command]],  # Adjusted type hint slightly
        input_type: Literal["list", "dict", "tool_calls"],
    ) -> list[Union[Command, list[ToolMessage], dict[str, list[ToolMessage]]]]:
        # preserve existing behavior for non-command tool outputs for backwards
        # compatibility
        if not any(isinstance(output, Command) for output in outputs):
            # TypedDict, pydantic, dataclass, etc. should all be able to load from dict
            return outputs if input_type == "list" else {self.messages_key: outputs}

        # LangGraph will automatically handle list of Command and non-command node
        # updates
        combined_outputs: list[
            Command | list[ToolMessage] | dict[str, list[ToolMessage]]
        ] = []

        # combine all parent commands with goto into a single parent command
        parent_command: Optional[Command] = None
        for output in outputs:
            if isinstance(output, Command):
                if (
                    output.graph is Command.PARENT
                    and isinstance(output.goto, list)
                    and all(isinstance(send, Send) for send in output.goto)
                ):
                    if parent_command:
                        parent_command = replace(
                            parent_command,
                            goto=cast(list[Send], parent_command.goto) + output.goto,
                        )
                    else:
                        parent_command = Command(graph=Command.PARENT, goto=output.goto)
                else:
                    combined_outputs.append(output)
            elif isinstance(
                output, ToolMessage
            ):  # Ensure we only process ToolMessages here
                combined_outputs.append(
                    [output] if input_type == "list" else {self.messages_key: [output]}
                )
            # else: # Should not happen if _run_one/_arun_one returns ToolMessage or Command
            #    pass # Or raise error

        if parent_command:
            combined_outputs.append(parent_command)
        return combined_outputs

    # _run_one remains the same as in the original ToolNode
    def _run_one(
        self,
        call: ToolCall,
        input_type: Literal["list", "dict", "tool_calls"],
        config: RunnableConfig,
    ) -> Union[ToolMessage, Command]:  # Return type hint clarification
        if invalid_tool_message := self._validate_tool_call(call):
            return invalid_tool_message

        try:
            # Pass the full tool call dictionary, augmented with type, as input
            # This matches the original ToolNode behavior and supports InjectedToolCallId
            tool_input = {**call, "type": "tool_call"}
            # Check if state/store injection is needed (handled by _inject_state/_inject_store called within _parse_input via inject_tool_args)
            # The result of inject_tool_args is already in the 'call' variable received here.

            response = self.tools_by_name[call["name"]].invoke(tool_input, config)

        except GraphBubbleUp as e:
            raise e
        except Exception as e:
            if isinstance(self.handle_tool_errors, tuple):
                handled_types: tuple = self.handle_tool_errors
            elif callable(self.handle_tool_errors):
                handled_types = _infer_handled_types(self.handle_tool_errors)
            else:
                # default behavior is catching all exceptions
                handled_types = (Exception,)

            # Unhandled
            if not self.handle_tool_errors or not isinstance(e, handled_types):
                raise e
            # Handled
            else:
                content = _handle_tool_error(e, flag=self.handle_tool_errors)
            return ToolMessage(
                content=content,
                name=call["name"],
                tool_call_id=call["id"],
                # It's good practice to indicate the error status
                # status="error", # Add this if ToolMessage supports it or adjust content
            )

        # Process response: Check if it's a Command or needs wrapping in ToolMessage
        if isinstance(response, Command):
            # Assuming _validate_tool_command handles Command validation/modification
            return self._validate_tool_command(response, call, input_type)
        elif isinstance(response, ToolMessage):
            # Ensure content is correctly formatted string/list
            response.content = cast(
                Union[str, list], msg_content_output(response.content)
            )
            # Ensure tool_call_id is set if the tool didn't set it
            if not response.tool_call_id:
                response.tool_call_id = call["id"]
            if not response.name:
                response.name = call["name"]
            return response
        else:
            # Wrap other return types in a ToolMessage
            return ToolMessage(
                content=cast(Union[str, list], msg_content_output(response)),
                name=call["name"],
                tool_call_id=call["id"],
            )

    # _arun_one remains the same as in the original ToolNode
    async def _arun_one(
        self,
        call: ToolCall,
        input_type: Literal["list", "dict", "tool_calls"],
        config: RunnableConfig,
    ) -> Union[ToolMessage, Command]:  # Return type hint clarification
        if invalid_tool_message := self._validate_tool_call(call):
            return invalid_tool_message

        try:
            # Pass the full tool call dictionary, augmented with type, as input
            # This matches the original ToolNode behavior and supports InjectedToolCallId
            tool_input = {**call, "type": "tool_call"}
            # State/store injection handled by _parse_input calling inject_tool_args

            response = await self.tools_by_name[call["name"]].ainvoke(
                tool_input, config
            )

        except GraphBubbleUp as e:
            raise e
        except Exception as e:
            if isinstance(self.handle_tool_errors, tuple):
                handled_types: tuple = self.handle_tool_errors
            elif callable(self.handle_tool_errors):
                handled_types = _infer_handled_types(self.handle_tool_errors)
            else:
                # default behavior is catching all exceptions
                handled_types = (Exception,)

            # Unhandled
            if not self.handle_tool_errors or not isinstance(e, handled_types):
                raise e
            # Handled
            else:
                content = _handle_tool_error(e, flag=self.handle_tool_errors)

            return ToolMessage(
                content=content,
                name=call["name"],
                tool_call_id=call["id"],
                # status="error", # Add if supported/needed
            )

        # Process response: Check if it's a Command or needs wrapping in ToolMessage
        if isinstance(response, Command):
            # Assuming _validate_tool_command handles Command validation/modification
            return self._validate_tool_command(response, call, input_type)
        elif isinstance(response, ToolMessage):
            # Ensure content is correctly formatted string/list
            response.content = cast(
                Union[str, list], msg_content_output(response.content)
            )
            # Ensure tool_call_id is set if the tool didn't set it
            if not response.tool_call_id:
                response.tool_call_id = call["id"]
            if not response.name:
                response.name = call["name"]
            return response
        else:
            # Wrap other return types in a ToolMessage
            return ToolMessage(
                content=cast(Union[str, list], msg_content_output(response)),
                name=call["name"],
                tool_call_id=call["id"],
            )

    # _parse_input remains the same as in the original ToolNode
    def _parse_input(
        self,
        input: Union[
            list[AnyMessage],
            dict[str, Any],
            BaseModel,
        ],
        store: Optional[BaseStore],
    ) -> Tuple[list[ToolCall], Literal["list", "dict", "tool_calls"]]:
        tool_calls_list: list[ToolCall] = []
        input_type: Literal["list", "dict", "tool_calls"] = "dict"  # Default assumption

        if isinstance(input, list):
            # Check if the input is directly a list of tool calls (used via Send)
            # Heuristic: Check if the first item looks like a tool call dict
            if (
                input
                and isinstance(input[0], dict)
                and input[0].get("type") == "tool_call"
            ):
                input_type = "tool_calls"
                # Validate all items are tool calls? For now, assume they are if first is.
                tool_calls_list = input
                # Inject state/store if needed (important for Send usage)
                tool_calls_list = [
                    self.inject_tool_args(call, input, store)
                    for call in tool_calls_list  # Pass 'input' which is the list itself for injection context
                ]
                return tool_calls_list, input_type
            elif input:
                input_type = "list"
                message: AnyMessage = input[-1]
            else:
                raise ValueError("Input list is empty")

        elif isinstance(input, dict) and (messages := input.get(self.messages_key, [])):
            if not messages:
                raise ValueError(
                    f"No messages found in state key '{self.messages_key}'"
                )
            input_type = "dict"
            message = messages[-1]
        elif hasattr(input, self.messages_key) and (
            messages := getattr(input, self.messages_key, None)
        ):
            if not messages:
                raise ValueError(
                    f"No messages found in state attribute '{self.messages_key}'"
                )
            # Assume dataclass-like state that can coerce from dict
            input_type = "dict"
            message = messages[-1]
        else:
            raise ValueError(
                "Unsupported input type or no messages found in input state"
            )

        if not isinstance(message, AIMessage):
            # Allow ToolMessages containing tool calls? The original didn't seem to.
            raise ValueError(f"Last message is not an AIMessage, found {type(message)}")

        if not message.tool_calls:
            # Handle cases where the AI message doesn't actually have tool calls
            # This might happen if the edge condition leads here incorrectly.
            # Return empty list? Or raise? Returning empty seems safer.
            return [], input_type

        # Inject state/store into tool calls before returning
        tool_calls_list = [
            self.inject_tool_args(call, input, store) for call in message.tool_calls
        ]
        return tool_calls_list, input_type

    # _validate_tool_call remains the same
    def _validate_tool_call(self, call: ToolCall) -> Optional[ToolMessage]:
        if (requested_tool := call.get("name")) not in self.tools_by_name:
            # Ensure 'id' exists before creating error message
            tool_call_id = call.get("id", "unknown_id")
            content = INVALID_TOOL_NAME_ERROR_TEMPLATE.format(
                requested_tool=requested_tool or "None",
                available_tools=", ".join(self.tools_by_name.keys()),
            )
            return ToolMessage(
                content,
                name=requested_tool or "invalid_tool",
                tool_call_id=tool_call_id,  # , status="error"
            )
        else:
            return None

    # _inject_state remains the same
    def _inject_state(
        self,
        tool_call: ToolCall,
        input_obj: Union[  # Renamed 'input' to avoid shadowing built-in
            list[AnyMessage],
            dict[str, Any],
            BaseModel,
        ],
    ) -> ToolCall:
        # Ensure 'name' exists in tool_call
        tool_name = tool_call.get("name")
        if not tool_name or tool_name not in self.tool_to_state_args:
            # Should have been caught by _validate_tool_call, but double-check
            return tool_call

        state_args = self.tool_to_state_args[tool_name]
        if not state_args:  # No state injection needed for this tool
            return tool_call

        current_tool_args = tool_call.get("args", {})
        tool_state_args = {}

        # Determine the source of state (list, dict, or object)
        state_source = input_obj
        if state_args and isinstance(input_obj, list):
            # Special handling if input is list but tool expects state dict fields
            required_fields = [f for f in state_args.values() if f]
            # Allow if only 'messages' (or None indicating full state) is needed
            if len(required_fields) == 1 and required_fields[0] == self.messages_key:
                state_source = {self.messages_key: input_obj}
            elif any(
                f is None for f in state_args.values()
            ):  # Check if full state obj needed
                state_source = {
                    self.messages_key: input_obj
                }  # Pass list under messages key
            else:
                err_msg = (
                    f"Invalid input to ToolNode. Tool {tool_name} requires "
                    f"graph state dict as input, but received a list."
                )
                if required_fields:
                    required_fields_str = ", ".join(required_fields)
                    err_msg += f" State should contain fields {required_fields_str}."
                raise ValueError(err_msg)

        # Extract state values based on source type
        if isinstance(state_source, dict):
            tool_state_args = {
                tool_arg: state_source.get(state_field) if state_field else state_source
                for tool_arg, state_field in state_args.items()
            }
        elif hasattr(state_source, "__dict__") or isinstance(
            state_source, BaseModel
        ):  # Check if object-like
            tool_state_args = {
                tool_arg: getattr(state_source, state_field)
                if state_field
                else state_source
                for tool_arg, state_field in state_args.items()
            }
        # else: state_source is likely still the list, handled above? Or error?
        # If state_source remains a list here, it means only the full list was required.
        elif isinstance(state_source, list):
            tool_state_args = {
                tool_arg: state_source  # Inject the whole list if state_field was None
                for tool_arg, state_field in state_args.items()
                if state_field is None
            }

        # Update tool_call args, preserving existing ones
        tool_call["args"] = {**current_tool_args, **tool_state_args}
        return tool_call

    # _inject_store remains the same
    def _inject_store(
        self, tool_call: ToolCall, store: Optional[BaseStore]
    ) -> ToolCall:
        # Ensure 'name' exists in tool_call
        tool_name = tool_call.get("name")
        if not tool_name or tool_name not in self.tool_to_store_arg:
            return tool_call

        store_arg = self.tool_to_store_arg[tool_name]
        if not store_arg:
            return tool_call

        if store is None:
            raise ValueError(
                "Cannot inject store into tools with InjectedStore annotations - "
                "please compile your graph with a store."
            )

        current_tool_args = tool_call.get("args", {})
        tool_call["args"] = {
            **current_tool_args,
            store_arg: store,
        }
        return tool_call

    # inject_tool_args remains the same
    def inject_tool_args(
        self,
        tool_call: ToolCall,
        input_obj: Union[  # Renamed 'input'
            list[AnyMessage],
            dict[str, Any],
            BaseModel,
        ],
        store: Optional[BaseStore],
    ) -> ToolCall:
        """Injects the state and store into the tool call.

        (See original ToolNode docstring for full description)
        """
        # Ensure 'name' exists before proceeding
        if tool_call.get("name") not in self.tools_by_name:
            # Tool validation should handle this, but good to check.
            return tool_call

        # Use deepcopy to avoid modifying the original tool call dict in the message
        tool_call_copy: ToolCall = deepcopy(tool_call)
        # Ensure 'args' key exists, even if empty, before injection
        if "args" not in tool_call_copy:
            tool_call_copy["args"] = {}

        tool_call_with_state = self._inject_state(tool_call_copy, input_obj)
        tool_call_with_store = self._inject_store(tool_call_with_state, store)
        return tool_call_with_store

    # _validate_tool_command remains the same
    def _validate_tool_command(
        self,
        command: Command,
        call: ToolCall,
        input_type: Literal["list", "dict", "tool_calls"],
    ) -> Command:
        updated_command = command  # Start with original command

        # Determine if update contains messages and where to find them
        messages_update: Optional[Union[list, dict]] = None
        copied_command = False  # Flag to track if deepcopy happened
        if isinstance(command.update, dict):
            # Check input type validity
            if input_type not in ("dict", "tool_calls"):
                raise ValueError(
                    f"Tools can provide a dict in Command.update only when using dict with '{self.messages_key}' key as ToolNode input or direct tool_calls list, "
                    f"got: {command.update} for tool '{call['name']}'"
                )
            # Make a deep copy ONLY if we need to modify it
            updated_command = deepcopy(command)
            copied_command = True
            messages_update = updated_command.update.get(self.messages_key)

        elif isinstance(command.update, list):
            # Check input type validity
            if input_type != "list":
                raise ValueError(
                    f"Tools can provide a list of messages in Command.update only when using list of messages as ToolNode input, "
                    f"got: {command.update} for tool '{call['name']}'"
                )
            updated_command = deepcopy(command)
            copied_command = True
            messages_update = updated_command.update
        else:
            # No message updates to validate or process, return original command
            return command

        # If no messages in the update, nothing to validate regarding ToolMessages
        # But return the deepcopied version if copy happened
        if not messages_update:
            return updated_command

        # Convert dict format messages to message objects if necessary
        # Operate on the messages list within the potentially deepcopied command
        if isinstance(updated_command.update, dict):
            messages_list = updated_command.update[self.messages_key]
        else:  # isinstance(updated_command.update, list)
            messages_list = updated_command.update

        messages_list = list(convert_to_messages(messages_list))

        has_matching_tool_message = False
        # modified_messages = False # No longer needed
        for i, message in enumerate(messages_list):
            if not isinstance(message, ToolMessage):
                continue

            if message.tool_call_id == call.get("id"):
                # Ensure name is set on the matching ToolMessage using direct mutation
                if message.name != call.get("name"):
                    # Directly mutate the message object within the list
                    message.name = call.get("name")
                    # modified_messages = True # No longer needed
                has_matching_tool_message = True
                # Optional: break if we only expect one match?

        # If we potentially modified the messages list (via convert_to_messages or direct mutation),
        # ensure the updated list is assigned back (necessary if convert_to_messages created new list)
        if copied_command:  # Ensure we update the copied command
            if isinstance(updated_command.update, dict):
                updated_command.update[self.messages_key] = messages_list
            elif isinstance(updated_command.update, list):
                updated_command.update = messages_list

        # Validate that a matching ToolMessage exists if command targets current graph
        if updated_command.graph is None and not has_matching_tool_message:
            tool_call_id = call.get("id", "unknown_id")
            example_update = (
                f'`Command(update={{{self.messages_key}: [ToolMessage("Success", tool_call_id="{tool_call_id}"), ...], ...}})`'
                if input_type in ("dict", "tool_calls")
                else f'`Command(update=[ToolMessage("Success", tool_call_id="{tool_call_id}"), ...], ...)`'
            )
            raise ValueError(
                f"Expected to have a matching ToolMessage in Command.update for tool '{call.get('name')}' (call_id: {tool_call_id}), got: {messages_update}. "
                "Every tool call (LLM requesting to call a tool) in the message history MUST have a corresponding ToolMessage when returning a Command targeting the current graph. "
                f"You can fix it by modifying the tool to return {example_update}."
            )

        return updated_command


__all__ = ["SequentialToolNode"]
