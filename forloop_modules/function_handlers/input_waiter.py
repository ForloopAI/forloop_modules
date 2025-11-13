# input_waiter.py
from forloop_modules.globals.variable_handler import LocalVariable, variable_handler
from forloop_modules.queries.node_context_requests_backend import post_console_log
import forloop_modules.flog as flog

_pending: dict[str, str] = {}
_prompts: dict[str, str] = {} 

def register_pending_input(node_uid: str, output_variable: str, prompt_text: str = None):
    _pending[node_uid] = output_variable
    if prompt_text:
        _prompts[node_uid] = prompt_text

def resolve_input(node_uid: str, value: str):
    var_name = _pending.pop(node_uid, None)
    prompt_text = _prompts.pop(node_uid, None) 

    if var_name is not None:
        # Use the proper variable handler method for both new and existing variables
        variable_handler.new_variable(var_name, value)

        try:
            msg = f"{prompt_text} = {value}" if prompt_text else f"{var_name} = {value}"
            post_console_log(message=msg, type="input")
        except Exception as e:
            flog.debug(f"Failed to post console log: {e}")