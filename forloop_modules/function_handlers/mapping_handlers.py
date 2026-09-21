import inspect
import ast
import re
import types
import threading
import sys
import hashlib

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.globals.variable_handler import variable_handler, defined_functions_dict, custom_icons_imports
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.errors.errors import SoftPipelineError
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler
import forloop_modules.function_handlers.auxilliary.forloop_code_eval as fce

####### PROBLEMATIC IMPORTS TODO: REFACTOR #######
#from src.gui.gui_layout_context import glc
#from src.gui.item_detail_form import ItemDetailForm #independent on GLC - but is Frontend -> Should separate to two classes
####### PROBLEMATIC IMPORTS TODO: REFACTOR #######

# Auxiliary function(s)
def get_function_name_from_code(code):
    codelines = code.split('\n')

    func_name = None
    for line in codelines:
        if "def" in line:
            func_name = line.split()[1] # def ... [0], func_name(args) ... [1]
            break

    if func_name and "(" in func_name:
        parenthesis_index = func_name.index("(")
        func_name = func_name[:parenthesis_index]

    return func_name

def find_imports_in_custom_code(code: str) -> list:
    imports = []

    for element in custom_icons_imports:
        if element["alias"] is not None:
            if element["alias"] in code:
                imports.append(f'import {element["library"]} as {element["alias"]}')
        elif element["parts"] is not None:
            parts = element["parts"].split(",")
            parts_to_import = ""

            for part in parts:
                part = part.strip()
                if part in code:
                    parts_to_import += f"{part}, "

            imports.append(f'from {element["library"]} import {parts_to_import}')
        else:
            if element["library"] in code:
                imports.append(f'import {element["library"]}')

    return imports


def find_variables_used_in_custom_code(code: str) -> str:
    variables_code = ""

    for variable in variable_handler.variables.values():
        if not inspect.isfunction(variable.value):
            if variable.name in code:
                if type(variable.value) == str:
                    variables_code += f'{variable.name} = "{variable.value}"\n'
                else:
                    variables_code += f'{variable.name} = {variable.value}\n'

    return variables_code

def define_function_variable(code, function_name, imports=[]):

    if function_name:
        d = {}
        fce.exec_code(code, globals(), d)
        func = d[function_name]
        defined_functions_dict[function_name] = {"function": func, "imports": imports, "code": code}

        address = hex(id(func))        
        # Try to create variable in variable explorer, but don't fail if server is not available
        try:
            variable_handler.new_variable(f"{function_name}_address", address)
        except Exception as e:
            # If server is not available, just log a warning and continue
            flog.warning(f'Could not create variable in variable explorer: {e}')
            flog.warning(f'Function "{function_name}" defined in memory only.')

        #variable_handler.update_data_in_variable_explorer(glc)
        flog.warning(f'New function defined: "{function_name}"')
    else:
        flog.error("Function name can't be of type None.")


def _coerce_literal(v):
    """Coerce a string to a Python literal when safe; otherwise return as-is."""
    if isinstance(v, str):
        try:
            return ast.literal_eval(v)
        except Exception:
            return v
    return v

def _get_meta_for_function(func_name: str):
    """
    Return the stored meta for func_name. If not in defined_functions_dict,
    try to get code from variable_handler additional_params. Return (imports, code).
    """
    meta = defined_functions_dict.get(func_name)
    if meta and "code" in meta:
        imports = list(set(meta.get("imports", [])))
        code = meta["code"]
        return imports, code

    # Fallback: look in variable_handler additional_params (DefineFunctionHandler stores 'code')
    var = getattr(variable_handler, "variables", {}).get(func_name)
    if var and isinstance(getattr(var, "additional_params", None), dict):
        code = var.additional_params.get("code")
        if code:
            return [], code

    raise SoftPipelineError(
        f"No stored code found for function '{func_name}'. "
        "Make sure it was created via DefineFunctionHandler so its code is available."
    )

def push_pipeline_global(name, value):
    """Mirror a variable update into the pipeline exec env when in pipeline mode."""
    try:
        rf = mapping_handlers_dict['RunFunction']
        if getattr(rf, "_env_mode", None) == "pipeline" and rf._pipeline_env:
            rf._pipeline_env.env[name] = value
    except Exception as e:
        flog.warning(f"push_pipeline_global failed for {name}: {e}")

###

class _ExecEnvManager:
    """
    Shared module-like environment for exec() so functions see globals via LEGB.
    - Persists across calls (like a real module).
    - Thread-safe exec.
    - Lets you sync in globals from variable_handler and/or any dict (e.g., globals()).
    - Caches code by hash to avoid redefining when source didn't change.
    """
    def __init__(self, name: str = "_forloop_runtime"):
        self._lock = threading.RLock()
        self._module = types.ModuleType(name)
        self.env = self._module.__dict__

        # make it module-like
        self.env["__builtins__"] = __builtins__
        self.env["__name__"] = name
        self.env["__package__"] = None
        sys.modules[name] = self._module

        # func_name -> sha256(total_code)
        self._code_hash: dict[str, str] = {}

    def _should_copy(self, name: str, val) -> bool:
        if not name or name.startswith("_"):
            return False
        if inspect.isfunction(val) or inspect.ismethod(val):
            return False
        if isinstance(val, types.ModuleType):
            return False
        if isinstance(val, type):
            return False
        return True

    def sync_from_variable_handler(self, overwrite: bool = False):
        try:
            for v in variable_handler.variables.values():
                name, val = v.name, v.value
                if val is None:
                    continue
                if not self._should_copy(name, val):
                    continue
                if overwrite or name not in self.env:
                    self.env[name] = val
        except Exception as e:
            flog.warning(f"[ExecEnv] variable_handler sync failed: {e}")

    def sync_from_dict(self, d: dict, overwrite: bool = False):
        for name, val in d.items():
            if not self._should_copy(name, val):
                continue
            if overwrite or name not in self.env:
                self.env[name] = val

    def define_or_reuse(self, func_name: str, total_code: str):
        """Exec total_code if it's new/changed; otherwise reuse existing def."""
        h = hashlib.sha256(total_code.encode("utf-8")).hexdigest()
        with self._lock:
            if self._code_hash.get(func_name) != h or func_name not in self.env:
                exec(total_code, self.env, self.env)
                self._code_hash[func_name] = h
        return self.env.get(func_name)



class ApplyMappingHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'ApplyMapping'
        self.fn_name = 'Apply Mapping'

        self.type_category = ntcm.categories.mapping
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options=None, node_detail_form=None):
        if options is None:
            options = {"columns": []}
        column_options = options["columns"]
        default = ""
        if column_options:
            column_options.insert(0, "All columns")
            default = column_options[0]
        axis_options = ["index", "columns"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Mapping pattern")
        fdl.entry(name="mapping_pattern", text="", category="arguments", input_types=["function", "dict"], required=True, row=2)
        fdl.label("Column(s)")
        fdl.comboentry(name="column_names", text=default, options=column_options, row=3)
        fdl.label("Axis")
        fdl.comboentry(name="axis", text="columns", options=axis_options, row=4)
        fdl.label("New column")
        fdl.entry(name="new_col_name", text="", category="arguments", input_types=["str"], required=True, row=5)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=6)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, df_entry, mapping_pattern, column_names, axis, new_var_name, new_col_name):

        if column_names == "All columns":
            if new_col_name != "" and not new_col_name.isspace():
                df_entry[new_col_name] = df_entry.apply(mapping_pattern, axis=axis)
                transformed_df = df_entry
            else:
                transformed_df = df_entry.apply(mapping_pattern, axis=axis)
        else:
            if new_col_name != "" and not new_col_name.isspace():
                df_entry[new_col_name] = df_entry[column_names[0]].map(mapping_pattern)
                transformed_df = df_entry
            else:
                df_entry[column_names[0]].map(mapping_pattern)
                transformed_df = df_entry

        flog.info("NEW DF ", transformed_df)
        variable_handler.new_variable(new_var_name, transformed_df)
        #variable_handler.update_data_in_variable_explorer(glc)

    def execute_with_params(self, params):

        df_entry = params["df_entry"]
        mapping_pattern = params["mapping_pattern"]
        column_names = params["column_names"]
        axis = params["axis"]
        new_var_name = params["new_var_name"]
        new_col_name = params["new_col_name"]
        flog.info("DF = ", df_entry)

        self.direct_execute(df_entry, mapping_pattern, column_names, axis, new_var_name, new_col_name)

    def execute(self, node_detail_form):
        """
        Execution of the apply mapping transformation
        """

        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        mapping_pattern = node_detail_form.get_chosen_value_by_name("mapping_pattern", variable_handler)
        column_names = node_detail_form.get_chosen_value_by_name("column_names", variable_handler)
        axis = node_detail_form.get_chosen_value_by_name("axis", variable_handler)
        new_col_name = node_detail_form.get_chosen_value_by_name("new_col_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        
        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, mapping_pattern, column_names, axis, new_var_name, new_col_name)

        # needs to be after update_node_fields_with_shown_dataframe and after variable handler update (direct execute)
        # variable_handler.last_active_dataframe_node_uid = item_detail_form.node_detail_form.node_uid
        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def export_code(self, node_detail_form):

        df_entry = node_detail_form.get_variable_name_or_input_value_by_element_name("df_entry")
        mapping_pattern = node_detail_form.get_variable_name_or_input_value_by_element_name("mapping_pattern")
        column_names = node_detail_form.get_variable_name_or_input_value_by_element_name("column_names")
        axis = node_detail_form.get_variable_name_or_input_value_by_element_name("axis")
        new_col_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_col_name")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        if column_names == "All columns":
            if new_col_name != "" and not new_col_name.isspace():
                code = f"{new_var_name} = {df_entry}.apply({mapping_pattern.__name__}, axis = {axis})"
            else:
                code = f"{df_entry}.apply({mapping_pattern.__name__}, axis = {axis})"
        else:
            if new_col_name != "" and not new_col_name.isspace():
                if inspect.isfunction(mapping_pattern):
                    code = f"{new_var_name} = {df_entry}[{column_names}].map({mapping_pattern.__name__})"
                elif type(mapping_pattern) == dict:
                    code = f"{new_var_name} = {df_entry}[{column_names}].map({mapping_pattern})"
            else:
                if inspect.isfunction(mapping_pattern):
                    code = f"{df_entry}.map({mapping_pattern.__name__})"
                elif type(mapping_pattern) == dict:
                    code = f"{df_entry}.map({mapping_pattern})"
                else:
                    code = "# APPLY MAPPING COULD NOT EXPORT CODE, NO INPUT"

        return code

    def export_imports(self, node_detail_form):
        imports = []

        return imports


class DefineFunctionHandler(AbstractFunctionHandler):

    def __init__(self):
        self.is_disabled = False  # Enable for pipeline mode support
        self.icon_type = 'DefineFunction'
        self.fn_name = 'Define Function'

        self.type_category = ntcm.categories.mapping
        self.docs_category = DocsCategories.control
        
        # Pipeline mode support
        self._pipeline_env: _ExecEnvManager | None = None

    def make_form_dict_list(self, *args, node_detail_form=None):
        # TODO: show name, args, arg types if any, return

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Code")
        fdl.entry(name="code_label", text="", category="arguments", input_types=["str"], required=True, row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)
        fdl.button(function=self.show_help, function_args=node_detail_form, text="Help", name="help")

        return fdl

    def show_help(self, node_detail_form):
        label_lines = [
            "Define function icon creates",
            "a new variable containing the function."
            "",
            "There are two ways to define a function:",
            "Classic function definition:",
            "def new_func(*args):",
            "    print('Hello world!')",
            "",
            "Lambda function definition:",
            "new_func = lambda x: x**2"
        ]

        new_caption = 'DefineFunction - Help'

        params_dict = {
            'label_lines': {'variable': None, 'value': label_lines},
            'new_caption': {'variable': None, 'value': new_caption}
        }

        ncrb.new_popup([500, 400], 'TextOnlyPopup', params_dict)

    def execute(self, node_detail_form):
        code_label = node_detail_form.get_chosen_value_by_name('code_label', variable_handler)

        self.direct_execute(code_label)

    def execute_with_params(self, params):
        code_label = params["code_label"]

        self.direct_execute(code_label)

    def make_flpl_node_dict(self, line_dict: dict) -> dict:
        """
        Creates a DefineFunction node dict from parsed code line_dict.
        
        For function definition like 'def square(x): return x * x', line_dict contains:
        - arguments: ["def square(x):\n    return x * x\n"]
        - function: None
        - new_var: None
        """
        node = {"type": self.icon_type, "params": {}}
        
        # Extract function code from arguments
        args = line_dict.get("arguments") or []
        code_label = args[0] if len(args) > 0 else ""
        
        # Set the node parameters in the expected format
        node["params"]["code_label"] = {"variable": None, "value": code_label}
        
        return node

    def use_pipeline_env(self, env: _ExecEnvManager):
        """Set the pipeline environment for function definition."""
        self._pipeline_env = env

    def direct_execute(self, code_label):

        func_name = get_function_name_from_code(code_label)
        
        try:
            init_line = code_label[:code_label.index('):') + 2].strip()
            code = code_label[code_label.index('):') + 2:]
        except Exception as e:
            flog.error(e)
            flog.error("Wrongly defined function.")
            return None

        imports = find_imports_in_custom_code(code)

        indented_imports = "\n".join("    " + line for line in imports)
        code = f"{init_line}\n{indented_imports}\n\n{code}"

        # Always define function in global scope for variable explorer
        define_function_variable(code, func_name, imports)
        
        # If pipeline environment is available, also define function there
        if self._pipeline_env:
            # Define function in pipeline environment
            self._pipeline_env.define_or_reuse(func_name, code)
            flog.info(f"[DefineFunction] Function '{func_name}' defined in pipeline environment.")

    def export_code(self, node_detail_form):

        code_label = node_detail_form.get_variable_name_or_input_value_by_element_name("code_label")

        variables_code = find_variables_used_in_custom_code(code_label)
        code = variables_code + "\n" + code_label
        return code

    def export_imports(self, node_detail_form):
        code_label = node_detail_form.get_chosen_value_by_name("code_label", variable_handler)

        imports = find_imports_in_custom_code(code_label)

        return imports


class DefineLambdaFunctionHandler(AbstractFunctionHandler):

    def __init__(self):
        self.is_disabled = True # FIXME: Needs refactor - check functionality, solve how to store and run functions
        self.icon_type = 'DefineLambdaFunction'
        self.fn_name = 'Define Lambda Function'

        self.type_category = ntcm.categories.mapping
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        # TODO: show name, args, arg types if any, return

        fdl = FormDictList()
        fdl.label("Define Anonymous (Lambda) Function")
        fdl.label("Name:")
        fdl.entry(name="func_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("Arguments:")
        fdl.entry(name="func_args", text="", input_types=["str"], required=True, row=2)
        fdl.label("Return:")
        fdl.entry(name="func_return", text="", input_types=["str"], required=True, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        func_name = node_detail_form.get_chosen_value_by_name('func_name', variable_handler)
        func_args = node_detail_form.get_chosen_value_by_name('func_args', variable_handler)
        func_return = node_detail_form.get_chosen_value_by_name('func_return', variable_handler)

        self.direct_execute(func_name, func_args, func_return)

    def execute_with_params(self, params):
        func_name = params["func_name"]
        func_args = params["func_args"]
        func_return = params["func_return"]

        self.direct_execute(func_name, func_args, func_return)

    def direct_execute(self, func_name, func_args, func_return):
        
        try:
            code = f'{func_name} = lambda {func_args}: {func_return}'
            code += "\n" + f'{func_name}.__name__ = "{func_name}"' #assign the name to lambda object
            define_function_variable(code, func_name)
        except Exception as e:
            flog.error(f"{e}")

    def export_code(self, node_detail_form):

        func_name = node_detail_form.get_variable_name_or_input_value_by_element_name("func_name")
        func_args = node_detail_form.get_variable_name_or_input_value_by_element_name("func_args")
        func_return = node_detail_form.get_variable_name_or_input_value_by_element_name("func_return")

        code = f'{func_name} = lambda {func_args}: {func_return}'

        return code

    def export_imports(self, node_detail_form):
        
        imports = []

        return imports


class RunFunctionHandler(AbstractFunctionHandler):

    def __init__(self):
        self.is_disabled = False
        self.icon_type = 'RunFunction'
        self.fn_name = 'Run Function'

        self.type_category = ntcm.categories.mapping
        self.docs_category = DocsCategories.control

        # env management
        self._exec_env = _ExecEnvManager()
        self._env_mode = "fresh"             # "fresh" (click-run) | "pipeline" (RunJob)
        self._pipeline_env: _ExecEnvManager | None = None

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Function:")
        fdl.entry(name="selected_function", text="", category="function", input_types=["function"], required=True, row=1)
        fdl.label("Return variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def make_flpl_node_dict(self, line_dict: dict) -> dict:
        """
        Creates a RunFunction node dict from parsed code line_dict.
        
        For assignment like 'square_res = square(5)', line_dict contains:
        - new_var: "square_res" 
        - function: "square(5)" (reconstructed call string)
        - arguments: ["5"]
        - instance_var: None
        """
        node = {"type": self.icon_type, "params": {}}
        
        # Extract function call string from function field
        function_call = line_dict.get("function") or ""
        
        # Extract new variable name from new_var field
        new_var_name = line_dict.get("new_var") or ""
        
        # Set the node parameters in the expected format
        node["params"]["selected_function"] = {"variable": None, "value": function_call}
        node["params"]["new_var_name"] = {"variable": None, "value": new_var_name}
        
        return node

    # Switch to fresh-per-call behaviour (click-run)
    def use_fresh_env(self):
        self._env_mode = "fresh"
        self._pipeline_env = None

    # Switch to persistent env for the whole job (RunJob)
    def use_pipeline_env(self, env: _ExecEnvManager):
        self._env_mode = "pipeline"
        self._pipeline_env = env

    def _parse_function_call_string(self, s):
        """
        Accepts: "square(5)", "square(5, y=10)", "square", "  square (  x )  "
        Returns: (name, pos_args_as_strings, kwargs_as_strings_dict), or (None, None, None) on failure.
        """
        s = s.strip()
        try:
            node = ast.parse(s, mode='eval').body
        except Exception as e:
            flog.error(f"[RunFunction] _parse_function_call_string parse error for {s!r}: {e}")
            return None, None, None

        # example: square(5, y=10)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id
            pos = [ast.unparse(a) for a in node.args]
            kwargs = {kw.arg: ast.unparse(kw.value) for kw in node.keywords if kw.arg}
            return name, pos, kwargs

        # plain name: square
        if isinstance(node, ast.Name):
            return node.id, [], {}

        flog.warning(f"[RunFunction] _parse_function_call_string unsupported AST: {ast.dump(node)}")
        return None, None, None

    def _resolve_selected_function(self, selected_function_raw):
        # Log selection type at info level without dumping full content

        if callable(selected_function_raw):
            return selected_function_raw, [], {}

        if isinstance(selected_function_raw, str):
            name, inline_pos, inline_kwargs = self._parse_function_call_string(selected_function_raw)
            if not name:
                msg = f'Could not parse function selector: {selected_function_raw!r}'
                flog.error(msg)
                raise SoftPipelineError(msg)

            # Try variable_handler
            try:
                var_obj = getattr(variable_handler, "variables", {}).get(name)
            except Exception:
                var_obj = None

            if var_obj is not None:
                fn = getattr(var_obj, "value", None)
                if callable(fn):
                    return fn, inline_pos, inline_kwargs

            # Try registry
            meta = defined_functions_dict.get(name)
            if meta:
                fn = meta.get("function")
                if callable(fn):
                    flog.info("[RunFunction] Resolved callable from defined_functions_dict.")
                    return fn, inline_pos, inline_kwargs

            # Try pipeline environment (in pipeline mode)
            if self._env_mode == "pipeline" and self._pipeline_env:
                fn = self._pipeline_env.env.get(name)
                if callable(fn):
                    flog.info("[RunFunction] Resolved callable from pipeline environment.")
                    return fn, inline_pos, inline_kwargs

            # Not found
            avail = sorted(
                list(defined_functions_dict.keys()) +
                [k for k, v in getattr(variable_handler, "variables", {}).items() if callable(getattr(v, "value", None))]
            )
            if self._env_mode == "pipeline" and self._pipeline_env:
                pipeline_functions = [k for k, v in self._pipeline_env.env.items() if callable(v)]
                avail.extend(pipeline_functions)
                avail = sorted(set(avail))
            
            msg = (f'Selected function "{selected_function_raw}" not found as a callable.\n'
                f'Available functions: {avail}')
            flog.error(msg)
            raise SoftPipelineError(msg)

        msg = f"Unsupported function selector type: {type(selected_function_raw).__name__}"
        flog.error(msg)
        raise SoftPipelineError(msg)

    def execute(self, node_detail_form):


        selected_function_raw = node_detail_form.get_chosen_value_by_name("selected_function", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        selected_function, inline_pos, inline_kwargs = self._resolve_selected_function(selected_function_raw)

        # Inspect signature
        try:
            full = inspect.getfullargspec(selected_function)
            func_args = full.args
            kwonly = full.kwonlyargs or []
        except Exception as e:
            flog.error(f"[RunFunction] getfullargspec failed: {e}")
            raise
        # If user typed inline args (e.g., "square(5)") – use those.
        if inline_pos or inline_kwargs:
            args_list = inline_pos[:]      # strings
            kwargs_dict = inline_kwargs.copy()
        else:
            # Get arguments from form (manual input mode)
            args_list = []
            for arg in func_args:
                val = node_detail_form.get_chosen_value_by_name(arg, variable_handler)
                args_list.append(val)

            kwargs_dict = {}
            for k in kwonly:
                val = node_detail_form.get_chosen_value_by_name(k, variable_handler)
                # Only add to kwargs_dict if the value is not None/empty
                if val is not None and val != "":
                    kwargs_dict[k] = val


        self.direct_execute(selected_function, new_var_name, args_list=args_list, kwargs_dict=kwargs_dict)

    def direct_execute(self, selected_function, new_var_name, args_list=None, kwargs_dict=None):
        if args_list is None:
            args_list = []
        if kwargs_dict is None:
            kwargs_dict = {}

        # Allow string selector too
        if not callable(selected_function):
            resolved_fn, inline_pos, inline_kwargs = self._resolve_selected_function(selected_function)
            selected_function = resolved_fn
            if not args_list:
                args_list = inline_pos or []
            if not kwargs_dict:
                kwargs_dict = inline_kwargs or {}

        func_name = selected_function.__name__
        
        if self._env_mode == "fresh":
            # Check if we have a direct function object or need to resolve from stored code
            try:
                # Try to get stored code for the function
                imports, code = _get_meta_for_function(func_name)
                variables_code = self.find_variables_used_in_function(
                    code,
                    list(args_list) + list(kwargs_dict.values()),
                    func_name=func_name,
                )
                total_code = (("\n".join(imports) + "\n") if imports else "") + variables_code + code
                env = {"__builtins__": __builtins__, "__name__": "__main__", "__package__": None}
                
                # Include global variables in the execution environment
                env.update(globals())
                
                fce.exec_code(total_code, env, env)
                func = env.get(func_name)
            except SoftPipelineError:
                # No stored code found, use the function object directly
                func = selected_function

        elif self._env_mode == "pipeline":
            # RUNJOB: single shared module env across nodes (function already defined)
            if not self._pipeline_env:
                raise SoftPipelineError("Pipeline env not initialized. Call use_pipeline_env() before RunJob.")
            # Function is already defined in the pipeline environment, just get it
            func = self._pipeline_env.env.get(func_name)

        else:
            raise SoftPipelineError(f"Unknown env mode: {self._env_mode}")

        if func is None:
            raise SoftPipelineError("Function not found after exec. Check stored code/imports.")

        # Evaluate arguments based on environment mode
        if self._env_mode == "pipeline":
            # Evaluate arguments in the pipeline environment
            coerced_pos = []
            for a in args_list:
                try:
                    # Try to evaluate the argument in the pipeline environment
                    evaluated_arg = eval(a, self._pipeline_env.env, self._pipeline_env.env)
                    coerced_pos.append(evaluated_arg)
                except Exception:
                    # Fall back to literal coercion if evaluation fails
                    coerced_pos.append(_coerce_literal(a))
            
            coerced_kwargs = {}
            for k, v in kwargs_dict.items():
                try:
                    # Try to evaluate the argument in the pipeline environment
                    evaluated_arg = eval(v, self._pipeline_env.env, self._pipeline_env.env)
                    coerced_kwargs[k] = evaluated_arg
                except Exception:
                    # Fall back to literal coercion if evaluation fails
                    coerced_kwargs[k] = _coerce_literal(v)
        else:
            # Fresh mode: evaluate arguments in the fresh environment context
            coerced_pos = []
            for a in args_list:
                try:
                    # Try to evaluate the argument in the fresh environment
                    evaluated_arg = eval(a, env, env)
                    coerced_pos.append(evaluated_arg)
                except Exception:
                    # Fall back to literal coercion if evaluation fails
                    coerced_pos.append(_coerce_literal(a))
            
            coerced_kwargs = {}
            for k, v in kwargs_dict.items():
                try:
                    # Try to evaluate the argument in the fresh environment
                    evaluated_arg = eval(v, env, env)
                    coerced_kwargs[k] = evaluated_arg
                except Exception:
                    # Fall back to literal coercion if evaluation fails
                    coerced_kwargs[k] = _coerce_literal(v)
        
        return_value = func(*coerced_pos, **coerced_kwargs)

        if return_value is not None:
            if "," in new_var_name:
                names = [n.strip() for n in new_var_name.split(",")]
                for i, n in enumerate(names):
                    variable_handler.new_variable(n, return_value[i])
            else:
                variable_handler.new_variable(new_var_name, return_value)





    def find_variables_used_in_function(self, function_code: str, args_list: list, func_name: str | None = None) -> str:
        """
        Emit Python assignments for variables that the function body actually references,
        while avoiding shadowing the function name and ensuring strings are safely repr()'d.
        """
        lines = []

        for v in variable_handler.variables.values():
            name = v.name
            val = v.value

            # 1) Never shadow the function we are (re)defining
            if func_name and name == func_name:
                continue

            # 2) Skip functions (we don't inline callables)
            if inspect.isfunction(val):
                continue

            # 3) If value looks like raw source code of a function, skip (defense)
            if isinstance(val, str) and val.lstrip().startswith(("def ", "lambda ")):
                continue

            # 4) Only inject when the identifier actually appears in the function code
            if not re.search(rf"\b{re.escape(name)}\b", function_code):
                continue

            # 5) Serialize safely
            try:
                if isinstance(val, str):
                    lines.append(f"{name} = {val!r}")   # repr -> escapes newlines
                else:
                    lines.append(f"{name} = {repr(val)}")
            except Exception as e:
                flog.warning(f"Skipping var {name}: not serializable ({e})")
                continue

        return ("\n".join(lines) + ("\n" if lines else ""))



    def export_code(self, node_detail_form):
        raw = node_detail_form.get_chosen_value_by_name("selected_function", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        if callable(raw):
            fn = raw
            func_name = raw.__name__
        else:
            name, _pos, _kwargs = self._parse_function_call_string(raw)
            if not name:
                raise SoftPipelineError("Could not parse function selector for export.")
            func_name = name
            # Try to resolve callable for arg introspection; optional for export
            var_obj = getattr(variable_handler, "variables", {}).get(name)
            if var_obj and callable(getattr(var_obj, "value", None)):
                fn = var_obj.value
            else:
                meta = defined_functions_dict.get(name)
                fn = meta.get("function") if meta else None

        imports, function_code = _get_meta_for_function(func_name)

        func_args = inspect.getfullargspec(fn).args if callable(fn) else []
        arg_values = {arg: node_detail_form.get_chosen_value_by_name(arg, variable_handler) for arg in func_args}

        args_code = ",".join(f"{k} = {v}" for k, v in arg_values.items())
        variables_code = self.find_variables_used_in_function(function_code, list(arg_values.values()), func_name=func_name)

        call_code = f"{func_name}({args_code})"
        if new_var_name and not new_var_name.isspace():
            return variables_code + "\n" + f"{new_var_name} = {call_code}"
        else:
            return variables_code + "\n" + call_code

    def export_imports(self, node_detail_form):
        raw = node_detail_form.get_chosen_value_by_name("selected_function", variable_handler)
        if callable(raw):
            name = raw.__name__
        else:
            name, _pos, _kwargs = self._parse_function_call_string(raw)
            if not name:
                return []

        try:
            _imports, code = _get_meta_for_function(name)
        except SoftPipelineError:
            return []

        imports = find_imports_in_custom_code(code)
        return imports

    def rebuild_icon_item_detail_form(self, image, last_loaded_function):

        selected_function = image.item_detail_form.get_chosen_value_by_name("selected_function", variable_handler)

        # idf_args = []
        # if len(image.item_detail_form.elements) > 3:
        #    for element in image.item_detail_form.elements[3:-3]:
        #        if type(element) == gc.Label:
        #            idf_args.append(element.text)

        # func_args = []
        # if selected_function:
        #    func_args = inspect.getfullargspec(selected_function).args

        # if inspect.isfunction(selected_function) and idf_args != func_args:
        if inspect.isfunction(selected_function) and selected_function.__name__ != last_loaded_function:
            self.scan_function_arguments(image)

        return selected_function.__name__

    def scan_function_arguments(self, image):
        
        typ = self.icon_type
        selected_function = image.item_detail_form.get_chosen_value_by_name('selected_function', variable_handler)

        element = image.item_detail_form.get_element_by_name('selected_function')
        
        variable_rect = element.forloop_temp_variable_rect

        full_arg_spec = inspect.getfullargspec(selected_function) 
        func_args = full_arg_spec.args
        arg_types_dict = full_arg_spec.annotations

        form_dict_list = image.item_detail_form.form_dict_list
        # TODO Daniel:
        #! REPAIR THIS !!!!!!!
        form_dict_list = mapping_handlers_dict[typ].make_form_dict_list(image)

        if func_args:
            for arg in func_args:
                if arg in arg_types_dict.keys():
                    form_dict_list.append({"Label": arg, "Entry": {"name": arg, "text": "", "category": "arguments", "input_types": [arg_types_dict[arg].__name__]}})
                else:
                    form_dict_list.append({"Label": arg, "Entry": {"name": arg, "text": "", "category": "arguments"}})

        # form_dict_list.append({"Label": "Return variable", "Entry": {"name":"new_var_name", "text": "", "category": "new_var"}})
        form_dict_list.append({"Button": {"function": mapping_handlers_dict[typ].execute,
                                          "function_args": [image], "text": "Execute"}})

        image.item_detail_form.elements = []

        #TEMP DISABLED
        #image.item_detail_form = ItemDetailForm(form_dict_list, typ, magnetic=False)
        #image.item_detail_form.generate_elements()
        # selected_func_entry = image.item_detail_form.elements[2]
        #selected_func_entry = image.item_detail_form.get_element_by_name('selected_function')
        
        #variable_rect.assign_to_entry(selected_func_entry)
        #TEMP DISABLED


#### TODO: Decide whether ImportPackagesHandler should be implemented or deleted
class ImportPackagesHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'ImportPackages'
        self.fn_name = 'Import Packages'

        self.type_category = ntcm.categories.mapping

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["numpy", "pandas", "scipy", "scrapy", "time"]
        form_dict_list = [
            {"Label": "Import Packages"},
            {"Label": "Favourites:", "Combobox": {"name": "selected_fav_imports", "options": options}},
            {"Label": "Import:", "Entry": {"name": "imports", "text": "pandas, numpy"}},
            {"Label": "_______________"},
            {"Label": "Optional arguments:"},
            {"Label": "Import as:", "Entry": {"name": "import_as", "text": ""}},
            {"Button": {"function": self.execute, "function_args": args, "text": "Execute"}}
        ]
        return (form_dict_list)

    def execute(self, args):
        image = args[0]
        item_detail_form = image.item_detail_form

        self.execute_with_params(item_detail_form.params)

    def execute_with_params(self, params):

        selected_fav_imports = params["selected_fav_imports"]
        imports = params["imports"]
        import_as = params["import_as"]

        self.direct_execute(selected_fav_imports, imports, import_as)

    def direct_execute(self, selected_fav_imports, imports, import_as):

        code = self.create_import_code(selected_fav_imports, imports, import_as)

        try:
            fce.exec_code(code, globals(), locals())
        except Exception as e:
            flog.error(e)

    def create_import_code(self, selected_fav_imports, imports, import_as, args):

        imports_combined = selected_fav_imports if selected_fav_imports else []  # if something is selected in combobox - take it else []

        if "," in imports:
            imports = imports.split(',')
            imports_combined = imports_combined + imports

        if "," in import_as:
            import_as = import_as.split(',')

        code = """
        """

        if len(imports_combined) == len(import_as):
            for i, element in enumerate(imports_combined):
                code += f"import {element}"

                if import_as[i] != '' and not import_as[i].isspace():
                    code += f" as {import_as[i]}\n"
                else:
                    code += "\n"

        return code

    def export_code(self, node_detail_form):

        selected_fav_imports = node_detail_form.get_chosen_value_by_name("selected_fav_imports", variable_handler)
        imports = node_detail_form.get_chosen_value_by_name("imports", variable_handler)
        import_as = node_detail_form.get_chosen_value_by_name("import_as", variable_handler)

        code = self.create_import_code(selected_fav_imports, imports, import_as)

        return code


    def export_imports(self, *args):
        imports = []
        return (imports)


mapping_handlers_dict = {
    'ApplyMapping': ApplyMappingHandler(),
    'DefineFunction': DefineFunctionHandler(),
    'DefineLambdaFunction': DefineLambdaFunctionHandler(),
    'RunFunction': RunFunctionHandler()
}