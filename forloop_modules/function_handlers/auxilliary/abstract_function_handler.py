import abc
import inspect
import ast
import re
from typing import Union, Any

import forloop_modules.globals.variable_handler as vh # DISABLED IN FORLOOP MODULES
import forloop_modules.queries.node_context_requests_backend as ncrb # DISABLED IN FORLOOP MODULES

from forloop_modules.errors.errors import CriticalPipelineError
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.globals.variable_handler import defined_functions_dict # DISABLED IN FORLOOP MODULES
from forloop_modules.node_detail_form import NodeField, NodeParams
from forloop_modules.utils.definitions import DIRECT_EXECUTE_CORE_HANDLERS

class Input(dict):
    """Serves as a variable input placeholder for code export and direct execute"""
    def assign(self,element_name,element_value):
        self[element_name]=element_value
        
    def __call__(self,element_name,inside_str=False):
        """inside_str ... parameter important for calling when Forloop variable is called inside of string"""
        if inside_str:
            return '"+'+self[element_name]+'+"'
        else:
            return self[element_name]


class AbstractFunctionHandler(abc.ABC):
    """Forces all handlers to have defined abstract methods"""
    
    type_category=ntcm.categories.unknown

    def __init__(self):
        self.icon_type = None
        self.is_cloud_compatible: bool = True
        self.is_disabled: bool = False
        self.code_import_patterns = []

    @abc.abstractmethod
    def make_form_dict_list(self, node_detail_form=None):
        pass

    @abc.abstractmethod
    def direct_execute(self, *args):
        pass

    def export_code(self, node_detail_form):
        if node_detail_form.typ in DIRECT_EXECUTE_CORE_HANDLERS: #! This is just temporary so as to test an experimental approach
            code = self.new_export_code_with_node_params(node_detail_form.node_params)
        else:
            code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return (imports)

    def make_flpl_node_dict(self, line_dict: dict) -> dict:

        """
        TODO: solve value inputs as rects from var explorer
        """

        form_dict_list = self.make_form_dict_list()
        node_dict = {"type": self.icon_type, "params": {}}

        arg_index = 0

        arguments = line_dict["arguments"] or []
        keyword_args = {}

        for i, arg in enumerate(arguments):
            first_keyword_arg_encountered = True
            if type(arg) == dict:
                keyword_args = {**keyword_args, **arg}
                if first_keyword_arg_encountered:
                    arguments = arguments[:i]
                    first_keyword_arg_encountered = False

        for form_dict in form_dict_list:
            if "Entry" in form_dict.keys():
                entry_name = form_dict["Entry"]["name"]
                if "category" in form_dict["Entry"].keys():
                    category = form_dict["Entry"]["category"]
                    if category == "arguments":
                        # First distribute positional arguments
                        ## True iff 1) at least 1 argument was entered and 2) index of the current argument entry < number of arguments entered 
                        if arguments and arg_index < len(arguments):
                            value = arguments[arg_index]  
                            arg_index += 1
                        else:
                            value = ""

                        for kwarg_name, kwarg_value in keyword_args.items():
                            if kwarg_name == entry_name:
                                value = kwarg_value
                    else:
                        value = line_dict[category]


                    # DISABLED IN FORLOOP MODULES
                    if value in defined_functions_dict.keys() or value in vh.new_var_names_from_parsing:
                        node_dict["params"][entry_name] = {"variable": value, "value": None}
                    else:    
                        node_dict["params"][entry_name] = {"variable": None, "value": value}
                    # DISABLED IN FORLOOP MODULES
                    
            elif "Combobox" in form_dict.keys():
                if "category" in form_dict["Combobox"].keys():
                    entry_name = form_dict["Combobox"]["name"]
                    category = form_dict["Combobox"]["category"]
                    if category == "arguments":
                        value = line_dict[category][arg_index] 
                        arg_index += 1
                    else:
                        value = line_dict[category]
                    node_dict["params"][entry_name] = {"variable": None, "value": value}

        return node_dict

    def rebuild_icon_item_detail_form(self, image, *args):
        pass
    
    #!#!#!#! EXPERIMENTAL part start #!#!#!#!
    """
    Contains a new approach to node code export from a general function (seeks to replace input_execute).
    
    For more information see chapter 'AbstractFunctionHandler.new_export_code_with_node_params' in 
    http://devwiki.forloop.ai/en/overview/experimental-features-and-approaches
    """
    

    def replace_strings(self, text, replacements):
        for target_string, replacement in replacements.items():
            # Construct the regular expression pattern with word boundaries
            pattern = r'\b' + re.escape(target_string) + r'\b'
            
            # Replace the target string with its replacement using re.sub
            text = re.sub(pattern, replacement, text)
        
        return text
    
    def _evaluate_argument(self, arg: Any) -> Union[Any, None]:
        """
        Safely evaluates the provided argument and returns the appropriate value.

        Attempts to evaluate the argument using `ast.literal_eval`. If successful, 
        returns the evaluated value. If a `ValueError` or `TypeError` occurs, 
        returns the argument unchanged. Raises `CriticalPipelineError` for 
        `SyntaxError`, `MemoryError`, or `RecursionError`.

        Args:
            arg (Any): The input to evaluate.

        Returns:
            Any: Evaluated value or the original argument.

        Raises:
            CriticalPipelineError: For critical errors during evaluation.
        """
        if not arg:
            return arg
        
        try:
            return ast.literal_eval(arg)
        except (ValueError, TypeError):
            return arg
        except (SyntaxError, MemoryError, RecursionError) as e:
            raise CriticalPipelineError(
                f"{self.icon_type}: invalid parameter value passed as input: '{arg}'."
            ) from e
    
    def _replace_key_with_variable_name_in_code(self, code: str, key: str, variable_name: str):
        code = code.replace(key, variable_name)
        # code = self.replace_strings(code, {key: variable_name})
        
        return code
        
    def _replace_key_with_non_str_value_in_code(self, code: str, key: str, value):
        code = code.replace(key, value)
        
        return code
        
    def _replace_key_with_str_value_in_code(self, code: str, key: str, value: str):
        code = code.replace(key, f'"{value}"')
        
        return code
    
    def _update_node_code_with_node_params_values(self, code: str, node_params: NodeParams, return_value_name: str = None) -> str:
        """Takes a code generated from direct_execute_core method and updates it with node_params values (input from the user).

        Args:
            code (str): Code retrieved from direct_execute_core method of the inspected node's handler.
            node_params (NodeParams): Node params containing user's input to be put into the code.

        Returns:
            str: A final code updated with values from node params, i.e. user input.
        """
        
        for element_name, element_value in node_params.code_repr().items():
            is_element_variable = element_value["is_variable"]
            value = element_value["code"]
            # if not value:
            #     value = node_params[element_name].get("export_code_default", "")
            if is_element_variable or element_name == return_value_name:
                code = self._replace_key_with_variable_name_in_code(code=code, key=element_name, variable_name=value)
            else:
                try:
                    evaluated_value = ast.literal_eval(value)
                    code = self._replace_key_with_non_str_value_in_code(code=code, key=element_name, value=evaluated_value)
                except Exception as e:
                    print(e)
                    code = self._replace_key_with_str_value_in_code(code=code, key=element_name, value=value)
                    
        return code
    
    #! EXPERIMENTAL - potentially replaces export_code_with_node_params
    def new_export_code_with_node_params(self, node_params: NodeParams) -> str:
        """Exports code of a handler from it's direct_execute_core method and fills it with values from NodeParams.


        Args:
            node_params (NodeParams): NodeParams retrieved from the node (user input in entries, comboboxes etc.)

        Returns:
            str: A node code updated with values from node params.
        """        
        
        if not isinstance(node_params, NodeParams):
            raise TypeError(f"Entered node_params must be a NodeParams object, not {type(node_params)}")
        
        ERROR_CODE_MESSAGE = "# Code could not be loaded..."
        
        if not hasattr(self, "direct_execute_core"):
            return ERROR_CODE_MESSAGE
            
        code_lines, _ = inspect.getsourcelines(self.direct_execute_core)
        
        if not code_lines:
            return ERROR_CODE_MESSAGE
        
        code_lines.pop(0) # Omit function definition
        
        return_value_name = None
        if code_lines[-1].strip().startswith("return"):
            # Remove "return" line if present
            return_value_name = code_lines[-1].split()[1].strip() #? What about more values returned in a tuple?
            code_lines.pop(-1)
            
        code = "".join(code_lines)
        
        code = self._update_node_code_with_node_params_values(code=code, node_params=node_params, return_value_name=return_value_name)
                    
        return code
    #!#!#!#!#! Experimental END #!#!#!#!##
    
    def _parse_code_lines_with_inputs_into_code(self, code_lines: list[str], node_params: NodeParams) -> str:
        """Takes code lines of an input_execute method of a node and parses it into a single code string, replacing inputs with user 
        inputs contained in node's NodeParams.

        Args:
            code_lines (list[str]): List of input_execute code lines.
            node_params (NodeParams): NodeParams containing the user input (text or variables in entries, combobox selections etc.) of the node.

        Returns:
            str: A final code of a node containing user input from entries, comboboxes etc.
        """        
        code_lines.pop(0) #ommit function definition
        
        if code_lines[-1].strip().startswith("return"):
            return_value_name = code_lines[-1].split()[1].strip() #? What about more values returned in a tuple?
            code_lines.pop(-1) # remove "return" line if present in the function code
        else:
            return_value_name = None
            
        code = "".join(code_lines)
        
        code = self._replace_inputs_with_variable_values_in_code(code=code, node_params=node_params, return_value_name=return_value_name)
    
        return code
    
    def _replace_inputs_with_variable_values_in_code(self, code:str, node_params: NodeParams, return_value_name: Union[str, None]) -> str:
        """Replaces inputs in code string with values from NodeParams.

        Args:
            code (str): Code string (without function definition row and return row).
            node_params (NodeParams): NodeParams containing the user input (text or variables in entries, combobox selections etc.) of the node.
            return_value_name (Union[str, None]): Name of a return value (to be replace in the rest of the code)

        Returns:
            str: A final code of a node containing user input from entries, comboboxes etc.
        """        
        
        for element_name, element_value in node_params.code_repr().items():

            to_be_replaced = 'inp("' + element_name + '")'
            
            if element_name == "new_var_name" and return_value_name is not None:
                # TODO: Generalize (this is cleaning handler specific) - Ask Tomas before
                # to_be_replaced = "df_new"
                to_be_replaced = return_value_name

            # TODO: DOES NOT SUPPORT F-STRINGS
            if element_value["is_variable"] or element_name == "new_var_name": # or not isinstance(v['code'], str):
                code=code.replace('"+inp("'+element_name+'")+"', element_value["code"])
                code=code.replace('"+inp("'+element_name+'",inside_str=True)+"','"+' + element_value["code"] + '+"')
                code=code.replace(to_be_replaced, element_value["code"])    
                # code=code.replace(to_be_replaced_inside_str, '"' + v["code"] + '"')
            else:
                code=code.replace('+inp("'+element_name+'")+',str(element_value["code"]))
                code=code.replace('"+inp("'+element_name+'",inside_str=True)+"',str(element_value["code"]))
                if isinstance(element_value["code"], str):
                    code = code.replace(to_be_replaced, '"' + element_value["code"] + '"')
                else:
                    code = code.replace(to_be_replaced, str(element_value["code"]))
                    
        return code
    
    def export_code_with_node_params(self, node_params: NodeParams) -> str:
        """Exports node code from it's input_execute function and replaces inputs with user input stored in NodeParams.

        Args:
            node_params (NodeParams): NodeParams containing the user input (text or variables in entries, combobox selections etc.) of the node.

        Raises:
            TypeError: Raised if the node_params argument is not a of NodeParams instance

        Returns:
            str: A final code of a node containing user input from entries, comboboxes etc.
        """        
        
        if not isinstance(node_params, NodeParams):
            raise TypeError(f"Entered node_params must be a NodeParams object, not {type(node_params)}")
        
        ERROR_CODE_MESSAGE = f"# {self.icon_type}: Code could not be loaded..."
        
        if not hasattr(self, "input_execute"):
            return ERROR_CODE_MESSAGE
            
        code_lines, _ = inspect.getsourcelines(self.input_execute)
        
        if not code_lines:
            return ERROR_CODE_MESSAGE
        
        code = self._parse_code_lines_with_inputs_into_code(code_lines=code_lines, node_params=node_params)

        return code

    #* Disabled - improved version is being developed and tested
    #! DO NOT DELETE YET
    """
    def export_code_with_node_params(self,node_params):
        # TODO: Probably do not include comments in code_export
        # TODO: Replace flog calls
        lines_of_code=inspect.getsourcelines(self.input_execute)[0]
        
        lines_of_code.pop(0) #omit function definition
        if lines_of_code[-1].strip().startswith("return"):
            lines_of_code.pop(-1)

        code = "".join(lines_of_code)
        
        for k,v in node_params.code_repr().items():

            to_be_replaced = 'inp("' + k + '")'
            
            if k == "new_var_name":
                # TODO: Generalize (this is cleaning handler specific) - Ask Tomas before
                to_be_replaced = "df_new"

            # TODO: DOES NOT SUPPORT F-STRINGS
            if v["is_variable"] or k == "new_var_name": # or not isinstance(v['code'], str):
                code=code.replace('"+inp("'+k+'")+"', v["code"])
                code=code.replace('"+inp("'+k+'",inside_str=True)+"','"+' + v["code"] + '+"')
                code=code.replace(to_be_replaced, v["code"])    
                # code=code.replace(to_be_replaced_inside_str, '"' + v["code"] + '"')
            else:
                code=code.replace('+inp("'+k+'")+',str(v["code"]))
                code=code.replace('"+inp("'+k+'",inside_str=True)+"',str(v["code"]))
                if isinstance(v["code"], str):
                    code = code.replace(to_be_replaced, '"' + v["code"] + '"')
                else:
                    code = code.replace(to_be_replaced, str(v["code"]))

        return (code)
    """

    # TODO: COULD THIS ENABLE EXPORTING WHOLE FORLOOP CODE BASE?
    # TODO: HOW TO DEAL WITH CLASS METHODS???
    def export_internal_function(self, function_name):
        lines_of_code = inspect.getsourcelines(function_name)[0]
        code = "".join(lines_of_code)

        return code
    
    def generate_shown_dataframe_option_field(self, df_variable_name: str, fields: list[dict]):
        # Hotfix for "untitled{i}" cases --> name wasn't synced properly
        df_variable_name = vh.variable_handler._set_up_unique_varname(df_variable_name)
        fields += [NodeField.field_init("shown_dataframe", df_variable_name, "df_variable_name")]

        return fields
    
    def refresh_shown_dataframe_option_field(self, fields: Union[list[dict], None], df_variable_name: str):
        if fields is None:
            fields = []
            
        fields = list(filter(lambda x: x["name"] not in ["shown_dataframe", "options"], fields))

        if df_variable_name is not None:
            fields = self.generate_shown_dataframe_option_field(df_variable_name, fields)

        return fields

    def update_node_fields_with_shown_dataframe(self, node_detail_form, df_variable_name):
        fields = self.refresh_shown_dataframe_option_field(node_detail_form.fields, df_variable_name)
        
        # TODO: can be probably removed once node reflection is divided to field update and params update
        # TODO2: removing this breaks the wizard cleaning
        # node_detail_form.fields = fields

        ncrb.update_node_by_uid(node_uid=node_detail_form.node_uid, typ=node_detail_form.typ, 
                                params=node_detail_form.node_params, fields=fields)
        
        return df_variable_name
