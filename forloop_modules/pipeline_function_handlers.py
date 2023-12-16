import operator
import inspect
import pandas as pd

from forloop_modules.globals.variable_handler import variable_handler, custom_icons_imports
#from src.gui.gui_layout_context import glc, ItemDetailForm # FRONTEND DEPENDENCY

##### BACKEND ONLY #####
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input 
import forloop_modules.function_handlers.auxilliary.forloop_code_eval as fce
from forloop_modules.function_handlers.control_flow_handlers import control_flow_handlers_dict

##### BACKEND ONLY #####

from forloop_modules.function_handlers.rpa_handlers import rpa_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.webscraping_handlers import webscraping_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.api_handlers import api_handlers_dict # FRONTEND DEPENDENCY
#from src.function_handlers.plot_handlers import plot_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.variable_handlers import variable_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.datetime_handlers import datetime_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.integration_handlers import integration_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.cleaning_handlers import cleaning_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.database_handlers import database_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.file_managment_handlers import file_managment_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.data_handlers import data_handlers_dict # FRONTEND DEPENDENCY
from forloop_modules.function_handlers.model_handlers import model_handlers_dict # PARTIAL FRONTEND DEPENDENCY
from forloop_modules.function_handlers.orchestration_handlers import orchestration_handlers_dict ##### BACKEND ONLY - TO BE IMPLEMENTED #####
from forloop_modules.function_handlers.mapping_handlers import mapping_handlers_dict, find_imports_in_custom_code, find_variables_used_in_custom_code # FRONTEND DEPENDENCY


#from src.custom_handler_template import create_new_custom_handler_class, get_instance_var_import_code


############### SIMILAR TO update_textarea IN forloop_cleaning --> TODO: CREATE ONE UNIVERSAL FUNCTION AVAILABLE TO ALL WITHOUT CYCLIC IMPORTS

# DEPRECATED - used nowhere
# def show_message_in_console(message: str, id: int = 0, prepend: str = "Output:\n"):
#     """
#     Prints a message with a specified prepend in the console.

#     Keywords:
#     message -- text to be printed (str)
#     id -- defines the ordering of the messages (int)
#     prepend -- specifies the message (error, warning, output etc.) (str)
#     """

#     glc.textareas.elements[id].text = f'{prepend} {message}'
#     glc.textareas.elements[id].label.text = f'{prepend} {message}'


######################################################


############################# START PIPELINE FUNCTION MODULES ##############################



class StatementHandler(AbstractFunctionHandler):
    icon_type = 'Statement'
    fn_name = 'Statement'
    operators = {
        '==': operator.eq,
        '!=': operator.ne,
        '>=': operator.ge,
        '<=': operator.le,
        '>': operator.gt,
        '<': operator.lt,
        'and': operator.and_,
        'or': operator.or_,
        'contains': operator.contains,
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.truediv,
        '^': operator.pow,
        '%': operator.mod,
        'round': round,
        'floor': int,
        'split': str.split,
        'strip': str.strip,
        'replace': str.replace
    }

    # options = ["+", "-", "*", "/", "^ (power)", "% (mod)", "round", "floor"]

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = list(self.operators.keys())
        form_dict_list = [
            {'Label': self.fn_name},
            {"Label": "Variable:", "Entry": {"name": "variable", "text": ""}},
            {"Label": "Value P:", "Entry": {"name": "value_p", "text": ""}},
            {"Label": "Operator:", "Combobox": {"name": "operator", "options": options}},
            {"Label": "Value Q:", "Entry": {"name": "value_q", "text": ""}},
        ]
        return form_dict_list

    def direct_execute(self, variable, value_p, operator, value_q):
        # def __new__(cls, variable, value_p, operator, value_q=None, *args, **kwargs):
        try:
            if operator == 'replace':
                var_value = variable_handler.variables.get(variable)
                if var_value is None or not isinstance(var_value.value, str):
                    pass
                    # logger.error(f'{variable} is not stored or is not a string')
                else:
                    string: str = variable_handler.variables[variable].value
                    variable_handler.variables[variable] = string.replace(value_p, value_q)
                return
            try:
                variable_handler.variables[variable] = self.operators[operator](value_p, value_q)
            except TypeError:
                variable_handler.variables[variable] = self.operators[operator](value_p)
        except Exception as e:
            # logger.exception(f'Error while running operation {value_p} {operator} {value_q}')
            pass


class CustomIconHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'CustomIcon'
        self.fn_name = 'Custom Icon'

        self.type_category = ntcm.categories.unknown

    def make_form_dict_list(self, node_detail_form=None):
        form_dict_list = [
            {"Label": "Custom Icon"},
            {"Label": "", "Entry": {"name": "code_label", "text": "", "category": "arguments"}},
            {"Button": {"function": self.execute, "function_args": node_detail_form, "text": "Execute"}}

        ]
        return (form_dict_list)

    def execute(self, node_detail_form):
        code_label = node_detail_form.get_chosen_value_by_name("code_label", variable_handler)

        self.direct_execute(code_label)

    def execute_with_params(self, params):

        code_label = params["code_label"]

        self.direct_execute(code_label)

    def direct_execute(self, code_label):

        imports = find_imports_in_custom_code(code_label)

        defined_variables = []

        for variable in variable_handler.variables.values():
            if not inspect.isfunction(variable.value) and not isinstance(variable.value, pd.DataFrame):
                if variable.name in code_label:
                    if type(variable.value) == str:
                        defined_variables.append(f'{variable.name} = "{variable.value}"')
                    else:
                        defined_variables.append(f"{variable.name} = {variable.value}")


        import_code = "\n".join(imports)
        var_code = "\n".join(defined_variables)
        
        total_code = import_code + "\n" + var_code + "\n" + code_label
        fce.exec_code(total_code, globals(), locals())


        code_lines = code_label.split("\n")
        code_lines = list(filter(lambda val: val != "", code_lines))

        if len(code_lines) < 2 and "=" in code_label:
            new_var_name = code_label[:code_label.index('=')].strip()
            new_var_value = globals()[new_var_name]

            variable_handler.new_variable(new_var_name, new_var_value)
            #variable_handler.update_data_in_variable_explorer(glc)

    def export_code(self, node_detail_form):
        code_label = node_detail_form.get_chosen_value_by_name("code_label", variable_handler)

        variables = find_variables_used_in_custom_code(code_label)

        code = variables + "\n" + code_label

        return code

    def export_imports(self, node_detail_form):
        code_label = node_detail_form.get_chosen_value_by_name("code_label", variable_handler)
        imports = find_imports_in_custom_code(code_label)
        """ #TODO ILYA?
        imports = ["import platform",
                   "import scrapy",
                   "from selenium import webdriver",
                   "from selenium.webdriver.common.desired_capabilities import DesiredCapabilities",
                   "from selenium.webdriver.firefox.options import Options",
                   "from webdriver_manager.firefox import GeckoDriverManager",
                   "import time",
                   "import pynput.keyboard",
                   "from scrapy.selector import Selector",
                   "import os",
                   "import time",
                   "import datetime",
                   "import random",
                   "import pandas as pd"]
        """

        return (imports)

        
    """
    def export_imports(self, *args):
        imports = ["import platform",
                   "import scrapy",
                   "from selenium import webdriver",
                   "from selenium.webdriver.common.desired_capabilities import DesiredCapabilities",
                   "from selenium.webdriver.firefox.options import Options",
                   "from scrapy.selector import Selector",
                   "from webdriver_manager.firefox import GeckoDriverManager"]
    
    """



pipeline_function_handler_dict = {
    'Statement': StatementHandler(), 
    'CustomIcon': CustomIconHandler(),
}

pipeline_function_handler_dict = {
    **pipeline_function_handler_dict, 
    **variable_handlers_dict, 
    **data_handlers_dict, 
    **mapping_handlers_dict,
    **api_handlers_dict,
    **cleaning_handlers_dict,
    **control_flow_handlers_dict,
    **database_handlers_dict,
    **file_managment_handlers_dict,
    **integration_handlers_dict,
    **model_handlers_dict,
    **orchestration_handlers_dict,
    #**plot_handlers_dict,
    **rpa_handlers_dict,
    **webscraping_handlers_dict,
    **datetime_handlers_dict
    }


for k, v in pipeline_function_handler_dict.items():
    if hasattr(v, "type_category"):
        ntcm.icon_type_to_category_dict[k] = v.type_category

