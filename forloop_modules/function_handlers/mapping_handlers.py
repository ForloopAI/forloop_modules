import inspect
import ast

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
            
        variable_handler.new_variable(function_name, func, additional_params={"code": code})
        #variable_handler.update_data_in_variable_explorer(glc)
        flog.warning(f'New function defined: "{function_name}"')
    else:
        flog.error("Function name can't be of type None.")

###


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
        self.is_disabled = True # FIXME: Needs refactor - better UX, check functionality, solve how to store functions
        self.icon_type = 'DefineFunction'
        self.fn_name = 'Define Function'

        self.type_category = ntcm.categories.mapping
        self.docs_category = DocsCategories.control

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

        code = init_line + "\n" + 4 * " " + "\n    ".join(imports) + "\n\n" + code

        define_function_variable(code, func_name, imports)

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
        self.is_disabled = True
        self.icon_type = 'RunFunction'
        self.fn_name = 'Run Function'

        self.type_category = ntcm.categories.mapping
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Function:")
        fdl.entry(name="selected_function", text="", category="function", input_types=["function"], required=True, row=1)
        fdl.label("Return variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        selected_function = node_detail_form.get_chosen_value_by_name("selected_function", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        func_args = inspect.getfullargspec(selected_function).args

        args_list = []
        for arg in func_args:
            arg_value = node_detail_form.get_chosen_value_by_name(arg, variable_handler)
            args_list.append(arg_value)

        self.direct_execute(selected_function, new_var_name, args_list)

    def execute_with_params(self, params, node_detail_form=None):

        selected_function = params["selected_function"]
        new_var_name = params["new_var_name"]

        func_args = inspect.getfullargspec(selected_function).args

        args_list = []
        for arg in func_args:
            arg_value = node_detail_form.get_chosen_value_by_name(arg, variable_handler)
            args_list.append(arg_value)

        self.direct_execute(selected_function, new_var_name, args_list)

    def direct_execute(self, selected_function, new_var_name, *args):
        
        # HACK: Disable the execution of the node with some feedback for a user until we implement security checks
        raise SoftPipelineError("Execution of this node is temporarily disabled.")
        
        args = args[0]

        imports = defined_functions_dict[selected_function.__name__]["imports"]
        imports = list(set(imports))

        code = defined_functions_dict[selected_function.__name__]["code"]

        variables_code = self.find_variables_used_in_function(code, args)
        
        total_code = "\n".join(imports) + "\n" + variables_code + code
        fce.exec_code(total_code, globals(), locals())

        return_value = None

        for i, arg in enumerate(args):      
            try:
                args[i] = ast.literal_eval(arg)
            except Exception as e:
                flog.warning(e)
                pass

        if args:
            return_value = selected_function(*args)
        else:
            return_value = selected_function()

        if return_value is not None:
            if "," in new_var_name:
                variables = new_var_name.split(",")
                for i, variable in enumerate(variables):
                    variable = variable.strip()
                    variable_handler.new_variable(variable, return_value[i])
            else:
                variable_handler.new_variable(new_var_name, return_value)

            #variable_handler.update_data_in_variable_explorer(glc)

    def find_variables_used_in_function(self, function_code: str, args_list: list) -> str:

        variables_code = ""

        for variable in variable_handler.variables.values():
            if not inspect.isfunction(variable.value):
                if variable.name in args_list or variable.name in function_code:
                    print(
                        f"VAR NAME = {variable.name}, VAR VALUE = {variable.value}, VAR TYPE = {type(variable.value)}")
                    if type(variable.value) == str:
                        variables_code += f'{variable.name} = "{variable.value}"\n'
                    else:
                        variables_code += f'{variable.name} = {variable.value}\n'

        return variables_code

    def export_code(self, node_detail_form):

        selected_function = node_detail_form.get_chosen_value_by_name("selected_function", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        function_code = defined_functions_dict[selected_function.__name__]["code"]
        func_args = inspect.getfullargspec(selected_function).args

        arg_values = {}
        for arg in func_args:
            arg_values[arg] = node_detail_form.get_chosen_value_by_name(arg, variable_handler)

        args_code = ""
        for arg_name, arg_value in arg_values.items():
            args_code += f'{arg_name} = {arg_value},'

        args_code = args_code[:-1]  # removing the last ','

        args_list = list(arg_values.values())
        variables_code = self.find_variables_used_in_function(function_code, args_list)

        if new_var_name and not new_var_name.isspace():
            code = variables_code + "\n" + f"{new_var_name} = {selected_function.__name__}({args_code})"
        else:
            code = variables_code + "\n" + f"{selected_function.__name__}({args_code})"

        return code

    def export_imports(self, node_detail_form):

        selected_function = node_detail_form.get_chosen_value_by_name("selected_function", variable_handler)
        code = defined_functions_dict[selected_function.__name__]["code"]
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