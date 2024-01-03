import os
import importlib

from tkinter.filedialog import askopenfile

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb
import forloop_modules.utils.script_utils as su

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler 


class PythonScriptHandler(AbstractFunctionHandler):
    icon_type = "PythonScript"
    fn_name = "Python Script"
    type_category = ntcm.categories.data

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Script path:")
        fdl.label("")

        return fdl

    def direct_execute(self):
        # def __new__(cls, *args, **kwargs):
        """Do nothing"""
        pass

    def export_code(self, *args):
        image = args[0]

        try:
            filename = image.item_detail_form.elements[2].text
            with open(filename, 'r') as f:
                code = f.read()
        except:
            code = """
            """
        
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)


class JupyterScriptHandler(AbstractFunctionHandler):
    icon_type = "JupyterScript"
    fn_name = "Jupyter Script"
    type_category = ntcm.categories.data

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Script path:")
        fdl.label("")

        return fdl

    def direct_execute(self):
        # def __new__(cls, *args, **kwargs):
        """Do nothing"""
        pass

    def export_code(self, *args):
        image = args[0]

        try:
            filename = image.item_detail_form.elements[2].text
            with open(filename, 'r') as f:
                code = f.read()
        except:
            code = """
            """
        
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)


class LoadPythonScriptHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadPythonScript"
        self.fn_name = "Load Python Script"

        self.type_category = ntcm.categories.model
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        file_types=[('Python scripts', '*.py')]
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File path:")
        fdl.entry(name="file_path", text="", required=True, type="file", file_types=file_types, row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", 
                   frontend_implementation=True, focused=True)

        return fdl

    def execute(self, node_detail_form):
        file_path = node_detail_form.get_chosen_value_by_name("file_path", variable_handler)
        
        self.direct_execute(file_path)

    def direct_execute(self, file_path):
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Given path `{file_path}` is not a file.")
        
        script_name = file_path.split("/")[-1]
            
        with open(file_path, "r") as file:
            code = file.read()
            
        su.create_new_script(script_name, text=code)

class LoadJupyterScriptHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadJupyterScript"
        self.fn_name = "Load Jupyter Script"

        self.type_category = ntcm.categories.model
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        file_types=[('Jupyter notebooks', '*.ipynb')]
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File path:")
        fdl.entry(name="file_path", text="", required=True, type="file", file_types=file_types, row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", 
                   frontend_implementation=True, focused=True)

        return fdl

    def execute(self, node_detail_form):
        file_path = node_detail_form.get_chosen_value_by_name("file_path", variable_handler)

        self.direct_execute(file_path)

    def direct_execute(self, file_path):
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Given path `{file_path}` is not a file.")
        
        script_name = file_path.split("/")[-1]
            
        with open(file_path, "r") as file:
            code = file.read()
            
        su.create_new_script(script_name, text=code)

class TrainModelHandler:
    def __init__(self):
        self.icon_type = 'TrainModel'
        self.fn_name = 'Train Model'

        self.type_category = ntcm.categories.model
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Input Data (X)")
        fdl.entry(name="input_data", text="", row=1)
        fdl.label("Dependent Variable (y)")
        fdl.entry(name="dependent_variable", text="", row=2)
        fdl.label("Model Filename")
        fdl.entry(name="model_filename", text="", row=3)
        fdl.label("Train Function Name")
        fdl.entry(name="model_function_name", text="", row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        pass

        """ TODO """


class PredictModelValuesHandler:
    def __init__(self):
        self.icon_type = 'PredictModelValues'
        self.fn_name = 'Predict Model Values'

        self.type_category = ntcm.categories.model
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Predict Values with Model")
        fdl.label("Input Data (X)")
        fdl.entry(name="input_data", text="", row=1)
        fdl.label("Model Filename (.py)")
        fdl.entry(name="model_filename", text="", input_types=["str"], row=2)
        fdl.label("Predict Function Name")
        fdl.entry(name="model_function_name", text="", input_types=["str"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        model_filename = node_detail_form.get_chosen_value_by_name("model_filename", variable_handler)
        model_function_name = node_detail_form.get_chosen_value_by_name("model_function_name", variable_handler)

        my_module = importlib.import_module(model_filename)
        input_data = 123

        my_module.pricing_model(input_data)

    def export_code(self, *args):
        image = args[0]
        params = image.item_detail_form.params

        input_data = params["input_data"]
        model_filename = params["model_filename"]
        model_function_name = params["model_function_name"]

        code = """
        input_data = {input_data}
        model_filename = {model_filename}
        model_function_name = {model_function_name}


        model_filename = item_detail_form.get_chosen_value_by_name("model_filename")
        model_function_name = item_detail_form.get_chosen_value_by_name("model_function_name")

        my_module = importlib.import_module(model_filename)
        input_data = 123

        my_module.pricing_model(input_data)

        #variable_handler.update_data_in_variable_explorer(glc)
        """

        return (code.format(input_data='"' + input_data + '"', model_filename='"' + model_filename + '"',
                            model_function_name='"' + model_function_name + '"'))

    def export_imports(self, *args):
        imports = ["import importlib"]
        return (imports)


model_handlers_dict = {
    "PythonScript": PythonScriptHandler(),
    "LoadPythonScript": LoadPythonScriptHandler(),
    "JupyterScript": JupyterScriptHandler(),
    "LoadJupyterScript": LoadJupyterScriptHandler(),
    "TrainModel": TrainModelHandler(),
    #"PredictModelValues": PredictModelValuesHandler()
}