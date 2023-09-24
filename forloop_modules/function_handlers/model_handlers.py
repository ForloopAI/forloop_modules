import os
import importlib

from tkinter.filedialog import askopenfile

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.globals.variable_handler import variable_handler

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

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File path:")
        fdl.entry(name="file_name", text="./my_python_file.py", required=True, input_types=["str"], row=1)
        fdl.button(function=self.open_python_script, function_args=node_detail_form, text="Look up file", enforce_required=False)
        fdl.label("Load and run")
        fdl.checkbox(name="load_and_run", bool_value=False, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_python_script(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('Python scripts', '*.py')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='file_name', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("file_name", variable_handler)
        load_and_run = node_detail_form.get_chosen_value_by_name("load_and_run", variable_handler)

        if filename and os.path.isfile(filename):
            
            # short_filename = filename.split("/")[-1]
            
            # py_script_image = self.itm.new_image_via_API([400, 180],"PythonScript")  # ,label_text=short_filename
            ncrb.new_node(pos=[400, 180], typ="PythonScript")
            # py_script_image.item_detail_form.elements[2].text = filename
            
            if load_and_run:
                self._run_python_script(filename)

        else:
            flog.error(f"Given path `{filename}` is not a file")
            return None

    def direct_execute(self, file_name):
        
        self._run_python_script(file_name)        

    def _run_python_script(self, filename: str):

        try:
            os.system(f'python "{filename}"')
        except Exception as e:
            print("RunPythonScript Error: ", e)


class LoadJupyterScriptHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadJupyterScript"
        self.fn_name = "Load Jupyter Script"

        self.type_category = ntcm.categories.model

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File path:")
        fdl.entry(name="file_name", text="./jupyter_ntb.ipynb", required=True, input_types=["str"], row=1)
        fdl.button(function=self.open_jupyter_script, function_args=node_detail_form, text="Look up file", enforce_required=False)
        fdl.label("Load and run")
        fdl.checkbox(name="load_and_run", bool_value=False, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

        # fdl = FormDictList()
        # fdl.label("Jupyter Notebook Script")
        # fdl.label("Script filename")
        # fdl.entry(name="script", text="script.py", input_types=["str"], row=1)

        # return fdl

    def open_jupyter_script(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('Jupyter notebooks', '*.ipynb')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='file_name', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("file_name", variable_handler)
        load_and_run = node_detail_form.get_chosen_value_by_name("load_and_run", variable_handler)

        if filename and os.path.isfile(filename):
            
            # short_filename = filename.split("/")[-1]
            
            # jupyter_image = self.itm.new_image_via_API([400, 180],"JupyterScript",label_text=short_filename)
            ncrb.new_node(pos=[500, 300], typ="JupyterScript")
            # jupyter_image.item_detail_form.elements[2].text = filename
            
            if load_and_run:
                self._run_jupyter_script(filename)

        else:
            flog.error(f"Given path `{filename}` is not a file")
            return None

    def direct_execute(self, filename):

        self._run_jupyter_script(filename)

        # command = "runipy"
        # location = "C:\\Users\\EUROCOM\\Documents\\Git\\ForloopAI\\forloop_platform_dominik\\"

        # os.chdir(location)
        # os.system("start cmd cd " + location + script + " /k " + command + " " + location + script)

    def _run_jupyter_script(self, filename):

        # short_filename = filename.split("/")[-1]
        # run_path = filename.replace(short_filename, "")
        
        # notebook_filename_out = f'executed_{short_filename}'
        # notebook_filename_out = os.path.join(run_path, notebook_filename_out)

        # with open(filename) as f:
        #     nb = nbformat.read(f, as_version=4)

        # ep = ExecutePreprocessor(timeout=600, kernel_name='python3')

        # try:
        #     out = ep.preprocess(nb, {'metadata': {'path': run_path}})
        # except CellExecutionError:
        #     out = None
        #     msg = 'Error executing the notebook "%s".\n\n' % filename
        #     msg += 'See notebook "%s" for the traceback.' % notebook_filename_out
        #     print(msg)
        #     raise
        # finally:
        #     with open(notebook_filename_out, mode='w', encoding='utf-8') as f:
        #         nbformat.write(nb, f)

        try:
            os.system(f"jupyter nbconvert --to notebook --execute {filename}")
            #os.system(f'runipy "{filename}"')
        except Exception as e:
            print("RunJupyterScript Error: ", e)


class TrainModelHandler:
    def __init__(self):
        self.icon_type = 'TrainModel'
        self.fn_name = 'Train Model'

        self.type_category = ntcm.categories.model

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