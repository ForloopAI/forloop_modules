import importlib
import os
import subprocess
import sys

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb
import forloop_modules.utils.script_utils as su
from forloop_modules.errors.errors import SoftPipelineError
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import (
    AbstractFunctionHandler,
)
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import (
    ntcm,
)
from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.globals.variable_handler import variable_handler


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

class RunPythonScriptHandler(AbstractFunctionHandler):
    """
    DANGER ZONE: This handler is allowed for local use only for now! Don't allow it in production!
    """
    
    def __init__(self):
        self.icon_type = "RunPythonScript"
        self.fn_name = "Run Python Script"

        self.type_category = ntcm.categories.model
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        script_names = []
        
        try:
            response = ncrb.get_all_scripts()
        
            if response.status_code == 200:
                scripts = response.json()["result"]["scripts"]
                
                # TODO: Using aet.project_uid is ok on local but incorrect in general --> change when allowed to run on remote
                script_names = [script["script_name"] for script in scripts if script["project_uid"] == aet.project_uid]
        except Exception as e:
            flog.warning(e)
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Script:")
        fdl.combobox(name="script_name", options=script_names, row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        script_name = node_detail_form.get_chosen_value_by_name("script_name", variable_handler)
        
        self.direct_execute(script_name)

    def direct_execute(self, script_name):
        """
        DANGER: The code runs without any checks!
        
        TODO 1: Solve security issues when running the code.
        TODO 2: Solve scanning for packages used by script and pip installing of the missing ones.
        """
        
        # HACK: Disable the execution of the node with some feedback for a user until we implement security checks
        # raise SoftPipelineError("Execution of this node is temporarily disabled.")
         
        script = su.get_script_by_name(script_name)
        script_text = script.get("text", "")
        
        ##### Experimental implementation
        self._execute_python_script(script_text=script_text)
        # self._execute_python_script_with_streaming(script_text=script_text)
        
        return
        
        random_id = su.generate_random_id()
        temp_file_name = f'temp_py_script_{random_id}.py'
        
        with open(temp_file_name, "w") as temp_file:
            temp_file.write(script_text)
            
        command = f'python3 {temp_file_name}'
        
        completed_process = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE, text=True)
        
        os.remove(temp_file_name)

        if completed_process.returncode == 0:
            output = completed_process.stdout
            flog.info(f"Command output:\n{output}")
        else:
            error_output = completed_process.stderr
            message = f'Executed script failed with the following traceback:\n{error_output}'
            flog.warning(message)
            
    def _install_package(self, package_name: str):
        """Install a package using pip and the current python executable."""
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

    def _uninstall_package(self, package_name: str):
        """Uninstall a package using pip and the current python executable."""
        subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", package_name])
            
    def _execute_python_script(self, script_text: str):
        # First attempt to run the script
        process = subprocess.Popen(
            [sys.executable, "-u", "-c", script_text],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            stdout, stderr = process.communicate()

            # Check for ImportError in stderr (if missing libraries)
            missing_libs = re.findall(r"No module named '(\w+)'", stderr)

            if missing_libs:
                for lib in missing_libs:
                    print(f"Installing missing library: {lib}")
                    self._install_package(lib)

                # Re-run the script after installing missing libraries
                process = subprocess.Popen(
                    [sys.executable, "-u", "-c", script_text],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                stdout, stderr = process.communicate()

                # Optional: uninstall the installed packages after use
                for lib in missing_libs:
                    print(f"Uninstalling library: {lib}")
                    self._uninstall_package(lib)

            # Print the final output
            if stdout:
                variable_handler.new_variable("script_stdout", stdout, is_result=True)
                print(f"stdout:\n{stdout}")
            if stderr:
                variable_handler.new_variable("script_stderr", stderr, is_result=True)
                print(f"stderr:\n{stderr}")

        except Exception as e:
            raise SoftPipelineError(f"Error while executing the script: {e}")

        finally:
            process.wait()

    def _execute_python_script_with_streaming(self, script_text: str):
        stdout_var_name = "script_stdout"
        stderr_var_name = "script_stderr"
        variable_handler.new_variable(stdout_var_name, "", is_result=True)
        variable_handler.new_variable(stderr_var_name, "", is_result=True)

        # Execute the script in real-time using the current Python interpreter
        process = subprocess.Popen(
            [sys.executable, "-u", "-c", script_text],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line-buffering for real-time output
        )

        # Reading stdout and stderr line by line
        try:
            # Print stdout in real-time with a delay
            for stdout_line in iter(process.stdout.readline, ""):
                # time.sleep(1)  # Delay before printing
                curr_stdout = variable_handler.get_variable_by_name(
                    stdout_var_name
                ).get("value", "")
                curr_stdout += stdout_line
                variable_handler.new_variable(
                    stdout_var_name, curr_stdout, is_result=True
                )
                print(f"stdout: {stdout_line}", end="")

            # Print stderr in real-time with a delay
            for stderr_line in iter(process.stderr.readline, ""):
                # time.sleep(1)  # Delay before printing
                curr_stderr = variable_handler.get_variable_by_name(
                    stderr_var_name
                ).get("value", "")
                curr_stderr += f"\n{stderr_line}"
                variable_handler.new_variable(
                    stderr_var_name, curr_stderr, is_result=True
                )
                print(f"stderr: {stderr_line}", end="")

        except Exception as e:
            print(f"Error while executing the script: {e}")

        finally:
            # Ensure the process has completed
            process.stdout.close()
            process.stderr.close()
            process.wait()
            
class RunJupyterScriptHandler(AbstractFunctionHandler):
    """
    DANGER ZONE: This handler is allowed for local use only for now! Don't allow it in production!
    """
    
    def __init__(self):
        self.icon_type = "RunJupyterScript"
        self.fn_name = "Run Jupyter Script"

        self.type_category = ntcm.categories.model
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        script_names = []
        
        try:
            response = ncrb.get_all_scripts()
        
            if response.status_code == 200:
                scripts = response.json()["result"]["scripts"]
                
                # TODO: Using aet.project_uid is ok on local but incorrect in general --> change when allowed to run on remote
                script_names = [script["script_name"] for script in scripts if script["project_uid"] == aet.project_uid]
        except Exception as e:
            flog.warning(e)
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Script:")
        fdl.combobox(name="script_name", options=script_names, row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        script_name = node_detail_form.get_chosen_value_by_name("script_name", variable_handler)
        
        self.direct_execute(script_name)

    def direct_execute(self, script_name):
        """
        DANGER: The code runs without any checks! 
        
        TODO: Solve security issues when running the code.
        """
        
        # HACK: Disable the execution of the node with some feedback for a user until we implement security checks
        raise SoftPipelineError("Execution of this node is temporarily disabled.")
    
        script = su.get_script_by_name(script_name)
        script_text = script.get("text", "")
        
        random_id = su.generate_random_id()
        temp_file_name = f'temp_jupyter_script_{random_id}.ipynb'
        
        with open(temp_file_name, "w") as temp_file:
            temp_file.write(script_text)
            
        command = f'jupyter nbconvert --to notebook --execute {temp_file_name}'
        
        completed_process = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE, text=True)
        os.remove(temp_file_name)

        if completed_process.returncode == 0:
            output = completed_process.stdout
            flog.info(f"Command output:\n{output}")
        else:
            error_output = completed_process.stderr
            message = f'Executed script failed with the following traceback:\n{error_output}'
            flog.warning(message)
            
        # NOTE: Alternative implementation, do not delete yet (if tempted to do so, ask Dominik first).
        # run_path = filename.replace(script_name, "")
        
        # notebook_filename_out = f'executed_{script_name}'
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
    "LoadPythonScript": LoadPythonScriptHandler(),
    "LoadJupyterScript": LoadJupyterScriptHandler(),
    "RunPythonScript": RunPythonScriptHandler(),
    "RunJupyterScript": RunJupyterScriptHandler(),
    "TrainModel": TrainModelHandler(),
    #"PredictModelValues": PredictModelValuesHandler()
}