import importlib
import importlib.util
import os
import re
import subprocess
import sys
from typing import Literal

from e2b_code_interpreter import CodeInterpreter

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
from forloop_modules.utils import synchronization_flags as sf


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
        fdl.entry(
            name="file_path",
            text="",
            required=True,
            type="file",
            file_types=file_types,
            row=1,
        )
        fdl.button(
            function=self.execute,
            function_args=node_detail_form,
            text="Execute",
            frontend_implementation=True,
            focused=True,
        )

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
        fdl.entry(
            name="file_path",
            text="",
            required=True,
            type="file",
            file_types=file_types,
            row=1,
        )
        fdl.button(
            function=self.execute,
            function_args=node_detail_form,
            text="Execute",
            frontend_implementation=True,
            focused=True,
        )

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

        super().__init__()

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        script_names = []
        
        try:
            response = ncrb.get_all_scripts()
        
            if response.status_code == 200:
                scripts = response.json()["result"]["scripts"]
                
                # TODO: Using aet.project_uid is ok on local but incorrect in general --> change
                # when allowed to run on remote
                script_names = [
                    script["script_name"]
                    for script in scripts
                    if script["project_uid"] == aet.project_uid
                ]
        except Exception as e:
            flog.warning(e)
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Script:")
        fdl.combobox(name="script_name", options=script_names, row=1)
        fdl.button(
            function=self.execute,
            function_args=node_detail_form,
            text="Execute",
            focused=True,
        )

        return fdl

    def execute(self, node_detail_form):
        script_name = node_detail_form.get_chosen_value_by_name("script_name", variable_handler)
        
        self.direct_execute(script_name)

    def direct_execute(self, script_name):
        """
        DANGER: The code runs without any checks! In production, use the E2B solution only!
        """

        script = su.get_script_by_name(script_name)
        script_text = script.get("text", "")

        variable_handler.new_variable("script_stdout", "", is_result=True)
        variable_handler.new_variable("script_stderr", "", is_result=True)

        if sf.E2B_API_KEY:
            ### Experimental E2B solution:
            self._execute_python_script_with_e2b(script_text=script_text)
        else:
            ### Local execution with stdout/stderr streaming -- good for testing instead of E2B
            self._execute_python_script_with_streaming(script_text=script_text)

            ## Local execution without stdout/stderr streaming -- stdout/stderr saved after
            ## pipeline is finished
            # self._execute_python_script(script_text=script_text)

    def _install_package(self, package_name: str):
        """Install a package using pip and the current python executable."""
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

    def _uninstall_package(self, package_name: str):
        """Uninstall a package using pip and the current python executable."""
        subprocess.check_call(
            [sys.executable, "-m", "pip", "uninstall", "-y", package_name]
        )

    def _get_imports_from_script(self, script_text: str):
        """
        Parse the script to extract imported modules.
        """
        imports = set()

        # Regex to match import statements
        import_pattern = re.compile(r"^\s*(?:import|from)\s+([a-zA-Z_][\w]*)", re.MULTILINE)

        for match in import_pattern.finditer(script_text):
            imports.add(match.group(1))

        return imports

    def _is_module_installed(self, module_name: str):
        """
        Check if a module is installed by attempting to find its spec.
        """
        return importlib.util.find_spec(module_name) is not None

    def _save_output_line_to_result(self, output_mode: Literal["stdout", "stderr"], line: str):
        """
        Adds a current stdout/stderr line to script_stdout/script_stderr result variable.

        Args:
            output_mode (Literal["stdout", "stderr"]): Specifies the type of output to save.
            line (str): A single line of stdout/stderr output obtained from script execution stream.
        """
        line = line.replace("'", '"')
        var_name = f"script_{output_mode}"
        curr_var = variable_handler.get_variable_by_name(var_name).get("value", "")
        curr_var += line

        variable_handler.new_variable(var_name, curr_var, is_result=True)

    def _execute_python_script(self, script_text: str):
        """
        Executes Python script (obtained from FL Script object) text via subprocess.Popen method.

        Missing libraries, if present in the script, are installed via pip and uninstalled after the
        execution.

        Args:
            script_text (str): Contents of .py script to be executed.

        Raises:
            SoftPipelineError: Raised in case of an Exception during script execution.
        """

        missing_libs = []

        try:
            # Get list of imports from the script
            imports = self._get_imports_from_script(script_text)

            # Check if each module is installed and install the missing ones
            for _import in imports:
                if not self._is_module_installed(_import):
                    self._install_package(_import)
                    missing_libs.append(_import)

            process = subprocess.Popen(
                [sys.executable, "-u", "-c", script_text],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = process.communicate()

            if stdout:
                variable_handler.new_variable("script_stdout", stdout, is_result=True)
            if stderr:
                variable_handler.new_variable("script_stderr", stderr, is_result=True)

        except Exception as e:
            raise SoftPipelineError(f"Error while executing the script: {e}")

        finally:
            process.wait()

            # Uninstall the installed packages after use
            for lib in missing_libs:
                self._uninstall_package(lib)

    def _execute_python_script_with_streaming(self, script_text: str):
        """
        Executes Python script (obtained from FL Script object) text via subprocess.Popen method
        with continuous streaming of stdout and stderr.

        Missing libraries, if present in the script, are installed via pip and uninstalled after the
        execution.

        Args:
            script_text (str): Contents of .py script to be executed.

        Raises:
            SoftPipelineError: Raised in case of an Exception during script execution.
        """

        def _stream_output(process: subprocess.Popen):
            """
            Streams stdout and stderr line by line in real-time and saves them into result vars.

            Args:
                process (subprocess.Popen): Popen process execution a python script.
            """

            for stdout_line in iter(process.stdout.readline, ""):
                if stdout_line:
                    self._save_output_line_to_result(output_mode="stdout", line=stdout_line)
            for stderr_line in iter(process.stderr.readline, ""):
                if stderr_line:
                    self._save_output_line_to_result(output_mode="stderr", line=stderr_line)
        
        missing_libs = []

        try:
            # Get list of imports from the script
            imports = self._get_imports_from_script(script_text)

            # Check if each module is installed and install the missing ones
            for _import in imports:
                if not self._is_module_installed(_import):
                    self._install_package(_import)
                    missing_libs.append(_import)

            # Execute the script in a subprocess
            process = subprocess.Popen(
                [sys.executable, "-u", "-c", script_text],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Stream stdout and stderr of the script in real-time into result vars
            _stream_output(process)

        except Exception as e:
            raise SoftPipelineError(f"Error while executing the script: {e}")

        finally:
            process.stdout.close()
            process.stderr.close()
            process.wait()

            # Uninstall the installed packages after use
            for lib in missing_libs:
                self._uninstall_package(lib)

    def _execute_python_script_with_e2b(self, script_text: str):
        """
        Executes Python script (obtained from FL Script object) text in an E2B code interpreter.

        E2B code interpreter (sandbox) is described in their docs: https://e2b.dev/docs
        Missing libraries, if present in the script, are installed via pip (in the E2B sandbox,
        not in our environment).

        Important: E2B_API_KEY must be present in .env file in root for successful operation.

        Args:
            script_text (str): Contents of .py script to be executed.

        Raises:
            SoftPipelineError: Raised in case of an Exception during E2B execution.
        """

        try:
            # Get list of imports from the script
            imports = self._get_imports_from_script(script_text)

            # TODO: Enable when python 3.10+ is supported
            # imports = [
            #     _import for _import in imports if _import not in sys.stdlib_module_names
            # ]

            with CodeInterpreter(api_key=sf.E2B_API_KEY) as sandbox:
                for _import in imports:
                    sandbox.notebook.exec_cell(f"!pip install {_import}")

                exec = sandbox.notebook.exec_cell(
                    script_text,
                    on_stderr=lambda stderr: self._save_output_line_to_result(
                        output_mode="stderr", line=str(stderr)
                    ),
                    on_stdout=lambda stdout: self._save_output_line_to_result(
                        output_mode="stdout", line=str(stdout)
                    ),
                )

                if exec.error:
                    self._save_output_line_to_result(
                        output_mode="stderr", line=exec.error.traceback
                    )

        except Exception as e:
            raise SoftPipelineError(f"Error while executing the script via E2B: {e}")


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
                
                # TODO: Using aet.project_uid is ok on local but incorrect in general --> change
                # when allowed to run on remote
                script_names = [
                    script["script_name"]
                    for script in scripts
                    if script["project_uid"] == aet.project_uid
                ]
        except Exception as e:
            flog.warning(e)
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Script:")
        fdl.combobox(name="script_name", options=script_names, row=1)
        fdl.button(
            function=self.execute,
            function_args=node_detail_form,
            text="Execute",
            focused=True,
        )

        return fdl

    def execute(self, node_detail_form):
        script_name = node_detail_form.get_chosen_value_by_name("script_name", variable_handler)
        
        self.direct_execute(script_name)

    def direct_execute(self, script_name):
        """
        DANGER: The code runs without any checks! 
        
        TODO: Solve security issues when running the code.
        """
        
        # HACK: Disable the execution of the node with some feedback for a user until we implement
        # security checks
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

class TrainModelHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'TrainModel'
        self.fn_name = 'Train Model'

        self.type_category = ntcm.categories.model
        self.docs_category = DocsCategories.control

        super().__init__()

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
        fdl.button(
            function=self.execute,
            function_args=node_detail_form,
            text="Execute",
            focused=True,
        )

        return fdl

    def execute(self, node_detail_form):
        """ TODO """
        pass

    def direct_execute(self):
        """ TODO """
        pass

    def export_code(self, node_detail_form):
        """ TODO """
        pass


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
        fdl.button(
            function=self.execute,
            function_args=node_detail_form,
            text="Execute",
            focused=True,
        )

        return fdl

    def execute(self, node_detail_form):
        model_filename = node_detail_form.get_chosen_value_by_name(
            "model_filename", variable_handler
        )
        model_function_name = node_detail_form.get_chosen_value_by_name(
            "model_function_name", variable_handler
        )

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

        return code.format(
            input_data='"' + input_data + '"',
            model_filename='"' + model_filename + '"',
            model_function_name='"' + model_function_name + '"',
        )

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