import requests
import json
import ast

from tkinter.filedialog import askopenfile
from pathlib import Path
from typing import Literal, Optional

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList 
from forloop_modules.globals.variable_handler import variable_handler

from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler


def parse_api_additional_params(additional_params):
    """
    Parses API additional params (headers, query params, cookies).
    Different behaviour is expected depending on input type:
        - dict: default expected type (is set up in input_types), e.g. {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/00.0", 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest'}
        - str: in case value was provided as variable from variable explorer, it would be passed to execute as a string -> need to load it
    """

    if not additional_params:
        return None

    if isinstance(additional_params, str):
        try:
            additional_params_value = ast.literal_eval(additional_params)
        except:
            flog.warning(f'Value {additional_params} could not be recognised as well-formatted dictionary and therefore will not be used in request.')
            additional_params_value = None

    elif isinstance(additional_params, dict):
        additional_params_value = additional_params
    else:
        flog.warning(f'Provided value for {additional_params} is neither type of dict, nor string, but {type(additional_params)} and therefore will not be used in request.')
        additional_params_value = None

    return additional_params_value

def create_requests_call_code(method: Literal["get", "post", "put", "delete"], url: str, headers: Optional[dict] = None, 
                              parameters: Optional[dict] = None, cookies: Optional[dict] = None, 
                              new_variable_name: Optional[str] = None, data_variable_name: Optional[str] = None) -> str:
    """Creates a complete code of selected HTTP request with all non-empty arguments present in the call.

    Args:
        method (Literal["get", "post", "put", "delete"]): API call type (requests method)
        url (str): Website url (user input)
        headers (Optional[dict], optional): HTTP headers (user input). Defaults to None.
        parameters (Optional[dict], optional): HTTP parameters (user input). Defaults to None.
        cookies (Optional[dict], optional): Cookies settings (user input). Defaults to None.
        new_variable_name (Optional[str], optional): Name of a new variable containing 'response.content' in the code. 
                                                     Defaults to None.
        data_variable_name (Optional[str], optional): Name of a variable storing data loaded from a file in the code. 
                                                      Defaults to None.

    Returns:
        str: Complete HTTP request code with non-empty arguments filled in as keyword arguments.
        
    Example:
        Input:
            method = "post"
            url = "forloop.ai/blog"
            data_variable_name = "test_data"
            new_variable_name = "test_var"
            
        Output:
            test_var = requests.post(url="forloop.ai/blog", data=test_data)
    """    
    arguments = {
        "headers": headers,
        "parameters": parameters,
        "cookies": cookies,
        "data": data_variable_name
    }
    
    code = f"response = requests.{method}(url={url}"
    
    for arg_name, arg_value in arguments.items():
        arg_value_is_non_empty = arg_value is not None and str(arg_value).strip() and str(arg_value).strip() != '""'
        if arg_value_is_non_empty:
            code += f", {arg_name}={arg_value}"
            
    code += ")\n"
    
    if new_variable_name is not None:
        code += f'{new_variable_name} = response.json()'
    
    return code

def create_data_extraction_from_file_code(filename: str, data_variable_name: str = "data"):
    """Creates a code snippet for opening a file and storing data from it.

    Args:
        filename (str): File path.
        data_variable_name (str, optional): A name of a variable which will store the data from the file. Defaults to "data".

    Returns:
        tuple[str, str]: A code snippet and a name of the "data variable".
    """    
    if filename and filename != '""':
        code = f'with open({filename}, "r") as file:\n    {data_variable_name} = file.read()\n\n\n'
    else:
        data_variable_name = None
        code = ""
        
    return code, data_variable_name
    

class GetRequestHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "GetRequest"
        self.fn_name = "Get Request"

        self.type_category = ntcm.categories.api

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Get API Request")
        fdl.label("URL")
        fdl.entry(name="url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Headers")
        fdl.entry(name="headers", text="", input_types=["dict"], row=2)
        fdl.label("Parameters")
        fdl.entry(name="parameters", text="", input_types=["dict"], row=3)
        fdl.label("Cookies")
        fdl.entry(name="cookies", text="", input_types=["dict"], row=4)
        fdl.label("Save as")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=5)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def get_api(self, url, headers, parameters, cookies):

        if "http" not in url:
            url = "http://" + url

        try:
            response = requests.get(url=url, params=parameters, headers=headers, cookies=cookies)
        except Exception as e:
            flog.error(f"Error while making GET request: {e}")
            return None

        try:
            result = response.json()
        except json.decoder.JSONDecodeError:
            result = None

        return result

    def direct_execute(self, url, headers, parameters, cookies, new_var_name):
        headers = parse_api_additional_params(headers)
        parameters = parse_api_additional_params(parameters)
        cookies = parse_api_additional_params(cookies)

        response = self.get_api(url=url, headers=headers, parameters=parameters, cookies=cookies)
        flog.info(f'GET request response: {response}')

        variable_handler.new_variable(new_var_name, response)
        #variable_handler.update_data_in_variable_explorer(glc)

    def execute_with_params(self, params):
        url = params["url"]
        headers = params["headers"]
        parameters = params["parameters"]
        cookies = params["cookies"]
        new_var_name = params["new_var_name"]

        self.direct_execute(url, headers, parameters, cookies, new_var_name)

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        headers = node_detail_form.get_chosen_value_by_name("headers", variable_handler)
        parameters = node_detail_form.get_chosen_value_by_name("parameters", variable_handler)
        cookies = node_detail_form.get_chosen_value_by_name("cookies", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(url, headers, parameters, cookies, new_var_name)

    def export_code(self, node_detail_form):
        url = node_detail_form.get_variable_name_or_input_value_by_element_name("url")
        header = node_detail_form.get_variable_name_or_input_value_by_element_name("headers")
        parameters = node_detail_form.get_variable_name_or_input_value_by_element_name("parameters")
        cookies = node_detail_form.get_variable_name_or_input_value_by_element_name("cookies")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code = create_requests_call_code(method="get", url=url, headers=header, parameters=parameters, 
                                         cookies=cookies, new_variable_name=new_var_name)

        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class PostRequestHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "PostRequest"
        self.fn_name = "Post Request"

        self.type_category = ntcm.categories.api

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Post API Request")
        fdl.label("URL")
        fdl.entry(name="url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Headers")
        fdl.entry(name="headers", text="", input_types=["dict"], row=2)
        fdl.label("Parameters")
        fdl.entry(name="parameters", text="", input_types=["dict"], row=3)
        fdl.label("Cookies")
        fdl.entry(name="cookies", text="", input_types=["dict"], row=4)
        fdl.label("Data")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=5)
        fdl.button(function=self.open_data_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name="lookup_txt_csv_file")
        fdl.label("Save as ")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=7)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_data_file(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('Text files', '*.txt'), ('CSV files', '*.csv')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def post_api(self, url, headers, parameters, cookies, data):

        if "http" not in url:
            url = "http://" + url

        try:
            response = requests.post(url=url, data=data, params=parameters, headers=headers, cookies=cookies)
        except Exception as e:
            flog.error(f"Error while making POST request: {e}")
            return None

        try:
            result = response.json()
        except json.decoder.JSONDecodeError:
            result = None

        return result

    def direct_execute(self, url, headers, parameters, cookies, filename, new_var_name):
        headers = parse_api_additional_params(headers)
        parameters = parse_api_additional_params(parameters)
        cookies = parse_api_additional_params(cookies)

        with Path(filename).open(mode='r') as f:
            data = f.read()

        response = self.post_api(url=url, headers=headers, parameters=parameters, data=data, cookies=cookies)
        flog.info(f'POST request response: {response}')

        variable_handler.new_variable(new_var_name, response)
        #variable_handler.update_data_in_variable_explorer(glc)

    def execute_with_params(self, params):
        url = params["url"]
        headers = params["headers"]
        parameters = params["parameters"]
        cookies = params["cookies"]
        filename = params["filename"]
        new_var_name = params["new_var_name"]

        self.direct_execute(url, headers, parameters, cookies, filename, new_var_name)

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        headers = node_detail_form.get_chosen_value_by_name("headers", variable_handler)
        parameters = node_detail_form.get_chosen_value_by_name("parameters", variable_handler)
        cookies = node_detail_form.get_chosen_value_by_name("cookies", variable_handler)
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(url, headers, parameters, cookies, filename, new_var_name)

    def export_code(self, node_detail_form):
        url = node_detail_form.get_variable_name_or_input_value_by_element_name("url")
        headers = node_detail_form.get_variable_name_or_input_value_by_element_name("headers")
        parameters = node_detail_form.get_variable_name_or_input_value_by_element_name("parameters")
        cookies = node_detail_form.get_variable_name_or_input_value_by_element_name("cookies")
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code, data_variable_name = create_data_extraction_from_file_code(filename=filename)
            
        code += create_requests_call_code(method="post", url=url, headers=headers, parameters=parameters, 
                                          cookies=cookies, new_variable_name=new_var_name, data_variable_name=data_variable_name)

        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class DeleteRequestHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "DeleteRequest"
        self.fn_name = "Delete Request"

        self.type_category = ntcm.categories.api

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Delete API Request")
        fdl.label("URL")
        fdl.entry(name="url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Headers")
        fdl.entry(name="headers", text="", input_types=["dict"], row=2)
        fdl.label("Parameters")
        fdl.entry(name="parameters", text="", input_types=["dict"], row=3)
        fdl.label("Cookies")
        fdl.entry(name="cookies", text="", input_types=["dict"], row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def delete_api(self, url, headers, parameters, cookies):

        if "http" not in url:
            url = "http://" + url

        try:
            response = requests.delete(url=url, params=parameters, headers=headers, cookies=cookies)
        except Exception as e:
            flog.error(f"Error while making DELETE request: {e}")
            return None

        try:
            result = response.json()
        except json.decoder.JSONDecodeError:
            result = None

        return result

    def direct_execute(self, url, headers, parameters, cookies):
        headers = parse_api_additional_params(headers)
        parameters = parse_api_additional_params(parameters)
        cookies = parse_api_additional_params(cookies)

        response = self.delete_api(url=url, headers=headers, parameters=parameters, cookies=cookies)
        flog.info(f'DELETE request response: {response}')

    def execute_with_params(self, params):
        url = params["url"]
        headers = params["headers"]
        parameters = params["parameters"]
        cookies = params["cookies"]

        self.direct_execute(url, headers, parameters, cookies)

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        headers = node_detail_form.get_chosen_value_by_name("headers", variable_handler)
        parameters = node_detail_form.get_chosen_value_by_name("parameters", variable_handler)
        cookies = node_detail_form.get_chosen_value_by_name("cookies", variable_handler)

        self.direct_execute(url, headers, parameters, cookies)

    def export_code(self, node_detail_form):
        url = node_detail_form.get_variable_name_or_input_value_by_element_name("url")
        headers = node_detail_form.get_variable_name_or_input_value_by_element_name("headers")
        parameters = node_detail_form.get_variable_name_or_input_value_by_element_name("parameters")
        cookies = node_detail_form.get_variable_name_or_input_value_by_element_name("cookies")

        code = create_requests_call_code(method="delete", url=url, headers=headers, parameters=parameters, 
                                          cookies=cookies)
        
        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class PutRequestHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "PutRequest"
        self.fn_name = "Put Request"

        self.type_category = ntcm.categories.api

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Put API Request")
        fdl.label("URL")
        fdl.entry(name="url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Headers")
        fdl.entry(name="headers", text="", input_types=["dict"], row=2)
        fdl.label("Parameters")
        fdl.entry(name="parameters", text="", input_types=["dict"], row=3)
        fdl.label("Cookies")
        fdl.entry(name="cookies", text="", input_types=["dict"], row=4)
        fdl.label("Data")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=5)
        fdl.button(function=self.open_data_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name="lookup_txt_csv_file")
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_data_file(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('Text files', '*.txt'), ('CSV files', '*.csv')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def put_api(self, url, headers, parameters, cookies, data):

        if "http" not in url:
            url = "http://" + url

        try:
            response = requests.put(url=url, params=parameters, headers=headers, cookies=cookies, data=data)
        except Exception as e:
            flog.error(f"Error while making PUT request: {e}")
            return None

        try:
            result = response.json()
        except json.decoder.JSONDecodeError:
            result = None

        return result

    def direct_execute(self, url, headers, parameters, cookies, filename):
        headers = parse_api_additional_params(headers)
        parameters = parse_api_additional_params(parameters)
        cookies = parse_api_additional_params(cookies)

        with Path(filename).open(mode='r') as f:
            data = f.read()

        response = self.put_api(url=url, headers=headers, parameters=parameters, cookies=cookies, data=data)
        flog.info(f'PUT request response: {response}')

    def execute_with_params(self, params):
        url = params["url"]
        headers = params["headers"]
        parameters = params["parameters"]
        cookies = params["cookies"]
        filename = params["filename"]

        self.direct_execute(url, headers, parameters, cookies, filename)

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        headers = node_detail_form.get_chosen_value_by_name("headers", variable_handler)
        parameters = node_detail_form.get_chosen_value_by_name("parameters", variable_handler)
        cookies = node_detail_form.get_chosen_value_by_name("cookies", variable_handler)
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)

        self.direct_execute(url, headers, parameters, cookies, filename)

    def export_code(self, node_detail_form):
        url = node_detail_form.get_variable_name_or_input_value_by_element_name("url")
        headers = node_detail_form.get_variable_name_or_input_value_by_element_name("headers")
        parameters = node_detail_form.get_variable_name_or_input_value_by_element_name("parameters")
        cookies = node_detail_form.get_variable_name_or_input_value_by_element_name("cookies")
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        
        code, data_variable_name = create_data_extraction_from_file_code(filename=filename)
        code += create_requests_call_code(method="put", url=url, headers=headers, parameters=parameters, cookies=cookies, 
                                         data_variable_name=data_variable_name)

        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


api_handlers_dict = {
    "GetRequest": GetRequestHandler(),
    "PostRequest": PostRequestHandler(),
    "DeleteRequest": DeleteRequestHandler(),
    "PutRequest": PutRequestHandler()
}