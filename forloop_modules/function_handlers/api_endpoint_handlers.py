


from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm


from forloop_modules.globals.variable_handler import variable_handler, LocalVariable
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList

import json
from fastapi import FastAPI

class FastAPIEndpointHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'FastAPIEndpoint'
        self.fn_name = 'FastAPI Endpoint'

        self.type_category = ntcm.categories.variable

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Endpoint URL")
        fdl.entry(name="url", text="", input_types=["str"], required=True, row=1)
        fdl.label("HTTP Method")
        fdl.combobox(name="method", options=["GET", "POST", "PUT", "DELETE"], required=True, row=2)
        fdl.label("Request Body")
        fdl.textarea(name="request_body", text="", row=3)
        fdl.label("New variable name")
        fdl.entry(name="new_variable_name", text="", input_types=["str"], row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, url, method, request_body, new_variable_name):
        app = FastAPI()

        @app.route(url, methods=[method])
        def handle_request():
            try:
                body = json.loads(request_body) if request_body else {}
                # Your code to handle the request and generate the response
                response = {"message": "Endpoint executed successfully"}

                return response
            except Exception as e:
                return {"error": str(e)}

        # Start the FastAPI app server and make a request to the generated endpoint
        # You can customize the host, port, and other settings as needed
        # In this example, we assume you have a separate mechanism to handle the server execution

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        method = node_detail_form.get_chosen_value_by_name("method", variable_handler)
        request_body = node_detail_form.get_chosen_value_by_name("request_body", variable_handler)
        new_variable_name = node_detail_form.get_chosen_value_by_name("new_variable_name", variable_handler)

        self.direct_execute(url, method, request_body, new_variable_name)

    def export_code(self, node_detail_form):
        url = node_detail_form.get_variable_name_or_input_value_by_element_name("url", is_input_variable_name=True)
        method = node_detail_form.get_variable_name_or_input_value_by_element_name("method")
        request_body = node_detail_form.get_variable_name_or_input_value_by_element_name("request_body")
        new_variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_variable_name", is_input_variable_name=True)

        code = f"""
@app.route("{url}", methods=["{method}"])
def handle_request():
    try:
        body = json.loads('{request_body}') if '{request_body}' else {{}}
        # Your code to handle the request and generate the response
        response = {{"message": "Endpoint executed successfully"}}

        return response
    except Exception as e:
        return {{"error": str(e)}}
"""

        return code

    def export_imports(self, *args):
        imports = ["from fastapi import FastAPI", "import json"]
        return imports





import os

class SystemctlServiceHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'SystemctlService'
        self.fn_name = 'Systemctl Service'

        self.type_category = ntcm.categories.system

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Service Name")
        fdl.entry(name="service_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("Executable Path")
        fdl.entry(name="executable_path", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Create Service", focused=True)

        return fdl

    def execute(self, node_detail_form):
        service_name = node_detail_form.get_chosen_value_by_name("service_name", variable_handler)
        executable_path = node_detail_form.get_chosen_value_by_name("executable_path", variable_handler)

        self.create_service(service_name, executable_path)

    def create_service(self, service_name, executable_path):
        service_content = f"""[Unit]
Description={service_name}
After=network.target

[Service]
ExecStart={executable_path}
Restart=always
User=root

[Install]
WantedBy=multi-user.target
"""

        service_file_path = f"/etc/systemd/system/{service_name}.service"

        try:
            with open(service_file_path, "w") as f:
                f.write(service_content)

            # Enable and start the service using systemctl
            os.system(f"systemctl enable {service_name}")
            os.system(f"systemctl start {service_name}")

            print(f"Service '{service_name}' created successfully.")
        except Exception as e:
            print(f"Failed to create service '{service_name}': {str(e)}")

    def export_code(self, node_detail_form):
        service_name = node_detail_form.get_variable_name_or_input_value_by_element_name("service_name", is_input_variable_name=True)
        executable_path = node_detail_form.get_variable_name_or_input_value_by_element_name("executable_path", is_input_variable_name=True)

        code = f"""service_content = '''
[Unit]
Description={service_name}
After=network.target

[Service]
ExecStart={executable_path}
Restart=always
User=root

[Install]
WantedBy=multi-user.target
'''

service_file_path = '/etc/systemd/system/{service_name}.service'

try:
    with open(service_file_path, 'w') as f:
        f.write(service_content)

    # Enable and start the service using systemctl
    os.system(f'systemctl enable {service_name}')
    os.system(f'systemctl start {service_name}')

    print(f'Service "{service_name}" created successfully.')
except Exception as e:
    print(f'Failed to create service "{service_name}": {str(e)}')
"""

        return code

    def export_imports(self, *args):
        imports = ["import os"]
        return imports