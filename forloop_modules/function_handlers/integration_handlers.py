import requests
import gspread
import os
import ast
import base64
import pickle
import pandas as pd
import keepvariable.keepvariable_core as kv
import json
from typing import Literal, get_args
from enum import Enum
from pathlib import Path
from email.message import EmailMessage
from pyairtable import Table
from tkinter.filedialog import askopenfile
from fastapi import HTTPException

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient import discovery
from google.oauth2 import service_account

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.errors.errors import SoftPipelineError
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input
from forloop_modules.function_handlers.file_managment_handlers import file_managment_handlers_dict
from forloop_modules.utils.definitions import GOOGLE_API_SERVICES, GOOGLE_API_SERVICE_INFO
from forloop_modules.integrations.slack_integration import SlackApiError, get_channels_in_workspace, send_message_to_slack_direct_execute
from forloop_modules.function_handlers.auxilliary.auxiliary_functions import parse_comboentry_input, parse_google_sheet_id_from_url
from config.config import other_config #TODO Dominik: Circular dependency to forloop_platform repository # not ideal #Maybe solve with os.environ?


def connect_to_google_service(service_name: str):
    
    try:
        if service_name in GOOGLE_API_SERVICES:

            SCOPES = GOOGLE_API_SERVICE_INFO[service_name]['scopes']
            version = GOOGLE_API_SERVICE_INFO[service_name]['version']
            print("SCOPES ", SCOPES)
            print("VERSION ", version)
            # Variable creds will store the user access token.
            # If no valid token found, we will create one.
            creds = None

            # The file token.pickle contains the user access token.
            # Check if it exists
            if os.path.exists(f'{service_name}_token.pickle'):

                # Read the token from the file and store it in the variable creds
                with open(f'{service_name}_token.pickle', 'rb') as token:
                    creds = pickle.load(token)

            # If credentials are not available or are invalid, ask the user to log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file('config/credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)

                # Save the access token in token.pickle file for the next run
                with open(f'{service_name}_token.pickle', 'wb') as token:
                    pickle.dump(creds, token)

            # Connect to the Gmail API
            # service = build('gmail', 'v1', credentials=creds)
            service = build(service_name, version, credentials=creds)

            return service

    except Exception as e:
        flog.warning(f'Error while connecting to google service {e}')

        # TODO: Change to API popup call
        # message = "Connection Error. Please, try again."
        # glc.show_warning_popup_message(message)

        file_delete_handler = file_managment_handlers_dict["DeleteFile"]
        file_delete_handler.direct_execute("gmail_token.pickle")  #deletes token.pickle via DeleteFileHandler

############ INTEGRATION HANDLERS ######################

class CreateEmailBodyHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "CreateEmailBody"
        self.fn_name = "Create Email Body"

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations
    
    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("OpenAI API key:")
        fdl.entry(name="api_key", text="", input_types=["str"], required=True, row=1)
        fdl.label("Recipient name:")
        fdl.entry(name="recipient_name", text="", input_types=["str"], required=False, row=2)
        fdl.label("Topic:")
        fdl.entry(name="topic", text="", input_types=["str"], required=False, row=3)
        fdl.label("Sender name:")
        fdl.entry(name="sender_name", text="", input_types=["str"], required=False, row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl
    
    def execute(self, node_detail_form):
        api_key = node_detail_form.get_chosen_value_by_name("api_key", variable_handler)
        recipient_name = node_detail_form.get_chosen_value_by_name("recipient_name", variable_handler)
        topic = node_detail_form.get_chosen_value_by_name("topic", variable_handler)
        sender_name = node_detail_form.get_chosen_value_by_name("sender_name", variable_handler)

        self.direct_execute(api_key, recipient_name, topic, sender_name)
    
    def direct_execute(self, api_key, recipient_name, topic, sender_name):
        variable_name = "email_body"

        inp = Input()
        inp.assign("api_key", api_key)
        inp.assign("recipient_name", recipient_name)
        inp.assign("topic", topic)
        inp.assign("sender_name", sender_name)

        try:
            openai_completition = self.input_execute(inp)
        except Exception as e:
            openai_completition = "Error while creating email body"
            flog.warning(f'Error while creating email body {e}')
        
        variable_handler.new_variable(variable_name, openai_completition)
        # variable_handler.update_data_in_variable_explorer(glc)

    
    def input_execute(self, inp):
        
        headers = {"Content-Type":"application/json", "Authorization":"Bearer " + inp("api_key")}
        data = {"model": "text-davinci-003", "prompt": "Create a body of the mail\nName of the recipient:" + inp("recipient_name") + "\nTopic: " + inp("topic") + "\nSender name:" + inp("sender_name"), "max_tokens": 1000, "temperature": 0}
        response = requests.post("https://api.openai.com/v1/completions", headers=headers, json=data)
        output = json.loads(response.text)
        if response.status_code in (200, 201):
            print(data)
            print(output)
            openai_completition = str(output["choices"][0]["text"])
        else:
            openai_completition = str(output["error"]["message"])

        return openai_completition

    

class SlackNotificationHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "SlackNotification"
        self.fn_name = "Slack Notification"

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):
        
        channels = get_channels_in_workspace(slack_token=other_config.SLACK_TOKEN)

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Channel")
        fdl.comboentry(name="channel_name", text="team-collaboration", options=channels, row=1)
        fdl.label("Text:")
        fdl.entry(name="text", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        channel_name = node_detail_form.get_chosen_value_by_name("channel_name", variable_handler)
        channel_name = parse_comboentry_input(channel_name)
        
        text = node_detail_form.get_chosen_value_by_name("text", variable_handler)

        self.direct_execute(channel_name, text)

    def direct_execute(self, channel_name, text, file_name=None, *args):
        inp = Input()
        inp.assign("channel_name", channel_name)
        inp.assign("text", text)
        inp.assign("file_name", file_name)
        inp.assign("args", args)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.warning(e)

    def input_execute(self, inp):
        send_message_to_slack_direct_execute(other_config.SLACK_TOKEN, inp("channel_name"), inp("text"), inp("file_name"), inp("args"))
        
    def _post_message_to_slack(self, client, channel_id: str, text: str = 'Test message'):

        try:
            # Call the conversations.list method using the WebClient
            result = client.chat_postMessage(
                channel=channel_id,
                text=text
            )
            
            print(result)

        except SlackApiError as e:
            print(f"Error: {e}")
            
    def _post_file_in_slack_channel(self, client, file_name, channel_id: str, comment: str = "Hey, let's share! :robot_face:"):

        try:
            # Call the files.upload method using the WebClient
            # Uploading files requires the `files:write` scope
            result = client.files_upload(
                channels=channel_id,
                initial_comment=comment,
                file=file_name,
            )
            
            print(result)

        except SlackApiError as e:
            print(f"Error uploading file: {e}")

    def export_code(self, *args):
        code = """
        """
        return (code)

    def export_imports(self, *args):
        imports = []
        return imports


class EmailNotificationHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "EmailNotification"
        self.fn_name = "Email Notification"

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        try:
            login_email = kv.load_variable_safe(varname="login_email")
        except (FileNotFoundError, KeyError):
            login_email = ""

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("To")
        fdl.entry(name="recipient", text=login_email, input_types=["str"], required=True, row=1)
        fdl.label("Subject")
        fdl.entry(name="subject", text="", input_types=["str"], row=2)
        fdl.label("Message")
        fdl.entry(name="message", text="", input_types=["str"], required=True, row=3)
        fdl.label("Remember login")
        fdl.checkbox(name="remember_login", bool_value=True, row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        recipient = node_detail_form.get_chosen_value_by_name("recipient", variable_handler)
        subject = node_detail_form.get_chosen_value_by_name("subject", variable_handler)
        message = node_detail_form.get_chosen_value_by_name("message", variable_handler)
        remember_login = node_detail_form.get_chosen_value_by_name("remember_login", variable_handler)

        self.direct_execute(recipient, subject, message, remember_login)

    def direct_execute(self, recipient, subject, message, remember_login, attachment_filename = None):
        inp = Input()
        inp.assign("recipient", recipient)
        inp.assign("subject", subject)
        inp.assign("message", message)
        inp.assign("attachment_filename", attachment_filename)
        inp.assign("remember_login", remember_login)

        self.input_execute(inp)

    def input_execute(self, inp):

        service = connect_to_google_service('gmail')
        
        if service is None:
            flog.warning("Connection to Gmail was not established.")
            return
        
        service.users().getProfile(userId='me').execute()
        message = self.create_message(inp("recipient"), inp("subject"), inp("message"), inp("attachment_filename"))
        self.send_message(service, 'me', message)
        print("Message sent")

        ## remember_login == False ==> deletes gmail token (next time the user will have to login again)
        if not inp("remember_login"):
            self._delete_gmail_token()

    def _delete_gmail_token(self):
        file_delete_handler = file_managment_handlers_dict["DeleteFile"]
        file_delete_handler.direct_execute("gmail_token.pickle")

    # TODO: Solve partial duplication in 'email_notifications' and 'pipeline_email_trigger'
    def connect_to_gmail_api(self):

        SCOPES = ['https://www.googleapis.com/auth/gmail.compose']

        # Variable creds will store the user access token.
        # If no valid token found, we will create one.
        creds = None

        # The file token.pickle contains the user access token.
        # Check if it exists
        if os.path.exists('gmail_token.pickle'):

            # Read the token from the file and store it in the variable creds
            with open('gmail_token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # If credentials are not available or are invalid, ask the user to log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('config/credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

            # Save the access token in token.pickle file for the next run
            with open('gmail_token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        # Connect to the Gmail API
        service = build('gmail', 'v1', credentials=creds)

        return service

    def create_message(self, to, subject, message_text, attachment_filename = None):

        message = EmailMessage()
        message.set_content(message_text)

        message['to'] = to
        message['subject'] = subject

        if attachment_filename is not None:
            if os.path.isfile(attachment_filename):

                with open(attachment_filename, 'rb') as fp:
                    attachment_data = fp.read()
                
                attachment_name = os.path.basename(attachment_filename)
               
                message.add_attachment(attachment_data, maintype="application", subtype="octet-stream", filename=attachment_name)

        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        return {
            'raw': raw_message
        }


    def send_message(self, service, user_id, message):
        try:
            message = service.users().messages().send(userId=user_id, body=message).execute()
            print('Message Id: %s' % message['id'])
            return message
        except Exception as e:
            print('An error occurred: %s' % e)
            return None

    def export_code(self, node_detail_form):
        recipient = node_detail_form.get_variable_name_or_input_value_by_element_name("recipient")
        subject = node_detail_form.get_variable_name_or_input_value_by_element_name("subject")
        message = node_detail_form.get_variable_name_or_input_value_by_element_name("message")
        remember_login = node_detail_form.get_variable_name_or_input_value_by_element_name("remember_login")

        code = f"""
        SCOPES = ['https://www.googleapis.com/auth/gmail.compose']

        def create_message(to, subject, message_text):
            message = MIMEText(message_text)
            message['to'] = to
            message['subject'] = subject
            raw_message = base64.urlsafe_b64encode(message.as_string().encode("utf-8"))
            return {{
                'raw': raw_message.decode("utf-8")
            }}

        def send_message(service, user_id, message):
            try:
                message = service.users().messages().send(userId=user_id, body=message).execute()
                print('Message Id: %s' % message['id'])
                return message
            except Exception as e:
                print('An error occurred: %s' % e)
                return None

        # Variable creds will store the user access token.
        # If no valid token found, we will create one.
        creds = None

        # The file token.pickle contains the user access token.
        # Check if it exists
        if os.path.exists('token.pickle'):

            # Read the token from the file and store it in the variable creds
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # If credentials are not available or are invalid, ask the user to log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('config/credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

            # Save the access token in token.pickle file for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        # Connect to the Gmail API
        service = build('gmail', 'v1', credentials=creds)

        recipient = "{recipient}"
        subject = "{subject}"
        message = "{message}"
        message = create_message(recipient, subject, message)
        send_message(service, 'me', message)
        """
        return (code)

    def export_imports(self, *args):
        imports = ["import os", "import base64", "from email.mime.text import MIMEText", "import pickle", 
        "from googleapiclient.discovery import build", "from google_auth_oauthlib.flow import InstalledAppFlow",
        "from google.auth.transport.requests import Request"]
        return (imports)


class PipedriveConnectHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'PipedriveConnect'
        self.fn_name = 'Pipedrive Connect'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Company domain name")
        fdl.entry(name="company_domain_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("Pipedrive token")
        fdl.entry(name="pipedrive_token", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, com_dom_name, pip_token):
        global company_domain_name, pipedrive_token

        inp = Input()
        inp.assign("company_domain_name", com_dom_name)
        inp.assign("pipedrive_token", pip_token)

        try:
            company_domain_name, pipedrive_token = self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
    
    def input_execute(self, inp):
        company_domain_name = inp("company_domain_name")
        pipedrive_token = inp("pipedrive_token")

        print("READY TO CONNECT")

        return company_domain_name, pipedrive_token

    def execute_with_params(self, params):
        company_domain_name = params["company_domain_name"]
        pipedrive_token = params["pipedrive_token"]

        self.direct_execute(company_domain_name, pipedrive_token)

    def execute(self, node_detail_form):
        company_domain_name = node_detail_form.get_chosen_value_by_name("company_domain_name", variable_handler)
        pipedrive_token = node_detail_form.get_chosen_value_by_name("pipedrive_token", variable_handler)

        self.direct_execute(company_domain_name, pipedrive_token)

    def export_imports(self, *args):
        imports = []
        return (imports)


class PipedriveGetStagesHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'PipedriveGetStages'
        self.fn_name = 'Pipedrive Get Stages'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Save as")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def get_stages(self):
        global company_domain_name, pipedrive_token

        url = "https://" + company_domain_name + ".pipedrive.com/api/v1/stages?api_token=" + pipedrive_token

        response = requests.get(url)
        print(response)
        
        return (response.json())

    def direct_execute(self, variable_name):
        if variable_name in variable_handler.variables:
            inp = Input()
            
            try:
                stages = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"{e}")

            variable_handler.new_variable(variable_name, stages)
            #variable_handler.update_data_in_variable_explorer(glc)
    
    def input_execute(self, inp):
        stages = self.get_stages()

        return stages

    def execute_with_params(self, params):
        new_var_name = params["new_var_name"]

        if new_var_name == "":
            print("'Save as' entry field can not be empty.")
            return None

        self.direct_execute(new_var_name)

    def execute(self, node_detail_form):
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(new_var_name)

    def export_code(self, node_detail_form):
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code = f"""    
        {new_var_name} = requests.get("https://" + company_domain_name + ".pipedrive.com/api/v1/stages?api_token=" + pipedrive_token).json()
        """

        # return(code.format(new_var_name= '"' + new_var_name + '"'))
        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class PipedriveGetUsersHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'PipedriveGetUsers'
        self.fn_name = 'Pipedrive Get Users'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Save as")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def get_users(self):
        global company_domain_name, pipedrive_token

        url = "https://" + company_domain_name + ".pipedrive.com/api/v1/users?api_token=" + pipedrive_token

        response = requests.get(url)
        print(response)
        # print(response.content)
        return (response.json())

    def direct_execute(self, variable_name):
        if variable_name in variable_handler.variables:
            inp = Input()
            
            try:
                users = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"{e}")

            variable_handler.new_variable(variable_name, users)
            ##variable_handler.update_data_in_variable_explorer(glc)
    
    def input_execute(self, inp):
        users = self.get_users()

        return users

    def execute_with_params(self, params):
        new_var_name = params["new_var_name"]

        if new_var_name == "":
            print("'Save as' entry field can not be empty.")
            return None

        self.direct_execute(new_var_name)

    def execute(self, node_detail_form):
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(new_var_name)

    def export_code(self, node_detail_form):
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code = f"""    
        {new_var_name} = requests.get("https://" + company_domain_name + ".pipedrive.com/api/v1/users?api_token=" + pipedrive_token).json()
        """

        # return(code.format(new_var_name= '"' + new_var_name + '"'))
        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class PipedriveAddPersonHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'PipedriveAddPerson'
        self.fn_name = 'Pipedrive Add Person'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Name")
        fdl.entry(name="name", text="", input_types=["str"], required=True, row=1)
        fdl.label("E-mail")
        fdl.entry(name="email", text="", input_types=["str"], row=2)
        fdl.label("Phone number")
        fdl.entry(name="phone", text="", input_types=["str", "int"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def add_person(self, name, email, phone):
        global company_domain_name, pipedrive_token

        url = "https://" + company_domain_name + ".pipedrive.com/api/v1/persons?api_token=" + pipedrive_token

        payload = {"name": name, "email": email, "phone": phone}

        response = requests.post(url, data=payload)

        # print(response)
        # print(response.content)
        return (response.content)

    def direct_execute(self, name, email, phone):
        inp = Input()
        inp.assign("name", name)
        inp.assign("email", email)
        inp.assign("phone", phone)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
    
    def input_execute(self, inp):
        self.add_person(inp("name"), inp("email"), inp("phone"))
        print("Added person -- name: ", inp("name"), ", e-mail: ", inp("email"), ", phone: ", inp("phone"))

    def execute_with_params(self, params):
        name = params["name"]
        email = params["email"]
        phone = params["phone"]

        self.direct_execute(name, email, phone)

    def execute(self, node_detail_form):
        name = node_detail_form.get_chosen_value_by_name("name", variable_handler)
        email = node_detail_form.get_chosen_value_by_name("email", variable_handler)
        phone = node_detail_form.get_chosen_value_by_name("phone", variable_handler)

        self.direct_execute(name, email, phone)

    def export_code(self, node_detail_form):
        name = node_detail_form.get_variable_name_or_input_value_by_element_name("name")
        email = node_detail_form.get_variable_name_or_input_value_by_element_name("email")
        phone = node_detail_form.get_variable_name_or_input_value_by_element_name("phone")

        code = f"""      
        payload = dict([("name", {name}), ("email", {email}), ("phone", {phone})])
        requests.post("https://" + company_domain_name + ".pipedrive.com/api/v1/persons?api_token=" + pipedrive_token, data = payload)
        """

        # return(code.format(name= '"' + name + '"', email= '"' + email + '"', phone= phone))
        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class PipedriveAddDealHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'PipedriveAddDeal'
        self.fn_name = 'Pipedrive Add Deal'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Title")
        fdl.entry(name="title", text="", input_types=["str"], required=True, row=1)
        fdl.label("Stage ID")
        fdl.entry(name="stage_id", text="", input_types=["str", "int", "float"], required=True, row=2)
        fdl.label("User ID")
        fdl.entry(name="user_id", text="", input_types=["str", "int", "float"], required=True, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def add_deal(self, title, stage_id, user_id):
        global company_domain_name, pipedrive_token

        url = "https://" + company_domain_name + ".pipedrive.com/api/v1/deals?api_token=" + pipedrive_token

        payload = {"title": title, "stage_id": stage_id, "user_id": user_id}
        response = requests.post(url, data=payload)

        return (response.content)

    def direct_execute(self, title, stage_id, user_id):
        inp = Input()
        inp.assign("title", title)
        inp.assign("stage_id", stage_id)
        inp.assign("user_id", user_id)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
    
    def input_execute(self, inp):
        self.add_deal(inp("title"), inp("stage_id"), inp("user_id"))
        print("Added deal -- title: ", inp("title"), ", stage_id: ", inp("stage_id"), ", user_id: ", inp("user_id"))

    def execute_with_params(self, params):
        title = params["title"]
        stage_id = params["stage_id"]
        user_id = params["user_id"]

        self.direct_execute(title, stage_id, user_id)

    def execute(self, node_detail_form):
        title = node_detail_form.get_chosen_value_by_name("title", variable_handler)
        stage_id = node_detail_form.get_chosen_value_by_name("stage_id", variable_handler)
        user_id = node_detail_form.get_chosen_value_by_name("user_id", variable_handler)

        self.direct_execute(title, stage_id, user_id)

    def export_code(self, node_detail_form):
        title = node_detail_form.get_variable_name_or_input_value_by_element_name("title")
        stage_id = node_detail_form.get_variable_name_or_input_value_by_element_name("stage_id")
        user_id = node_detail_form.get_variable_name_or_input_value_by_element_name("user_id")

        code = f"""
        payload = dict([("title", {title}), ("stage_id", {stage_id}), ("user_id", {user_id})])   
        requests.post("https://" + company_domain_name + ".pipedrive.com/api/v1/deals?api_token=" + pipedrive_token, data = payload)
        """

        # return(code.format(title= '"' + title + '"', stage_id= '"' + stage_id + '"', user_id= user_id))
        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class PipedriveDeleteDealHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'PipedriveDeleteDeal'
        self.fn_name = 'Pipedrive Delete Deal'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Deal ID")
        fdl.entry(name="deal_id", text="", input_types=["str", "int", "float"], required=True, row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def delete_deal(self, deal_id):
        global company_domain_name, pipedrive_token

        url = "https://" + company_domain_name + ".pipedrive.com/api/v1/deals/" + str(
            deal_id) + "?api_token=" + pipedrive_token
        # payload={"stateId":1,"title":"CZ, Praha – CZ, Brno","currencyId":1,"loadingAddress":{"label":None,"placeId":None,"buildingNumber":None,"street":"Václavské náměstí","city":"Praha","zipCode":"110 00","countryCode":"CZ","gps":{"latitude":50.08528,"longitude":14.42623}},"loadingDates":{"isInterval":True,"openingTimeInMinutes":480,"closingTimeInMinutes":1200,"interval":{"referenceDate":"2021-05-21T00:00:00.000Z","includeWeekends":False}},"unloadingAddress":{"label":None,"placeId":None,"buildingNumber":None,"street":None,"city":"Brno","zipCode":"602 00","countryCode":"CZ","gps":{"latitude":49.19876,"longitude":16.59706}},"unloadingDates":{"isInterval":True,"openingTimeInMinutes":480,"closingTimeInMinutes":1200,"interval":{"referenceDate":"2021-05-28T00:00:00.000Z","includeWeekends":False}},"userTimeZone":120,"ftl":True,"packets":[{"weight":15000,"count":1,"itemTypeId":3,"length":13600,"width":2450,"height":2700}],"isDangerous":False,"isUnstackable":False,"hydraulicPlatform":False,"hydraulicArm":False,"walkingFloor":False,"antiSlipFloor":False,"dump":False,"validTillUtcTime":"2021-05-27T22:00:00.000Z","regionId":1,"mailToContacts":False,"requestUuid":"99c38e97-80a5-4e6b-93f2-5bb9d3152425","loadTypeId":1,"carTypeId":32}
        response = requests.delete(url)

        return (response.content)

    def direct_execute(self, deal_id):
        inp = Input()
        inp.assign("deal_id", deal_id)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
    
    def input_execute(self, inp):
        self.delete_deal(inp("deal_id"))
        print("Deleted deal -- deal_id: ", inp("deal_id"))

    def execute_with_params(self, params):
        deal_id = params["deal_id"]

        self.direct_execute(deal_id)

    def execute(self, node_detail_form):
        deal_id = node_detail_form.get_chosen_value_by_name("deal_id", variable_handler)

        self.direct_execute(deal_id)

    def export_code(self, node_detail_form):
        deal_id = node_detail_form.get_variable_name_or_input_value_by_element_name("deal_id")

        code = f"""
        requests.delete("https://" + company_domain_name + ".pipedrive.com/api/v1/deals/" + str({deal_id}) + "?api_token=" + pipedrive_token)
        """

        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class PipedriveAddNoteHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'PipedriveAddNote'
        self.fn_name = 'Pipedrive Add Note'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Content")
        fdl.entry(name="content", text="", input_types=["str"], required=True, row=1)
        fdl.label("Deal ID")
        fdl.entry(name="deal_id", text="", input_types=["str", "int", "float"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def add_note(self, content, deal_id):
        global company_domain_name, pipedrive_token

        url = "https://" + company_domain_name + ".pipedrive.com/api/v1/notes?api_token=" + pipedrive_token
        payload = {"content": content, "deal_id": deal_id}

        response = requests.post(url, data=payload)

        return (response.content)

    def direct_execute(self, content, deal_id):
        inp = Input()
        inp.assign("content", content)
        inp.assign("deal_id", deal_id)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
    
    def input_execute(self, inp):
        self.add_note(inp("content"), inp("deal_id"))
        print("Added note to deal -- deal_id: ", inp("deal_id"), ", content: ", inp("content"))

    def execute_with_params(self, params):
        content = params["content"]
        deal_id = params["deal_id"]

        self.direct_execute(content, deal_id)

    def execute(self, node_detail_form):
        content = node_detail_form.get_chosen_value_by_name("content", variable_handler)
        deal_id = node_detail_form.get_chosen_value_by_name("deal_id", variable_handler)

        self.direct_execute(content, deal_id)

    def export_code(self, node_detail_form):
        content = node_detail_form.get_variable_name_or_input_value_by_element_name("content")
        deal_id = node_detail_form.get_variable_name_or_input_value_by_element_name("deal_id")

        code = f"""
        payload = dict([("content", {content}), ("deal_id", {deal_id})])
        requests.post("https://" + company_domain_name + ".pipedrive.com/api/v1/notes?api_token=" + pipedrive_token , data = payload)
        """

        return code

    def export_imports(self, *args):
        imports = ["import requests"]
        return (imports)


class LoadGoogleSheetHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadGoogleSheet"
        self.fn_name = "Load Google Sheet"

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        sheet_options = ["Sheet1", "Sheet2", "Sheet3"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Sheet URL:")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet name:")
        fdl.entry(name="google_file_name", text="Sheet1", input_types=["str"], required=True, row=2)
        fdl.label("New variable name:")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        google_file_name = node_detail_form.get_chosen_value_by_name("google_file_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        
        new_var_name = variable_handler._set_up_unique_varname(new_var_name)
        fields = self.generate_shown_dataframe_option_field(new_var_name)

        self.direct_execute(sheet_url, google_file_name, new_var_name)

        # Deprecated as DataFrame node is being deprecated, TODO: delete the DF node is definitely removed
        # response = ncrb.new_node(pos=[500, 300], typ="DataFrame", fields=fields)
        # if response.status_code in (200, 201):
        #     result=json.loads(response.content.decode('utf-8'))
        #     node_uid = result["uid"]


        #     ncrb.update_last_active_dataframe_node_uid(node_uid)

        #     global loaded_filename
        #     loaded_filename = new_var_name

        #     return new_var_name
        # else:
        #     raise HTTPException(status_code=response.status_code, detail="Error requesting new node from api")

    def direct_execute(self, sheet_url, google_file_name, new_var_name):
        try:
            google_file_id = parse_google_sheet_id_from_url(sheet_url)
        except Exception as e:
            raise SoftPipelineError("Provided Sheet URL is of incorrect format") from e

        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("google_file_name", google_file_name)

        try:
            df = self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"Error loading Google Sheet: {e}")
            df = pd.DataFrame()

        variable_handler.new_variable(new_var_name, df)
        ##variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))
        
        sh = gc.open_by_key(inp("google_file_id"))
        worksheet = sh.worksheet(inp("google_file_name"))
        list_of_lists = worksheet.get_all_values()   

        df = pd.DataFrame(list_of_lists[1:], columns=list_of_lists[0])

        return df

    def export_imports(self, *args):
        imports = ["import gspread", "import pandas as pd"]
        return (imports)


class CopyGoogleSheetHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'CopyGoogleSheet'
        self.fn_name = 'Copy Google Sheet'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        #fdl.label("Sheet ID:")
        #fdl.entry(name="google_file_id", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet URL:")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet copy name:")
        fdl.entry(name="copied_filename", text="", input_types=["str"], required=True, row=2)
        fdl.label("E-mail")
        fdl.entry(name="email", text="", input_types=["str"], required=True, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, sheet_url, copied_filename, email):
        try:
            google_file_id = parse_google_sheet_id_from_url(sheet_url)
        except Exception as e:
            raise SoftPipelineError("Provided Sheet URL is of incorrect format") from e
        
        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("copied_filename", copied_filename)
        inp.assign("email", email)

        #gc = gspread.service_account(Path("config/google_service_account_credentials.json"))


        try:
            self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
    
    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))

        gc.copy(inp("google_file_id"), inp("copied_filename"), copy_permissions=True)
        
        worksheet = gc.open(inp("copied_filename"))
        print("GOOGLE SHEET COPIED")
        worksheet.share(inp("email"), perm_type='user', role='writer')
        print("GOOGLE SHEET PERMISSION GRANTED")

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        copied_filename = node_detail_form.get_chosen_value_by_name("copied_filename", variable_handler)
        email = node_detail_form.get_chosen_value_by_name("email", variable_handler)

        self.direct_execute(sheet_url, copied_filename, email)

    def export_imports(self, *args):
        imports = ["import gspread"]
        return (imports)


class UpdateCellHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'UpdateCell'
        self.fn_name = 'Update Cell'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        options = ["Sheet1", "Sheet2", "Sheet3"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        #fdl.label("Sheet ID:")
        #fdl.entry(name="google_file_id", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet URL:")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet name")
        fdl.comboentry(name="google_file_name", text="Sheet1", options=options, row=2)
        #fdl.entry(name="google_file_name", text="", input_types=["str"], required=True, row=2)
        fdl.label("Cell name")
        fdl.entry(name="cell_name", text="A1", input_types=["str"], required=True, row=3)
        fdl.label("Value")
        fdl.entry(name="value", text="", required=True, row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl
    
    def parse_inupt(self, sheet_url):
        google_file_id = sheet_url.split("/")[-2]

        return google_file_id

    def direct_execute(self, sheet_url, google_file_name, cell_name, value):
        google_file_id = self.parse_inupt(sheet_url)

        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("google_file_name", google_file_name)
        inp.assign("cell_name", cell_name)
        inp.assign("value", value)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
        """

        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))


        try:
            sh = gc.open_by_key(google_file_id)
            worksheet = sh.worksheet(google_file_name)
            worksheet.update(cell_name, value)
        except Exception as e:
            flog.error(message=f"{e}")
        """
    
    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))

        sh = gc.open_by_key(inp("google_file_id"))
        worksheet = sh.worksheet(inp("google_file_name"))
        worksheet.update(inp("cell_name"), inp("value"))

    def execute_with_params(self, params):
        sheet_url = params["sheet_url"]
        google_file_name = params["google_file_name"]
        cell_name = params["cell_name"]
        value = params["value"]

        self.direct_execute(sheet_url, google_file_name, cell_name, value)

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        
        google_file_name = node_detail_form.get_chosen_value_by_name("google_file_name", variable_handler)
        google_file_name = parse_comboentry_input(google_file_name)
        
        cell_name = node_detail_form.get_chosen_value_by_name("cell_name", variable_handler)
        value = node_detail_form.get_chosen_value_by_name("value", variable_handler)

        self.direct_execute(sheet_url, google_file_name, cell_name, value)

    def export_imports(self, *args):
        imports = ["import gspread"]
        return (imports)


class DeleteSheetRowHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'DeleteSheetRow'
        self.fn_name = 'Delete Sheet Row'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        options = ["Sheet1", "Sheet2", "Sheet3"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        #fdl.label("Sheet ID:")
        #fdl.entry(name="google_file_id", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet URL:")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet name")
        fdl.comboentry(name="google_file_name", text="Sheet1", options=options, row=2)
        #fdl.entry(name="google_file_name", text="", input_types=["str"], required=True, row=2)
        fdl.label("Start row number")
        fdl.entry(name="start_row", text="", input_types=["int"], required=True, row=3)
        fdl.label("Stop row number")
        fdl.entry(name="stop_row", text="", input_types=["int"], row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, sheet_url, google_file_name, start_row, stop_row):
        try:
            google_file_id = parse_google_sheet_id_from_url(sheet_url)
        except Exception as e:
            raise SoftPipelineError("Provided Sheet URL is of incorrect format") from e

        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("google_file_name", google_file_name)
        inp.assign("start_row", start_row)
        inp.assign("stop_row", stop_row)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
        
        """

        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))


        try:
            sh = gc.open_by_key(google_file_id)
            worksheet = sh.worksheet(google_file_name)
        except Exception as e:
            flog.error(message=f"{e}")
            return None

        if stop_row == "":
            try:
                worksheet.delete_row(int(start_row))
            except ValueError:
                print('ValueError Exception Raised, argument must be an integer.')
        else:
            try:
                for i in range(int(start_row), int(stop_row) + 1):
                    worksheet.delete_row(int(start_row))
            except ValueError:
                print('ValueError Exception Raised, argument must be an integer.')
        """
    
    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))

        sh = gc.open_by_key(inp("google_file_id"))
        worksheet = sh.worksheet(inp("google_file_name"))

        for i in range(int(inp("start_row")), int(inp("stop_row")) + 1):
            worksheet.delete_row(i)

    def execute_with_params(self, params):
        sheet_url = params["sheet_url"]
        google_file_name = params["google_file_name"]
        start_row = params["start_row"]
        stop_row = params["stop_row"]

        self.direct_execute(sheet_url, google_file_name, start_row, stop_row)

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        
        google_file_name = node_detail_form.get_chosen_value_by_name("google_file_name", variable_handler)
        google_file_name = parse_comboentry_input(google_file_name)
        
        start_row = node_detail_form.get_chosen_value_by_name("start_row", variable_handler)
        stop_row = node_detail_form.get_chosen_value_by_name("stop_row", variable_handler)

        self.direct_execute(sheet_url, google_file_name, start_row, stop_row)

    def export_imports(self, *args):
        imports = ["import gspread"]
        return (imports)
    
    
def create_new_google_spreadsheet_and_share_it_with_user(sheet_name, email):
    creds = service_account.Credentials.from_service_account_file(
        'config/google_service_account_credentials.json', scopes=['https://www.googleapis.com/auth/spreadsheets'])

    service = discovery.build('sheets', 'v4', credentials=creds)

    spreadsheet_body = {
        'properties': {
            'title': str(sheet_name)
        }
    }

    request = service.spreadsheets().create(body=spreadsheet_body)
    new_sheet_response = request.execute()

    #* COPY newly created sheet and share it with the entered e-mail address
    gc = gspread.service_account("config/google_service_account_credentials.json")
    gc.copy(new_sheet_response["spreadsheetId"], sheet_name, copy_permissions=True)
    worksheet = gc.open(sheet_name)
    worksheet.share(email, perm_type='user', role='writer')

    return worksheet

OPERATION_OPTIONS = Literal["Append", "Overwrite"]

def append_or_overwrite_data_in_sheet(data:list, worksheet:gspread.Worksheet, operation:OPERATION_OPTIONS):
    """Auxiliary function for appending or overwriting data in Google Spreadsheet

    Args:
        data (list): data in a form of list of lists
        worksheet (gspread.Worksheet): Worksheet object
        operation (Literal["append", "overwrite"]): either "append" or "overwrite"

    Returns:
        gspread.Spreadsheet: edited Spreadsheet object
    """ 
    
    def _overwrite_worksheet_with_new_data(worksheet: gspread.Worksheet, data): 
        worksheet.clear()
        worksheet.append_rows(data)
    
    options = get_args(OPERATION_OPTIONS)
    assert operation in options, f"'{operation}' is not in {options}" 
    
    if operation == "Append":
        worksheet.append_rows(data)
    elif operation == "Overwrite":
        _overwrite_worksheet_with_new_data(worksheet, data)
        
    return worksheet

class NewGoogleSheetHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'NewGoogleSheet'
        self.fn_name = 'New Google Sheet'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("New Google Sheets Worksheet")
        fdl.label("Sheet name")
        fdl.entry(name="sheet_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("E-mail")
        fdl.entry(name="email", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)
        fdl.label("Note: The sheet will appear as a shared one")
        fdl.label("  - choose 'own by anyone' option in Sheets")

        return fdl
    
    def execute(self, node_detail_form):
        sheet_name = node_detail_form.get_chosen_value_by_name("sheet_name", variable_handler)
        email = node_detail_form.get_chosen_value_by_name("email", variable_handler)

        self.direct_execute(sheet_name, email)

    def direct_execute(self, sheet_name, email):
        if sheet_name != "":
            inp = Input()
            inp.assign("sheet_name", sheet_name)
            inp.assign("email", email)

            try:
                worksheet = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"{e}")
                worksheet = None
            
            if worksheet is not None:
                variable_handler.new_variable(f"{sheet_name}_id", worksheet.id)
                ##variable_handler.update_data_in_variable_explorer(glc)
    
    def input_execute(self, inp):
        sheet_name = inp("sheet_name")
        email = inp("email")

        worksheet = create_new_google_spreadsheet_and_share_it_with_user(sheet_name, email)

        return worksheet

    def execute_with_params(self, params):
        sheet_name = params["sheet_name"]
        email = params["email"]

        self.direct_execute(sheet_name, email)

    def export_code(self, node_detail_form):
        sheet_name = node_detail_form.get_variable_name_or_input_value_by_element_name("sheet_name")
        email = node_detail_form.get_variable_name_or_input_value_by_element_name("email")

        code = f"""
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        SERVICE_ACCOUNT_FILE = Path('config/google_service_account_credentials.json')

        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        service = discovery.build('sheets', 'v4', credentials = creds)

        spreadsheet_body = dict([
            ('properties': dict([
                ('title': {sheet_name})
            ])
            )
        ])

        request = service.spreadsheets().create(body = spreadsheet_body)
        new_sheet_response = request.execute()

        ### COPY newly created sheet and share it with the entered e-mail address
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))
        gc.copy(new_sheet_response['spreadsheetId'], {sheet_name}, copy_permissions = True)
        worksheet = gc.open({sheet_name})
        worksheet.share({email}, perm_type = 'user', role = 'writer')

        ### Create new variable containing sheet ID
        {sheet_name}_id = worksheet.id
        print(worksheet.id)
        """

        # return(code.format(sheet_name= '"' + sheet_name + '"', email= '"' + email + '"'))
        return code

    def export_imports(self, *args):
        imports = ["from googleapiclient import discovery", "from google.oauth2 import service_account"]
        return (imports)


class InsertIntoSheetHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "InsertIntoSheet"
        self.fn_name = "Insert Into Sheet"

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        options = ["Overwrite", "Append"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Sheet URL")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet name")
        fdl.entry(name="sheet_name", text="Sheet1", input_types=["str"], required=True, row=2)
        fdl.label("DataFrame")
        fdl.entry(
            name="df_entry", text="", input_types=["DataFrame"], required=True, show_info=True,
            row=3
        )
        fdl.label("Operation")
        fdl.combobox(name="operation", options=options, default="Append", row=4)
        fdl.button(
            function=self.execute, function_args=node_detail_form, text="Execute", focused=True
        )
        return fdl

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name(
            "sheet_url", variable_handler
        )
        sheet_name = node_detail_form.get_chosen_value_by_name("sheet_name", variable_handler)
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        operation = node_detail_form.get_chosen_value_by_name("operation", variable_handler)
        self.direct_execute(sheet_url, sheet_name, df_entry, operation)

    def direct_execute(self, sheet_url, sheet_name, df_entry, operation):
        try:
            google_file_id = parse_google_sheet_id_from_url(sheet_url)
        except Exception as e:
            raise SoftPipelineError("Provided Sheet URL is of incorrect format") from e

        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("sheet_name", sheet_name)
        inp.assign("df_entry", df_entry)
        inp.assign("operation", operation)

        try:
            self.input_execute(inp)
        except gspread.exceptions.APIError as e:
            raise SoftPipelineError(
                "No permissions to open/modify the provided Google Sheets"
            ) from e
        except gspread.exceptions.SpreadsheetNotFound as e:
            raise SoftPipelineError("No Google Sheet found under the provided url") from e

    def input_execute(self, inp: Input) -> None:
        insert_df: pd.DataFrame = inp("df_entry")
        insert_df = insert_df.fillna('')

        column_names = insert_df.columns.tolist()
        rows = insert_df.values.tolist()

        insert_data: list[list[str]] = []
        insert_data.append(column_names)
        insert_data.extend(rows)

        gc = gspread.service_account("config/google_service_account_credentials.json")
        sh = gc.open_by_key(inp("google_file_id"))
        try:
            worksheet = sh.worksheet(inp("sheet_name"))
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=inp("sheet_name"), rows="100", cols="20")

        if inp("operation") == "Append":
            worksheet.append_rows(insert_data[1:])  # Omit column names row
        elif inp("operation") == "Overwrite":
            worksheet.clear()
            worksheet.update(insert_data)

    def export_imports(self, *args):
        imports = ["import pandas as pd", "import gspread"]
        return (imports)    
    
class StoreDfInNewSheetHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "StoreDfInNewSheet"
        self.fn_name = "Store Df In New Sheet"

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Spreadsheet name")
        fdl.entry(name="sheet_name", text="", category="arguments", input_types=["str"], required=True, row=2)
        fdl.label("Email")
        fdl.entry(name="email", text="", category="arguments", input_types=["str"], required=True, row=3)
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        sheet_name = node_detail_form.get_chosen_value_by_name("sheet_name", variable_handler)
        email = node_detail_form.get_chosen_value_by_name("email", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(df, sheet_name, email, new_var_name)
        
    def direct_execute(self, df, sheet_name, email, new_var_name):
        
        # TODO: Refactor the endpoint so it works (either finish datasets or introduce a new approach)
        # Disabled until then. Do not delete yet!
        # Comment author: Daniel
        """dataset_uid = None
        response = ncrb.store_df_to_google_sheet(dataset_uid, sheet_name, email)
        
        if response.status_code in (200, 201):
            result = json.loads(response.content.decode('utf-8'))
            url = result.get("detail", {}).get("url")
            
            if url is not None:
                variable_handler.new_variable(new_var_name, url)"""
               
        # HACK: Hotfix for the broken enpoint call above so it works. 
        spreadsheet = create_new_google_spreadsheet_and_share_it_with_user(sheet_name, email)
        worksheet = spreadsheet.worksheets()[0]
        
        data = df.fillna('')
        list_data = data.values.tolist()
        list_data.insert(0, data.keys().tolist())
        worksheet = append_or_overwrite_data_in_sheet(list_data, worksheet, "Overwrite")
        
        variable_handler.new_variable(new_var_name, worksheet.url)
        
    def input_execute(self, inp):
        # TODO: Implement
        
        return None

    def export_code(self, node_detail_form):
        sheet_url = node_detail_form.get_variable_name_or_input_value_by_element_name("sheet_url")
        sheet_name = node_detail_form.get_variable_name_or_input_value_by_element_name("sheet_name")
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        operation = node_detail_form.get_variable_name_or_input_value_by_element_name("operation")

        code_base = f"""
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))
        sh = gc.open_by_key({sheet_url})
        worksheet = sh.worksheet({sheet_name})
        """

        code_append = """
        worksheet.append_rows(list_data)
        """

        code_overwrite = """
        worksheet.clear()
        worksheet.update(list_data)
        """

        if operation == "Append":
            code = code_base + code_append
        elif operation == "Overwrite":
            code = code_base + code_overwrite

        # return(code.format(filename= '"' + filename + '"', header= header, google_file_id= '"' + google_file_id + '"', sheet_name= '"' + sheet_name + '"', operation= '"' + operation + '"'))
        return code

    def export_imports(self, *args):
        imports = ["import pandas as pd", "import gspread"]
        return (imports)


class AirtableConnectHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'AirtableConnect'
        self.fn_name = 'Airtable Connect'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("API key")
        fdl.entry(name="apikey", text="", input_types=["str"], required=True, row=1)
        fdl.label("Base ID")
        fdl.entry(name="base_id", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, apikey_var, base_id_var):
        global apikey, base_id

        if apikey_var != "" and base_id_var != "":
            inp = Input()
            inp.assign("apikey", apikey_var)
            inp.assign("base_id", base_id_var)

            try:
                apikey, base_id = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"{e}")

        """
        apikey = apikey_var
        base_id = base_id_var
        print("CONNECTION ESTABLISHED")
        """
    
    def input_execute(self, inp):
        apikey = inp("apikey")
        base_id = inp("base_id")
        print("CONNECTION ESTABLISHED")

        return apikey, base_id
        
    # def execute_with_params(self, params):

    #    global apikey, base_id

    #    apikey = params["apikey"]
    #    base_id = params["base_id"]

    #    print("CONNECTION ESTABLISHED")

    def execute(self, node_detail_form):
        apikey = node_detail_form.get_chosen_value_by_name("apikey", variable_handler)
        base_id = node_detail_form.get_chosen_value_by_name("base_id", variable_handler)

        self.direct_execute(apikey, base_id)

    def export_imports(self, *args):
        imports = []
        return (imports)


class AirtableAddRecordHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'AirtableAddRecord'
        self.fn_name = 'Airtable Add Record'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Table name")
        fdl.entry(name="table_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("Value")
        fdl.entry(name="record", text="", input_types=["dict"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, table_name, record):
        global apikey, base_id

        if apikey != "" and base_id != "":
            inp = Input()
            inp.assign("table_name", table_name)
            inp.assign("record", record)
            inp.assign("apikey", apikey)
            inp.assign("base_id", base_id)

            try:
                response = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"{e}")
            
            variable_handler.new_variable(table_name, response)
            #variable_handler.update_data_in_variable_explorer(glc)

        """
        table = Table(apikey, base_id, table_name)

        try:
            response = table.create(ast.literal_eval(record))
            print('Add record response: ', response)
        except Exception as e:
            print('Airtable add record WARNING:', e)
            # return None

        # return response
        """
    
    def input_execute(self, inp):
        table = Table(inp("apikey"), inp("base_id"), inp("table_name"))

        response = table.create(ast.literal_eval(inp("record")))
        print('Add record response: ', response)

        return response

    def execute_with_params(self, params):

        table_name = params["table_name"]
        record = params["record"]

        self.direct_execute(table_name, record)

    def execute(self, node_detail_form):
        table_name = node_detail_form.get_chosen_value_by_name("table_name", variable_handler)
        record = node_detail_form.get_chosen_value_by_name("record", variable_handler)

        self.direct_execute(table_name, record)

    def export_code(self, node_detail_form):
        table_name = node_detail_form.get_variable_name_or_input_value_by_element_name("table_name")
        record = node_detail_form.get_variable_name_or_input_value_by_element_name("record")

        code = f"""
        table = Table(apikey, base_id, {table_name})
        table.create({record})
        """

        return code

    def export_imports(self, *args):
        imports = ["from pyairtable import Table", "import ast"]
        return (imports)


class AirtableParseDataHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'AirtableParseData'
        self.fn_name = 'Airtable Parse Data'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        options = ["Overwrite", "Append"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Table name")
        fdl.entry(name="table_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("Filename")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.open_data_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name="lookup_csv_file")
        fdl.label("Operation")
        fdl.combobox(name="operation", options=options, default="Append", row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_data_file(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('CSV files', '*.csv')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def direct_execute(self, table_name: str, filename, operation):
        global apikey, base_id

        if apikey != "" and base_id != "":
            inp = Input()
            inp.assign("filename", filename)
            inp.assign("table_name", table_name)
            inp.assign("operation", operation)
            inp.assign("apikey", apikey)
            inp.assign("base_id", base_id)

            try:
                created_batch = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"Airtable parse data WARNING: {e}")
            
            variable_handler.new_variable(table_name, created_batch)
            #variable_handler.update_data_in_variable_explorer(glc)
            
        """
        table = Table(apikey, base_id, table_name)

        df = pd.read_csv(filename)
        record = df.to_dict('records')

        if operation == "Append":
            try:
                response = table.batch_create(record)
            except Exception as e:
                print(e)
                # return None
        elif operation == "Overwrite":
            id_list = [x['id'] for x in table.all()]
            table.batch_delete(id_list)
            try:
                response = table.batch_create(record)
            except Exception as e:
                print(e)
                # return None

        # return response
        """
    
    def input_execute(self, inp):
        table = Table(inp("apikey"), inp("base_id"), inp("table_name"))

        df = pd.read_csv(inp("filename"))
        record = df.to_dict('records')

        if inp("operation") == "Overwrite":
            id_list = [x['id'] for x in table.all()]
            table.batch_delete(id_list)

        created_batch = table.batch_create(record)
        
        return created_batch

    def execute_with_params(self, params):

        table_name = params["table_name"]
        filename = params["filename"]
        operation = params["operation"]

        self.direct_execute(table_name, filename, operation)

    def execute(self, node_detail_form):
        table_name = node_detail_form.get_chosen_value_by_name("table_name", variable_handler)
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        operation = node_detail_form.get_chosen_value_by_name("operation", variable_handler)

        self.direct_execute(table_name, filename, operation)

    def export_code(self, node_detail_form):
        table_name = node_detail_form.get_variable_name_or_input_value_by_element_name("table_name")
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        operation = node_detail_form.get_variable_name_or_input_value_by_element_name("operation")

        code_1 = f"""
        table = Table(apikey, base_id, {table_name})

        df = pd.read_csv({filename})
        record = df.to_dict('records')
        """

        overwrite_code = """
        id_list = [x['id'] for x in table.all()]
        table.batch_delete(id_list)
        """

        code_2 = """
        table.batch_create(record)
        """

        if operation == "Append":
            code = code_1 + code_2
        elif operation == "Overwrite":
            code = code_1 + overwrite_code + code_2
        else:
            code = "" # No option selected in combobox

        return code

    def export_imports(self, *args):
        imports = ["from pyairtable import Table", "import pandas as pd"]
        return (imports)


class AirtableReadTableHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'AirtableReadTable'
        self.fn_name = 'Airtable Read Table'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Table name")
        fdl.entry(name="table_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("Save as")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, table_name, new_var_name):
        global apikey, base_id

        if apikey != "" and base_id != "":
            inp = Input()
            inp.assign("table_name", table_name)
            inp.assign("new_var_name", new_var_name)
            inp.assign("apikey", apikey)
            inp.assign("base_id", base_id)

            try:
                table_data = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"Airtable read table WARNING: {e}")
            
            variable_handler.new_variable(new_var_name, table_data)
            #variable_handler.update_data_in_variable_explorer(glc)

        """
        table = Table(apikey, base_id, table_name)

        table_data = [x['fields'] for x in table.all()]

        variable_handler.new_variable(new_var_name, table_data)
        #variable_handler.update_data_in_variable_explorer(glc)

        # return response
        """
    
    def input_execute(self, inp):
        table = Table(apikey, base_id, inp("table_name"))

        table_data = [x['fields'] for x in table.all()]

        return table_data

    def execute_with_params(self, params):
        table_name = params["table_name"]
        new_var_name = params["new_var_name"]

        self.direct_execute(table_name, new_var_name)

    def execute(self, node_detail_form):
        table_name = node_detail_form.get_chosen_value_by_name("table_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(table_name, new_var_name)

    def export_code(self, node_detail_form):
        table_name = node_detail_form.get_variable_name_or_input_value_by_element_name("table_name")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code = f"""
        table = Table(apikey, base_id, {table_name})
        {new_var_name} = [x['fields'] for x in table.all()]
        """

        return code

    def export_imports(self, *args):
        imports = ["from pyairtable import Table"]
        return (imports)


integration_handlers_dict = {
    'PipedriveConnect': PipedriveConnectHandler(),
    'PipedriveGetStages': PipedriveGetStagesHandler(),
    'PipedriveGetUsers': PipedriveGetUsersHandler(),

    'PipedriveAddPerson': PipedriveAddPersonHandler(),
    'PipedriveAddDeal': PipedriveAddDealHandler(),
    'PipedriveDeleteDeal': PipedriveDeleteDealHandler(),
    'PipedriveAddNote': PipedriveAddNoteHandler(),

    'LoadGoogleSheet': LoadGoogleSheetHandler(),
    'CopyGoogleSheet': CopyGoogleSheetHandler(),
    'InsertIntoSheet': InsertIntoSheetHandler(),
    'UpdateCell': UpdateCellHandler(),
    'DeleteSheetRow': DeleteSheetRowHandler(),
    'NewGoogleSheet': NewGoogleSheetHandler(),
    'ParseDataToSheet': ParseDataToSheetHandler(),
    'StoreDfInNewSheet': StoreDfInNewSheetHandler(),

    'AirtableConnect': AirtableConnectHandler(),
    'AirtableAddRecord': AirtableAddRecordHandler(),
    'AirtableParseData': AirtableParseDataHandler(),
    'AirtableReadTable': AirtableReadTableHandler(),

    'SlackNotification': SlackNotificationHandler(),
    'EmailNotification': EmailNotificationHandler(),

    'CreateEmailBody': CreateEmailBodyHandler(),
}