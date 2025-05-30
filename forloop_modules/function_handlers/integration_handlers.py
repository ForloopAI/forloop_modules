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
import sys

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient import discovery
from google.oauth2 import service_account

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.docs import Docs
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.errors.errors import SoftPipelineError, CriticalPipelineError
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input
from forloop_modules.function_handlers.file_managment_handlers import file_managment_handlers_dict
from forloop_modules.utils.definitions import GOOGLE_API_SERVICES, GOOGLE_API_SERVICE_INFO
from forloop_modules.integrations.slack_integration import SlackApiError, get_channels_in_workspace, send_message_to_slack_direct_execute
from forloop_modules.function_handlers.auxilliary.auxiliary_functions import parse_comboentry_input, parse_google_sheet_id_from_url
from config.config import other_config #TODO Dominik: Circular dependency to forloop_platform repository # not ideal #Maybe solve with os.environ?

import webbrowser
from e2b_desktop import Sandbox
from forloop_modules.utils import synchronization_flags as sf



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
        fdl.label("File")
        fdl.entry(name="file_name", text="/path/to/file", input_types=["str"], required=False, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True, row=4)

        return fdl

    def execute(self, node_detail_form):
        channel_name = node_detail_form.get_chosen_value_by_name("channel_name", variable_handler)
        channel_name = parse_comboentry_input(channel_name)
        
        text = node_detail_form.get_chosen_value_by_name("text", variable_handler)

        self.direct_execute(channel_name, text)

    def direct_execute(self, channel_name, text, file_name=None):
        inp = Input()
        inp.assign("channel_name", channel_name)
        inp.assign("text", text)
        inp.assign("file_name", file_name)

        try:
            self.input_execute(inp)
        except Exception as e:
            flog.warning(e)

    def input_execute(self, inp):
        send_message_to_slack_direct_execute(other_config.SLACK_TOKEN, inp("channel_name"), inp("text"), inp("file_name"))
        
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

    def export_code(self, node_details_form):
        code = """
        """
        return (code)

    def export_imports(self):
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
        fdl.label("File")
        fdl.entry(name="attachment_filename", text="/path/to/file", input_types=["str"], required=False, row=5)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True, row=6)

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

    def direct_execute(self, company_domain_name, pipedrive_token):
        global company_domain_name_, pipedrive_token_

        inp = Input()
        inp.assign("company_domain_name", company_domain_name)
        inp.assign("pipedrive_token", pipedrive_token)

        try:
            company_domain_name_, pipedrive_token_ = self.input_execute(inp)
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
        global company_domain_name_, pipedrive_token_

        url = "https://" + company_domain_name_ + ".pipedrive.com/api/v1/stages?api_token=" + pipedrive_token_

        response = requests.get(url)
        print(response)
        
        return (response.json())

    def direct_execute(self, new_var_name):
        if new_var_name in variable_handler.variables:
            inp = Input()
            
            try:
                stages = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"{e}")

            variable_handler.new_variable(new_var_name, stages)
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
        global company_domain_name_, pipedrive_token_

        url = "https://" + company_domain_name_ + ".pipedrive.com/api/v1/users?api_token=" + pipedrive_token_

        response = requests.get(url)
        print(response)
        # print(response.content)
        return (response.json())

    def direct_execute(self, new_var_name):
        if new_var_name in variable_handler.variables:
            inp = Input()
            
            try:
                users = self.input_execute(inp)
            except Exception as e:
                flog.error(message=f"{e}")

            variable_handler.new_variable(new_var_name, users)
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
        global company_domain_name_, pipedrive_token_

        url = "https://" + company_domain_name_ + ".pipedrive.com/api/v1/persons?api_token=" + pipedrive_token_

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
        global company_domain_name_, pipedrive_token_

        url = "https://" + company_domain_name_ + ".pipedrive.com/api/v1/deals?api_token=" + pipedrive_token_

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
        global company_domain_name_, pipedrive_token_

        url = "https://" + company_domain_name_ + ".pipedrive.com/api/v1/deals/" + str(
            deal_id) + "?api_token=" + pipedrive_token_
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
        global company_domain_name_, pipedrive_token_

        url = "https://" + company_domain_name_ + ".pipedrive.com/api/v1/notes?api_token=" + pipedrive_token_
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
    """
    Load Google Sheet Node serves, as the name suggests, for loading a Google sheet into Forloop. It requires three 
    entries: *Google file ID*, *sheet name* and *new variable name*. It then creates a new Forloop object with an interactive
    icon which can be used for further analysis in the same manner as a data table.
    """
    
    def __init__(self):
        self.icon_type = "LoadGoogleSheet"
        self.fn_name = "Load Google Sheet"

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = "In order to succesfully load a Google spreadsheet, three parameters are required as user entries."
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Sheet URL", name="sheet_url",
                                          description="A URL of the desired Google spreadsheet.",
                                          typ="string", example="https://docs.google.com/spreadsheets/d/ 1FApy2bGcFFmpg-lTNS8HWpq-fpHlGcJhvq-DXhr4b1o /edit#gid=0")
        self.docs.add_parameter_table_row(title="Sheet name", name="sheet_name", 
                                          description="The name of the loaded spreadsheet. It is essential to write the name in its full form, i.e. if the name is *test_sheet* then writing *test sheet* will not work.",
                                          example=["Sheet1", "sheet_1", "list 1"])
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                                          description="A name that will be used for the newly created icon of the loaded spreadsheet. Therefore this field requires an arbitrary string.",
                                          example="my_sheet_df")

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Sheet URL:")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet name:")
        fdl.entry(name="sheet_name", text="Sheet1", input_types=["str"], required=True, row=2)
        fdl.label("New variable name:")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        sheet_name = node_detail_form.get_chosen_value_by_name("sheet_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        
        new_var_name = variable_handler._set_up_unique_varname(new_var_name)
        fields = self.generate_shown_dataframe_option_field(new_var_name)

        self.direct_execute(sheet_url, sheet_name, new_var_name)

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

    def direct_execute(self, sheet_url, sheet_name, new_var_name):
        try:
            google_file_id = parse_google_sheet_id_from_url(sheet_url)
        except Exception as e:
            raise SoftPipelineError("Provided Sheet URL is of incorrect format") from e

        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("sheet_name", sheet_name)

        try:
            df = self.input_execute(inp)
        except Exception as e:
            raise CriticalPipelineError("Loading of Google sheet failed unexpectedly.") from e

        variable_handler.new_variable(new_var_name, df)
        ##variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))
        
        sh = gc.open_by_key(inp("google_file_id"))
        worksheet = sh.worksheet(inp("sheet_name"))
        list_of_lists = worksheet.get_all_values()   

        df = pd.DataFrame(list_of_lists[1:], columns=list_of_lists[0])

        return df

    def export_imports(self, *args):
        imports = ["import gspread", "import pandas as pd"]
        return (imports)


class CopyGoogleSheetHandler(AbstractFunctionHandler):
    """
    Copy Google Sheet Node makes a copy of the spreadsheet identified by *Sheet ID* for the Google account belonging to
    the E-mail address. An arbitrary name can be chosen for such copy.
    """

    def __init__(self):
        self.icon_type = 'CopyGoogleSheet'
        self.fn_name = 'Copy Google Sheet'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = "In order to succesfully copy a Google spreadsheet, three parameters are required as user entries."
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Sheet URL", name="sheet_url",
                                          description="A URL of the desired Google spreadsheet.",
                                          typ="string", example="https://docs.google.com/spreadsheets/d/ 1FApy2bGcFFmpg-lTNS8HWpq-fpHlGcJhvq-DXhr4b1o /edit#gid=0")
        self.docs.add_parameter_table_row(title="Sheet copy name", name="copied_filename", 
                                          description="The entered string will be used as a name for the newly created copy of the original spreadsheet.",
                                          example=["copied_sheet_1"])
        self.docs.add_parameter_table_row(title="E-mail", name="email", 
                                          description="E-mail address with an access to Google worspace. The copy of the original spreadsheet will be created in spreadsheets on this account.",
                                          example="john.doe@gmail.com")

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
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

        try:
            self.input_execute(inp)
        except Exception as e:
            raise SoftPipelineError("Copy operation on Google sheet failed unexpectedly.") from e
    
    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))

        gc.copy(inp("google_file_id"), inp("copied_filename"), copy_permissions=True)
        
        worksheet = gc.open(inp("copied_filename"))
        worksheet.share(inp("email"), perm_type='user', role='writer')

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        copied_filename = node_detail_form.get_chosen_value_by_name("copied_filename", variable_handler)
        email = node_detail_form.get_chosen_value_by_name("email", variable_handler)

        self.direct_execute(sheet_url, copied_filename, email)

    def export_imports(self, *args):
        imports = ["import gspread"]
        return (imports)


class UpdateCellHandler(AbstractFunctionHandler):
    """
    Update Cell Node can be used for entering new data in a specific single cell of a spreadsheet or altering already
    existing data in such a cell.
    """

    def __init__(self):
        self.icon_type = 'UpdateCell'
        self.fn_name = 'Update Cell'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = "Update Cell Node requires 4 parameters as user entries, all of which must be filled in."
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Sheet URL", name="sheet_url",
                                          description="A URL of the desired Google spreadsheet.",
                                          typ="string", example="https://docs.google.com/spreadsheets/d/ 1FApy2bGcFFmpg-lTNS8HWpq-fpHlGcJhvq-DXhr4b1o /edit#gid=0")
        self.docs.add_parameter_table_row(title="Sheet name", name="sheet_name", 
                                          description="The name of the spreadsheet, whose cell is going to be updated. It is essential to write the name in its full form, i.e. if the name is *test_sheet* then writing *test sheet* will not work.",
                                          typ="string", example=["Sheet1", "sheet_1", "list 1"])
        self.docs.add_parameter_table_row(title="Cell name", name="cell_name", 
                                          description="Specifies which cell will get updated.",
                                          typ="string", example="B4")
        self.docs.add_parameter_table_row(title="Value", name="value", 
                                          description="Data to be inserted into the specified cell.",
                                          typ="string", example="B4")

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Sheet URL:")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet name")
        fdl.entry(name="sheet_name", text="Sheet1", input_types=["str"], required=True, row=2)
        fdl.label("Cell name")
        fdl.entry(name="cell_name", text="A1", input_types=["str"], required=True, row=3)
        fdl.label("Value")
        fdl.entry(name="value", text="", required=True, row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl
    
    def parse_inupt(self, sheet_url):
        google_file_id = sheet_url.split("/")[-2]

        return google_file_id

    def direct_execute(self, sheet_url, sheet_name, cell_name, value):
        google_file_id = self.parse_inupt(sheet_url)

        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("sheet_name", sheet_name)
        inp.assign("cell_name", cell_name)
        inp.assign("value", value)

        try:
            self.input_execute(inp)
        except Exception as e:
            raise SoftPipelineError("Updating Google sheet cell value failed unexpectedly.") from e
    
    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))

        sh = gc.open_by_key(inp("google_file_id"))
        worksheet = sh.worksheet(inp("sheet_name"))
        worksheet.update(inp("cell_name"), inp("value"))

    def execute_with_params(self, params):
        sheet_url = params["sheet_url"]
        sheet_name = params["sheet_name"]
        cell_name = params["cell_name"]
        value = params["value"]

        self.direct_execute(sheet_url, sheet_name, cell_name, value)

    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        
        sheet_name = node_detail_form.get_chosen_value_by_name("sheet_name", variable_handler)
        sheet_name = parse_comboentry_input(sheet_name)
        
        cell_name = node_detail_form.get_chosen_value_by_name("cell_name", variable_handler)
        value = node_detail_form.get_chosen_value_by_name("value", variable_handler)

        self.direct_execute(sheet_url, sheet_name, cell_name, value)

    def export_imports(self, *args):
        imports = ["import gspread"]
        return (imports)


class DeleteSheetRowHandler(AbstractFunctionHandler):
    """
    Delete Sheet Row Node serves to deletion of a single row or a series of rows in a Google spreadsheet.
    """

    def __init__(self):
        self.icon_type = 'DeleteSheetRow'
        self.fn_name = 'Delete Sheet Row'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """Delete Sheet Row Node requires 3 parameters (Sheet ID, Sheet name, Start row number)
        to delete a single row and 4 parameters (Sheet ID, Sheet name, Start row number, Stop row number) to delete a
        series of rows."""
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Sheet URL", name="sheet_url",
                                          description="A URL of the desired Google spreadsheet.",
                                          typ="string", example="https://docs.google.com/spreadsheets/d/ 1FApy2bGcFFmpg-lTNS8HWpq-fpHlGcJhvq-DXhr4b1o /edit#gid=0")
        self.docs.add_parameter_table_row(title="Sheet name", name="sheet_name", 
                                          description="The name of the spreadsheet, whose row(s) is/are going to be deleted. It is essential to write the name in its full form, i.e. if the name is ‘test_sheet’ then writing ‘test sheet’ will not work.",
                                          typ="string", example=["Sheet1", "sheet_1", "list 1"])
        self.docs.add_parameter_table_row(title="Start row number", name="start_row", 
                                          description="A number of the row to be deleted or the initial row of the series to be deleted. The numbering preserves the Python logic, ie. **the first row corresponds to number 0**!",
                                          typ="integer", example=["0", "12", "546"])
        self.docs.add_parameter_table_row(title="Stop row number", name="stop_row", 
                                          description="A number of the last row of the series of rows to be deleted. If left blank, only a single row, i.e. row no. Start row number + 1 (Python logic), will be deleted.",
                                          typ="integer")

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Sheet URL:")
        fdl.entry(name="sheet_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Sheet name")
        fdl.entry(name="sheet_name", text="Sheet1", input_types=["str"], required=True, row=2)
        fdl.label("Start row number")
        fdl.entry(name="start_row", text="", input_types=["int"], required=True, row=3)
        fdl.label("Stop row number")
        fdl.entry(name="stop_row", text="", input_types=["int"], row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl
    
    def execute(self, node_detail_form):
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
        
        sheet_name = node_detail_form.get_chosen_value_by_name("sheet_name", variable_handler)
        sheet_name = parse_comboentry_input(sheet_name)
        
        start_row = node_detail_form.get_chosen_value_by_name("start_row", variable_handler)
        stop_row = node_detail_form.get_chosen_value_by_name("stop_row", variable_handler)

        self.direct_execute(sheet_url, sheet_name, start_row, stop_row)
        
    def execute_with_params(self, params):
        sheet_url = params["sheet_url"]
        sheet_name = params["sheet_name"]
        start_row = params["start_row"]
        stop_row = params["stop_row"]

        self.direct_execute(sheet_url, sheet_name, start_row, stop_row)

    def direct_execute(self, sheet_url, sheet_name, start_row, stop_row):
        try:
            google_file_id = parse_google_sheet_id_from_url(sheet_url)
        except Exception as e:
            raise SoftPipelineError("Provided Sheet URL is of incorrect format") from e
        
        try:
            start_row = int(start_row)
            stop_row = int(stop_row) if stop_row else None
        except Exception as e:
            raise SoftPipelineError("Argument must be and integer.") from e

        inp = Input()
        inp.assign("google_file_id", google_file_id)
        inp.assign("sheet_name", sheet_name)
        inp.assign("start_row", start_row)
        inp.assign("stop_row", stop_row)

        try:
            self.input_execute(inp)
        except Exception as e:
            raise SoftPipelineError("Delete Google sheet row operation failed unexpectedly.") from e
        
    def input_execute(self, inp):
        gc = gspread.service_account(Path("config/google_service_account_credentials.json"))

        sh = gc.open_by_key(inp("google_file_id"))
        worksheet = sh.worksheet(inp("sheet_name"))

        if inp("stop_row") is None:
            worksheet.delete_row(inp("stop_row"))
        else:
            for i in range(inp("start_row"), inp("stop_row") + 1):
                worksheet.delete_row(i)

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
    """
    New Google Sheet Node serves for creation of a new Google sheet on a specified Google account. It requires two 
    entries: users e-mail address and a name which will be given to the newly created sheet.
    
    After a successful creation of a Google sheet a new variable named '[sheet_name]_url' (where [sheet_name] is the 
    name of a sheet you provide) containing it's url will be created. 
    """

    def __init__(self):
        self.icon_type = 'NewGoogleSheet'
        self.fn_name = 'New Google Sheet'

        self.type_category = ntcm.categories.integration
        self.docs_category = DocsCategories.integrations
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = "In order to succesfully create a new Google spreadsheet, two parameters are required as user entries."
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Sheet name", name="sheet_name",
                                          description="The entered string will be used as a name for the newly created spreadsheet.",
                                          typ="string", example="Test sheet")
        self.docs.add_parameter_table_row(title="E-mail", name="email", 
                                          description="E-mail address with an access to Google worspace. The new spreadsheet will be created in spreadsheets on this account.",
                                          example="john.doe@gmail.com")

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("New Google Sheets Worksheet")
        fdl.label("Sheet name")
        fdl.entry(name="sheet_name", text="", input_types=["str"], required=True, row=1)
        fdl.label("E-mail")
        fdl.entry(name="email", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)
        fdl.label("Note: A new variable containing sheet's url will be crated")
        fdl.label("under a name '[sheet_name]_url' where [sheet_name]")
        fdl.label("is the sheet name you provided.")
        fdl.label("Note 2: The sheet will appear as a shared one")
        fdl.label("  - choose 'own by anyone' option in Sheets")

        return fdl
    
    def execute(self, node_detail_form):
        sheet_name = node_detail_form.get_chosen_value_by_name("sheet_name", variable_handler)
        email = node_detail_form.get_chosen_value_by_name("email", variable_handler)

        self.direct_execute(sheet_name, email)

    def direct_execute(self, sheet_name, email):
        if not sheet_name:
            raise SoftPipelineError("The 'Sheet name' argument must be specified.")
        
        inp = Input()
        inp.assign("sheet_name", sheet_name)
        inp.assign("email", email)

        try:
            worksheet = self.input_execute(inp)
        except Exception as e:
            raise SoftPipelineError("Create new Google sheet operation failed unexpectedly.") from e
        
        variable_handler.new_variable(f"{sheet_name}_url", worksheet.url)
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
        {sheet_name}_url = worksheet.url
        """

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
        sheet_url = node_detail_form.get_chosen_value_by_name("sheet_url", variable_handler)
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
        except (gspread.exceptions.APIError, PermissionError) as e:
            raise SoftPipelineError("No permissions to open/modify the provided Google Sheets") from e
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
            cell_values = worksheet.get_all_values()
            are_all_sheet_cells_empty = all(all(cell == '' for cell in row) for row in cell_values)
            
            if are_all_sheet_cells_empty:
                worksheet.append_rows(insert_data)
            else:
                # In case of some data present we ommit column names (first row)
                worksheet.append_rows(insert_data[1:])
        elif inp("operation") == "Overwrite":
            worksheet.clear()
            worksheet.update(insert_data)

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

    def direct_execute(self, apikey, base_id):
        global apikey_, base_id_

        if apikey != "" and base_id != "":
            inp = Input()
            inp.assign("apikey", apikey)
            inp.assign("base_id", base_id)

            try:
                apikey_, base_id_ = self.input_execute(inp)
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
        global apikey_, base_id_

        if apikey_ != "" and base_id != "":
            inp = Input()
            inp.assign("table_name", table_name)
            inp.assign("record", record)
            inp.assign("apikey", apikey_)
            inp.assign("base_id", base_id_)

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
        global apikey_, base_id_

        if apikey_ != "" and base_id_ != "":
            inp = Input()
            inp.assign("filename", filename)
            inp.assign("table_name", table_name)
            inp.assign("operation", operation)
            inp.assign("apikey", apikey_)
            inp.assign("base_id", base_id_)

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
        global apikey_, base_id_

        if apikey_ != "" and base_id_ != "":
            inp = Input()
            inp.assign("table_name", table_name)
            inp.assign("new_var_name", new_var_name)
            inp.assign("apikey", apikey_)
            inp.assign("base_id", base_id_)

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




class E2BDesktopClickHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = True
        self.icon_type = "E2BDesktopClick"
        self.fn_name = "E2BDesktopClick"
        self.type_category = ntcm.categories.rpa

        super().__init__()


    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("X:")
        fdl.entry(name="x", text="0", row=1, input_types=["int", "float"], show_info=True, required=True)
        fdl.label("Y:")
        fdl.entry(name="y", text="0", row=2, input_types=["int", "float"], show_info=True, required=True)
        fdl.label("Pipette current position (Press Enter Key)")
        fdl.label("(X,Y) = (0,0)")
        fdl.label("Double click")
        fdl.checkbox(name="double_click", bool_value=False, row=5)
        

        return fdl

    def execute(self, node_detail_form):
        x = node_detail_form.get_chosen_value_by_name("x", variable_handler)
        y = node_detail_form.get_chosen_value_by_name("y", variable_handler)
        double_click = node_detail_form.get_chosen_value_by_name("double_click", variable_handler)

        self.direct_execute(x, y, double_click)

    def direct_execute(self, x, y, double_click):
        if x == "":
            x = 0
        if y == "":
            y = 0
        
        x = int(x)
        y = int(y)
        
        clicks=1
        if double_click:
            clicks=2

        inp=Input()
        inp.assign("x",x)
        inp.assign("y",y)
        inp.assign("clicks",clicks)
        
        if sys.platform!="linux" and sys.platform!="linux2": #pyautogui not supported on linux
            self.input_execute(inp)
        else:
            flog.info("Clicking with Forloop is disabled on linux OS")

    def input_execute(self, inp):
        
                
        # With custom configuration
        desktop = Sandbox(api_key = sf.E2B_API_KEY,
            display=":0",  # Custom display (defaults to :0)
            resolution=(1920, 1080),  # Custom resolution
            dpi=96,  # Custom DPI
        )
        
        
        # Start the stream
        desktop.stream.start()
        
        # Get stream URL
        url = desktop.stream.get_url()
        print(url)
        
        
        # Open a URL in the default browser
        webbrowser.open(url)
        desktop.wait(10000)
                
        if int(inp("clicks"))==2:
            desktop.double_click(x=int(inp("x")), y=int(inp("y")))
       
        else:
            desktop.left_click(x=int(inp("x")), y=int(inp("y")))
       

    def export_imports(self, *args):
        imports = ["import e2b"]
        return (imports)


#################################################################################################
# from e2b-dev/open-computer-use (https://github.com/e2b-dev/open-computer-use) # TODO refactor #
#################################################################################################
# START

TYPING_DELAY_MS = 12
TYPING_GROUP_SIZE = 50

tools = {
    "stop": {
        "description": "Indicate that the task has been completed.",
        "params": {},
    }
}


class StreamingFFMpegSandbox(Sandbox):

    def start_stream(self):
        # Command to start streaming using ffmpeg
        command = "ffmpeg -f x11grab -s 1024x768 -framerate 30 -i {self._display} -vcodec libx264 -preset ultrafast -tune zerolatency -f mpegts -listen 1 http://localhost:8080"
        # Run the command in the background
        process = self.commands.run(
            command,
            background=True,
        )
        self.process = process
        return f"https://{self.get_host(8080)}"

    def kill(self):
        # Kill the streaming process along with the sandbox
        if hasattr(self, "process"):
            self.process.kill()
        super().kill()



class SandboxAgent:

    def __init__(self, sandbox, output_dir=".", save_logs=True):
        super().__init__()
        self.messages = []  # Agent memory
        self.sandbox = sandbox  # E2B sandbox
        self.latest_screenshot = None  # Most recent PNG of the screen
        self.image_counter = 0  # Current screenshot number
        self.tmp_dir = tempfile.mkdtemp()  # Folder to store screenshots

        # Set the log file location
        if save_logs:
            logger.log_file = f"{output_dir}/log.html"

        print("The agent will use the following actions:")
        for action, details in tools.items():
            param_str = ", ".join(details.get("params").keys())
            print(f"- {action}({param_str})")

    def call_function(self, name, arguments):

        func_impl = getattr(self, name.lower()) if name.lower() in tools else None
        if func_impl:
            try:
                result = func_impl(**arguments) if arguments else func_impl()
                return result
            except Exception as e:
                return f"Error executing function: {str(e)}"
        else:
            return "Function not implemented."

    @staticmethod
    def tool(description, params):
        def decorator(func):
            tools[func.__name__] = {"description": description, "params": params}
            return func

        return decorator

    def save_image(self, image, prefix="image"):
        self.image_counter += 1
        filename = f"{prefix}_{self.image_counter}.png"
        filepath = os.path.join(self.tmp_dir, filename)
        if isinstance(image, Image.Image):
            image.save(filepath)
        else:
            with open(filepath, "wb") as f:
                f.write(image)
        return filepath

    def screenshot(self):
        file = self.sandbox.screenshot()
        filename = self.save_image(file, "screenshot")
        logger.log(f"screenshot {filename}", "gray")
        self.latest_screenshot = filename
        with open(filename, "rb") as image_file:
            return image_file.read()

    @tool(
        description="Run a shell command and return the result.",
        params={"command": "Shell command to run synchronously"},
    )
    def run_command(self, command):
        result = self.sandbox.commands.run(command, timeout=5)
        stdout, stderr = result.stdout, result.stderr
        if stdout and stderr:
            return stdout + "\n" + stderr
        elif stdout or stderr:
            return stdout + stderr
        else:
            return "The command finished running."

    @tool(
        description="Run a shell command in the background.",
        params={"command": "Shell command to run asynchronously"},
    )
    def run_background_command(self, command):
        self.sandbox.commands.run(command, background=True)
        return "The command has been started."

    @tool(
        description="Send a key or combination of keys to the system.",
        params={"name": "Key or combination (e.g. 'Return', 'Ctl-C')"},
    )
    def send_key(self, name):
        self.sandbox.press(name)
        return "The key has been pressed."

    @tool(
        description="Type a specified text into the system.",
        params={"text": "Text to type"},
    )
    def type_text(self, text):
        self.sandbox.write(text, chunk_size=TYPING_GROUP_SIZE, delay_in_ms=TYPING_DELAY_MS)
        return "The text has been typed."

    def click_element(self, query, click_command, action_name="click"):
        """Base method for all click operations"""
        self.screenshot()
        position = grounding_model.call(query, self.latest_screenshot)
        dot_image = draw_big_dot(Image.open(self.latest_screenshot), position)
        filepath = self.save_image(dot_image, "location")
        logger.log(f"{action_name} {filepath})", "gray")

        x, y = position
        self.sandbox.move_mouse(x, y)
        click_command()
        return f"The mouse has {action_name}ed."

    @tool(
        description="Click on a specified UI element.",
        params={"query": "Item or UI element on the screen to click"},
    )
    def click(self, query):
        return self.click_element(query, self.sandbox.left_click)

    @tool(
        description="Double click on a specified UI element.",
        params={"query": "Item or UI element on the screen to double click"},
    )
    def double_click(self, query):
        return self.click_element(query, self.sandbox.double_click, "double click")

    @tool(
        description="Right click on a specified UI element.",
        params={"query": "Item or UI element on the screen to right click"},
    )
    def right_click(self, query):
        return self.click_element(query, self.sandbox.right_click, "right click")

    def append_screenshot(self):
        return vision_model.call(
            [
                *self.messages,
                Message(
                    [
                        self.screenshot(),
                        "This image shows the current display of the computer. Please respond in the following format:\n"
                        "The objective is: [put the objective here]\n"
                        "On the screen, I see: [an extensive list of everything that might be relevant to the objective including windows, icons, menus, apps, and UI elements]\n"
                        "This means the objective is: [complete|not complete]\n\n"
                        "(Only continue if the objective is not complete.)\n"
                        "The next step is to [click|type|run the shell command] [put the next single step here] in order to [put what you expect to happen here].",
                    ],
                    role="user",
                ),
            ]
        )

    def run(self, instruction):

        self.messages.append(Message(f"OBJECTIVE: {instruction}"))
        logger.log(f"USER: {instruction}", print=False)

        should_continue = True
        while should_continue:
            # Stop the sandbox from timing out
            self.sandbox.set_timeout(60)

            content, tool_calls = action_model.call(
                [
                    Message(
                        "You are an AI assistant with computer use abilities.",
                        role="system",
                    ),
                    *self.messages,
                    Message(
                        logger.log(f"THOUGHT: {self.append_screenshot()}", "green")
                    ),
                    Message(
                        "I will now use tool calls to take these actions, or use the stop command if the objective is complete.",
                    ),
                ],
                tools,
            )

            if content:
                self.messages.append(Message(logger.log(f"THOUGHT: {content}", "blue")))

            should_continue = False
            for tool_call in tool_calls:
                name, parameters = tool_call.get("name"), tool_call.get("parameters")
                should_continue = name != "stop"
                if not should_continue:
                    break
                # Print the tool-call in an easily readable format
                logger.log(f"ACTION: {name} {str(parameters)}", "red")
                # Write the tool-call to the message history using the same format used by the model
                self.messages.append(Message(json.dumps(tool_call)))
                result = self.call_function(name, parameters)

                self.messages.append(
                    Message(logger.log(f"OBSERVATION: {result}", "yellow"))
                )
# END
#################################################################################################
# from e2b-dev/open-computer-use (https://github.com/e2b-dev/open-computer-use) # TODO refactor #
#################################################################################################



class E2BDesktopPromptHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = True
        self.icon_type = "E2BDesktopPrompt"
        self.fn_name = "E2BDesktopPrompt"
        self.type_category = ntcm.categories.rpa

        super().__init__()


    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Prompt:")
        fdl.entry(name="prompt", text="0", row=1, input_types=["str"], show_info=True, required=True)
        fdl.label("Pipette current position (Press Enter Key)")
        fdl.label("prompt = 'Open web browser and search for the current EUR to USD exchange rate'")
        return fdl

    def execute(self, node_detail_form):
        prompt = node_detail_form.get_chosen_value_by_name("prompt", variable_handler)

        self.direct_execute(prompt)

    def direct_execute(self, prompt):
        if not prompt:
            raise ValueError("Provide a prompt")

        inp=Input()
        inp.assign("prompt", prompt)

        self.input_execute(inp)

    def input_execute(self, inp):

        sandbox = StreamingFFMpegSandbox(api_key=sf.E2B_API_KEY)

        agent = SandboxAgent(sandbox)

        sandbox.stream.start()

        # Get stream URL
        url = desktop.stream.get_url()
        print(url)

        webbrowser.open(url)
        
        agent.run(inp("prompt"))
        
    def export_imports(self, *args):
        imports = ["import e2b"]
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

    'AirtableConnect': AirtableConnectHandler(),
    'AirtableAddRecord': AirtableAddRecordHandler(),
    'AirtableParseData': AirtableParseDataHandler(),
    'AirtableReadTable': AirtableReadTableHandler(),

    'SlackNotification': SlackNotificationHandler(),
    'EmailNotification': EmailNotificationHandler(),

    'CreateEmailBody': CreateEmailBodyHandler(),
    'E2BDesktopClick': E2BDesktopClickHandler(),
    'E2BDesktopPrompt': E2BDesktopPromptHandler(),
}