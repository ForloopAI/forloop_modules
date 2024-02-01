
import requests
from typing import Optional

from pathlib import Path
import sys

import forloop_modules.flog as flog
import json

#Duplicate in ncrb due to circular imports, remove from here once auth not used in ncrb
if sys.platform == "darwin":  # MAC OS
    config_path = 'config/server_config_remote.ini'
else:
    config_path = 'config/server_config.ini'

with Path(config_path).open(mode='r') as f:
    rows = f.readlines()


SERVER = rows[0].split("=")[1].strip()
PORT = rows[1].split("=")[1].strip()
BASE_API = f'{SERVER}:{PORT}/api/v1'


#! Duplicate functions to those in ncrb (they are here due to circular imports)
def initialize_last_or_new_project_by_email(email: str):
    """Duplicate in ncrb due to circular imports, remove from here once auth not used in ncrb"""
    payload = {"email": email}

    url = f'{BASE_API}/initialize_last_or_new_project_by_email'

    response = requests.post(url, json = payload)
    flog.info(f'Response: {response.text}')

    return response

def initialize_last_or_new_pipeline(project_uid: str):
    """Duplicate in ncrb due to circular imports, remove from here once auth not used in ncrb"""   
    payload = {"project_uid": project_uid}

    url = f'{BASE_API}/initialize_last_or_new_pipeline?project_uid='+str(project_uid)

    response = requests.post(url, json = payload)
    flog.info(f'Response: {response.text}')

    return response

#{"user":{"email":"dominik@forloop.ai","auth0_subject_id":"google-oauth2|117904919065527607983","given_name":"Dominik","family_name":"Vach","picture_url":"https://lh3.googleusercontent.com/a/ACg8ocJesTb0ftxmvNbTJHwFf3nfYojgdq4eTslDZ8ewPxbC_A=s96-c"},"session":{"auth0_session_id":"lCkKE8qIUgjxSU3J9q6lZntc2ZUDat6T","version":null,"platform_type":"cloud","ip":null,"mac_address":null,"hostname":null}}
def refresh_user_and_session_heartbeat(email: str):
    """Duplicate in ncrb due to circular imports, remove from here once auth not used in ncrb"""
    payload = {"user":{"email":email,"auth0_subject_id":None,"given_name":None,"family_name":None,"picture_url":None},"session":{"auth0_session_id":None,"version":None,"platform_type":"desktop","ip":None,"mac_address":None,"hostname":None}}
    url = f'{BASE_API}/refresh_user_and_session_heartbeat'

    response = requests.post(url, json = payload)
    flog.info(f'Response: {response.text}')

    return response


#####! Duplicate functions end #####


class ActiveEntityTracker:
    """The purpose of this structure is to contain information about last active entities of a given runtime - either from Execution core or from the GUI
    Examples of what it can store: last_active_df, last_active_project, last_active_pipeline etc.
    It is user specific but doesnt explicitly require user's auth details
    
    It doesnt necessarily have to be a singleton - Idea: one GUI can have one AET (activeentitytracker), 
    one exec core ~ one AET until it switches to a new job for a different user where it can be reinitialized
    
    """
    def __init__(self):
        self.project_uid: str = None #Todo: rename to active
        self.active_user_uid: str = None
        self.active_session_uid: str = None
        self.active_pipeline_uid: str = None
        self.active_script_uid: str = None
        self.active_pipeline_job_uid: Optional[str] = None
        self.home_folder = None     # To be refactored to another location (should be stores as user metadata) - refactored to AET for now
 
    
    def set_home_folder(self):
        """
        Sets the default folder for platform outputs.
        """

        # TODO: for now the value is hardcoded, and home_folder is added to all paths manually.
        # The plan is to introduce custom Forloop-Path object, that should (among other things) help to simplify this.

        flog.warning('Setting the home folder')
        self.home_folder = 'output/'
        Path(self.home_folder).mkdir(exist_ok=True)

    def _initialize_project_and_pipeline_after_login(self, email: str):
        project_response = initialize_last_or_new_project_by_email(email)
        self.project_uid = json.loads(project_response.content.decode('utf-8'))["uid"]
        pipeline_response = initialize_last_or_new_pipeline(self.project_uid)
        self.active_pipeline_uid= json.loads(pipeline_response.content.decode('utf-8'))["uid"]
        user_response = refresh_user_and_session_heartbeat(email)
        self.active_user_uid = json.loads(user_response.content.decode('utf-8'))["uid"]
        
        
        

    def set_project_and_pipeline_uid(
        self, project_uid: Optional[str] = None, pipeline_uid: Optional[str] = None
    ) -> None:
        self.project_uid = project_uid if project_uid is not None else self.project_uid
        self.active_pipeline_uid = pipeline_uid if pipeline_uid is not None else self.active_pipeline_uid

    def set_pipeline_job_uid(self, pipeline_job_uid: str) -> None:
        self.active_pipeline_job_uid = pipeline_job_uid

aet=ActiveEntityTracker()