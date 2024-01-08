import json
import random
import string

from typing import Optional

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.globals.active_entity_tracker import aet

class ScriptNotFoundError(Exception):
    pass

class AmbiguousRequestError(Exception):
    pass

def create_new_script(script_name: str = "untitled", text: str = ""):
    """Creates a new script on the API and in case of a successful call it stores it's uid into auth --> aet.active_script_uid.

    Args:
        script_name (str, optional): A name of the new script. Defaults to "untitled".
        text (str, optional): Text that will put in as a code of a new script. Defaults to "".
    """    
    response = ncrb.new_script(script_name=script_name, text=text)
    
    if response.ok:
        new_script_uid = json.loads(response.content.decode('utf-8'))["uid"]
        aet.active_script_uid = new_script_uid
        flog.info(f'New script created, uid "{new_script_uid}" stored.')
    else:
        flog.error(f'Error {response.status_code}: {response.reason}.')
        
def update_active_script(code: str, script_name: Optional[str] = "untitled"):
    """If active script already exists, i.e. aet.active_script_uid is not None, the last active script is updated, else a new script is initialized.

    Args:
        code (str): Text to be put into an updated or a new script as its code.
        script_name (Optional[str], optional): A new name of an existing script (in case of creating a new one, the name won't be passed). Defaults to None.
    """    
    if aet.active_script_uid is not None:
        response = ncrb.update_script_by_uid(aet.active_script_uid, script_name=script_name, text=code)
        
        if not response.ok:
            flog.error(f'Error {response.status_code}: {response.reason}.')
    else:
        create_new_script(text=code)
        
def get_script_by_name(script_name: str):
    response = ncrb.get_all_scripts()
        
    if not response.ok:
        raise Exception(f'Error {response.status_code} - {response.reason}.')
    
    scripts = response.json()
    scripts_matching_name = [script for script in scripts if script["script_name"] == script_name 
                                and script["project_uid"] == aet.project_uid]
    
    if len(scripts_matching_name) == 0:
        raise ScriptNotFoundError(f'No script named "{script_name}" found for project_uid == {aet.project_uid}.')
    elif len(scripts_matching_name) > 1:
        message = f'Ambiguous request: {len(scripts_matching_name)} scripts named "{script_name}" found.'
        raise AmbiguousRequestError(message)
    
    script = scripts_matching_name[0]
    
    return script
        
def generate_random_id(size: int = 6, chars: str = string.ascii_letters + string.digits):
    random_id = "".join(random.choice(chars) for _ in range(size))
    
    return random_id
