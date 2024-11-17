#### GLC, GOM dependencies forbidden !!!
"""In this file all functions should have response-like return value"""

import json
import sys
from inspect import Parameter, Signature
from pathlib import Path
from typing import Any, Generator, Optional

from httpx import Response

import forloop_modules.flog as flog
from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.queries.db_model_templates import (
    APIDatabase,
    APIDataset,
    APIDbTable,
    APIEdge,
    APIFile,
    APIInitialVariable,
    APIPipeline,
    APIPopup,
    APIProject,
    APIScript,
    APITrigger,
    APIVariable,
)
from forloop_modules.utils.http_client import HttpClient

if sys.platform == "darwin":  # MAC OS
    config_path = 'config/server_config_remote.ini'
else:
    config_path = 'config/server_config.ini'

with Path(config_path).open(mode='r') as f:
    rows = f.readlines()

SERVER = rows[0].split("=")[1].strip()
PORT = rows[1].split("=")[1].strip()
BASE_API = f'{SERVER}:{PORT}/api/v1'
http_client = HttpClient()

RESOURCES = {
    "databases": ["get_all", "get", "new", "delete", "update"],
    "dbtables": ["get_all", "get", "new", "delete", "update"],
    "files": ["get_all", "get", "new", "delete", "update"],
    "scripts": ["get_all", "get", "new", "delete", "update"],
    "datasets": ["get_all", "get", "new", "delete", "update"],
    "edges": ["get_all", "get", "new", "delete", "update"],
    "variables": ["get_all", "get", "new", "delete"],
    "popups": ["get_all", "get", "delete"],
    "nodes": ["get_all", "get", "delete"],
    "pipelines": ["get_all", "get", "new", "delete", "update"],
    "initial_variables": ["get_all", "get", "new", "delete"],
}

DB_API_BODY_TEMPLATE = {
    "projects": APIProject,
    "triggers": APITrigger,
    "datasets": APIDataset,
    "dbtables": APIDbTable,
    "scripts": APIScript,
    "files": APIFile,
    "databases": APIDatabase,
    "pipelines": APIPipeline,
    "edges": APIEdge,
    "variables": APIVariable,
    "popups": APIPopup,
    "initial_variables": APIInitialVariable,
}


def set_stored_project_uid_and_pipeline_uid_to_factory_payload(payload: dict):
    """
    Sets project_uid and pipeline_uid values in payload from those stored in auth handler.

    Args:
        payload (dict): API request payload.
    """
    # This approach might not be safe if API validation becomes strict (not all calls take both
    # project_uid and pipeline_uid as parameters)
    # TODO: Think about a better implementation (due to the point above)
    payload['project_uid'] = aet.project_uid
    payload['pipeline_uid'] = aet.active_pipeline_uid


def get_all_factory(resource_name):
    resource_url = f'{SERVER}:{str(PORT)}/api/v1/{resource_name}'
    def get_all():
        response = http_client.get(resource_url)
        #flog.info(f'GET all response: {response.text}')
        return response
    get_all.__name__ = f'get_all_{resource_name}'
    return get_all

def get_factory(resource_name):
    def get(resource_uid):
        resource_url = f'{BASE_API}/{resource_name}/{resource_uid}'
        response = http_client.get(resource_url)
        #flog.info(f'GET response: {response.text}')
        return response
    get.__name__ = f'get_{resource_name}_by_uid'
    return get

def delete_factory(resource_name):
    def delete(resource_uid):
        resource_url = f'{BASE_API}/{resource_name}/{resource_uid}'
        response = http_client.delete(resource_url)
        #flog.info(f'DELETE response: {response.text}')
        return response
    delete.__name__ = f'delete_{resource_name}_by_uid'
    return delete



def new_factory(resource_name, model):
    #list of pydantic attributes
    model_attribute_names = list(vars(model()).keys())
    #remove the uid attribute – see VariableModel and APIVariable
    model_attribute_names_without_uid = [v for v in model_attribute_names if v != 'uid']
    #the new function (eg new_database) will expect same args as what the attributes are (without uid)
    params = [Parameter(name, Parameter.POSITIONAL_OR_KEYWORD) for name in model_attribute_names_without_uid]
    
    def new(*args,model_attribute_names_without_uid=model_attribute_names_without_uid, **kwargs):
        payload = {
            param: arg
            for param, arg in zip(model_attribute_names_without_uid, args)
        }
        payload.update(kwargs)
        set_stored_project_uid_and_pipeline_uid_to_factory_payload(payload)
        if issubclass(model, APIVariable):
            payload["pipeline_job_uid"] = aet.active_pipeline_job_uid

        #flog.info(f'New {resource_name} payload: {payload}')
        resource_url = f'{BASE_API}/{resource_name}'

        response = http_client.post(resource_url, json=payload)
        #flog.info(f'New {resource_name} response: {response.text}')
        if not response.ok:
            flog.error(response.json())
            # response.raise_for_status()
        return response

    new.__signature__ = Signature(params)
    new.__name__ = f'new_{resource_name}'

    return new

def update_factory(resource_name, model):
    #list of pydantic class attributes
    model_attribute_names = list(vars(model()).keys())
    #remove the uid attribute – see VariableModel and APIVariable
    model_attribute_names_without_uid = [v for v in model_attribute_names if v != 'uid']
    params = [Parameter(name, Parameter.POSITIONAL_OR_KEYWORD) for name in model_attribute_names_without_uid]

    def update(uid, *args, model_attribute_names_without_uid=model_attribute_names_without_uid, **kwargs):
        payload = {
            param: arg
            for param, arg in zip(model_attribute_names_without_uid, args)
        }
        payload.update(kwargs)
        set_stored_project_uid_and_pipeline_uid_to_factory_payload(payload)
        if issubclass(model, APIVariable):
            payload["pipeline_job_uid"] = aet.active_pipeline_job_uid

        #flog.info(f'Update {resource_name} payload: {payload}')
        resource_url = f'{BASE_API}/{resource_name}/{uid}'

        response = http_client.put(resource_url, json=payload)
        #flog.info(f'Update {resource_name} response: {response.text}')
        if not response.ok:
            flog.error(response.json())
        return response

    update.__signature__ = Signature([Parameter('uid', Parameter.POSITIONAL_OR_KEYWORD)] + params)
    update.__name__ = f'update_{resource_name}_by_uid'

    return update


for resource_name, actions in RESOURCES.items():
    resource_name_singular = resource_name[:-1]
    for action in actions:
        function_name = None
        if action == 'get_all':
            fn = get_all_factory(resource_name)
            function_name = f'{action}_{resource_name}'
        elif action == 'get':
            fn = get_factory(resource_name_singular)
            function_name = f'{action}_{resource_name_singular}_by_uid'
        elif action == 'new':
            model = DB_API_BODY_TEMPLATE[resource_name]
            fn = new_factory(resource_name, model)
            function_name = f'{action}_{resource_name_singular}'
        elif action == 'delete':
            fn = delete_factory(resource_name_singular)
            function_name = f'{action}_{resource_name_singular}_by_uid'
        elif action == 'update':
            model = DB_API_BODY_TEMPLATE[resource_name]
            fn = update_factory(resource_name_singular, model)
            function_name = f'{action}_{resource_name_singular}_by_uid'
        else:
            raise Exception('Unknown action')

        globals()[function_name] = fn




# def get_project_key() -> Optional[str]:
#     project_key = auth.user_email
#     if project_key is not None:
#         project_key = project_key.replace("@", "at")

#     return project_key


def get_project_uid() -> Optional[str]:
    project_uid = aet.project_uid
    
    return project_uid

def remove_none_values_from_payload(payload:dict) -> dict:
    payload = {key:value for key, value in payload.items() if value is not None}
    
    return payload


############### Nodes #################

def new_node(pos, typ, params_dict: Optional[dict] = None, fields: Optional[list] = None, visible: Optional[bool] = True):
    project_uid = aet.project_uid
    pipeline_uid = aet.active_pipeline_uid
    
    payload = {
        "pos": pos,
        "typ": typ,
        "params": params_dict,
        "fields": fields,
        "pipeline_uid": pipeline_uid,
        "project_uid": project_uid,
        "visible": visible
    }
    payload = {k:v for k,v in payload.items() if v is not None} # sending None breaks node reflection, no need to send empty field on node generation


    flog.info(f'New Node payload: {payload}')

    url = f'{BASE_API}/nodes'

    response = http_client.post(url, json=payload)
    flog.info(f'New Node response: {response.text}')

    return response


def direct_execute_node(node_uid):
    
    payload = dict()

    flog.info(f'Direct execute Node payload: {payload}')

    url = f'{BASE_API}/node_direct_execute/{node_uid}'

    response = http_client.post(url, json=payload)
    flog.info(f'Direct execute Node response: {response.text}')
    
    return response



def export_node_code(node_uid):
    

    payload = dict()

    flog.info(f'Export Node code payload: {payload}')

    url = f'{BASE_API}/nodes/{node_uid}/export_code'

    response = http_client.post(url, json=payload)
    flog.info(f'Export Node code response: {response.text}')
    
    return response
    


def get_node_by_uid(node_uid):
    url = f'{BASE_API}/nodes/{node_uid}'

    response = http_client.get(url)
    flog.info(f'GET Node response: {response.text}')
    
    return response


def get_all_nodes():
    url = f'{BASE_API}/nodes'
    response = http_client.get(url)
    flog.debug(f'GET all Nodes response: {response.text}')

    return response
    
def delete_node_by_uid(node_uid):
    url = f'{BASE_API}/nodes/{node_uid}'
    response = http_client.delete(url)
    flog.info(f'DELETE Node response: {response.text}')

    return response
    
# def delete_node_by_uid(node_uid):
#     url = f'{BASE_API}/node/{node_uid}'
#
#     response = http_client.delete(url)
#     flog.info(f'DELETE Node response: {response.text}')
#
#     return response
    

def delete_all_nodes():
    pipeline_uid = aet.active_pipeline_uid
    url = f'{BASE_API}/pipelines/{pipeline_uid}/nodes'
    response = http_client.delete(url)
    response.raise_for_status()
    return response


def move_node_by_uid(node_uid, new_pos):
    payload = {
        "new_pos": new_pos,
    }


    flog.info(f'Move Node payload: {payload}')
    url = f'{BASE_API}/nodes/{node_uid}/move'
    response = http_client.put(url, json=payload)
    flog.info(f'Move Node response: {response.text}')
    return response

def node_breakpoint_status(node_uid, breakpoint_status):
    payload = {
        "uid": node_uid,
        "status": breakpoint_status,
    }

    flog.info(f'Node breakpoint status: {payload}')
    url = f'{BASE_API}/nodes/{node_uid}/breakpoint'
    response = http_client.put(url, json=payload)
    flog.info(f'Node breakpoint status: {response.text}')
    return response

def node_disabled_status(node_uid, disabled_status):
    payload = {
        "uid": node_uid,
        "status": disabled_status,
    }

    flog.info(f'Node disabled status: {payload}')
    url = f'{BASE_API}/nodes/{node_uid}/status'
    response = http_client.put(url, json=payload)
    flog.info(f'Node disabled status: {response.text}')
    return response

def update_node_params_by_node_detail_form(node_detail_form):
    node_params = node_detail_form.node_params.params_dict_repr()
    node_uid = node_detail_form.node_uid
    update_node_by_uid(node_uid, params=node_params)


def update_node_by_uid(node_uid, pos=None, typ=None, params=None, fields=None):
    # TODO: if inputs are None, it should not put them in payload
    # TODO: rename params to params_dict as in other functions

    project_uid = aet.project_uid
    pipeline_uid = aet.active_pipeline_uid

    #Do not erase this until checked that all variables correctly parsed from json payload
    #payload_pos=json.dumps(pos) #pos was sometimes dumped as "[123,233]" which could not be processed by API
    #if '"' in payload_pos: #Remove apostrophes from list:
    #    payload_pos=payload_pos.replace('"','')
    #payload='{"pos":'+payload_pos+',"typ":'+json.dumps(typ)+',"params":'+json.dumps(params)+',"fields":'+json.dumps(fields)+',"project_uid":"'+project_uid+'"}'#+'}' #
    #response=http_client.put(SERVER+":"+str(PORT)+"/api/v1/nodes/"+str(node_uid),data=payload)
    #return(response)

    # type=None has to be send, otherwise the icon type changes to "Custom"
    payload = {
        "pos": pos,
        "typ": typ,
        "params": params,
        "fields": fields,
        "project_uid": project_uid,
        "pipeline_uid": pipeline_uid
    }

    flog.info(f'Updated Node payload: {payload}')

    url = f'{BASE_API}/nodes/{node_uid}'

    response = http_client.put(url, json=payload)
    flog.info(f'Updated Node response: {response.text}')

    return response


####### SPECIAL NODE - ITEM DETAIL FORM NODE BUTTON FUNCTIONS #########

def node_button_click(node_uid, button_name):
    
    payload = {
        "button_name": button_name
    }

    flog.info(f'Direct execute Node payload: {payload}')

    url = f'{BASE_API}/node_click_button/{node_uid}'

    response = http_client.post(url, json=payload)
    flog.info(f'Direct execute Node response: {response.text}')
    
    return response


# unwraps button function_args into multiple parameters (cant call node_button_click with function_args directly as it is stored in a list, but the function expects 2 parameters)
def node_button_click_wrapper(args):
    assert len(args) == 2
    node_button_click(args[0], args[1])









############### Edges #################
"""In this file all functions should have response-like return value"""


#Todo
# def new_edge(from_node_uid, to_node_uid, visible:bool=True):
#     project_uid = aet.project_uid
#
#     payload = {
#         "from_node_uid": from_node_uid,
#         "to_node_uid": to_node_uid,
#         "project_uid": project_uid,
#         "visible": visible
#     }
#
#     flog.info(f'New Edge payload: {payload}')
#
#     url = f'{BASE_API}/edges'
#
#     response = http_client.post(url, json=payload)
#     flog.info(f'New Edge response: {response.text}')
#
#     return response


# def get_all_edges():
#     url = f'{BASE_API}/edges'
#
#     response = http_client.get(url)
#     flog.info(f'GET all Edges response: {response.text}')
#
#     return response

def get_edges_by_node_uid(node_uid:str):
    url = f'{BASE_API}/nodes/{node_uid}/edges'
    response = http_client.get(url)
    flog.debug(f'GET Edges by Node Uid response: {response.text}')

    return response

def get_edge_by_connected_node_uids(from_node_uid:str, to_node_uid:str):
    url = f'{BASE_API}/edges/?from_node_uid={from_node_uid}&to_node_uid={to_node_uid}'
    response = http_client.get(url)
    flog.info(f'GET Edge by Connected Node Uids response: {response.text}')

    return response

    

def delete_edge_by_uid(uid):
    url = f'{BASE_API}/edges/{uid}'

    response = http_client.delete(url)
    flog.info(f'DELETE Edge response: {response.text}')

    return response


def delete_all_edges():
    pipeline_uid = aet.active_pipeline_uid
    url = f'{BASE_API}/pipelines/{pipeline_uid}/edges'
    response = http_client.delete(url)
    response.raise_for_status()
    return response

### LAST ACTIVE DF


def update_last_active_dataframe_node_uid(last_active_dataframe_node_uid: Optional[str]):
    payload = {
        "project_uid": aet.project_uid,
        "last_active_dataframe_node_uid": last_active_dataframe_node_uid
    }

    flog.info(f'Last active DF node_uid payload: {payload}')

    url = f'{BASE_API}/last_active_dataframe_node_uid'

    response = http_client.put(url, json=payload)
    flog.info(f'Last active DF node_uid response: {response.text}')

    return response


def get_last_active_dataframe_node_uid():
    url = f'{BASE_API}/last_active_dataframe_node_uid?project_uid={aet.project_uid}'
    response = http_client.get(url)
    flog.debug(f'GET Last active DF node_uid response: {response.text}')

    return response


##### VARIABLES #####


########### Variables ###############
"""In this file all functions should have response-like return value"""


# def new_variable(name, value, type=None, size=None):
#     # TODO: solve DFs, dicts, lists, objects etc
#
#     project_uid = get_project_uid()
#
#     payload = {
#         "name": name,
#         "value": value,
#         "type": type,
#         "size": size,
#         "project_uid": project_uid
#     }
#
#     flog.info(f'New Variable payload: {payload}')
#
#     url = f'{BASE_API}/variables'
#
#     response = http_client.post(url, json=payload)
#     flog.info(f'New Variable response: {response.text}')
#
#     return response


def get_variable(uid: str) -> Optional[dict]:
    url = f'{BASE_API}/variables/{uid}'
    response = http_client.get(url)
    try:
        response.raise_for_status()
    except Exception:
        if response.status_code == 404:
            return None
        raise
    return response.json()


def get_variable_by_name(variable_name: str):
    pipeline_job_uid = aet.active_pipeline_job_uid
    url = f'{BASE_API}/variables?name={variable_name}&pipeline_job_uid={pipeline_job_uid}'

    response = http_client.get(url)
    response.raise_for_status()
    flog.info(f'GET Variable by name response: {response.text}')

    return response


# def get_all_variables():
#     url = f'{BASE_API}/variables'
#
#     response = http_client.get(url)
#     flog.info(f'GET all Variables response: {response.text}')
#
#     return response


    

def delete_variable_by_uid(variable_uid):
    url = f'{BASE_API}/variables/{variable_uid}'

    response = http_client.delete(url)
    response.raise_for_status()
    flog.info(f'DELETE Variable response: {response.text}')

    return response


def delete_all_variables():
    pipeline_uid = aet.active_pipeline_uid
    url = f'{BASE_API}/pipelines/{pipeline_uid}/variables'
    response = http_client.delete(url)
    response.raise_for_status()
    return response


def update_variable_by_uid(variable_uid: str, name: str, value: Any, is_result: bool = None, type = None, size: Optional[int] = None):
    project_uid = aet.project_uid
    pipeline_uid = aet.active_pipeline_uid
    pipeline_job_uid = aet.active_pipeline_job_uid

    if type is None:
        for std_type in [str,int,list,dict,float,bool]:
            if isinstance(value,std_type):
                type=std_type.__name__
                
    payload = {
        "name": name,
        "value": value,
        "type": type,
        "size": size,
        "project_uid": project_uid,
        "pipeline_uid": pipeline_uid,
        "pipeline_job_uid": pipeline_job_uid
        }
    if is_result is not None:
        payload["is_result"] = is_result

    response=http_client.put(f"{BASE_API}/variables/{variable_uid}",json=payload)
    result=json.loads(response.content.decode('utf-8'))
    return(response)

  
# def update_variable_by_uid(variable_uid,name,value,type=None,size=None):
#     # TODO: don't we miss project_uid here?
    
#     payload = {
#         "name": name,
#         "value": value,
#         "type": type,
#         "size": size
#     }

#     flog.info(f'Updated Variable payload: {payload}')
#     url = f'{BASE_API}/variable/{variable_uid}'
    
#     response = http_client.put(url, json=payload)
#     flog.info(f'Updated Variable response: {response.text}')

#     return response


def get_job_variables():
    job_uid = aet.active_pipeline_job_uid
    url = f'{BASE_API}/jobs/{job_uid}/variables'
    response = http_client.get(url)
    response.raise_for_status()
    return response

def cancel_pipeline_job():
    job_uid = aet.active_pipeline_job_uid
    url = f'{BASE_API}/jobs/{job_uid}/cancel'
    response = http_client.post(url)
    response.raise_for_status()
    return response

def cancel_prototype_job(uid: str):
    url = f'{BASE_API}/prototype_jobs/{uid}/cancel'
    response = http_client.post(url=url)
    
    return response

def consume_execution_stream(job_uid: str) -> Generator[dict, None, None]:
    """Run in a separate thread as this is a blocking operation."""
    url = f'{BASE_API}/jobs/{job_uid}/execution_stream'
    yield from http_client.sse_stream("GET", url, as_dict=True)


##### INITIAL VARIABLES #####


def get_initial_variable_by_name(uid: str):
    pipeline_uid = aet.active_pipeline_uid
    url = f'{BASE_API}/initial_variables?name={uid}&pipeline_uid={pipeline_uid}'

    response = http_client.get(url)
    response.raise_for_status()
    return response


def delete_initial_variable_by_uid(uid):
    response = http_client.delete(f'{BASE_API}/initial_variables/{uid}')
    response.raise_for_status()
    return response


def delete_all_initial_variables():
    pipeline_uid = aet.active_pipeline_uid
    response = http_client.delete(f'{BASE_API}/pipelines/{pipeline_uid}/initial_variables')
    response.raise_for_status()
    return response


def update_initial_variable_by_uid(
    variable_uid: str, name: str, value: Any, is_result: bool, type=None, size: Optional[int] = None
):
    project_uid = aet.project_uid
    pipeline_uid = aet.active_pipeline_uid
    pipeline_job_uid = aet.active_pipeline_job_uid

    if type is None:
        for std_type in [str, int, list, dict, float, bool]:
            if isinstance(value, std_type):
                type = std_type.__name__

    payload = {
        "name": name, "value": value, "type": type, "size": size, "project_uid": project_uid,
        "pipeline_uid": pipeline_uid, "pipeline_job_uid": pipeline_job_uid, "is_result": is_result
    }
    response = http_client.put(f"{BASE_API}/initial_variables/{variable_uid}", json=payload)
    response.raise_for_status()
    return response


############# Dbtables ###############
#
# def new_dbtable(name, pos, columns, is_rolled, database_uid, project_uid):
#     # payload = '{"name":"' + str(name) + '","pos":'+ json.dumps(pos) + ',"columns":' + json.dumps(columns) + ',"is_rolled":' + json.dumps(
#     #     is_rolled) + ',"database_uid":"' + database_uid + '","project_uid":"' + project_uid + '"}'  # TODO: solve DFs, dicts, lists, objects etc.
#     # #
#     #
#     payload = {
#         "name": str(name),
#         "pos": pos,
#         "columns": columns,
#         "is_rolled": is_rolled,
#         "database_uid": database_uid,
#         "project_uid": project_uid
#     }
#     flog.info(payload)
#     response = http_client.post(SERVER + ":" + str(PORT) + "/api/v1/dbtables", json=payload)
#     return (response)


# def get_all_dbtables():
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/dbtables")
#     return(response)

# def get_dbtable_by_uid(dbtable_uid):
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/dbtable/"+str(dbtable_uid))
#     return(response)
#
# def update_dbtable_by_uid(dbtable_uid, name, pos, columns, is_rolled, database_uid, project_uid):
#     # payload = '{"name":"' + str(name) + '","pos":' + json.dumps(pos) + ',"columns":' + json.dumps(
#     #     columns) + ',"is_rolled":' + json.dumps(
#     #     is_rolled) + ',"database_uid":"' + database_uid + '","project_uid":"' + project_uid + '"}'  # TODO: solve DFs, dicts, lists, objects etc.
#     payload = {
#         "name": str(name),
#         "pos": pos,
#         "columns": columns,
#         "is_rolled": is_rolled,
#         "database_uid": database_uid,
#         "project_uid": project_uid
#     }
#     flog.info(payload)
#     response = http_client.put(SERVER + ":" + str(PORT) + "/api/v1/dbtable/"+str(dbtable_uid), json=payload)
#     return (response)

##### files #####


########### files ###############
"""In this file all functions should have response-like return value"""


# def new_file(file_name: str, data=None):
#     project_uid = "0"
#
#     # TODO: not tested with data
#     payload={
#         "file_name": file_name,
#         "data": json.dumps(data),
#         "project_uid": project_uid
#     }
#     flog.info(f'New File payload: {payload}')
#     print("REQUEST", SERVER, PORT, payload)
#     response=http_client.post(SERVER+":"+str(PORT)+"/api/v1/files",json=payload)
#     flog.info(f'New File response: {response.text}')
#
#     return(response)

def upload_urls_from_file(path:str):
    file = {'file': open(path, 'rb')}
    response = http_client.post(SERVER+":"+str(PORT)+"/api/v1/upload_urls_from_file", files=file)
    flog.info(f"file upload response: {response.text}")

    return response


# def get_file_by_name(file_name):
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/get_file_by_name/"+str(file_name))
#     return(response)


# def get_all_files():
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/files")
#     return(response)

    
# def delete_file_by_uid(file_uid):
#     response=http_client.delete(SERVER+":"+str(PORT)+"/api/v1/file/"+str(file_uid))
#     return(response)
    

def delete_all_files():
    # TODO FIX PROJECT UID
    # project_uid = 
    project_uid = get_project_uid()

    payload='{"project_uid":"'+project_uid+'"}'
    flog.info(payload)
    response=http_client.delete(SERVER+":"+str(PORT)+"/api/v1/files",data=payload)
    return(response)
    




# def update_file_by_uid(file_uid,name,value,type=None,size=None):
#     payload='{"name":'+json.dumps(name)+',"value":'+json.dumps(value)+',"type":'+json.dumps(type)+',"size":'+json.dumps(size)+'}' #
#     flog.info(payload)
#     response=http_client.put(SERVER+":"+str(PORT)+"/api/v1/file/"+str(file_uid),data=payload)
#     result=json.loads(response.content.decode('utf-8'))
#     return(response)




# def delete_dbtable_by_uid(dbtable_uid):
#     response = http_client.delete(SERVER + ":" + str(PORT) + "/api/v1/dbtable/" + str(dbtable_uid))
#     return (response)


    


############## Pipelines ##############
"""In this file all functions should have response-like return value"""

# def get_all_pipelines():
#     url = f'{BASE_API}/pipelines'
#
#     response = http_client.get(url)
#     flog.info(f'GET all Pipelines response: {response.text}')
#
#     return response


# def delete_pipeline_by_uid(pipeline_uid):
#     url = f'{BASE_API}/pipeline/{pipeline_uid}'
#
#     response = http_client.delete(url)
#     flog.info(f'DELETE Pipeline response: {response.text}')
#
#     return response
#
#
# def new_pipeline(pos, typ, params_dict):
#     project_uid = get_project_uid()
#
#     payload = {
#         "pos": pos,
#         "typ": typ,
#         "params": params_dict,
#         "project_uid": project_uid
#     }
#
#     flog.info(f'New Pipeline payload: {payload}')
#
#     url = f'{BASE_API}/pipelines'
#
#     response = http_client.post(url, json=payload)
#     flog.info(f'New Pipeline response: {response.text}')
#
#     return response

def pipeline_refresh_building_blocks(pipeline_uid:str): 
    url = f'{BASE_API}/pipeline_refresh_building_blocks/{pipeline_uid}'

    response = http_client.get(url)
    flog.info(f'GET Pipeline Refresh Building Blocks response: {response.text}')

    return response

def pipeline_refresh_running_blocks(pipeline_uid:str): 
    url = f'{BASE_API}/pipeline_refresh_running_blocks/{pipeline_uid}'

    response = http_client.get(url)
    flog.info(f'GET Pipeline Refresh Building Blocks response: {response.text}')

    return response

def activate_pipeline(pipeline_uid: str, project_uid: str):
    url = f'{BASE_API}/projects/{project_uid}/pipelines/{pipeline_uid}/activate'

    response = http_client.post(url)
    response.raise_for_status()
    return response


############ Popups ###################
"""In this file all functions should have response-like return value"""

def new_popup_wrapper(args_list):
    assert len(args_list) > 1
    new_popup(*args_list)


def new_popup(pos, typ, params_dict=None):
    """
    Generates new Popup via API.
    :param pos: position of new Popup as [x, y]
    :param typ: Popup type, e.g. InactiveBrowserPopup, ConfirmActionPopup, etc.
    :param params_dict: Popup params (if any) as
            params_dict = {
                'xpath': {'variable': None, 'value': '/main/div/button'}
            }

    # TODO: add default values for pos as [500, 400]
    """

    project_uid = get_project_uid()

    if params_dict is None:
        params_dict = dict()

    payload = {
        "pos": pos,
        "typ": typ,
        "params": params_dict,
        "project_uid": project_uid
    }

    flog.info(f'New Popup payload: {payload}')

    url = f'{BASE_API}/popups'

    response = http_client.post(url, json=payload)
    flog.info(f'New Popup response: {response.text}')

    return response


def update_popup_by_uid(popup_uid, pos=None, typ=None, params_dict=None):
    """
    Updates existing Popup via API.
    :param popup_uid: Popup ID
    :param pos: updated position of the Popup as [x, y]
    :param typ: updated Popup type, e.g. InactiveBrowserPopup, ConfirmActionPopup, etc.
    :param params_dict: updated Popup params (if any) as
            params_dict = {
                'xpath': {'variable': None, 'value': '/main/div/button'}
            }

    TODO: is Popup typ param needed? What are possible use-cases of changing Popup type?
    TODO: shouldn't pos param be mandatory or have default value not None?
        There was already situation, when None as default here caused the problem with changing all Popups to Custom type,
        since typ was not specified in function call
    TODO: if inputs are None, it should not put them in payload
    """

    project_uid = get_project_uid()

    payload = {}
    payload["uid"]=popup_uid
    
    if pos is not None:
        payload["pos"]=pos
    if typ is not None:
        payload["typ"]=typ
    if params_dict is not None:
        payload["params"]=params_dict
    payload["project_uid"]=project_uid
    
    
    flog.info(f'Updated Popup payload: {payload}')

    url = f'{BASE_API}/popup/{popup_uid}'

    response = http_client.put(url, json=payload)
    flog.info(f'Updated Popup response: {response.text}')

    return response


# def get_all_popups():
#     url = f'{BASE_API}/popups'
#
#     response = http_client.get(url)
#     flog.info(f'GET all Popups response: {response.text}')
#
#     return response


# def get_popup_by_uid(popup_uid):
#     url = f'{BASE_API}/popup/{popup_uid}'
#
#     response = http_client.get(url)
#     flog.info(f'GET Popup response: {response.text}')
#
#     return response


# def delete_popup_by_uid(popup_uid):
#     url = f'{BASE_API}/popup/{str(popup_uid)}'
#     response = http_client.delete(url)
#     flog.info(f'DELETE Popup response: {response.text}')
#
#     return response


############### Scripts ###############

# def new_script(script_name, text="", project_uid="0"):
#
#     payload = f'{{"script_name":"{script_name}", "text":{json.dumps(text)}, "project_uid":{project_uid}}}'
#
#     flog.info(f'New Script payload: {payload}')
#
#     url = f'{SERVER}:{PORT}/api/v1/scripts'
#
#     response = http_client.post(url, data=payload)
#     flog.info(f'New Script response: {response.text}')
#
#     return response
#
#
# def update_script_by_uid(script_uid, script_name, text, project_uid="0"):
#
#     payload = f'{{"script_name":"{script_name}", "text":{json.dumps(text)}, "project_uid":{project_uid}}}'
#
#     flog.info(f'Update Script by Uid Payload: {payload}')
#
#     response = http_client.put(SERVER + ":" + str(PORT) + "/api/v1/script/" + str(script_uid), data=payload)
#     result = json.loads(response.content.decode('utf-8'))
#
#     flog.info(f'Update Script by Uid Result: {result}')
#
#     return (response)



# def get_all_scripts():
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/scripts")
#     print("Get all scripts response: ", response)
#     return(response)



# def get_script_by_uid(script_uid):
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/script/"+str(script_uid))
#     return(response)
#
#
# def delete_script_by_uid(script_uid):
#     response=http_client.delete(SERVER+":"+str(PORT)+"/api/v1/script/"+str(script_uid))
#     return(response)

def update_last_active_script(script_uid: Optional[str] = None):
    payload = {
        "project_uid": aet.project_uid,
        "uid": script_uid
    }
    flog.info(f'Last active Script payload: {payload}')

    url = f'{BASE_API}/last_active_script'
    response = http_client.put(url, json=payload)
    
    flog.info(f'Last active Script response: {response.text}')

    return response


def get_last_active_script():
    url = f'{BASE_API}/last_active_script?project_uid={aet.project_uid}'

    response = http_client.get(url)
    flog.info(f'GET Last active Script response: {response.text}')

    return response

#~#~#~#~#~##~#~#~#~# SCRIPTS END #~#~#~#~#~##~#~#~#~#

def run_pipeline_to_code_conversion():
    payload = {
        "pipeline_uid": aet.active_pipeline_uid,
        "project_uid": aet.project_uid
    }
    url = f"{BASE_API}/pipeline_to_code"
    response = http_client.post(url=url, json=payload)
    
    return response

def run_code_to_pipeline_conversion():
    payload = {
        "pipeline_uid": aet.active_pipeline_uid,
        "project_uid": aet.project_uid
    }
    url = f"{BASE_API}/code_to_pipeline"
    response = http_client.post(url=url, json=payload)
    
    return response

def run_inspect_node_code(node_uid:str):
    payload = {
        "uid": node_uid,
        "project_uid": aet.project_uid
    }
    url = f"{BASE_API}/inspect_node_code"
    response = http_client.post(url=url, json=payload)
    return response

def get_all_databases_by_project_uid():
    response = http_client.get(SERVER+":"+str(PORT)+"/api/v1/databases")
        
    if response.status_code != 200:
        raise Exception(f'Error {response.status_code}: {response.reason}.')
    
    databases = response.json()['databases']
    project_databases = [database for database in databases if database["project_uid"] == aet.project_uid]
    
    return project_databases

# def get_all_databases():
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/databases")
#     return(response)

# def delete_database_by_uid(database_uid):
#     response=http_client.delete(SERVER+":"+str(PORT)+"/api/v1/databases/"+str(database_uid))
#     flog.info(f'DELETE Database response: {response.text}')
#     return(response)

# def new_database(database_name, server, port, database, username, password, dialect, project_uid="0"):
#     payload = {
#         "database_name": database_name,
#         "server": server,
#         "port": port,
#         "database": database,
#         "username": username,
#         "password": password,
#         "dialect": dialect,
#         }
#     flog.info(f'New Dataset payload: {payload}')
#
#     url = f'{SERVER}:{PORT}/api/v1/databases'
#
#     response = http_client.post(url, json=payload)
#     flog.info(f'New Database response: {response.text}')
#
#     return response

# def update_database_by_uid(database_uid, database_name, server, port, database, username, password, dialect, project_uid="0"):
#     payload = {
#         "database_name": database_name,
#         "server": server,
#         "port": port,
#         "database": database,
#         "username": username,
#         "password": password,
#         "dialect": dialect,
#         }
#     flog.info(f'New Dataset payload: {payload}')
#
#     url = f'{SERVER}:{PORT}/api/v1/databases/{database_uid}'
#
#     response = http_client.put(url, json=payload)
#     flog.info(f'New Database response: {response.text}')
#
#     return response


# def delete_script_by_uid(script_uid):
#     response=http_client.delete(SERVER+":"+str(PORT)+"/api/v1/script/"+str(script_uid))
#     return response

# def get_all_datasets():
#     response=http_client.get(SERVER+":"+str(PORT)+"/api/v1/datasets")
#     return(response)

# def delete_dataset_by_uid(dataset_uid):
#     response=http_client.delete(SERVER+":"+str(PORT)+"/api/v1/dataset/"+str(dataset_uid))
#     return(response)

def store_df_to_google_sheet(dataset_uid:str, sheet_name:str, email:str):
    
    payload = {
        "dataset_uid": dataset_uid,
        "sheet_name": sheet_name,
        "email": email
    }

    flog.info(f'Store Df to Google Sheet payload: {payload}')

    url = f'{SERVER}:{PORT}/api/v1/store_df_to_google_sheet'

    response = http_client.post(url, json = payload)
    flog.info(f'Store Df to Google Sheet response: {response.text}')

    return response



def initialize_last_or_new_project_by_email(email):
    
    payload = {
        "email": email
    }


    url = f'{BASE_API}/initialize_last_or_new_project_by_email'

    response = http_client.post(url, json=payload)
    flog.info(f'Response: {response.text}')

    return response

def initialize_last_or_new_pipeline(project_uid):
    
    payload = {
        "project_uid": project_uid
    }


    url = f'{BASE_API}/initialize_last_or_new_pipeline?project_uid='+str(project_uid)

    response = http_client.post(url, json=payload)
    flog.info(f'Response: {response.text}')

    return response

# def new_dataset(dataset_name, data="", project_uid="0"):
#     payload = {
#         "dataset_name": dataset_name,
#         "data": data,
#         "project_uid": project_uid
#         }
#     flog.info(f'New Dataset payload: {payload}')
#
#     url = f'{SERVER}:{PORT}/api/v1/datasets'
#
#     response = http_client.post(url, json=payload)
#     flog.info(f'New Script response: {response.text}')
#
#     return response

def get_next_node_predictions(initial_node_uid: Optional[str] = None, is_used_for_autopilot: bool = False):
    payload = {
        "node_uid": initial_node_uid,
        "is_used_for_autopilot": is_used_for_autopilot
    }

    flog.info(f'GET Next Node Predictions payload: {payload}')
    
    url = f'{SERVER}:{PORT}/api/v1/next_node_predictions'

    response = http_client.get(url, json=payload)
    flog.info(f'GET Next Node Predictions response: {response.text}')

    return response

def confirm_selected_node_prediction(node_uid: str, is_used_for_autopilot: bool = False):
    payload = {
        "node_uid": node_uid,
        "is_used_for_autopilot": is_used_for_autopilot
    }

    flog.info(f'POST Next Node Predictions payload: {payload}')
    
    url = f'{SERVER}:{PORT}/api/v1/next_node_predictions'

    response = http_client.post(url, json=payload)
    flog.info(f'POST Next Node Predictions response: {response.text}')

    return response

def get_form_dict_list_templates():
    url = f'{BASE_API}/node_defs'
    response = http_client.get(url)
    flog.debug(f'GET form dict list templates: {response.text}')

    return response

def get_chatgpt_adjustment(user_input_text, openai_api_key):
    payload = {
        "user_input_text": user_input_text,
        "openai_api_key" : openai_api_key
    }

    flog.info(f'pipeline_adjustment_chatgpt_nlp payload: {payload}')

    url = f'{BASE_API}/pipeline_adjustment_chatgpt_nlp'

    response = http_client.post(url, data=json.dumps(payload))
    flog.info(f'Chatgpt adjustment: {response.text}')

    return response

def process_api_adjustments(adjustments_dict, project_uid):
    payload = {
        "adjustments_dict": adjustments_dict,
        "project_uid": project_uid
    }

    flog.info(f'api_pipeline_adjustments payload: {payload}')

    url = f'{BASE_API}/api_pipeline_adjustments'

    response = http_client.post(url, data=json.dumps(payload))
    # flog.info(f'Chatgpt adjustment: {response.text}')

    return response

def get_user_logs():
    url = f'{BASE_API}/user_logs'
    response = http_client.get(url)
    return response


def clean_data(data):
    url = f'{BASE_API}/clean_data'
    response = http_client.post(url, data=data)

    return response


def scan_website_and_take_screenshot_test(email, url):
    payload={"email":email,
             "url":url,
              "incl_tables": True,
              "incl_bullets": True,
              "incl_texts": True,
              "incl_headlines": True,
              "incl_links": True,
              "incl_images": True,
              "incl_buttons": True,
             "xpath":""}
    url = f'{BASE_API}/website_screenshot_and_scan_test'
    response=http_client.post(url=url,json=payload)
    print(json.loads(response.content))
    return response



def finalize_pipeline(project_uid):
    payload={"project_uid":project_uid,
           }
    url = f'{BASE_API}/finalize_pipeline'
    response=http_client.post(url=url,json=payload)
    print(json.loads(response.content))
    return response


def pipeline_direct_execute(pipeline_uid: str, payload: dict):
    url = f'{BASE_API}/pipelines/{pipeline_uid}/direct_execute'
    response = http_client.post(url=url, json=payload)
    response.raise_for_status()

    return response


def filter_webpage_elements_based_on_objective(elements: list[dict], objective: str):
    payload = {
        "elements": elements,
        "objective": objective
    }
    
    url = f'{BASE_API}/filter_webpage_elements_based_on_objective'
    response = http_client.post(url=url, json=payload, timeout=None)
    
    return response
