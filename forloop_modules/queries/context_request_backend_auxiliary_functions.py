import json
from typing import Optional, Union

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

def get_and_subset_requested_objects(ncrb_function, func_args:Optional[list]=None, func_kwargs:Optional[dict]=None, 
                                     applied_key_sequence:list=[], filter_by_project_uid:bool=False) -> Union[list,dict,None]:
    """
    Calls an ncrb function, transforms it's response into a result dictionary and returns it.

    Parameters:
        ncrb_function: API call function from node_context_requests_backend 
        
        func_args (type: list | None): arguments passed into the ncrb function (positional or keyword), e.g. node_uid, edge_uid etc. 
        
        func_kwargs (type: dict | None): keyword arguments passed into the ncrb function, e.g. "visible"
        
        applied_key_sequence (type: list): a sequence of keys that get a certain subset from the original result dict
        
        filter_by_project_uid (type: bool): decides wether to take only the result with the current project_uid (True) or all results (False)
        
            Example: When we call get_all_nodes, the result has a structure
                {
                    'nodes': [
                        ...
                    ]
                }
                In this case we usually want only the contents under 'nodes' key. To get that we call
                `get_and_subset_requested_objects(ncrb.get_all_nodes, applied_key_sequence=['nodes'])`.
                This will return `result['nodes']`. 

                For more keys it works similarly: `result[key1][key2]...[keyn]`

    Returns:
        result (type: list |dict | None): if response.status_code == 200 ==> (list | dict) with a result else None 
    """
    
    if func_args is None:
        func_args = []
        
    if func_kwargs is None:
        func_kwargs = {}

    response = ncrb_function(*func_args, **func_kwargs)

    if response.status_code == 200:
        result = json.loads(response.content.decode('utf-8'))

        if applied_key_sequence:
            for key in applied_key_sequence:
                result = result[key]
                
        if filter_by_project_uid:
        
            result = filter_items_by_project_uid(result)
        return result
    else:
        flog.warning(f'Status code {response.status_code}: {response.reason_phrase}')
    
def filter_items_by_project_uid(items:list):
    user_items = items
    is_items_argument_a_list_of_dicts = all([type(item) == dict for item in items]) if type(items) == list else False
    
    if is_items_argument_a_list_of_dicts:
        project_uid = ncrb.get_project_uid()
        
        user_items = [x for x in items if x.get("project_uid") == project_uid] 

    return user_items