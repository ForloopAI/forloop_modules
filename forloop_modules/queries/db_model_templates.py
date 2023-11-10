import datetime
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

from pydantic import BaseModel, Field
from pydantic.functional_validators import field_validator


##### DO NOT DELETE THIS SECTION -> Dominik will do it later

#class APIEmail(BaseModel):
#    email: Optional[str] = ""


#class APIUrl(BaseModel):
#    url: Optional[str] = ""


#class APIToggleStatus(BaseModel):
#    status: bool = False


#class APIFindSimilarItemsBody(BaseModel):
#    selected_elements_xpaths:List

# class APIFinalizePipeline(BaseModel):
#     project_uid: str=""
#     url: str=""


# class APIFormDictList(BaseModel):
#     typ:str="Custom"
#     form_dict_list:Dict[str, Any]
#     stage:str="default"


# class APINode(BaseModel):
#     pos:Union[None,List[int]]=[0,0]
#     typ:Union[None,str]="Custom"
#     params:Union[None,Dict[str, Any]]
#     fields:Optional[List[Any]]=[]
#     is_active:bool=False
#     pipeline_uid: str="0"
#     project_uid:str="0"
#     visible: bool = True
#     is_breakpoint_enabled : bool = False
#     is_disabled: bool = False


# class APIEdge(BaseModel):
#     from_node_uid:str = "0"
#     to_node_uid:str = "0"
#     channel:Any=None
#     pipeline_uid:str="0"
#     project_uid:str="0"
#     visible:bool=True



# class VariableModel(BaseModel):
#     uid: int = ""
#     name: str = ""
#     value: Any = ""
#     type: Union[str, None] = None
#     size: Union[int, None] = None
#     pipeline_uid: Optional[str] = "0"
#     project_uid: Optional[str] = ""


# class APIVariable(VariableModel):
#     uid: Any = Field(None, exclude=True, alias="_do_not_send_in_request")



class DeleteUidObject(BaseModel):
    project_uid: str = "0"


# class APIPipeline(BaseModel):
#     name: str = "untitled"
#     start_node_uid: str = "0"
#     is_active: bool = False
#     # nodes_uids:List[int]
#     # edges_uids:List[int]
#     # variables_uids:List[int]
#     active_nodes_uids: List[int] = []
#     remaining_nodes_uids: List[int] = []
#     project_uid: str = "0"
#     # project_uid:str=""


# class APIPopup(BaseModel):
#     pos: List[int] = [0, 0]
#     typ: Union[None, str] = "Custom"
#     params: Union[None, Dict[str, Any]]
#     project_uid: str = ""


# class APIProject(BaseModel):
#     project_name: str = ""
#     project_key: str = ""
#     project_uid: str = ""
#     last_active_pipeline_uid: Optional[str] = None


class TriggerFrequencyEnum(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class APITrigger(BaseModel):
    name: Optional[str] = None
    first_run_utc: datetime.datetime
    frequency: TriggerFrequencyEnum
    pipeline_uid: str
    project_uid: str

    @field_validator("first_run", mode="after")
    @classmethod
    def check_round_minutes(cls, value: datetime.datetime) -> datetime.datetime:
        if not value.second:
            raise ValueError("The date must have seconds set to zero.")
        return value


class DbDialectEnum(str, Enum):
    MYSQL = "mysql"
    POSTGRES = "postgres"
    MONGO = "mongo"


class APIDatabase2(BaseModel):
    """
    Cleaned up APIDatabase schema used only in ScrapingPipelineBuilders, copied to retain
    backwards-compatibility of the old version with Desktop.
    """

    name: str
    server: str
    port: int
    database: str
    username: str
    password: str
    dialect: DbDialectEnum
    project_uid: str


class APIDatabase(BaseModel):
    database_name: str = ""
    server: str = ""
    port: int = ""
    database: str = ""
    username: str = ""
    password: str = ""
    dialect: str = ""
    project_uid: str = "0"


class APIDbTable(BaseModel):
    name: str = ""
    pos: Union[None, List[int]] = [0, 0]
    columns: List[Dict[str, str]] = [{"name": "", "type": "", "db_key": ""}]
    is_rolled: bool = False
    database_uid: str = "0"
    project_uid: str = "0"


# TODO:
# from src import database_model_templates
# class APIDbTable(dh.DbTableModel):
#     uid: Optional[str] = None


class APIDataset(BaseModel):
    dataset_name: str = ""
    data: Any = ""
    project_uid: str = "0"


class APIFile(BaseModel):
    file_name: str = ""
    data: Any = ""
    project_uid: str = "0"
    upload_status: str = "Not started"


class APIScript(BaseModel):
    script_name: str = ""
    text: str = ""
    project_uid: str = "0"


# class APIUser(BaseModel):
#     email: str = ""  # auth0 response - email
#     auth0_subject_id: str = ""  # auth0 response - auth_method_id
#     given_name: str = ""  # auth0 response - given_name
#     family_name: str = ""  # auth0 response - family_name
#     picture_url: str = ""  # auth0 response - picture


# class APISession(BaseModel):
#     user_uid: str = ""
#     auth0_session_id: str = ""  # auth0 response - session_id
#     version: str = ""  # forloop platform version
#     platform_type: str = ""  # cloud or desktop
#     ip: str = ""  # only in desktop/execution core version
#     mac_address: str = ""  # only in desktop/execution core version
#     hostname: str = ""  # only in desktop/execution core version
#     start_datetime_utc: str = ""  # this needs to be str
#     last_datetime_utc: str = ""  # this needs to be str
#     total_time: int = 0


class APIScanWebpage(BaseModel):
    email: str
    url: str
    incl_tables: Optional[bool]
    incl_bullets: Optional[bool]
    incl_texts: Optional[bool]
    incl_headlines: Optional[bool]
    incl_links: Optional[bool]
    incl_images: Optional[bool]
    incl_buttons: Optional[bool]
    xpath: Optional[str]


# class APIPaginationMode(BaseModel):
#     urls: List[str]
#     dataset_file_path: Optional[str]
#     dataset_url_column: Optional[str]
#     next_page_button_xpath: Optional[str]
#     number_of_pages: Optional[int]


# class APIMultipleXPaths(BaseModel):
#     url: str
#     xpaths: List[str]


# class APIMultipleXPathsMultipleURLs(BaseModel):
#     urls: str
#     xpaths: List[str]


# class StoreDfToGoogleSheet(BaseModel):
#     dataset_uid: str
#     sheet_name: str
#     email: str
    
    
    
    
    
    
    
    
    
    
    

#<<<<<<< HEAD
#=======
"""############ DISABLED IN DEV ####################"""


class APIFinalizePipeline(BaseModel):
    project_uid: str=""
    

class APIFormDictList(BaseModel):
    typ:str="Custom"
    form_dict_list:dict[str, Any]
    stage:str="default"


class APINode(BaseModel):
    pos: Optional[list[int]] = [0, 0]  # Should be a tuple[int, int] but dbhydra doesn't support tuples
    typ: Optional[str] = "Custom"
    params: Optional[dict] = {}
    fields: Optional[list] = []
    is_active: bool = False
    pipeline_uid: str = "0"
    project_uid: str = "0" # TODO: Is this necessary? Node is indirectly linked to a project via pipeline
    visible: bool = True
    is_breakpoint_enabled: bool = False
    is_disabled: bool = False

class APIEdge(BaseModel): #Added default values
    from_node_uid: str = "0"
    to_node_uid: str = "0"
    channel: Any = None
    pipeline_uid: str = "0"
    project_uid: str = "0" # TODO: Is this necessary? Node is indirectly linked to a project via pipeline
    visible: bool = True

    # FIXME: Edge validation should be performed here (not in the endpoints)
    # TODO: Platform instantiates Edges with this validator violated - hence it's commented out for
    # compatibility's sake
    # @root_validator
    # @classmethod
    # def check_nodes_different(cls, values: dict) -> None:
    #     if values.get("to_node_uid") == values.get("from_node_uid"):
    #         raise ValueError("from_node_uid and to_node_uid must be different")
    #     return values


class VariableModel(BaseModel):
    uid: str = "0"
    name: str = ""
    value: Any = "" # strings, bytes, numbers, tuples, lists, dicts, sets, booleans, and None (anything evaluatable by ast.literal_eval)
    type: Optional[str] = None # Type can be enforced or autoinferred
    size: Optional[int] = None
    is_result: bool = False
    pipeline_uid: str = "0"
    project_uid: str = "0" # TODO: Is this necessary? Node is indirectly linked to a project via pipeline

    @field_validator("name", "value")
    @classmethod
    def check_for_single_quote_mark(cls, value: str) -> str:
        """HACK: Current DBHydra's implementation of INSERT method will remove any ' mark from the
        variable while inserting a record - even if this ' mark is a part of the inserted string.
        """
        if isinstance(value, str) and "'" in value:
            raise ValueError("Single quotation marks are not permitted")
        return value

    # NOTE: Should we enforce variable name to be a valid Python variable name?
    # What are possible repercussions of having Unicode characters in names?
    # @field_validator("name", mode="before")
    # @classmethod
    # def check_name_chars(cls, value: str) -> str:
    #     regex = r"^[a-zA-Z0-9_]*$"
    #     if not re.match(regex, value):
    #         raise ValueError("Variable name must only be composed of a-z, A-Z, 0-9, _ characters")
    #     return value


class APIVariable(VariableModel):
    uid : Any = Field(None, exclude=True, alias="_do_not_send_in_request")

#############################################################
# BELOW ARE 2 PROPOSALS FOR DESIGN OF API VALIDATION SCHEMAS
# IN COMPARISON TO SOLUTION ABOVE, THEY ALLOW:
# - COMPLETE AND CORRECT VALIDATION OF INPUT AND OUTPUT
# - DOCUMENTATION GENERATION OF RESPONSE SCHEMAS
# - CASTING DATA TO CUSTOM-NAMED JSON FIELDS (E.G. FOR OUTPUT)

# 1. TWO SEPARATE SCHEMAS FOR INPUT AND OUTPUT 
# (WHERE OUTPUT ALWAYS MAPS TO INTERNAL FORM OF DATA...
# e.g. APIVariable <-> Variable (dataclass)
# AND INPUT SCHEMA ONLY HOLD PARAMETERS PROVIDED BY THE CLIENT
# - MORE EXTENSIVE
# - MORE CODE REPETITION
# - MORE READABLE

# class APIVariable(BaseModel):
#     uid: str
#     name: str
#     value: str
#     type: str
#     size: int
#     pipeline_uid: str
#     project_uid: str

# class APIVariableInput(BaseModel):
#     name: str
#     value: str
#     pipeline_uid: str
#     project_uid: str

# 2. TWO SEPARATE SCHEMAS, WHERE INPUT SCHEMA IS A BASE AND OUTPUT SCHEMA IS A SUBCLASS
# - MORE SUCCINCT
# - LESS CODE REPETITION
# - LESS READABLE

# class APIVariable(APIVariableInput):
#     uid: str
#     size: int

# class APIVariableInput(BaseModel):
#     name: str
#     value: str
#     type: str
#     pipeline_uid: str
#     project_uid: str
#############################################################

# class DeleteUidObject(BaseModel):
#     project_uid:str="0"


class APIPipeline(BaseModel):
    name: Optional[str] = None
    start_node_uid: str = "0" # TODO: to be deprecated after introducing ExecCore for local execution
    is_active: bool = False # TODO: to be deprecated after introducing ExecCore for local execution
    #nodes_uids:list[int]
    #edges_uids:list[int]
    #variables_uids:list[int]
    active_nodes_uids: list[int] = [] # TODO: to be deprecated after introducing ExecCore for local execution
    remaining_nodes_uids: list[int] = [] # TODO: to be deprecated after introducing ExecCore for local execution
    project_uid: str = "0"


class APIPopup(BaseModel):
    pos: list[int] = [0, 0]
    typ: Union[None, str] = "Custom"
    params: Union[None, dict[str, Any]]
    project_uid: str = ""


class APIProject(BaseModel):
    project_name: str
    project_key: str
    last_active_pipeline_uid: Optional[str] = None

    
# class APITrigger(BaseModel):
#     name: str=""
#     machine_uid:str=""
#     pipeline_uid:str=""
#     first_run: datetime.datetime
#     frequency: int=""
#     project_uid:str="0"
    
# class APIDatabase(BaseModel):
#     database_name: str=""
#     server: str
#     port: int
#     database: str
#     username: str
#     password: str    
#     dialect: str
#     project_uid:str="0"
    
# class APIDbTable(BaseModel):
#     name: str=""
#     pos: Union[None, list[int]] = [0, 0]
#     columns: list[dict[str,str]]=[{"name":"","type":"","db_key":""}]
#     is_rolled: bool = False
#     database_uid: str="0"
#     project_uid:str="0"

#TODO:
# from src import database_model_templates
# class APIDbTable(dh.DbTableModel):
#     uid: Optional[str] = None
    
    
# class APIDataset(BaseModel):
#     dataset_name: str=""
#     data: Any
#     project_uid:str="0"
    
    
# class APIFile(BaseModel):
#     file_name: str=""
#     data: Any
#     project_uid:str="0"
#     upload_status:str="Not started"
    
    
# class APIScript(BaseModel):
#     script_name: str=""
#     text: str=""
#     project_uid:str="0"
    
    

class APIUser(BaseModel):
    email: str #auth0 response - email
    auth0_subject_id: str #auth0 response - auth_method_id
    given_name: str #auth0 response - given_name
    family_name: str #auth0 response - family_name
    picture_url: str #auth0 response - picture


class PlatformEnum(str, Enum):
    """Simple enum listing options for platform_type attribute in Session dataclass/validation schema."""

    CLOUD = "cloud"
    DESKTOP = "desktop"


# Commented out fields are internal REST API fields, not used on frontend
class APISession(BaseModel):
    # user_uid: Optional[str] = None
    auth0_session_id: str  # auth0 response - session_id
    version: Optional[str] = None  # forloop platform version
    platform_type: PlatformEnum  # cloud or desktop
    ip: Optional[str] = None  # only in desktop/execution core version
    mac_address: Optional[str] = None  # only in desktop/execution core version
    hostname: Optional[str] = None  # only in desktop/execution core version
    # start_datetime_utc: datetime.datetime# = datetime.datetime.utcnow()
    # last_datetime_utc: Optional[datetime.datetime]# = datetime.datetime.utcnow()
    # total_time: int = 0


# class APIScanWebpage(BaseModel):
#     url: str
#     incl_tables: Optional[bool]
#     incl_bullets: Optional[bool]
#     incl_texts: Optional[bool]
#     incl_headlines: Optional[bool]
#     incl_links: Optional[bool]
#     incl_images: Optional[bool]
#     incl_buttons: Optional[bool]
#     xpath: Optional[str]
    
    
    
class APIPaginationMode(BaseModel):
    urls: list[str]
    dataset_file_path: Optional[str]
    dataset_url_column: Optional[str]
    next_page_button_xpath: Optional[str]
    number_of_pages: Optional[int]
    
    
class APIMultipleXPaths(BaseModel):
    url: str
    xpaths: list[str]
    
    
class APIMultipleXPathsMultipleURLs(BaseModel):
    urls: str
    xpaths: list[str]


class StoreDfToGoogleSheet(BaseModel):
    dataset_uid: str
    sheet_name: str
    email: str

class APIEmail(BaseModel):
    email: Optional[str] = ""
    #email: str #Jakub branch


class APIUrl(BaseModel):
    url: Optional[str]=""


class APIToggleStatus(BaseModel):
    status: bool = False


class APIButtonName(BaseModel):
    button_name:str=""


class APIFindSimilarItemsBody(BaseModel):
    selected_elements_xpaths:list


ItemType = TypeVar("ItemType")
class Paged(BaseModel, Generic[ItemType]):
    items: list[ItemType]
    page: int
    size: int
    pages: int


"""############ END OF DISABLED IN DEV ####################"""
#>>>>>>> origin/jakub

# @app.get("/api/v1/project/{uid}")
# def get_project(uid: Optional[str]=None):
#     """
#     uid: unique id of a project is required
#     """
#     matching_items=[x for x in node_context_manager.projects if x.uid==uid]
#     if len(matching_items)==1:
#         item=matching_items[0]
#         return {
#             "uid":item.uid,
#             "project_name":item.project_name,
#             "project_uid":item.project_uid
#             }
#     else:
#         return {"ok":False}
        
    
