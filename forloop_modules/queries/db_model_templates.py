import datetime
from enum import Enum
from typing import Annotated, Any, Dict, Generic, List, Literal, Optional, TypeVar, Union

from pydantic import AfterValidator, BaseModel, Field, PlainSerializer, model_validator
from pydantic.functional_validators import field_validator


def is_date_utc(date: datetime.datetime) -> datetime.datetime:
    # NOTE: For naive datetime objects, date.utcoffset will return None
    # NOTE: For TZ-aware datetime objects, date.utcoffset will return timedelta with offset to UTC TZ
    # NOTE: Using DataFrame for storing resources casts datetimes into pandas custom Timestamp
    # objects, which might unexpectedly cast TZ info to/from naive/aware datetime
    if date.utcoffset() is not None and date.utcoffset() != datetime.timedelta(0):
        raise ValueError('Only datetimes in a UTC zone are allowed')
    date = date.replace(tzinfo=None) # Cast to naive datetime after validation, otherwise XlsxDB persistence will fail
    return date

# NOTE: Only UTC timestamps should be accepted by API server.
# NOTE: Currently used only for APITriggers, as zulu datetime formating can potentially break Desktop
UTCDatetime = Annotated[
    datetime.datetime,
    AfterValidator(is_date_utc),  # Validate if the date is in UTC zone (zulu format)
    PlainSerializer(lambda date: f"{date.isoformat()}Z", when_used='json') # Serialize Timestamps to zulu format
]


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

class JobSortableColumnsEnum(str, Enum):
    STATUS = "status"
    CREATED_AT = "created_at"
    STARTED_AT = "started_at"
    COMPLETED_AT = "completed_at"
    PIPELINE_UID = "pipeline_uid"


class JobStatusEnum(str, Enum):
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"  # Job in the process of being canceled, but not yet canceled
    CANCELED = "CANCELED"

    @classmethod
    def is_finished(cls, status: 'JobStatusEnum'):
        """Check if the job status is in a 'finished' subset."""
        return status in [cls.COMPLETED, cls.FAILED, cls.CANCELED]

class PipelineJobStats(BaseModel):
    webpage_count: int
    webpage_avg_cycle_time: float
    node_count: int
    node_avg_cycle_time: float


class APINodeJob(BaseModel):
    uid: str
    status: JobStatusEnum
    created_at: UTCDatetime
    completed_at: Optional[UTCDatetime] = None
    message: Optional[str] = None
    pipeline_uid: str  # TODO: Remove when PrototypeJobs are implemented
    pipeline_job_uid: Optional[str] = None # TODO: Change to required when PrototypeJobs are implemented


class APIOperationJob(BaseModel):
    uid: str
    status: JobStatusEnum
    created_at: UTCDatetime
    completed_at: Optional[UTCDatetime] = None
    message: Optional[str] = None
    prototype_job_uid: str


class APIPipelineJob(BaseModel):
    uid: str
    machine_uid: Optional[str] = None
    status: JobStatusEnum
    created_at: UTCDatetime
    started_at: Optional[UTCDatetime] = None
    completed_at: Optional[UTCDatetime] = None
    message: Optional[str] = None
    pipeline_uid: str
    jobs: list[APINodeJob] = Field(default_factory=list)
    stats: Optional[PipelineJobStats] = None


class TriggerFrequencyEnum(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class APITrigger(BaseModel):
    name: Optional[str] = None
    first_run_date: UTCDatetime
    frequency: TriggerFrequencyEnum
    pipeline_uid: str
    project_uid: str


class APIDatabase(BaseModel):
    database_name: str = ""
    server: str = ""
    port: int = ""
    database: str = ""
    username: str = ""
    password: str = ""
    dialect: str = ""
    new: bool = False
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


class APIVariableToBeAssignedToFile(BaseModel):
    variable_name: str = ""
    node_uid: str = ""
    project_uid: str = "0"
    pipeline_uid: str = "0"
    

class APILoadScriptViaNode(BaseModel):
    node_uid: str = "0"
    project_uid: str = "0"

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
    
class APIFilterWebpageElements(BaseModel):
    elements: list[dict]
    objective: str


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


class APILastActiveDFNodeUid(BaseModel):
    project_uid: str = "0"
    last_active_dataframe_node_uid: Optional[str]
    
    
    
    
    
    
    
    
    

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


class APINodeExecute(BaseModel):
    typ: str
    params: dict
    fields: Optional[list] = None


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
        """
        HACK: Current DBHydra's implementation of INSERT method will remove any ' mark from the
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


class InitialVariableModel(BaseModel):
    uid: str = "0"
    name: str = ""
    value: Any = "" # strings, bytes, numbers, tuples, lists, dicts, sets, booleans, and None (anything evaluatable by ast.literal_eval)
    is_result: bool = False  # TODO: Remove when PrototypeJobs are implemented
    type: Optional[str] = None # Type can be enforced or autoinferred
    size: Optional[int] = None
    pipeline_uid: str = "0"
    project_uid: str = "0" # TODO: Is this necessary? Node is indirectly linked to a project via pipeline

    @field_validator("name", "value")
    @classmethod
    def check_for_single_quote_mark(cls, value: str) -> str:
        """
        HACK: Current DBHydra's implementation of INSERT method will remove any ' mark from the
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


class APIInitialVariable(InitialVariableModel):
    uid : Any = Field(None, exclude=True, alias="_do_not_send_in_request")

#############################################################
# DOMINIK: I prefer #2 but there might be collision with dbhydra models

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

# DOMINIK: I prefer this + I would explicitly add in variable input comment "uid and XYZ are inherited"

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

class APIExecutePipelineSchema(BaseModel):
    name: str
    user_email: str

class APIPopup(BaseModel):
    pos: list[int] = [0, 0]
    typ: Union[None, str] = "Custom"
    params: Union[None, dict[str, Any]]
    project_uid: str = ""


class APIProject(BaseModel):
    project_name: str
    project_key: str
    last_active_pipeline_uid: Optional[str] = None

class APIUserFlowStep(BaseModel):
    user_uid: str
    step_identifier: str
    step_data: str
    timestamp_utc: datetime.datetime

    



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


# Commented out fields are internal REST API fields, not used on frontend
class APISession(BaseModel):
    # user_uid: Optional[str] = None
    auth0_session_id: str  # auth0 response - session_id
    version: Optional[str] = None  # forloop platform version
    platform_type: Literal["cloud", "desktop"]  # cloud or desktop
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
    xpaths: List[Union[str, List[str]]]
    
    
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

class APINewDbTable(BaseModel):
    server: str
    database_name: str
    port: int
    username: str
    password: str
    dialect: Literal["MySQL"] # TODO: Enable other dialects like PostgreSQL
    columns: list[str]
    elements: list[dict]

class APIUrl(BaseModel):
    url: Optional[str]=""


class APIToggleStatus(BaseModel):
    status: bool = False


class APIButtonName(BaseModel):
    button_name:str=""
    

class LastActiveScriptUid(BaseModel):
    project_uid: str
    uid: Union[str, None] = None
    
class APIInspectNodeCode(BaseModel):
    uid: str
    project_uid: str
    
class APIPipelineToCode(BaseModel):
    pipeline_uid: str
    project_uid: str
    
class APICodeToPipeline(BaseModel):
    pipeline_uid: str
    project_uid: str
    


class PipelineAdjustmentUsingChatGPT(BaseModel):
    user_input_text: str
    openai_api_key: str

class PipelineAdjustmentDict(BaseModel):
    adjustments_dict: list
    project_uid: str


class APIFindSimilarItemsBody(BaseModel):
    selected_elements_xpaths:list

class APIConvertToScrapingNode(BaseModel):
    selected_elements_xpaths: list
    pipeline_uid: str
    project_uid: str

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
        
    
