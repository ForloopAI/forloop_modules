import ast
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional, Set, Union

import numpy as np
import pandas as pd

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb
from forloop_modules.globals.active_entity_tracker import aet

#from src.df_column_category_predictor import (
#   classify_df_column_categories,
#   DataFrameColumnCategoryAnalysis
# )
from forloop_modules.redis.redis_connection import (
    get_initial_variable_redis_name,
    get_variable_redis_name,
    kv_redis,
)
from forloop_modules.utils.definitions import (
    JSON_SERIALIZABLE_TYPES_AS_STRINGS,
    REDIS_STORED_TYPES_AS_STRINGS,
)
from forloop_modules.utils.pickle_serializer import (
    save_data_dict_to_pickle_folder,
)
from forloop_modules.utils.various import (
    is_value_redis_compatible,
    is_value_serializable,
)


@dataclass
class File:
    path: str = ""
    file_name: str=""
    # suffix: str=""
    data: Any=None

    def to_dict(self):
        return {
            "path": self.path,
            "file_name": self.file_name,
            "data": self.data,
        }  # , "suffix": self.suffix


@dataclass
class DataFrameWizardScanAnalysis:
    is_analyzed: bool = False
    empty_rows: Set[int] = field(default_factory=set)
    duplicated_rows: Set[int] = field(default_factory=set)
    empty_columns: Set[str] = field(default_factory=set)
    id_columns: Set[str] = field(default_factory=set)
    result: Dict = field(default_factory=dict)


class LocalVariableHandler:
    def __init__(self):
        self.is_refresh_needed = False  # When VariableHandler gets set with a button, this is toggled to True - Frontend then can react to state changes
        self.last_dataframe = None  # stores DF, not variable name, this is temporary - should be replaced with self.last_active_df_variable but carefully - the latter one stores variable not DF
        self.last_active_df_variable = None  # is assigned when df or cleaning icon selected
        self._last_active_dataframe_node_uid = None
        self.dataframe_scan_analyses_records: Dict[str, DataFrameWizardScanAnalysis] = dict()
        self.dataframe_column_category_predictions: Dict[str, Any] = dict() #Dict[str, DataFrameColumnCategoryAnalysis]
        self.variables_to_be_created_in_subpipeline = []
        self.variables={}
        self.variable_uid_variable_dict={} #ANALOGY of dicts in GLC, new implementation - contains nodes in API -> reflecting status of server nodes via API

        self._handler_mode: Literal["initial_variable", "variable"] = "initial_variable"

    @property
    def is_empty(self):
        return len(self.variables) == 0

    @property
    def handler_mode(self):
        return self._handler_mode

    @handler_mode.setter
    def handler_mode(self, value: Any) -> None:
        raise AttributeError(
            "Use `change_variable_mode` method to change the mode of LocalVariableHandler."
        )
    
    @property
    def last_active_dataframe_node_uid(self):
        return self._last_active_dataframe_node_uid

    # temporary until cyclic import of ncrb in cleaning handlers is resolved
    @last_active_dataframe_node_uid.setter
    def last_active_dataframe_node_uid(self, node_uid:int):
        self._last_active_dataframe_node_uid = node_uid

    def get_variable_redis_name(self, name: str) -> str:
        if self.handler_mode == "initial_variable":
            return get_initial_variable_redis_name(name, aet.active_pipeline_uid)
        elif self.handler_mode == "variable":
            return get_variable_redis_name(name, aet.active_pipeline_job_uid)
        else:
            raise ValueError(f"Variable mode {self.handler_mode} is not supported.")

    def change_variable_mode(self, mode: Literal["initial_variable", "variable"]) -> None:
        """Change state specifying on which type of variable is the handler currently operating."""
        if mode not in ["variable", "initial_variable"]:
            raise ValueError(f"Variable mode `{mode}` is not supported.")
        self._handler_mode = mode
        self.variables.clear()


    def _set_up_unique_varname(self, name: str) -> str:
        i = 2

        if not name:
            name = 'untitled1'
            # If untitled_i already exists -> try untitled_i+1
            while self._is_varname_duplicated(name):
                name = f'untitled{i}'
                i += 1
        elif " " in name:
            name = "_".join(name.split())

        return name

    def _is_varname_duplicated(self, name: str) -> bool:
        names = list(self.variables.keys())
        checker = name in names
        return checker

    def get_local_variable_by_name(self, name):
        """new function because of serialization"""
        local_variable = self.variables.get(name)
        if local_variable is None:
            flog.warning(f"A variable '{name}' was not found in LocalVariableHandler.")
            return

        # serialization for objects
        if local_variable.typ in REDIS_STORED_TYPES_AS_STRINGS:
            value = kv_redis.get(self.get_variable_redis_name(name))
            value.attrs["name"] = local_variable.name

            variable = self.get_variable_by_name(name)
            local_variable = LocalVariable(
                variable["uid"], variable["name"], value, variable["is_result"]
            )
            return local_variable
        else:
            return local_variable

    def get_int_to_str_col_name_mapping(self, df: pd.DataFrame) -> dict[int, str]:
        """
        find columns whose name type is int and create mapping between their int name and its string form
        :param df:
        :type df:
        :return:
        :rtype:
        """
        int_columns = []
        for column_name in df.columns:
            if isinstance(column_name, int):
                int_columns.append(column_name)

        return {column_name: str(column_name) for column_name in int_columns}

    def new_file(self, path):
        # TODO is temporary until workflow for files is introduced
        name, *suffix = path.split(".")
        suffix = ".".join(suffix)
        name = path.split("/")[-1]
        file = File(path, name)  # suffix
        file_variable = self.create_file(file)

    def create_file(self, file: File, project_uid=None):
        # TODO is temporary until workflow for files is introduced

        # TODO: NEVER USED, NOT TESTED (NCRB + LOCAL VARIABLE CALL WITH 4 PARAMETERS INSTEAD OF 2)
        response = ncrb.get_variable_by_name(file.file_name)
        result = json.loads(response.content)
        uid = result["uid"]
        is_result = result["is_result"]

        variable = LocalVariable(uid, file.file_name, file, is_result)

        ncrb.new_file(file.file_name)
        ncrb.upload_urls_from_file(file.path)

        return variable

    def process_dataframe_variable_on_initialization(self, name, value):
        self.dataframe_scan_analyses_records[name] = DataFrameWizardScanAnalysis()
        value = value.rename(columns=self.get_int_to_str_col_name_mapping(value))
        # self.dataframe_column_category_predictions[name] = classify_df_column_categories(value)
        value.attrs["name"] = name

        return value

    def _determine_value_size(self, value: Any) -> Union[int, tuple]:
        if isinstance(value, (pd.DataFrame, np.ndarray)):
            size = value.shape
        else:
            try:
                size = len(value)
            except TypeError:
                size = 1
                
        return size

    def new_variable(
        self,
        name: str,
        value: Any,
        size: Union[int, tuple, None] = None,
        is_result: Optional[bool] = None,
        additional_params: Optional[dict] = None,
        project_uid: Optional[str] = None,
    ) -> tuple[dict, bool]:
        """
        Create a new (initial)variable (or update an existing one) on the server via API call and
        store (update, if existing) a new local (initial)variable.

        Args:
            name (str): Variable name.
            value (Any): Variable value.
            size (Union[int, tuple, None], optional): Variable value size (detected automatically,
                if not provided). Defaults to None.
            is_result (Optional[bool], optional): Marks a result variable (used to store pipeline
                jobs results). Defaults to None.
            additional_params (Optional[dict], optional): Optional parameters passed to redis.
                Defaults to None.
            project_uid (Optional[str], optional). Defaults to None.

        Returns:
            tuple[dict, bool]: variable: a variable dict from API response, is_new_variable: tells
                if the var was created or updated
        """        
        if additional_params is None:
            additional_params = {}
            
        if size is None:
            size = self._determine_value_size(value=value)

        name = self._set_up_unique_varname(name)

        if isinstance(value, pd.DataFrame):
            value = self.process_dataframe_variable_on_initialization(name, value)

        if name in self.variables.keys():
            variable = self.update_variable(name, value, size, is_result, additional_params)
            is_new_variable=False
        else:
            variable=self.create_variable(name, value, size, is_result, additional_params)
            is_new_variable=True

        return variable, is_new_variable

    def create_variable(
        self,
        name: str,
        value: Any,
        size: Union[int, tuple, None] = None,
        is_result: Optional[bool] = None,
        additional_params: Optional[dict] = None,
        project_uid: Optional[str] = None,
    ) -> dict:
        if additional_params is None:
            additional_params = {}

        if size is None:
            size = self._determine_value_size(value=value)

        if self.handler_mode == "initial_variable":
            ncrb_fn = ncrb.new_initial_variable
        elif self.handler_mode == "variable":
            ncrb_fn = ncrb.new_variable

        optional_args = {"is_result": is_result} if is_result is not None else {}
        # serialization for objects
        # TODO: FFS FIXME:
        if is_value_serializable(value):
            response = ncrb_fn(name=name, value=value, size=size, **optional_args)
        else:
            if is_value_redis_compatible(value):
                kv_redis.set(
                    self.get_variable_redis_name(name), value, additional_params
                )
            else:
                data_dict = {}
                data_dict[name] = value
                folder = ".//file_transfer"
                save_data_dict_to_pickle_folder(
                    data_dict, folder, clean_existing_folder=False
                )
            # TODO: FILE TRANSFER MISSING

            response = ncrb_fn(
                name=name,
                value="",
                type=type(value).__name__,
                size=size,
                **optional_args,
            )

        result = response.json()
        self.create_local_variable(
            result["uid"],
            result["name"],
            result["value"],
            result["is_result"],
            result["type"],
        )
        return result

    def update_variable(
        self,
        name: str,
        value: Any,
        size: Union[int, tuple, None] = None,
        is_result: Optional[bool] = None,
        additional_params: Optional[dict] = None,
        project_uid: Optional[str] = None,
    ) -> dict:
        if additional_params is None:
            additional_params = {}

        if size is None:
            size = self._determine_value_size(value=value)

        if self.handler_mode == "initial_variable":
            ncrb_update_by_uid_fn = ncrb.update_initial_variable_by_uid
        elif self.handler_mode == "variable":
            ncrb_update_by_uid_fn = ncrb.update_variable_by_uid

        variable = None
        try:  # Function to run even if no variable is found
            variable = self.get_variable_by_name(name)
        except Exception:
            flog.warning(f"Variable '{name}' was not found in LocalVariableHandler.")

        if variable is not None:
            size = size if size is not None else variable["size"]
            is_result = is_result if is_result is not None else variable["is_result"]
            ncrb_fn_kwargs = {
                "variable_uid": variable["uid"],
                "name": name,
                "value": value,
                "size": size,
                "is_result": is_result,
            }

            if is_value_serializable(value):
                response = ncrb_update_by_uid_fn(**ncrb_fn_kwargs)
            else:
                ncrb_fn_kwargs.update(value="")

                if is_value_redis_compatible(value):
                    kv_redis.set(
                        self.get_variable_redis_name(name), value, additional_params
                    )
                else:
                    data_dict = {}
                    data_dict[name] = value
                    folder = ".//file_transfer"
                    save_data_dict_to_pickle_folder(
                        data_dict, folder, clean_existing_folder=False
                    )
                response = ncrb_update_by_uid_fn(
                    type=type(value).__name__, **ncrb_fn_kwargs
                )

            result = response.json()
            self.update_local_variable(
                result["name"], result["value"], result["is_result"], result["type"]
            )
            return result
        
    def delete_variable(self, var_name: str):
        if self.last_active_df_variable is not None and var_name == self.last_active_df_variable.name:
            self.last_active_df_variable = None

        if self.handler_mode == "initial_variable":
            ncrb_delete_by_uid = ncrb.delete_initial_variable_by_uid
        elif self.handler_mode == "variable":
            ncrb_delete_by_uid = ncrb.delete_variable_by_uid

        variable = self.get_variable_by_name(var_name)
        ncrb_delete_by_uid(variable["uid"])
        self.delete_local_variable(var_name)

    def create_local_variable(
        self,
        uid: str,
        name: str,
        value: Any,
        is_result: bool,
        type: Optional[str] = None,
    ):
        if type in JSON_SERIALIZABLE_TYPES_AS_STRINGS and type != "str":
            value = ast.literal_eval(str(value))
        elif type in REDIS_STORED_TYPES_AS_STRINGS:
            value = kv_redis.get(self.get_variable_redis_name(name))

            if isinstance(value, pd.DataFrame):
                value = self.process_dataframe_variable_on_initialization(name, value)

        variable = LocalVariable(uid, name, value, is_result)
        self.variables[name] = variable
        return variable

    def update_local_variable(self, name, value, is_result: bool, type=None):
        if type in JSON_SERIALIZABLE_TYPES_AS_STRINGS and type != "str":
            value = ast.literal_eval(str(value))
        elif type in REDIS_STORED_TYPES_AS_STRINGS:
            value = kv_redis.get(self.get_variable_redis_name(name))

        variable = self.variables[name]
        self.variables[name].value = value  # Update
        self.variables[name].is_result = is_result

        return variable

    def save_variables_as_initial_variables(self):
        """Save all currently loaded Variables as InitialVariables."""
        if not self._handler_mode == "variable":
            raise AttributeError(
                "LocalVariableHandler must be in 'variable' mode to resave results."
            )

        for variable in self.variables.values():
            if is_value_serializable(variable.value):
                ncrb.new_initial_variable(name=variable.name, value=variable.value)
            else:
                value = kv_redis.get(self.get_variable_redis_name(variable.name))
                if is_value_redis_compatible(variable.value):
                    kv_redis.set(f'stored_initial_variable_{variable.name}', value)
                else:
                    data_dict = {}
                    data_dict[variable.name] = variable.value
                    folder = ".//file_transfer"
                    save_data_dict_to_pickle_folder(data_dict, folder, clean_existing_folder=False)
                #TODO: FILE TRANSFER MISSING

                optional_args = (
                    {"is_result": variable.is_result} if variable.is_result is not None else {}
                )
                ncrb.new_initial_variable(
                    name=variable.name, value="", type=type(variable.value).__name__,
                    **optional_args
                )

    def delete_local_variable(self, var_name:str):
        self.variables.pop(var_name)

    def get_variable_by_name(self, name: str) -> dict:
        """Get Variable/InitialVariable uid based on its name and the pipeline it's assigned to."""
        if self.handler_mode == "initial_variable":
            ncrb_get_by_name_fn = ncrb.get_initial_variable_by_name
        elif self.handler_mode == "variable":
            ncrb_get_by_name_fn = ncrb.get_variable_by_name

        response = ncrb_get_by_name_fn(name)
        response.raise_for_status()
        return response.json()

    #   TODO: fix dependencies
    # def update_data_in_variable_explorer(self, glc): #TODO Dominik: Refactor out, shouldnt be here
    #    self.is_refresh_needed = False
    #    if hasattr(glc, "variable_explorer"):

    #        glc.variable_explorer.update_data(self.stored_variable_df)

    # stored_variables_list = [[x.name, x.typ, x.size, x.value] for x in self.values()]
    # stored_variable_df = pd.DataFrame(stored_variables_list, columns=["Name", "Type", "Size", "Value"])
    # glc.variable_explorer.update_data(stored_variable_df)

    def populate_df_analysis_record(
        self, name, empty_rows, duplicated_rows, empty_columns, id_columns, result
    ):
        df_analysis = DataFrameWizardScanAnalysis()
        df_analysis.is_analyzed = True
        df_analysis.empty_rows = empty_rows
        df_analysis.duplicated_rows = duplicated_rows
        df_analysis.empty_columns = empty_columns
        df_analysis.id_columns = id_columns
        df_analysis.result = result

        self.dataframe_scan_analyses_records[name] = df_analysis

    def empty_df_analysis_record(self, name):
        self.dataframe_scan_analyses_records[name] = DataFrameWizardScanAnalysis()

    def df_already_scanned(self, name):
        if name in self.dataframe_scan_analyses_records:
            return self.dataframe_scan_analyses_records[name].is_analyzed
        return False

    # DB Connections
    # TODO: IT IS POSSIBLE TO HAVE DUPLICATE DB NAMES
    def new_database_connection(self, db_connection):
        # TODO: should check whether db connection is valid and raise exception if not -> Dominik: I dont agree
        #assert db_connection.is_valid_db_connection
        self.db_connections.append(db_connection)


    def get_selected_db_connection(self, selected_db_name):
        valid_dbs = {x.database + " (" + x.server + ")": x for x in self.db_connections if hasattr(x, "database")}
        db_connection = valid_dbs.get(selected_db_name, None)

        return db_connection

    def get_db_connection(self, db_name):
        for connection in self.db_connections:
            if connection.database == db_name:
                return connection


class LocalVariable:
    """
    Formerly ForloopStoredVariable - renamed to LocalVariable
    Dfs, Lists, Dicts (JSON) - objects visible and possible to manipulate
    """
    instance_counter = 0

    def __init__(self, uid: str, name: str, value: Any, is_result: bool):
        self.name = name
        self.value = value
        self.is_result = is_result
        self.uid = uid

    def __str__(self):
        return f'{self.value}'

    def __repr__(self):
        return f'{self.value}'

    @property
    def typ(self):
        return type(self.value).__name__

    @property
    def size(self) -> int:
        if self.typ in ["DataFrame", "ndarray"]:
            return self.value.shape
        
        try:
            return len(self.value)
        except TypeError:
            return 1

    def __len__(self):
        return self.size
