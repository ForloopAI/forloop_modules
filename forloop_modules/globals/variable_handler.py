# TODO: solve circle imports
# from gui_layout_context import GuiLayoutContext

import pandas as pd

from typing import Dict, Set
from dataclasses import dataclass, field
#from src.df_column_category_predictor import classify_df_column_categories

from forloop_modules.globals.local_variable_handler import LocalVariableHandler, File, LocalVariable  # dont remove file and local variable


@dataclass
class DataFrameWizardScanAnalysis:
    is_analyzed: bool = False
    empty_rows: Set[int] = field(default_factory=set)
    duplicated_rows: Set[int] = field(default_factory=set)
    empty_columns: Set[str] = field(default_factory=set)
    id_columns: Set[str] = field(default_factory=set)
    result: Dict = field(default_factory=dict)



# class VariableHandler:#(dict):
#     """Handles functions and information related to the stored variables in TempVariableRect objects
#     ... stores variables - Keys: variable_names, Values: objects of type Variable
#     should be independent on Frontend
#     """
#     _instance = None

#     def __init__(self):
#         # TODO: init method should be removed, because if there is a variable with same name it will be overriden if initted again
#         #super().__init__()
#         """
#         self.variables["cislo1"] = 123
#         self.variables["test"] = 5
#         self.variables["counter"] = 2
#         self.variables["cislo2"] = 157
#         self.variables["slovo"] = "ahoj"
#         self.variables["list1"] = [7, 8, 5, 4, 9, 7, 1]
#         self.variables["dict1"] = {'id': 1, 'order': 10}
#         """
#         self.is_refresh_needed = False  # When VariableHandler gets set with a button, this is toggled to True - Frontend then can react to state changes
#         self.last_dataframe = None  # stores DF, not variable name, this is temporary - should be replaced with self.last_active_df_variable but carefully - the latter one stores variable not DF
#         self.last_active_df_variable = None  # is assigned when df or cleaning icon selected
#         self.dataframe_scan_analyses_records: Dict[str, DataFrameWizardScanAnalysis] = dict()
#         self.dataframe_column_category_predictions: Dict[str, DataFrameColumnCategoryAnalysis] = dict()
#         self.db_connections = []
#         self.variables_to_be_created_in_subpipeline = []
#         self.variables={}

        

#     # def __new__(cls, *args, **kwargs):
#     #     if not isinstance(cls._instance, cls):
#     #         cls._instance = dict.__new__(cls)
#     #     return cls._instance

#     def get_int_to_str_col_name_mapping(self, df: pd.DataFrame) -> Dict[int, str]:
#         """
#         find columns whose name type is int and create mapping between their int name and its string form
#         :param df:
#         :type df:
#         :return:
#         :rtype:
#         """
#         int_columns = []
#         for column_name in df.columns:
#             if isinstance(column_name, int):
#                 int_columns.append(column_name)

#         return {column_name: str(column_name) for column_name in int_columns}

#     def new_variable(self, name, value, type=None, size=None, project_uid=""): #TODO later: rename everywhere to "create_or_update_variable" or similar
#         # TODO: implement __setitem__
#         name = self._set_up_unique_varname(name)
#         if isinstance(value, pd.DataFrame):
#             self.dataframe_scan_analyses_records[name] = DataFrameWizardScanAnalysis()
#             value = value.rename(columns=self.get_int_to_str_col_name_mapping(value))
#             # self.dataframe_column_category_predictions[name] = classify_df_column_categories(value)
#             # print(self.dataframe_column_category_predictions[name])
#             value.attrs["name"] = name
#         variable=None
#         if name in self.variables.keys():
#             variable=self.update_variable(name, value, project_uid)
#             is_new_variable=False
#         else:
#             variable=self.create_variable(name, value, project_uid)
#             is_new_variable=True
        
        
#         return(variable, is_new_variable) #Variable
    
#     def create_variable(self, name, value, project_uid=""):
#         variable=Variable(name, value) #Create new 
#         self.variables[name]=variable
#         #self.variables[name] = value
#         return(variable)
    
#     def update_variable(self, name, value, project_uid=""):
        
#         variable=self.variables[name]
#         self.variables[name].value=value #Update
#         return(variable)

#     def _set_up_unique_varname(self, name: str) -> str:
#         i = 2

#         if not name:
#             name = 'untitled1'
#             # If untitled_i already exists -> try untitled_i+1
#             while self._is_varname_duplicated(name):
#                 name = f'untitled{i}'
#                 i += 1
#         elif " " in name:
#             name = "_".join(name.split())
#         print(f'new unique name = ', name)
#         return name

#     def _is_varname_duplicated(self, name: str) -> bool:
#         names = self.stored_variable_df['Name'].tolist()  # All current variables' names
#         checker = name in names
#         return checker

#     def __setitem__(self, key, value):
        
#         #ncrb.update_variable_by_uid(variable_uid, name, value)
#         super().__setitem__(key, value)#self.variables[key].value
#         self.is_refresh_needed = True

#     def delete_variable(self, var_name: str):
#         if self.last_active_df_variable is not None and var_name == self.last_active_df_variable.name:
#             self.last_active_df_variable = None
            
#         self.variables.pop(var_name)
        

#     #   TODO: fix dependencies
#     def update_data_in_variable_explorer(self, glc): #TODO Dominik: Refactor out, shouldnt be here
#         self.is_refresh_needed = False
#         if hasattr(glc, "variable_explorer"):
#             stored_variables_list = []
#             for x in self.variables.values():
#                 stored_variables_list.insert(0, [x.name, x.typ, x.size, x.value])
#                 # Temporally commented, causes problems. If you need this, pls contact Dominik or Ilya - to be deprecated in two months (11.7.2022)
#                 # if len(str(x.value)) < 30:
#                     # stored_variables_list.insert(0, [x.name, x.typ, x.size, x.value])
#                 # else:
#                     # stored_variables_list.insert(0, [x.name, x.typ, x.size, str(x.value)[0:20]])
#             self.stored_variable_df = pd.DataFrame(stored_variables_list, columns=["Name", "Type", "Size", "Value"])
#             glc.variable_explorer.update_data(self.stored_variable_df)

#         # stored_variables_list = [[x.name, x.typ, x.size, x.value] for x in self.values()]
#         # stored_variable_df = pd.DataFrame(stored_variables_list, columns=["Name", "Type", "Size", "Value"])
#         # glc.variable_explorer.update_data(stored_variable_df)

#     def populate_df_analysis_record(self, name, empty_rows, duplicated_rows, empty_columns, id_columns, result):
#         df_analysis = DataFrameWizardScanAnalysis()
#         df_analysis.is_analyzed = True
#         df_analysis.empty_rows = empty_rows
#         df_analysis.duplicated_rows = duplicated_rows
#         df_analysis.empty_columns = empty_columns
#         df_analysis.id_columns = id_columns
#         df_analysis.result = result

#         self.dataframe_scan_analyses_records[name] = df_analysis

#     def empty_df_analysis_record(self, name):
#         self.dataframe_scan_analyses_records[name] = DataFrameWizardScanAnalysis()

#     def df_already_scanned(self, name):
#         if name in self.dataframe_scan_analyses_records:
#             return self.dataframe_scan_analyses_records[name].is_analyzed
#         return False

#     # DB Connections
#     # TODO: IT IS POSSIBLE TO HAVE DUPLICATE DB NAMES
#     def new_database_connection(self, db_connection):
#         # TODO: should check whether db connection is valid and raise exception if not
#         assert db_connection.is_valid_db_connection
#         self.db_connections.append(db_connection)

#     def get_selected_db_connection(self, selected_db_name):
#         valid_dbs = {x.database + " (" + x.server + ")": x for x in self.db_connections if hasattr(x, "database")}
#         db_connection = valid_dbs.get(selected_db_name, None)

#         return db_connection

    #TODO: is this some bug?
    def get_db_connection(self, db_name):
        for connection in self.db_connections:
            if connection.database == db_name:
                return connection


# api_variable_handler = VariableHandler()
variable_handler = LocalVariableHandler()

defined_functions_dict = {}  # used by function handlers
custom_icons_imports = []

new_var_names_from_parsing = []
