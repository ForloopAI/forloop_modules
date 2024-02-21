import ast

import pandas as pd
import numpy as np
import dbhydra.dbhydra_core as dh
import forloop_modules.queries.node_context_requests_backend as ncrb

from typing import List, Optional, Union, Tuple
from collections import Counter
from difflib import SequenceMatcher

import forloop_modules.flog as flog

# TODO: REFACTOR PROBLEMATIC IMPORTS
#from src.gui.gui_layout_context import glc

from forloop_modules.errors.errors import CriticalPipelineError, SoftPipelineError
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.docs import Docs
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.globals.docs_categories import DocsCategories
# from src.server.node_context_manager import variable_handler

from forloop_modules.utils.definitions import FL_DATA_TYPES
from forloop_modules.utils.str_helpers import get_str_cosine_similarity
from forloop_modules.utils.pandas_operations import find_replace, find_value, sort, df_difference, ensure_list

from forloop_modules.function_handlers.transformations.imputation import KNNImputation
from forloop_modules.function_handlers.transformations.outliers import detect_numeric_outliers
from forloop_modules.function_handlers.transformations.basic_transforms import find_empty_rows, remove_empty_rows, add_column_constant, get_valid_columns, \
    detect_and_fix_data_shift, round_to_higher_frequency, find_empty_rows_indices, cast_user_input_to_proper_type, \
    cast_str_to_numeric


def get_df_entry_name(df_entry: Union[pd.DataFrame, str]):
    if isinstance(df_entry, pd.DataFrame):
        return df_entry.attrs["name"]
    else:
        return "NO_INPUT"


class NewDataFrameHandler(AbstractFunctionHandler):
    """Create an empty or pre-filled DataFrame from input data."""
    def __init__(self):
        self.icon_type = "NewDataFrame"
        self.fn_name = "New DataFrame"

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = f"{self.icon_type} Node takes 3 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Data", name="data", description="Data to be inserted into the DF",
            typ="list of lists", example=["[['a','b','c'],[1,2,3]]"]
        )
        self.docs.add_parameter_table_row(
            title="Columns", name="columns", description=
            "Column name string or a list with column names. Must be of same length as Values parameter",
            typ="list of strings", example=['price', "['price', 'publish_date']"]
        )
        self.docs.add_parameter_table_row(
            title="New variable name", name="new_var_name",
            description="A name for the new Dataframe variable", typ="String",
            example="df_2"
        )

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None):
        fdl = FormDictList()

        fdl.label("Create a new DataFrame")
        fdl.label("Data")
        fdl.entry(name="data", text="", input_types=["list", "dict"], required=False, row=1)
        fdl.label("Columns")
        fdl.entry(name="columns", text="", input_types=["list"], required=False, row=2)
        fdl.label("New Variable")
        fdl.entry(
            name="new_var_name", text="", category="new_var", input_types=["str"], required=True,
            row=3
        )
        fdl.button(
            name="execute", function=self.execute, function_args=node_detail_form, text="Execute",
            focused=True
        )

        return fdl

    def execute(self, node_detail_form):
        data = node_detail_form.get_chosen_value_by_name("data", variable_handler)
        columns = node_detail_form.get_chosen_value_by_name("columns", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)
        self.direct_execute(data, columns, new_var_name)
        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        columns = params["columns"]
        data = params["data"]
        new_var_name = params["new_var_name"]

        self.direct_execute(columns, data, new_var_name)

    def direct_execute(self, data, columns, new_var_name):
        if isinstance(data, list) and not all(type(row) == list for row in data):
            raise CriticalPipelineError(
                "When Data is provided as a list, it must contain lists of values for each row"
            )
        elif isinstance(data, dict) and \
             not all(isinstance(key, str) and isinstance(value, list) for key, value in data.items()):
            raise CriticalPipelineError(
                "When Data is provided as a dict, keys must be strings and values must be lists"
            )
        elif data != "" and isinstance(data, str):
            raise CriticalPipelineError("Data cannot be provided as a string")

        if (isinstance(columns, list) and not all(type(column) == str for column in columns)) or \
           columns != "" and isinstance(columns, str):
            raise CriticalPipelineError("Columns must be provided as a list of strings")

        if columns != "" and len(columns) != len(data[0]):
            raise CriticalPipelineError("Provided Data rows must be of the same length as Columns")

        data = None if data == "" else data
        columns = None if columns == "" else columns

        inp = Input()
        inp.assign("data", data)
        inp.assign("columns", columns)
        new_df = self.input_execute(inp)

        if new_var_name in variable_handler.variables.keys():
            variable_handler.update_variable(new_var_name, new_df)
        else:
            variable_handler.create_variable(new_var_name, new_df)

    def input_execute(self, inp: Input) -> pd.DataFrame:
        new_df = pd.DataFrame(inp["data"], columns=inp["columns"])
        return new_df

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return imports


class DropColumnHandler(AbstractFunctionHandler):
    """
    Drop Column Node drops selected column(s) in a data table.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'DropColumn'
        self.fn_name = 'Drop Column'

        self.code_import_patterns = ['drop']
        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """The Drop Column Node requires three parameters, input dataframe, a column to be 
        deleted and a name for a new variable. Dataframe entry input expects variable rectangle with Dataframe, input 
        Columns can be selected from the combobox and new bariable expects string."""
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Column(s)", name="column_name", 
                      description="A name of the column or name of columns to be deleted. More than one column can be selected from the combobox."
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'drop_column_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Drop Column")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column(s)")
        fdl.combobox(name="column_name", options=columns, row=2)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column_name = node_detail_form.get_chosen_value_by_name("column_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column_name, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column_name = params["column_name"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, column_name, new_var_name)

    def debug(self, df_entry: pd.DataFrame, column_name: List[str], new_var_name: str):
        flog.debug("APPLY DROP COLUMN")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS = {column_name}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, column_name: str, new_var_name: str):
        self.debug(df_entry, column_name, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        if not column_name:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("No column selected for drop operation.")

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("column_name", column_name)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        ###variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = inp("df_entry").drop(inp("column_name"), axis=1)

        return df_new

    def export_imports(self, *args):
        imports = []
        return imports

class SelectColumnsHandler(AbstractFunctionHandler):
    """
    Select Column Node takes out selected column(s) from a data table.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'SelectColumns'
        self.fn_name = 'Select Columns'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """The Select Column Node requires at least one parameter, a column(s) to be selected from the data table."""
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Columns to select", name="column_names", 
                      description="A name of the column or name of columns to be selected from the data. More than one column can be chosen in the combobox."
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'select_columns_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Select Columns")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Columns to select")
        fdl.combobox(name="column_names", options=columns, multiselect_indices={}, row=2)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        execution of the select columns transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column_names = node_detail_form.get_chosen_value_by_name("column_names", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column_names, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column_names = params["column_names"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, column_names, new_var_name)

    def debug(self, df_entry: pd.DataFrame, column_names: list, new_var_name: str):
        flog.debug("APPLY SELECT COLUMNS")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS = {column_names}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, column_names: Union[list, str], new_var_name: str):
        self.debug(df_entry, column_names, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        if not column_names:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("No columns entered for a DF selection.")
        
        column_names = column_names if isinstance(column_names, list) else [column_names]

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("column_names", column_names)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        ###variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        valid_column_names = get_valid_columns(inp("df_entry"), inp("column_names"))
        df_new = inp("df_entry")[valid_column_names]

        return df_new

    def export_imports(self, *args):
        imports = ["import pandas as pd", self.export_internal_function(get_valid_columns)]
        return imports


class ConstantColumnHandler(AbstractFunctionHandler):
    """
    Add Constant Column Node creates a new constant column of length equal to that of the data frame.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'ConstantColumn'
        self.fn_name = 'Add Constant Column'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """Add Constant Column Node requires 2 parameters, a *value*, i.e. a number which will 
        fill in the constant column, and a new *column name*."""
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Value", name="value", 
                      description="A constant value filling the new column.", 
                      typ="Int | float")
        self.docs.add_parameter_table_row(title="Column name", name="column", 
                      description="A name (header) of the newly created constant column.",
                      typ="String", example="'A_new_constant_column'"
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'add_constant_to_column_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        fdl = FormDictList(docs=self.docs)
        fdl.label("Add Constant column")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Value")
        fdl.entry(name="value", text="0", input_types=["int", "float"], row=2)
        fdl.label("Column name")
        fdl.entry(name="column_name", text="constant", input_types=["str"], row=3)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        value = node_detail_form.get_chosen_value_by_name("value", variable_handler)
        column_name = node_detail_form.get_chosen_value_by_name("column_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, value, column_name, new_var_name)
        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        value = params["value"]
        column = params["column"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, value, column, new_var_name)

    def debug(self, df_entry: pd.DataFrame, value, new_colname: str, new_var_name: str):
        flog.debug("APPLY ADD CONSTANT COLUMN")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"VALUE = {value}")
        flog.debug(f"COLUMN = {new_colname}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, value):
        value = cast_user_input_to_proper_type(value)
        return value

    def direct_execute(self, df_entry: pd.DataFrame, value, column_name: str, new_var_name: str):
        # TODO: add inplace=False default parameter
        self.debug(df_entry, value, column_name, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        if not value or not column_name:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Both 'Value' and 'Column name' must be provided.")
        
        value = self.parse_input(value)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("value", value)
        inp.assign("new_colname", column_name)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        ##variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = add_column_constant(inp("df_entry"), value=inp("value"), new_colname=inp("new_colname"))

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["value"]["value"] = self.parse_input(node_detail_form.node_params["value"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = [self.export_internal_function(add_column_constant)]
        return imports


class RenameColumnHandler(AbstractFunctionHandler):
    """
    Rename Column Node sets a new name (header) to the selected column.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'RenameColumn'
        self.fn_name = 'Rename Column'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """The Rename Column Node requires 2 parameters (other than input dataframe and new 
        dataframe name), a *column* whose name is to be changed and the *new name*."""
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Column", name="old_col_name", 
                      description="The column to be renamed. The name of the column can be either written or selected from the combobox.")
        self.docs.add_parameter_table_row(title="New column name", name="new_col_name", 
                      description="A new name which will be used as a header for the selected column",
                      typ="String", example="'“New_column_name”'"
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'rename_column_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Rename Column")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column")
        fdl.combobox(name="old_col_name", options=columns, row=2)
        fdl.label("New column name")
        fdl.entry(name="new_col_name", text="", input_types=["str"], row=3)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        old_col_name = node_detail_form.get_chosen_value_by_name("old_col_name", variable_handler)
        new_col_name = node_detail_form.get_chosen_value_by_name("new_col_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, old_col_name, new_col_name, new_var_name)
        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        old_col_name = params["old_col_name"]
        new_col_name = params["new_col_name"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, old_col_name, new_col_name, new_var_name)

    def debug(self, df_entry: pd.DataFrame, old_col_name: List[str], new_col_name: str, new_var_name: str):
        flog.debug("APPLY RENAME COLUMN")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"OLD COLUMN = {old_col_name}")
        flog.debug(f"NEW COLUMN = {new_col_name}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, old_col_name: str, new_col_name: str, new_var_name: str):
        self.debug(df_entry, old_col_name, new_col_name, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        if not old_col_name or not new_col_name:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Both old and new column names must be filled in.")

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("old_col_name", old_col_name)
        inp.assign("new_col_name", new_col_name)

        if new_col_name in df_entry.columns:
            flog.warning("New column name already present in DataFrame")
            
        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        ##variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = inp("df_entry").rename(columns={inp("old_col_name"): inp("new_col_name")}, errors='raise')

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["old_col_name"]["value"] = self.parse_input(node_detail_form.node_params["old_col_name"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports

class CastColumnTypeHandler(AbstractFunctionHandler):
    """CastColumnType Node changes data type of the selected column."""

    def __init__(self):
        super().__init__()
        self.icon_type = 'CastColumnType'
        self.fn_name = 'Cast Column Type'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()

    def _init_docs(self):
        parameter_description = """The CastColumnType Node requires 2 parameters (other than input dataframe and new 
        dataframe name), a *column* whose data type is to be changed and the *new column type*."""

        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)

        self.docs.add_parameter_table_row(
            title="Dataframe",
            name="df_entry",
            description="Dataframe variable rectangle.",
            typ="Dataframe"
        )

        self.docs.add_parameter_table_row(
            title="Column",
            name="col_name",
            description="The column to be casted. The name of the column can be either written or selected from the combobox."
        )

        self.docs.add_parameter_table_row(
            title="New column type",
            name="new_col_type",
            description="A new column data type",
            typ="String"
        )

        self.docs.add_parameter_table_row(
            title="New variable name",
            name="new_var_name",
            description="A name for the new Dataframe variable",
            typ="String",
            example="'casted_column_df'"
        )

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])

        column_dtypes = ['str', 'int', 'float', 'datetime', 'boolean']

        fdl = FormDictList(docs=self.docs)

        fdl.label("Cast Column Type")

        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)

        fdl.label("Column")
        fdl.combobox(name="col_name", options=columns, row=2)

        # TODO: Multiple columns (pass dict as arg)?
        fdl.label("New column type")
        fdl.combobox(name="new_col_type", options=column_dtypes, row=3)

        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)

        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        col_name = node_detail_form.get_chosen_value_by_name("col_name", variable_handler)
        new_col_type = node_detail_form.get_chosen_value_by_name("new_col_type", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, col_name, new_col_type, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        col_name = params["col_name"]
        new_col_type = params["new_col_type"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, col_name, new_col_type, new_var_name)

    def debug(self, df_entry: pd.DataFrame, col_name: List[str], new_col_type: str, new_var_name: str):
        flog.debug("APPLY CAST COLUMN TYPE")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMN = {col_name}")
        flog.debug(f"NEW COLUMN TYPE = {new_col_type}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, col_name: str, new_col_type: str, new_var_name: str, ):
        self.debug(df_entry, col_name, new_col_type, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        if not col_name or not new_col_type:
            if not col_name:
                message = "No column  selected."
            else:
                message = "'New column type' argument is required."
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError(message)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("col_name", col_name)
        inp.assign("new_col_type", new_col_type)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)

    def input_execute(self, inp):
        col_name = inp("col_name")
        new_col_type = inp("new_col_type")

        dtype_to_pd_dtype = {
            'str': 'str',
            'int': 'int64',
            'float': 'float64',
            'datetime': 'datetime64[s]',
            'boolean': 'bool'
        }

        df_new: pd.DataFrame = inp("df_entry").copy()
        df_new[col_name] = df_new[col_name].astype(dtype_to_pd_dtype[new_col_type])

        return df_new

    def export_code(self, node_detail_form):
        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports

class ExplodeColumnHandler(AbstractFunctionHandler):
    """ExplodeColumn Node flattens (explodes) dict-like values in column to new columns """

    def __init__(self):
        super().__init__()
        self.icon_type = 'ExplodeColumn'
        self.fn_name = 'Explode Column'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()

    def _init_docs(self):
        parameter_description = """The ExplodeColumn Node requires 1 parameter (other than input dataframe and new 
        dataframe name), a *column* which should be exploded."""

        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)

        self.docs.add_parameter_table_row(
            title="Dataframe",
            name="df_entry",
            description="Dataframe variable rectangle.",
            typ="Dataframe"
        )

        self.docs.add_parameter_table_row(
            title="Column",
            name="col_name",
            description="The column to be exploded. The name of the column can be either written or selected from the combobox."
        )

        self.docs.add_parameter_table_row(
            title="New variable name",
            name="new_var_name",
            description="A name for the new Dataframe variable",
            typ="String",
            example="'exploded_column_df'"
        )

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)

        fdl.label("Explode Column")

        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)

        fdl.label("Column")
        # TODO: add support for multiple columns
        fdl.combobox(name="col_name", options=columns, row=2)

        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)

        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        col_name = node_detail_form.get_chosen_value_by_name("col_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, col_name, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        col_name = params["col_name"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, col_name, new_var_name)

    def direct_execute(self, df_entry: pd.DataFrame, col_name: List[str], new_var_name: str, ):
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("col_name", col_name)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)

    def input_execute(self, inp):
        col_name = inp("col_name")

        df_new: pd.DataFrame = inp("df_entry").copy()

        cols_to_explode = [col_name]

        for col in cols_to_explode:
            df_new[col] = df_new[col].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
            df_tmp = pd.json_normalize(df_new[col]).add_prefix(f'{col}.')
            df_new = pd.concat([df_new, df_tmp], axis=1).drop(columns=col)

        return df_new

    def export_code(self, node_detail_form):
        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports

class RemoveEmptyRowsHandler(AbstractFunctionHandler):
    """
    Removes empty rows. If some ID columns are filled, but other columns are empty, the filled columns can be ignored.
    """
    # TODO: Should record users' id column option as feedback for wizard scanner
    def __init__(self):
        super().__init__()
        self.icon_type = 'RemoveEmptyRows'
        self.fn_name = 'Remove Empty Rows'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Mode", name="mode", 
                      description="Decide whether to detect or remove empty rows.")
        self.docs.add_parameter_table_row(title="ID Columns", name="id_columns", 
                      description="Choose in which columns the id values are filled (columns can be ignored)."
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'remove_empty_rows_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Remove empty rows")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Mode")
        fdl.combobox(name="mode", options=["detect", "remove"], default="remove", row=2)
        fdl.label("ID Columns")
        fdl.combobox(name="id_columns", options=columns, row=3)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        mode = node_detail_form.get_chosen_value_by_name("mode", variable_handler)
        id_columns = node_detail_form.get_chosen_value_by_name("id_columns", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, mode, id_columns, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        mode = params["mode"]
        id_columns = params["id_columns"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, mode, id_columns, new_var_name)

    def debug(self, df_entry: pd.DataFrame, mode: str, id_columns: List[str], new_var_name: str):
        flog.debug("APPLY REMOVE EMPTY ROWS")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"MODE = {mode}")
        flog.debug(f"ID COLUMNS = {id_columns}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, id_columns: List[str]):
        if not isinstance(id_columns, list):
            id_columns = [id_columns]
        return id_columns

    def direct_execute(self, df_entry: pd.DataFrame, mode: str, id_columns: List[str], new_var_name: str):
        self.debug(df_entry, mode, id_columns, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        id_columns = self.parse_input(id_columns)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("mode", mode)
        inp.assign("id_columns", id_columns)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        if inp("mode") == "detect":
            df_new = find_empty_rows(inp("df_entry"), inp("id_columns"))
        elif inp("mode") == "remove":
            df_new = remove_empty_rows(inp("df_entry"), inp("id_columns"))

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["id_columns"]["value"] = self.parse_input(node_detail_form.node_params["id_columns"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = [self.export_internal_function(find_empty_rows_indices),
                   self.export_internal_function(find_empty_rows),
                   self.export_internal_function(remove_empty_rows)]
        return imports


class RemoveDuplicatesHandler(AbstractFunctionHandler):
    """
    Removes all duplicate values in selected columns while keeping the first/last occurence or none.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'RemoveDuplicates'
        self.fn_name = 'Remove Duplicates'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Keep value", name="keep", 
                      description="Decide handler behaviour. Either keep only **first**, **last**, or **none** of outliers.")
        self.docs.add_parameter_table_row(title="Considered Columns", name="subset", 
                      description="Choose in which columns the duplicates should be detected."
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'remove_duplicates_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])
        keep_options = ['first', 'last', 'none']

        fdl = FormDictList(docs=self.docs)
        fdl.label("Remove Duplicates")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Keep value")
        fdl.combobox(name="keep", options=keep_options, default=keep_options[0], row=2)
        fdl.label("Considered Columns")
        fdl.combobox(name="subset", options=columns, row=3)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", category="new_var", text="", row=4)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the remove duplicates transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        keep = node_detail_form.get_chosen_value_by_name("keep", variable_handler)
        subset = node_detail_form.get_chosen_value_by_name("subset", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, subset, keep, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        keep = params["keep"]
        subset = params["subset"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, subset, keep, new_var_name)

    def debug(self, df_entry, subset: List[str], keep, new_var_name):
        flog.debug("APPLY REMOVE DUPLICATES")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"MODE = {subset}")
        flog.debug(f"ID COLUMNS = {keep}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, df_entry: pd.DataFrame, subset: List[str], keep: str):
        if keep == 'none':
            keep = False
        if len(subset) == 0:
            subset = df_entry.columns.tolist()
        return subset, keep

    def direct_execute(self, df_entry: pd.DataFrame, subset: List[str], keep: str, new_var_name: str):
        self.debug(df_entry, subset, keep, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        subset, keep = self.parse_input(df_entry, subset, keep)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("keep", keep)
        inp.assign("subset", subset)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = inp("df_entry").drop_duplicates(subset=inp("subset"), keep=inp("keep"))

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["subset"]["value"], node_detail_form.node_params["keep"]["value"] = \
            self.parse_input(node_detail_form.node_params["df_entry"]["variable"], node_detail_form.node_params["subset"]["value"], node_detail_form.node_params["keep"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports


class ReplaceHandler(AbstractFunctionHandler):
    """
    Replace String Node replaces all strings (or sub-strings) in selected column which either contain some pattern or exactly match it.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'Replace'
        self.fn_name = 'Replace String'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """Replace String Node requires 4 parameters:"""
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Replace in columns", name="columns", 
                      description="Column(s) selected for the string replacement.")
        self.docs.add_parameter_table_row(title="Match type", name="match", 
                      description="**pattern** → all strings containing the pattern will be replaced; **exact** → only string exactly matching the pattern will be replaced")
        self.docs.add_parameter_table_row(title="Replace substring", name="replace_substring", 
                      description="Enables to replace substrings.")
        self.docs.add_parameter_table_row(title="Pattern", name="pattern", 
                      description="The pattern used in the replacement process.",
                      typ="String"
                      )
        self.docs.add_parameter_table_row(title="Replacement", name="replacement", 
                      description="The string which will be replaced for all the selected values.",
                      typ="String"
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String"
                      )

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])
        match_type = ['pattern', 'exact']

        fdl = FormDictList(docs=self.docs)
        fdl.label("Replace")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Replace in Columns")
        fdl.combobox(name="columns", options=columns, multiselect_indices={}, row=2)
        fdl.label("Match Type")
        fdl.combobox(name="match", options=match_type, default=match_type[0], row=3)
        fdl.label("Replace Substring:")
        fdl.checkbox(name="replace_substring", bool_value=False, row=4)
        fdl.label("Pattern")
        fdl.entry(name="pattern", text="", row=5)
        fdl.label("Replacement")
        fdl.entry(name="replacement", text="", row=6)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", category="new_var",text="", row=7)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        execution of the search transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        columns = node_detail_form.get_chosen_value_by_name("columns", variable_handler)
        match = node_detail_form.get_chosen_value_by_name("match", variable_handler)
        replace_substring = node_detail_form.get_chosen_value_by_name("replace_substring", variable_handler)
        pattern = node_detail_form.get_chosen_value_by_name("pattern", variable_handler)
        replacement = node_detail_form.get_chosen_value_by_name("replacement", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, columns, match, replace_substring, pattern, replacement, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        columns = params["columns"]
        match = params["match"]
        replace_substring = params["replace_substring"]
        pattern = params["pattern"]
        replacement = params["replacement"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, columns, match, replace_substring, pattern, replacement, new_var_name)

    def direct_execute(self, df_entry: pd.DataFrame, columns, match: str, replace_substring: bool, pattern: str,
                       replacement: str,
                       new_var_name: str):
        """
        Replace given pattern by a replacement
        Pattern is searched in search columns
        Match can be either exact or pattern
        User can also search for substrings, if replace substring is True then match is changed to "pattern"
        pd handles substrings as "pattern" -> the input substring is modified internally to pattern
        """
        self.debug(df_entry, columns, match, replace_substring, pattern, replacement, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        columns, match = self.parse_input(columns, match, replace_substring)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("columns", columns)
        inp.assign("match", match)
        inp.assign("pattern", pattern)
        inp.assign("replacement", replacement)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = find_replace(inp("df_entry"), pattern=inp("pattern"), replacement=inp("replacement"),
                              search_cols=inp("columns"), match=inp("match"))

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["df_entry"]["variable"], node_detail_form.node_params["match"]["value"] = \
            self.parse_input(node_detail_form.node_params["df_entry"]["variable"], node_detail_form.node_params["match"]["value"], node_detail_form.node_params["replace_substring"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = ["print('test1')",
                   self.export_internal_function(ensure_list),
                   "print('test2')",
                   self.export_internal_function(find_replace),
                   "print('test1')"]
        return imports
    
    def debug(self, df_entry: pd.DataFrame, search_cols, match: str, replace_substring, pattern: str, replacement: str,
              new_var_name: str):
        flog.debug("APPLY REPLACE")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"SEARCH COLUMNS = {search_cols}")
        flog.debug(f"MATCH = {match}")
        flog.debug(f"REPLACE SUBSTRING = {replace_substring}")
        flog.debug(f"PATTERN = {pattern}")
        flog.debug(f"REPLACEMENT = {replacement}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, columns, match: str, replace_substring: bool):
        if len(columns) == 0:
            columns = None
        if replace_substring is True:
            match = 'pattern'

        return columns, match

class StripColumnHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'StripColumn'
        self.fn_name = 'Strip Column'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])
        strip_modes = ["remove leading", "remove trailing", "remove all"]

        fdl = FormDictList()
        fdl.label("Strip Column")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column(s)")
        fdl.combobox(name="column_name", options=columns, row=2)
        fdl.label("Strip mode")
        fdl.combobox(name="strip_mode", options=strip_modes, default="remove all", row=3)
        fdl.label("Remove Specific characters")
        fdl.entry(name="specific_characters", text="", input_types=["str"], row=4)
        fdl.label("If left blank, handler removes whitespaces")
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=6)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def debug(self, df_entry: pd.DataFrame, column_name: List[str], strip_mode: str, specific_characters: str, new_var_name: str):
        flog.debug("APPLY DROP COLUMN")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS = {column_name}")
        flog.debug(f"STRIP MODE = {strip_mode}")
        flog.debug(f"SPECIFIC CHARACTERS = {specific_characters}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, column_name: List[str], strip_mode: str, specific_characters: str, new_var_name: str):
        self.debug(df_entry, column_name, strip_mode, specific_characters, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("column_name", column_name)
        inp.assign("strip_mode", strip_mode)
        inp.assign("specific_characters", specific_characters)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = inp("df_entry").copy()
        strip_mode = inp("strip_mode")
        if strip_mode == "remove all":
            df_new[inp("column_name")] = df_new[inp("column_name")].str.strip(inp("specific_characters"))
        if strip_mode == "remove leading":
            df_new[inp("column_name")] = df_new[inp("column_name")].str.lstrip(inp("specific_characters"))
        if strip_mode == "remove trailing":
            df_new[inp("column_name")] = df_new[inp("column_name")].str.rstrip(inp("specific_characters"))

        return df_new

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column_name = params["column_name"]
        strip_mode = params["strip_mode"]
        specific_characters = params["specific_characters"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, column_name, strip_mode, specific_characters, new_var_name)

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column_name = node_detail_form.get_chosen_value_by_name("column_name", variable_handler)
        strip_mode = node_detail_form.get_chosen_value_by_name("strip_mode", variable_handler)
        specific_characters = node_detail_form.get_chosen_value_by_name("specific_characters", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column_name, strip_mode, specific_characters, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    # TODO: Implement export_code
    def export_code(self, node_detail_form):

        code = f""""""

        return code

    def export_imports(self, *args):
        imports = []
        return imports


class SearchHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'Search'
        self.fn_name = 'Search String'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])
        match_type = ['pattern', 'exact']

        fdl = FormDictList()
        fdl.label("Search")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Search in Columns")
        fdl.combobox(name="columns", options=columns, multiselect_indices={}, row=2)
        fdl.label("Match Type")
        fdl.combobox(name="match", options=match_type, default=match_type[0], row=3)
        fdl.label("Pattern")
        fdl.entry(name="pattern", text="", row=4)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", row=5)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        execution of the search transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        columns = node_detail_form.get_chosen_value_by_name("columns", variable_handler)
        match = node_detail_form.get_chosen_value_by_name("match", variable_handler)
        pattern = node_detail_form.get_chosen_value_by_name("pattern", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, columns, match, pattern, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        columns = params["columns"]
        match = params["match"]
        pattern = params["pattern"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, columns, match, pattern, new_var_name)

    def debug(self, df_entry: pd.DataFrame, search_cols, match: str, pattern: str, new_var_name: str):
        flog.debug("APPLY Search")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"SEARCH COLUMNS = {search_cols}")
        flog.debug(f"MATCH = {match}")
        flog.debug(f"PATTERN = {pattern}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, columns, match: str, pattern: str, new_var_name: str):
        """
        Search given pattern
        Pattern is searched in search columns
        Match can be either exact or pattern
        """
        self.debug(df_entry, columns, match, pattern, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("columns", columns)
        inp.assign("match", match)
        inp.assign("pattern", pattern)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = find_value(inp("df_entry"), value=inp("pattern"), search_cols=inp("columns"), match=inp("match"))
        return df_new

    def export_code(self, node_detail_form):
        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = [self.export_internal_function(ensure_list),
                   self.export_internal_function(find_value)]
        return imports


class SortHandler(AbstractFunctionHandler):
    """
    Sorts selected columns in either ascending or descending order.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'Sort'
        self.fn_name = 'Sort'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="First Sort column", name="column_name1", 
                      description="Names of the columns to be sorted (first order sort).",
                      example="We have a dataframe containing columns Name, Surname, Age, Salary and want to sort it in ascending order by Age and Salary → we will enter: Age, Salary."
                      )
        self.docs.add_parameter_table_row(title="Ascending", name="ascending1", 
                      description="Tick the checkbox for ascending sort.")
        self.docs.add_parameter_table_row(title="Second Sort column", name="column_name2", 
                      description="Names of the columns to be sorted (second order sort).",
                      example="We have a dataframe containing columns Name, Surname, Age, Salary and want to sort it in ascending order by Age and Salary → we will enter: Age, Salary."
                      )
        self.docs.add_parameter_table_row(title="Ascending", name="ascending2", 
                      description="Tick the checkbox for ascending sort."
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'sort_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Sort")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("First sort")
        fdl.label("Column")
        fdl.combobox(name="column_name1", options=columns, row=3)
        fdl.label("Ascending")
        fdl.checkbox(name="ascending1", bool_value=True, row=4)
        fdl.label("Second sort")
        fdl.label("Column")
        fdl.combobox(name="column_name2", options=columns, row=6)
        fdl.label("Ascending")
        fdl.checkbox(name="ascending2", bool_value=True, row=7)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=8)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        execution of the sort transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column_name1 = node_detail_form.get_chosen_value_by_name("column_name1", variable_handler)
        ascending1 = node_detail_form.get_chosen_value_by_name("ascending1", variable_handler)
        column_name2 = node_detail_form.get_chosen_value_by_name("column_name2", variable_handler)
        ascending2 = node_detail_form.get_chosen_value_by_name("ascending2", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column_name1, ascending1, column_name2, ascending2, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column_name1 = params["column_name1"]
        ascending1 = params["ascending1"]
        column_name2 = params["column_name2"]
        ascending2 = params["ascending2"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, column_name1, ascending1, column_name2, ascending2, new_var_name)

    def debug(self, df_entry: pd.DataFrame, col_name1: List[str], ascending1: str,
              col_name2: List[str], ascending2: str, new_var_name: str):
        flog.debug("APPLY SORT")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS NAME 1 = {col_name1}")
        flog.debug(f"ASCENDING 1 = {ascending1}")
        flog.debug(f"COLUMNS NAME 2 = {col_name2}")
        flog.debug(f"ASCENDING 2 = {ascending2}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, column_name1: List[str], ascending1: str,
                       column_name2: List[str], ascending2: str, new_var_name: str, *args):
        """
        sort transformation wrapper
        """
        self.debug(df_entry, col_name1, ascending1, col_name2, ascending2, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        if not col_name1:
            raise SoftPipelineError("Missing column to sort.")
        
        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("col_name1", col_name1)
        inp.assign("ascending1", ascending1)
        inp.assign("col_name2", col_name2)
        inp.assign("ascending2", ascending2)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        if inp("col_name2"):
            df_new = sort(inp("df_entry"), col_names=[inp("col_name1"), inp("col_name2")], ascending=[inp("ascending1"), inp("ascending2")])
        else:
            df_new = sort(inp("df_entry"), col_names=[inp("col_name1")], ascending=[inp("ascending1")])

        return df_new

    def export_code(self, node_detail_form):
        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = [self.export_internal_function(sort)]
        return imports


class ColumnWiseShiftHandler(AbstractFunctionHandler):
    """
    Inspect two dataframe columns, shift their values so that corresponding values (e.g. name and domain) are located 
    on the same row. Inserts empty cell or deletes filled cell where necessary.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'ColumnWiseShift'
        self.fn_name = 'Column-Wise Shift'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Mode", name="mode", 
                      description="Choose whether rows that do not match should be **removed** or **kept** with the other column being shifted and inserted with nan"
                      )
        self.docs.add_parameter_table_row(title="Reference column", name="complete_col", 
                      description="Choose the first column for comparison")
        self.docs.add_parameter_table_row(title="Incomplete column", name="incomplete_col", 
                      description="Choose the second column for comparison"
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'column_wise_shift_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Shift values in a given column")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Shift Mode")
        fdl.combobox(name="mode", options=["keep", "remove"], default="remove", row=2)
        fdl.label("Reference column")
        fdl.combobox(name="complete_col", options=columns, row=3)
        fdl.label("Incomplete shifted column")
        fdl.combobox(name="incomplete_col", options=columns, row=4)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", required=True, row=5)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        mode = node_detail_form.get_chosen_value_by_name("mode", variable_handler)
        complete_col_name = node_detail_form.get_chosen_value_by_name("complete_col", variable_handler)
        incomplete_col_name = node_detail_form.get_chosen_value_by_name("incomplete_col", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, complete_col_name, incomplete_col_name, mode, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        mode = params["mode"]
        complete_col_name = params["complete_col"]
        incomplete_col_name = params["incomplete_col"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, complete_col_name, incomplete_col_name, mode, new_var_name)

    def debug(self, df_entry: pd.DataFrame, complete_column_name: str, incomplete_column_name: str, mode: str,
              new_var_name: str):
        flog.debug("APPLY COLUMN WISE SHIFT")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COMPLETE COLUMN NAME = {complete_column_name}")
        flog.debug(f"INCOMPLETE COLUMN NAME = {incomplete_column_name}")
        flog.debug(f"MODE = {mode}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, complete_column_name: str, incomplete_column_name: str, mode: str,
                       new_var_name: str, *args):
        self.debug(df_entry, complete_column_name, incomplete_column_name, mode, new_var_name)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("mode", mode)
        inp.assign("complete_col", complete_column_name)
        inp.assign("incomplete_col", incomplete_column_name)

        df_new = pd.DataFrame()
        try:
            df_new = self.input_execute(inp)
            flog.debug("Result column wise shift")
            flog.debug(f"{df_new}")
        except KeyError as e:
            flog.warning(f"One of columns is not present in DataFrame")
            flog.warning(f"{e}")
        except Exception as e:
            flog.error(f"{e}")

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = detect_and_fix_data_shift(inp("complete_col"), inp("incomplete_col"), inp("df_entry"), inp("mode"))

        return df_new

    def export_imports(self, *args):
        imports = [self.export_internal_function(detect_and_fix_data_shift)]
        return imports


class DifferenceDataHandler(AbstractFunctionHandler):
    """
    Compare two dataframes with **similar columns** and return their difference.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'DifferenceData'
        self.fn_name = 'Find Difference in Data'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="First dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Subtract Dataframe", name="df_entry2", 
                      description="Variable rectangle storing subtracted dataframe."
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'data_difference_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """

        fdl = FormDictList(docs=self.docs)
        fdl.label("Difference Data")
        fdl.label("Main DF - Subtracted DF = New Variable")
        fdl.label("Main Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=2)
        fdl.label("Subtracted DataFrame")
        fdl.entry(name="df_entry2", text="", input_types=["DataFrame"], required=True, row=3)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=4)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        df_entry2 = node_detail_form.get_chosen_value_by_name("df_entry2", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, df_entry2, new_var_name)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        df_entry2 = params["df_entry2"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, df_entry2, new_var_name)

    def debug(self, df_entry: pd.DataFrame, right_df: pd.DataFrame, new_var_name: str):
        flog.debug("Find Difference in Data")
        flog.debug(f"DF = {df_entry.head()}")
        flog.debug(f"RIGHT DF NAME = {right_df.head()}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, right_df: pd.DataFrame, new_var_name: str, *args):
        self.debug(df_entry, right_df, new_var_name)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("right_df", right_df)

        df_new = pd.DataFrame()
        try:
            if len(inp("df_entry").columns) == len(inp("right_df").columns):
                # different ordering matters!
                flag = (inp("df_entry").columns == inp("right_df").columns).all()
                if not flag:
                    flog.error("WRONG INPUT: DIFFERENT COLUMNS")
                    return

                df_new = self.input_execute(inp)
            else:
                flog.error(f"Different number of columns")
        except Exception as e:
            flog.error(f"UNKNOWN ERROR")
            flog.error(f"{e}")

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = df_difference(inp("df_entry").copy(), inp("right_df"))

        return df_new

    def export_imports(self, *args):
        imports = [self.export_internal_function(df_difference)]
        return imports

class OutliersHandler(AbstractFunctionHandler):
    """
    Detect or remove given number of percentage of outliers in a given column(s)
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'Outliers'
        self.fn_name = 'Detect or Remove Outliers'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Outlier mode", name="mode", 
                      description="Decide whether to detect or remove outliers."
                      )
        self.docs.add_parameter_table_row(title="Columns", name="columns", 
                      description="Choose in which columns the outliers should be detected."
                      )
        self.docs.add_parameter_table_row(title="Ratio as outliers", name="ratio", 
                      description="Choose percentage of outliers to be found. Choose either ratio or top n.",
                      typ="Float", example="0.1 | 0.8 | 1.0"
                      )
        self.docs.add_parameter_table_row(title="Top N as outliers", name="top_n", 
                      description="Choose number of outliers to be found. Choose either ratio or top n.",
                      typ="Integer", example="10 | 20 | 38"
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'outliers_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Detect or Remove Outliers")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Outlier Mode")
        fdl.combobox(name="mode", options=["detect", "remove"], default="remove", row=2)
        fdl.label("Columns")
        fdl.combobox(name="columns", options=columns, multiselect_indices={}, row=3)
        fdl.label("Ratio as outliers")
        fdl.entry(name="ratio", text="0.025", row=4)
        fdl.label("Top N as outliers")
        fdl.entry(name="top_n", text="", row=5)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", required=True, row=6)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        execution of the search transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        mode = node_detail_form.get_chosen_value_by_name("mode", variable_handler)
        columns = node_detail_form.get_chosen_value_by_name("columns", variable_handler)
        ratio = node_detail_form.get_chosen_value_by_name("ratio", variable_handler)
        top_n = node_detail_form.get_chosen_value_by_name("top_n", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, mode, columns, top_n, ratio, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        mode = params["mode"]
        columns = params["columns"]
        ratio = params["ratio"]
        top_n = params["top_n"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, mode, columns, top_n, ratio, new_var_name)

    def debug(self, df_entry: pd.DataFrame, mode: str, columns, top_n, ratio, new_var_name: str):
        flog.debug("APPLY OUTLIERS")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"MODE = {mode}")
        flog.debug(f"COLUMNS = {columns}")
        flog.debug(f"TOP_N = {top_n}")
        flog.debug(f"RATIO = {ratio}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, ratio, top_n, columns):
        # TODO: FIX
        if ratio is not None:
            try:
                ratio = float(ratio)
                top_n = None
            except:
                ratio = 0.025
                top_n = None
        elif top_n is not None:
            try:
                ratio = None
                top_n = int(top_n)
            except:
                top_n = None
                ratio = 0.025
        else:
            ratio = 0.025
            top_n = None
        if columns == " ":  # default value
            columns = None

        return ratio, top_n, columns

    def remove_outliers(self, data: pd.DataFrame, outliers):
        return data.drop(outliers.index, axis=0)

    def direct_execute(self, df_entry: pd.DataFrame, mode: str, columns, top_n, ratio, new_var_name: str, *args):
        self.debug(df_entry, mode, columns, top_n, ratio, new_var_name)
        ratio, top_n, columns = self.parse_input(ratio, top_n, columns)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("mode", mode)
        inp.assign("columns", columns)
        inp.assign("ratio", ratio)
        inp.assign("top_n", top_n)

        try:
            df_new = self.input_execute(inp)
            flog.debug(f"df_new: {df_new}")
        except KeyError:
            df_new = df_entry.copy()
            flog.warning(f'One of columns {inp("columns")} is not present in DataFrame')
        except Exception as e:
            df_new = pd.DataFrame()
            flog.error(f"ERROR: {e}")

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        data = KNNImputation().fit_transform(inp("df_entry"), inp("columns"), return_valid_only=True)

        flog.debug(f"outliers")
        flog.debug(f"filled df: {data}")

        outliers = detect_numeric_outliers(data, cols=inp("columns"), top_n=inp("top_n"), top_n_percent=inp("ratio"),
                                           tree_size=min(128, int(inp("df_entry").shape[0] / 5)))

        flog.debug(f"outliers: {outliers}")

        if inp("mode") == 'detect':
            df_new = inp("df_entry").iloc[outliers.index]
        else:
            df_new = self.remove_outliers(inp("df_entry"), outliers)

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["ratio"]["value"], node_detail_form.node_params["top_n"]["value"], node_detail_form.node_params["columns"]["value"] = \
            self.parse_input(node_detail_form.node_params["ratio"]["value"], node_detail_form.node_params["top_n"]["value"], node_detail_form.node_params["columns"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        
        return imports

class FilterHandler(AbstractFunctionHandler):
    """
    Takes a dataset and removes or selects the data that satisfies given expression. For example let’s say we have a 
    phonebook of all employees working in an international company and we want to select only the contacts for those 
    who work in Germany. Then we would pass an expression looking something like *`country` == ‘Germany’*.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'FilterString'
        self.fn_name = 'Filter String'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        parameters_description = "Filter Data Node requires at least 1 parameter:"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Column(s)", name="column_name", 
                      description="Column(s) selected for the string filtering."
                      )
        self.docs.add_parameter_table_row(title="Filter by string", name="filtered_str", 
                      description="Filter string which is to be filtered.",
                      typ="String"
                      )
        self.docs.add_parameter_table_row(title="Keep matched or drop", name="matched_or_others", 
                      description="Set icon to either keep or drop rows satisfying the expression"
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'filter_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Filter Column")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column(s)")
        fdl.combobox(name="column_name", options=columns, row=2)
        fdl.label("Filter by string")
        fdl.entry(name="filtered_str", text="", input_types=["str"], row=3)
        fdl.label("Keep matched or other rows")
        fdl.combobox(name="matched_or_others", options=["matched", "others"], default="matched", row=4)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=5)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column_name = node_detail_form.get_chosen_value_by_name("column_name", variable_handler)
        filtered_str = node_detail_form.get_chosen_value_by_name("filtered_str", variable_handler)
        matched_or_others = node_detail_form.get_chosen_value_by_name("matched_or_others", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column_name, filtered_str, matched_or_others, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column_name = params["column_name"]
        filtered_str = params["filtered_str"]
        matched_or_others = params["matched_or_others"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, column_name, filtered_str, matched_or_others, new_var_name)

    def debug(self, df_entry: pd.DataFrame, column_name: List[str], filtered_str: str, matched_or_others: str,
              new_var_name: str):
        flog.debug("APPLY FILTER")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMN NAME = {column_name}")
        flog.debug(f"FILTERED STR = {filtered_str}")
        flog.debug(f"MATCHED OR OTHERS = {matched_or_others}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry: pd.DataFrame, column_name: Union[list[str], str], filtered_str: str, matched_or_others: str,
                       new_var_name: str, *args):
        """
        Filter columns text based on a given (sub)string.
        The filtered rows can be either preserved and others deleted if matched_or_others is "matched"
        or the filtered rows can be droped and the others preserven when matched_or_others is "others"
        """
        self.debug(df_entry, column_name, filtered_str, matched_or_others, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        if not column_name:
            raise SoftPipelineError("No column selected for filtering.")

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("column_name", column_name)
        inp.assign("filtered_str", filtered_str)
        inp.assign("matched_or_others", matched_or_others)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        if inp("matched_or_others") == "matched":
            df_new = inp("df_entry")[inp("df_entry")[inp("column_name")].str.contains(inp("filtered_str"), na=False)]
        elif inp("matched_or_others") == "others":
            df_new = inp("df_entry")[~inp("df_entry")[inp("column_name")].str.contains(inp("filtered_str"), na=False)]

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["column_name"]["value"] = node_detail_form.node_params["column_name"]["value"][0]

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports


class ConcatHandler(AbstractFunctionHandler):
    """
    Concatenates values in the two selected dataframes into a new one.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'Concatenate'
        self.fn_name = 'Concatenate'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry2", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Append", name="axis", 
                      description="Choose whether rows or columns should be appended"
                      )
        self.docs.add_parameter_table_row(title="Join", name="join", 
                      description="Choose whether join executed should be inner or outer."
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'concat_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        fdl = FormDictList(docs=self.docs)

        fdl.label("Concatenate Dfs")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Dataframe 2")
        fdl.entry(name="df_entry2", text="", category="instance_var", input_types=["DataFrame"], required=True, row=2)
        fdl.label("Append")
        fdl.combobox(name="axis", options=["rows", "columns"], default="rows", row=3)
        fdl.label("Join")
        fdl.combobox(name="join", options=["outer", "inner"], default="outer", row=4)
        # TODO: CHECK DUPLICATE IDS USING CHECK_INTEGRITY PARAMETER
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=5)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        df_entry2 = node_detail_form.get_chosen_value_by_name("df_entry2", variable_handler)
        axis = node_detail_form.get_chosen_value_by_name("axis", variable_handler)
        join = node_detail_form.get_chosen_value_by_name("join", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, df_entry2, axis, join, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        df_entry2 = params["df_entry2"]
        axis = params["axis"]
        join = params["join"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, df_entry2, axis, join, new_var_name)

    def debug(self, df_entry, df_entry2, axis: int, join: str, new_var_name: str):
        flog.debug("APPLY CONCAT")
        for i, df_entry in enumerate([df_entry, df_entry2]):
            flog.debug(f"DF{i} = {df_entry}")
        flog.debug(f"AXIS = {axis}")
        flog.debug(f"JOIN = {join}")
        flog.debug(f"NEW VAR = {new_var_name}")

    
    def direct_execute(self, df_entry: pd.DataFrame, df_entry2: pd.DataFrame, axis: int, join: str,new_var_name: str, *args):
        if not isinstance(df_entry, pd.DataFrame) or not isinstance(df_entry2, pd.DataFrame):
            raise SoftPipelineError("Both 'Dataframe' and 'Dataframe 2' arguments must be of type 'DataFrame'.")
        
        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("df_entry2", df_entry2)
        inp.assign("axis", axis)
        inp.assign("join", join)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        
    def input_execute(self, inp):
        df_new = pd.concat([inp("df_entry"), inp("df_entry2")], axis=inp("axis"), join=inp("join"))

        return df_new

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return imports


class JoinHandler(AbstractFunctionHandler):
    """
    Joins two dataframes on (possibly muptiple) columns.
    """
    # TODO: ADD LSUFFIX AND RSUFFIX OPTION
    def __init__(self):
        super().__init__()
        self.icon_type = 'Join'
        self.fn_name = 'Join'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry2", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="On columns", name="on", 
                      description="Choose columns on which join should be executed."
                      )
        self.docs.add_parameter_table_row(title="How", name="how", 
                      description="Choose join mode - **left**, **right**, **outer**, **inner**, **cross**"
                      )
        self.docs.add_parameter_table_row(title="New variable name", name="new_var_name", 
                      description="A name for the new Dataframe variable", 
                      typ="String", example="'join_dfs_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Join DataFrames")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Dataframe 2")
        fdl.entry(name="df_entry2", text="", category="instance_var", input_types=["DataFrame"], required=True, row=2)
        fdl.label("On Columns")
        fdl.combobox(name="on", options=columns, multiselect_indices={}, row=3)
        fdl.label("How")
        fdl.combobox(name="how", options=["left", "right", "outer", "inner", "cross"], default="left", row=4)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=5)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        df_entry2 = node_detail_form.get_chosen_value_by_name("df_entry2", variable_handler)
        on = node_detail_form.get_chosen_value_by_name("on", variable_handler)
        how = node_detail_form.get_chosen_value_by_name("how", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, df_entry2, on, how, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        df_entry2 = params["df_entry2"]
        on = params["on"]
        how = params["how"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, df_entry2, on, how, new_var_name)

    def debug(self, df_entry1: Union[str, pd.DataFrame], df_entry2: Union[str, pd.DataFrame],
              on: Optional[str], how: str, new_var_name: str):
        flog.debug("APPLY DF JOIN")
        flog.debug(f"DF1 = {df_entry1}")
        flog.debug(f"DF2 = {df_entry2}")
        flog.debug(f"ON = {on}")
        flog.debug(f"HOW = {how}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, on):
        if on == [' '] or on == ' ':
            on = []

        return on

    def direct_execute(self, df_entry: Union[str, pd.DataFrame], df_entry2: Union[str, pd.DataFrame],
                       on: Optional[str], how: str, new_var_name: str, *args):
        self.debug(df_entry, df_entry2, on, how, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame) or not isinstance(df_entry2, pd.DataFrame):
            raise SoftPipelineError("Both 'Dataframe' and 'Dataframe 2' arguments must be of type 'DataFrame'.")
        
        on = self.parse_input(on)

        inp = Input()
        inp.assign("df_entry1", df_entry)
        inp.assign("df_entry2", df_entry2)
        inp.assign("on", on)
        inp.assign("how", how)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        if inp("on"):
            df_new = inp("df_entry1").join(inp("df_entry2").set_index(inp("on")), on=inp("on"), how=inp("how"),
                                          lsuffix="_L", rsuffix="_R")
        else:
            df_new = pd.merge(inp("df_entry1"), inp("df_entry2"), left_index=True, right_index=True)
        df_new = df_new.reset_index(drop=True)

        return df_new

    def export_code(self, node_detail_form):

        # direct execute accepts df_entry1, but form dict list has df_entry
        # copy the value of df_entry and remove it
        if "df_entry" in node_detail_form.node_params:
            node_detail_form.node_params["df_entry1"] = node_detail_form.node_params["df_entry"]
            node_detail_form.node_params.pop('df_entry', None)
        node_detail_form.node_params["on"]["value"] = self.parse_input(node_detail_form.node_params["on"]["value"])


        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return imports


class AnalyzeDataFrameHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'AnalyzeDataFrame'
        self.fn_name = 'Analyze Data Frame'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, node_detail_form=None, **kwargs) -> FormDictList:
        fdl = FormDictList()
        fdl.label("Analyze DataFrame")
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=1)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)
        return fdl

    def execute(self, node_detail_form):
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(new_var_name)

    def execute_with_params(self, params):
        new_var_name = params["new_var_name"]

        self.direct_execute(new_var_name)

    def direct_execute(self, new_var_name, *args):
        df=pd.DataFrame([1,2,3],columns=["needs_fix"]) #should be replaced by "last_active_df"

        inp = Input()
        inp.assign("df", df)

        column_type_pair_dict = self.input_execute(inp)

        variable_handler.new_variable(new_var_name, column_type_pair_dict)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        migrator = dh.Migrator()
        columns, return_types = migrator.extract_columns_and_types_from_df(inp("df"))
        column_type_pair_dict = dict(zip(columns, return_types))

        return column_type_pair_dict

    def export_imports(self, *args):
        imports = []
        return imports

    def make_flpl_node_dict(self, line_dict: dict) -> dict:
        node = {"type": "UseKey",
                "params": {"code_label": {"variable": None, "value": None}}}  # TODO: finish the node var
        return node


class SplitColumnHandler(AbstractFunctionHandler):
    """
    Splits strings in a given column on a given substring and only keeps element on a given position of a resulting split list.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'SplitString'
        self.fn_name = 'Split String'
        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Column", name="column", 
                      description="Column selected for the string splitting."
                      )
        self.docs.add_parameter_table_row(title="Split on", name="split_on", 
                      description="String on which the icon splits the strings in a given column.",
                      typ="String"
                      )
        self.docs.add_parameter_table_row(title="Select index", name="select_index", 
                      description="Index position of resulting split list, on which the result should be stored, starting from 0.",
                      typ="String"
                      )
        self.docs.add_parameter_table_row(title="Keep old column", name="keep_old", 
                      description="Decide whether the old column is or be dropped or not."
                      )
        self.docs.add_parameter_table_row(title="New column", name="new_col_name", 
                      description="A name for the new Dataframe column.",
                      typ="String"
                      )
        self.docs.add_parameter_table_row(title="New variable", name="new_var_name", 
                      description="A name for the new Dataframe variable.", 
                      typ="String", example="'split_column_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Split String")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column")
        fdl.combobox(name="column", options=columns, row=2)
        fdl.label("Split on")
        fdl.entry(name="split_on", text="_", row=3, input_types=["str"])
        fdl.label("Select index")
        fdl.entry(name="select_index", text="0", row=4, input_types=["int"])
        fdl.label("Keep old column")
        fdl.checkbox(name="keep_old", bool_value=True, row=5)
        fdl.label("New column")
        fdl.entry(name="new_col_name", text="", input_types=["str"], row=6)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=7)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column = node_detail_form.get_chosen_value_by_name("column", variable_handler)
        split_on = node_detail_form.get_chosen_value_by_name("split_on", variable_handler)
        select_index = node_detail_form.get_chosen_value_by_name("select_index", variable_handler)
        keep_old = node_detail_form.get_chosen_value_by_name("keep_old", variable_handler)
        new_col_name = node_detail_form.get_chosen_value_by_name("new_col_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column, split_on, select_index, keep_old, new_col_name, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column = params["column"]
        split_on = params["split_on"]
        select_index = params["select_index"]
        keep_old = params["keep_old"]
        new_col_name = params["new_col_name"]
        new_var_name = params["new_var_name"]
        self.direct_execute(df_entry, column, split_on, select_index, keep_old, new_col_name, new_var_name)

    def debug(self, df_entry: Union[str, pd.DataFrame], column: str, split_on, select_index, keep_old: bool,
              new_col_name: str, new_var_name: str):
        flog.debug("APPLY DF JOIN")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMN = {column}")
        flog.debug(f"SPLIT ON = {split_on}")
        flog.debug(f"SELECT INDEX = {select_index}")
        flog.debug(f"KEEP OLD = {keep_old}")
        flog.debug(f"NEW COL = {new_col_name}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, select_index, new_col_name, column):
        select_index = int(select_index)
        # define augment column name if not available
        if not new_col_name:
            new_col_name = f"{column}_{select_index}"

        return select_index, new_col_name

    def direct_execute(self, df_entry: Union[str, pd.DataFrame], column: str, split_on, select_index, keep_old: bool,
                       new_col_name: str, new_var_name: str, *args):
        """
        Split values in a given column by a given "split_on" string and take {select_index} th value or None
        Store new value into new_col_name column
        If keep old is True, keep old column, otherwise delete it
        """
        self.debug(df_entry, column, split_on, select_index, keep_old, new_col_name, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        all_required_fields_filled = column and split_on and select_index
        if not all_required_fields_filled:
            raise SoftPipelineError("Some required arguments are missing.")

        select_index, new_col_name = self.parse_input(select_index, new_col_name, column)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("column", column)
        inp.assign("split_on", split_on)
        inp.assign("select_index", select_index)
        inp.assign("keep_old", keep_old)
        inp.assign("new_col_name", new_col_name)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        column_index = inp("df_entry").columns.get_loc(inp("column"))
        keep_old = inp("keep_old")
        # assign columns depending on desire to keep old column
        if keep_old:
            columns = inp("df_entry").columns.tolist()
        else:
            columns = inp("df_entry").columns.difference([inp("column")], sort=False).tolist()
        columns_reordered = columns[:column_index] + [inp("new_col_name")] + columns[column_index:]

        all_splits = inp("df_entry")[inp("column")].astype(str).str.split(pat=inp("split_on"), n=min(1, inp("select_index")), expand=True)

        selected_split = all_splits.iloc[:, inp("select_index")]

        df_new = inp("df_entry")[columns].copy()
        df_new[inp("new_col_name")] = selected_split
        df_new = df_new[columns_reordered]

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["select_index"]["value"], node_detail_form.node_params["new_col_name"]["value"] = \
            self.parse_input(node_detail_form.node_params["select_index"]["value"], node_detail_form.node_params["new_col_name"]["value"], node_detail_form.node_params["column"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return imports


class ExtractStringHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'ExtractString'
        self.fn_name = 'Extract String'
        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList()
        fdl.label("Extract String")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column")
        fdl.combobox(name="column", options=columns, row=2)
        fdl.label("Extract pattern")
        fdl.entry(name="extract_pattern", text="_", row=3, input_types=["str"])
        fdl.label("Keep old column")
        fdl.checkbox(name="keep_old", bool_value=True, row=4)
        fdl.label("Concatenate groups")
        fdl.checkbox(name="concat_groups", bool_value=True, row=5)
        fdl.label("New column")
        fdl.entry(name="new_col_name", text="", input_types=["str"], row=6)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=6)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column = node_detail_form.get_chosen_value_by_name("column", variable_handler)
        extract_pattern = node_detail_form.get_chosen_value_by_name("extract_pattern", variable_handler)
        keep_old = node_detail_form.get_chosen_value_by_name("keep_old", variable_handler)
        concat_groups = node_detail_form.get_chosen_value_by_name("concat_groups", variable_handler)
        new_col_name = node_detail_form.get_chosen_value_by_name("new_col_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column, extract_pattern, keep_old, concat_groups, new_col_name, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column = params["column"]
        extract_pattern = params["extract_pattern"]
        keep_old = params["keep_old"]
        new_col_name = params["new_col_name"]
        new_var_name = params["new_var_name"]
        self.direct_execute(df_entry, column, extract_pattern, keep_old, new_col_name, new_var_name)

    def debug(self, df_entry: Union[str, pd.DataFrame], column: str, extract_pattern, keep_old: bool,
              concat_groups: bool, new_col_name: str, new_var_name: str):
        flog.debug("APPLY Extract String")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMN = {column}")
        flog.debug(f"extract_pattern = {extract_pattern}")
        flog.debug(f"KEEP OLD = {keep_old}")
        flog.debug(f"CONCATENATE GROUPS = {concat_groups}")
        flog.debug(f"NEW COL = {new_col_name}")
        flog.debug(f"NEW VAR = {new_var_name}")


    def direct_execute(self, df_entry: Union[str, pd.DataFrame], column: str, extract_pattern, keep_old: bool,
                       concat_groups: bool, new_col_name: str, new_var_name: str, *args):
        """
        Split values in a given column by a given "split_on" string and take {select_index} th value or None
        Store new value into new_col_name column
        If keep old is True, keep old column, otherwise delete it
        """
        self.debug(df_entry, column, extract_pattern, keep_old, concat_groups, new_col_name, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("column", column)
        inp.assign("extract_pattern", extract_pattern)
        inp.assign("keep_old", keep_old)
        inp.assign("concat_groups", concat_groups)
        inp.assign("new_col_name", new_col_name)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        keep_old = inp("keep_old")

        extracted_data = inp("df_entry")[inp("column")].astype(str).str.extractall(inp("extract_pattern")).unstack(level=-1)
        if inp("concat_groups"):
            # concatenate all found groups (concatenating strings is the same as summing them "A" + "B" = "AB")
            # using .values because it is faster
            extracted_data = pd.DataFrame(extracted_data.astype(str).values.sum(axis=1))

        if not inp("new_col_name"):
            extracted_data.columns = [' '.join(map(str, col)).strip() for col in extracted_data.columns.values]
        else:
            if len(extracted_data.columns) > 1:
                extracted_data.columns = [inp("column")+ "_" + str(index) for index in range(len(extracted_data.columns))]
            else:
                extracted_data.columns = [inp("new_col_name")]

        if keep_old:
            columns = inp("df_entry").columns.tolist()
        else:
            columns = inp("df_entry").columns.difference([inp("column")], sort=False).tolist()

        column_index = inp("df_entry").columns.get_loc(inp("column"))
        columns_reordered = columns[:column_index] + extracted_data.columns.tolist() + columns[column_index:]

        df_new = inp("df_entry")[columns].copy()
        df_new[extracted_data.columns] = extracted_data
        df_new = df_new[columns_reordered]

        return df_new

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return imports


class KNNImputationHandler(AbstractFunctionHandler):
    """
    Apply numeric KNN Imputation on selected column.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'KNNImputation'
        self.fn_name = 'KNN Imputation'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Column", name="imp_cols", 
                      description="Choose in which column values should be imputed."
                      )
        self.docs.add_parameter_table_row(title="New variable", name="new_var_name", 
                      description="A name for the new Dataframe variable.", 
                      typ="String", example="'KNN_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList(docs=self.docs)
        fdl.label("Apply KNN Imputation")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        # TODO:
        # fdl.label("Directory")
        # fdl.entry(name="directory", text=""),
        fdl.label("Imputed Columns")
        fdl.combobox(name="imp_cols", options=columns, row=2)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=3)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        columns = node_detail_form.get_chosen_value_by_name("imp_cols", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, columns, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        columns = params["imp_cols"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, columns, new_var_name)

    def debug(self, df_entry: pd.DataFrame, columns: List[str], new_var_name: str):
        flog.debug("APPLY KNN IMPUTATION")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS = {columns}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, columns: List[str]):
        if not isinstance(columns, list):
            columns = [columns]

        return columns

    def direct_execute(self, df_entry: pd.DataFrame, imp_cols: List[str], new_var_name: str, *args):
        self.debug(df_entry, imp_cols, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        imp_cols = self.parse_input(imp_cols)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("imp_cols", imp_cols)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = KNNImputation().fit_transform(inp("df_entry"), inp("imp_cols"))

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["imp_cols"]["value"] = self.parse_input(node_detail_form.node_params["imp_cols"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports


class ImputationHandler(AbstractFunctionHandler):
    """
    Apply numeric or categorical imputation on selected column.
    """
    def __init__(self):
        super().__init__()
        self.icon_type = 'Imputation'
        self.fn_name = 'Imputation'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Imputed Column", name="imp_cols", 
                      description="Choose in which columns values should be imputed."
                      )
        self.docs.add_parameter_table_row(title="Impute choice", name="imputation", 
                      description="Choose how the values should be generated. Either choose function (or zero) from combobox, or write value in entry."
                      )
        self.docs.add_parameter_table_row(title="New variable", name="new_var_name", 
                      description="A name for the new Dataframe variable.", 
                      typ="String", example="'imputation_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])
        impute_options = ["mean", "median", "0", "max", "min", "sum"]

        fdl = FormDictList(docs=self.docs)
        fdl.label("Apply Imputation")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Imputed Columns")
        fdl.combobox(name="columns", options=columns, row=2)
        fdl.label("Impute choice")
        fdl.comboentry(name="imputation", text="", options=impute_options, row=3)
        # TODO: ADD CHOICE TO IMPUTE COLUMNS EITHER SEPARATELY OR TOGETHER
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=4)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        columns = node_detail_form.get_chosen_value_by_name("columns", variable_handler)
        imput_choice = node_detail_form.get_chosen_value_by_name("imputation", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, columns, imput_choice, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        columns = params["columns"]
        imput_choice = params["imputation"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, columns, imput_choice, new_var_name)

    def debug(self, df_entry: pd.DataFrame, columns: List[str], imput_choice: List[str], new_var_name: str):
        flog.debug("APPLY IMPUTATION")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS = {columns}")
        flog.debug(f"IMPUTATION CHOICE = {imput_choice}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, imput_choice: List[str]):
        # impute_options = {"mean": mean,
        #                   "median": median,
        #                   "0": 0,
        #                   "max": max,
        #                   "min" : min,
        #                   "sum"" sum"}
        # if imput_choice:
        #     imput_choice = imput_choice[0]
        #
        # imput_choice = impute_options.get(imput_choice, 0)
        if imput_choice:
            imput_choice = imput_choice[0]
        else:
            imput_choice = 0

        return imput_choice

    def direct_execute(self, df_entry: pd.DataFrame, columns: List[str], imput_choice: List[str], new_var_name: str,
                       *args):
        self.debug(df_entry, columns, imput_choice, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        imput_choice: Union[str, int] = self.parse_input(imput_choice)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("imp_cols", columns)
        inp.assign("imputation", imput_choice)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        imput_mapping = {}
        clean_data = KNNImputation().clean_data(inp("df_entry"))
        if not inp("imp_cols"):
            columns = clean_data.columns.to_list()
        else:
            columns = [column for column in inp("imp_cols") if column in clean_data]

        for column in columns:
            if inp("imputation").isdigit():
                imput_mapping[column] = int(inp("imputation"))
            else:
                try:
                    imput_mapping[column] = float(inp("imputation"))
                except ValueError:
                    imput_mapping[column] = clean_data[column].agg(inp("imputation"))
            if pd.api.types.is_integer_dtype(clean_data[column].dtype):
                imput_mapping[column] = round(imput_mapping[column])

        clean_data = clean_data.fillna(value=imput_mapping)
        df_new = inp("df_entry").copy()
        df_new.update(clean_data)

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["imputation"]["value"] = \
            self.parse_input(node_detail_form.node_params["imputation"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports


class AggregateDataHandler(AbstractFunctionHandler):
    """
    Group (not required) data and execute numerical and/or categorical aggregations on given column(s)
    """
    def __init__(self):
        self.icon_type = 'AggregateGroupedData'
        self.fn_name = 'Aggregate Grouped Data'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Dataframe", name="df_entry", 
                      description="Dataframe variable rectangle.", 
                      typ="Dataframe")
        self.docs.add_parameter_table_row(title="Columns to Group by", name="columns_group", 
                      description="Choose columns on which group by should be executed."
                      )
        self.docs.add_parameter_table_row(title="Columns to Aggregate", name="columns_aggr", 
                      description="Choose columns on which group by should be executed."
                      )
        self.docs.add_parameter_table_row(title="Numeric Aggregations", name="num_aggr", 
                      description="Choose numerical aggregations that should be executed on numerical columns - **sum, mean, median, max, min, count, mode**."
                      )
        self.docs.add_parameter_table_row(title="Categorical Aggregations", name="categ_aggr", 
                      description="Choose categorical aggregations that should be executed on categorical columns - **mode**."
                      )
        self.docs.add_parameter_table_row(title="New variable", name="new_var_name", 
                      description="A name for the new Dataframe variable.", 
                      typ="String", example="'aggregated_df'")

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])
        numeric_aggregations = ['sum', 'mean', 'median', 'max', 'min', 'count', 'mode']
        category_aggregations = ['count', 'mode']

        fdl = FormDictList(docs=self.docs)
        fdl.label("Aggregate Grouped Data")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Columns to Group by")
        fdl.combobox(name="columns_group", options=columns, multiselect_indices={}, row=2)
        fdl.label("Columns to Aggregate")
        fdl.combobox(name="columns_aggr", options=columns, multiselect_indices={}, row=3)
        fdl.label("Numeric Aggregations")
        fdl.combobox(name="num_aggr", options=numeric_aggregations, multiselect_indices={}, row=4)
        fdl.label("Categorical Aggregations")
        fdl.combobox(name="categ_aggr", options=category_aggregations, multiselect_indices={}, row=5)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=6)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        columns_group = params["columns_group"]
        columns_aggr = params["columns_aggr"]
        num_aggr = params["num_aggr"]
        categ_aggr = params["categ_aggr"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, columns_group, columns_aggr, num_aggr, categ_aggr, new_var_name)

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        columns_group = node_detail_form.get_chosen_value_by_name("columns_group", variable_handler)
        columns_aggr = node_detail_form.get_chosen_value_by_name("columns_aggr", variable_handler)
        num_aggr = node_detail_form.get_chosen_value_by_name("num_aggr", variable_handler)
        categ_aggr = node_detail_form.get_chosen_value_by_name("categ_aggr", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, columns_group, columns_aggr, num_aggr, categ_aggr, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def debug(self, df_entry, groupby_columns, aggregate_columns, numerical_aggregations, categorical_aggregations,
              new_var_name):
        flog.debug("Aggregate Grouped DATA")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"GROUPBY COLUMNS = {groupby_columns}")
        flog.debug(f"AGGREGATE COLUMNS = {aggregate_columns}")
        flog.debug(f"NUM AGGREGATIONS = {numerical_aggregations}")
        flog.debug(f"CATEGORICAL AGGREGATIONS = {categorical_aggregations}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, df_entry, groupby_columns, aggregate_columns, numerical_aggregations,
                    categorical_aggregations):
        # if no columns to aggregate are specified, use all columns
        if aggregate_columns == " ":
            aggregate_columns = df_entry.columns.to_list()
        if groupby_columns == " ":
            groupby_columns = []
        if numerical_aggregations == " ":
            numerical_aggregations = []
        if categorical_aggregations == " ":
            categorical_aggregations = []

        return aggregate_columns, groupby_columns, numerical_aggregations, categorical_aggregations

    def build_agg_dict(self, aggregations: List[str], valid_columns: List[str], is_to_be_grouped: bool):
        """
        Construct aggregation dictionary

        :param aggregations: Chosen aggregations
        :type aggregations: List of strings
        :param valid_columns: Chosen columns on which aggregations are to be executed
        :type valid_columns: List of strings
        :param is_to_be_grouped: Flag to determine whether dataframe is to be grouped
        :type is_to_be_grouped: Bool
        :return: Dictionary between column names and aggregations to be executed on them
        """
        # DO NOT DELETE:
        # pd.Series.mode can not be replaced with 'mode'
        # also mode returns series instead of scalar and has to be handled on its own
        # https://github.com/pandas-dev/pandas/issues/11562
        aggregation_dict = {'sum': 'sum', 'mean': 'mean', 'max': 'max', "median": 'median', 'min': 'min',
                            'count': 'count', 'mode': pd.Series.mode}
        agg_dict = {}
        dict_values = [aggregation_dict[j] for j in aggregations]

        for i in valid_columns:
            agg_dict.update({i: dict_values})

        return agg_dict

    def execute_groupby_and_agg(self, grouped_df, aggregations, valid_columns, groupby_columns):
        agg_dict = self.build_agg_dict(aggregations, valid_columns, bool(groupby_columns))

        result = grouped_df.agg(agg_dict)
        # aggregation without groupping has different format - 2 rows below change it so the output is similar
        if not bool(groupby_columns) and "mode" not in aggregations:
            result = result.unstack().to_frame().T

        # result of agg is usually multiindex, the row below "flattens" multiindex into single string names
        result.columns = ['_'.join(reversed(col)) for col in result.columns]

        return result

    def direct_execute(self, df_entry, groupby_columns, aggregate_columns, numerical_aggregations,
                       categorical_aggregations, new_var_name):
        # TODO: returns empty columns as well
        self.debug(df_entry, groupby_columns, aggregate_columns, numerical_aggregations, categorical_aggregations,
                   new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        aggregate_columns, groupby_columns, numerical_aggregations, categorical_aggregations = self.parse_input(
            df_entry, groupby_columns, aggregate_columns, numerical_aggregations,
            categorical_aggregations)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("columns_group", groupby_columns)
        inp.assign("columns_aggr", aggregate_columns)
        inp.assign("num_aggr", numerical_aggregations)
        inp.assign("categ_aggr", categorical_aggregations)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = pd.DataFrame()
        result_numeric = pd.DataFrame()
        result_categorical = pd.DataFrame()
        result_mode = pd.DataFrame()
        numerical_aggregations = inp("num_aggr")

        if inp("columns_group"):
            grouped_df = inp("df_entry").groupby(inp("columns_group"))
        else:
            grouped_df = inp("df_entry")

        # check if some numerical aggregations are wanted
        valid_numeric_columns = inp("df_entry")[inp("columns_aggr")].select_dtypes(include=np.number).columns.to_list()
        if numerical_aggregations and len(valid_numeric_columns) > 0:
            # mode aggregations has to be handled on its own and then joined together with others
            if "mode" in numerical_aggregations:
                numerical_aggregations = list(set(numerical_aggregations) - {"mode"})
                result_mode = self.execute_groupby_and_agg(grouped_df, ["mode"], valid_numeric_columns, inp("columns_group"))
            result_numeric = self.execute_groupby_and_agg(grouped_df, numerical_aggregations, valid_numeric_columns,
                                                          inp("columns_group"))

            if len(result_mode) > 0:
                result_numeric = result_numeric.join(result_mode, how='inner')

        valid_columns_cat = inp("df_entry")[inp("columns_aggr")].select_dtypes(exclude=np.number).columns.to_list()
        if inp("categ_aggr") and len(valid_columns_cat) > 0:
            result_categorical = self.execute_groupby_and_agg(grouped_df, inp("categ_aggr"), valid_columns_cat,
                                                              inp("columns_group"))

        if len(result_categorical) > 0 and len(result_numeric) > 0:
            df_new = result_numeric.join(result_categorical, how='inner')
        elif len(result_numeric) > 0:
            df_new = result_numeric
        elif len(result_categorical) > 0:
            df_new = result_categorical

        if inp("columns_group") and len(df_new) > 0:
            df_new.reset_index(inplace=True)

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["columns_aggr"]["value"], node_detail_form.node_params["columns_group"]["value"],\
            node_detail_form.node_params["num_aggr"]["value"], node_detail_form.node_params["categ_aggr"]["value"]= \
            self.parse_input(node_detail_form.node_params["df_entry"]["variable"], node_detail_form.node_params["columns_group"]["value"], node_detail_form.node_params["columns_aggr"]["value"],
                             node_detail_form.node_params["num_aggr"]["value"], node_detail_form.node_params["categ_aggr"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = ["import numpy as np",
                   "import pandas as pd",
                   self.export_internal_function(self.build_agg_dict),
                   self.export_internal_function(self.execute_groupby_and_agg)]
        return imports


class MathOperationHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'MathOperation'
        self.fn_name = 'Math Operation'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList()
        fdl.label('Math Operation')
        fdl.label("DataFrame")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Mode")
        fdl.combobox(name="mode", options=["+", "-", "*", "/"], default="+", row=2)
        fdl.label("Column 1")
        fdl.combobox(name="first_column", options=columns, row=3)
        fdl.label("Column 2")
        fdl.combobox(name="second_column", options=columns, row=4)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=5)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        mode = node_detail_form.get_chosen_value_by_name("mode", variable_handler)
        first_column = node_detail_form.get_chosen_value_by_name("first_column", variable_handler)
        second_column = node_detail_form.get_chosen_value_by_name("second_column", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, mode, first_column, second_column, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        mode = params["mode"]
        first_column = params["first_column"]
        second_column = params["second_column"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, mode, first_column, second_column, new_var_name)

    def debug(self, df_entry, mode, first_column, second_column: List[str], new_var_name):
        flog.debug('Math Operation')
        flog.debug(f"DF1 = {df_entry}")
        flog.debug(f"MODE = {mode}")
        flog.debug(f"FIRST COLUMN = {first_column}")
        flog.debug(f"OTHER COLUMNS = {second_column}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, second_column: List[str]):
        if second_column:
            second_column = second_column[0]

        return second_column

    def direct_execute(self, df_entry, mode, first_column_name, second_column_name: List[str], new_var_name):
        # TODO: CHECKBOX TO CREATE NEW DF OR ADD NEW COLUMN TO OLD ONE
        # TODO: COLUMN NAME OPTIONAL
        self.debug(df_entry, mode, first_column_name, second_column_name, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        second_column_name: str = self.parse_input(second_column_name)

        all_required_fields_filled = mode and first_column_name and second_column_name
        if not all_required_fields_filled:
            raise SoftPipelineError("Some required arguments are missing.")
        
        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("mode", mode)
        inp.assign("first_column", first_column_name)
        inp.assign("second_column", second_column_name)

        result = self.input_execute(inp)

        # TODO: enable multiple columns when combobox enables entry input
        # if inp("mode") in ["+", "-"]:
        #     second_column = inp("df_entry")[inp("second_column_name")].sum(axis=1)
        # elif inp("mode") in ["*", "/"]:
        #     second_column = inp("df_entry")[inp("second_column_name")].product(axis=1)
        #
        # result = function(first_column, second_column)

        variable_handler.new_variable(new_var_name, result)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        math_function_dict = {
            "+": lambda x, y: x + y,
            "-": lambda x, y: x - y,
            "*": lambda x, y: x * y,
            "/": lambda x, y: x / y
        }

        function = math_function_dict[inp("mode")]
        first_column = inp("df_entry")[inp("first_column")]
        try:
            second_column = cast_str_to_numeric(inp("second_column"))
        except:
            second_column = inp("df_entry")[inp("second_column")]

        df_new = function(first_column, second_column).to_frame()

        return df_new

    def export_code(self, node_detail_form):
        
        if "second_column_name" in node_detail_form.node_params:
            node_detail_form.node_params["second_column"] = node_detail_form.node_params["second_column_name"]
            node_detail_form.node_params.pop("second_column_name", None)
        if "first_column_name" in node_detail_form.node_params:
            node_detail_form.node_params["first_column"] = node_detail_form.node_params["first_column_name"]
            node_detail_form.node_params.pop("first_column_name", None)

        node_detail_form.node_params["second_column"]["value"] = self.parse_input(node_detail_form.node_params["second_column"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports


class FindJoinColumnHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'FindJoinColumn'
        self.fn_name = 'Find Join Column'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        fdl = FormDictList()
        fdl.label('Find Join Column')
        fdl.label("Main DataFrame")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Additional DF")
        fdl.entry(name="df_entry2", text="", input_types=["DataFrame"], required=True, row=2)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=3)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        df_entry2 = node_detail_form.get_chosen_value_by_name("df_entry2", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, df_entry2, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        df_entry2 = params["df_entry2"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, df_entry2, new_var_name)

    def debug(self, df_entry, df_entry2, new_var_name):
        flog.debug('Find Join Column')
        flog.debug(f"DF1 = {df_entry.head()}")
        flog.debug(f"DF2 = {df_entry2.head()}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def get_category_columns(self, df_column_categories, data_type):
        return [column_name for column_name, column_prediction in df_column_categories.items()
                if column_prediction.predicted_category == data_type]

    def direct_execute(self, df_entry, df_entry2, new_var_name):
        # TODO: minimal_overlap could be user input
        self.debug(df_entry, df_entry2, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame) or not isinstance(df_entry2, pd.DataFrame):
            raise SoftPipelineError("Both 'Main Dataframe' and 'Additional DF' argument must be of type 'DataFrame'.")

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("df_entry2", df_entry2)
        inp.assign("dataframe_column_category_predictions", variable_handler.dataframe_column_category_predictions)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = pd.DataFrame()
        all_df_categorized = all(df.attrs["name"] in inp("dataframe_column_category_predictions")
                                 for df in [inp("df_entry"), inp("df_entry2")])
        if all_df_categorized:
            df1_column_categories = inp("dataframe_column_category_predictions")[inp("df_entry").attrs["name"]]
            df2_column_categories = inp("dataframe_column_category_predictions")[inp("df_entry2").attrs["name"]]

            for data_type in FL_DATA_TYPES:
                # get all columns evaluated with `data_type` category
                df1_columns = self.get_category_columns(df1_column_categories, data_type)
                df2_columns = self.get_category_columns(df2_column_categories, data_type)

                if df1_columns and df2_columns:
                    # generate all possible df1 x df2 join combinations
                    combinations = [(x, y) for x in df1_columns for y in df2_columns]
                    possible_joins = {}

                    for column_name1, column_name2 in combinations:
                        # compute column name string similarity
                        column_name_similarity = max(get_str_cosine_similarity(column_name1, column_name2), 0.1)

                        # compute percentual columns overlap in both directions
                        perc1 = inp("df_entry")[column_name1].isin(inp("df_entry2")[column_name2]).mean()
                        perc2 = inp("df_entry2")[column_name2].isin(inp("df_entry")[column_name1]).mean()

                        minimal_overlap = 0.4
                        # add record if there is good overlap in both directions
                        if perc1 > minimal_overlap and perc2 > minimal_overlap:
                            poss_join = {}
                            poss_join["Main DF column"] = column_name1
                            poss_join["Additional DF column"] = column_name2
                            poss_join["A to B similarity"] = perc1
                            poss_join["B to A similarity"] = perc2
                            poss_join["column names similarity"] = column_name_similarity
                            poss_join["Table A name in column B name"] = int(
                                inp("df_entry").attrs["name"].split(".")[-1] in column_name2)
                            poss_join["Table B name in column A name"] = int(
                                inp("df_entry2").attrs["name"].split(".")[-1] in column_name1)
                            possible_joins[f"{column_name1}, {column_name2}"] = poss_join

                    # preprocess resulting dataframe
                    df_new = pd.DataFrame.from_dict(possible_joins, orient='index').reset_index(drop=True)
                    df_new.rename(columns={"index": "possible join"})
                    sum_evaluations = df_new.iloc[:, 1:].sum(axis=1)
                    df_new = df_new.iloc[sum_evaluations.sort_values(ascending=False).index]

        return df_new

    def export_imports(self, *args):
        imports = [self.export_internal_function(self.get_category_columns),
                   self.export_internal_function(get_str_cosine_similarity)]
        return imports


class RoundToHigherFrequencyHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'RTHF'
        self.fn_name = 'Round To Higher Frequency'

        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])
        round_type = ["Fill forward", "Fill backward", "Fill nearest"]

        fdl = FormDictList()
        fdl.label("Timeseries Round To Higher Frequency")
        fdl.label("DataFrame")
        fdl.entry(name="df_entry", text="", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column with values")
        fdl.combobox(name="round_column", options=columns, row=2)
        fdl.label("Its low frequency indices")
        fdl.combobox(name="lower_freq_col", options=columns, row=3)
        fdl.label("DataFrame 2")
        fdl.entry(name="df_entry2", text="", input_types=["DataFrame"], row=4)
        fdl.label("Desired higher frequency indices")
        fdl.comboentry(name="higher_freq_col", text="", options=columns, row=5)
        fdl.label("Fill mode")
        fdl.combobox(name="round_type", options=round_type, row=6)
        fdl.label("New column name")
        fdl.entry(name="new_colname", text="", required=True, row=7)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=8)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column = node_detail_form.get_chosen_value_by_name("round_column", variable_handler)
        initial_freq = node_detail_form.get_chosen_value_by_name("lower_freq_col", variable_handler)
        df_entry2 = node_detail_form.get_chosen_value_by_name("df_entry2", variable_handler)
        final_freq = node_detail_form.get_chosen_value_by_name("higher_freq_col", variable_handler)
        round_type = node_detail_form.get_chosen_value_by_name("round_type", variable_handler)
        new_colname = node_detail_form.get_chosen_value_by_name("new_colname", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column, initial_freq, df_entry2, final_freq, round_type, new_colname,
                            new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column = params["round_column"]
        initial_freq = params["lower_freq_col"]
        df_entry2 = params["df_entry2"]
        final_freq = params["higher_freq_col"]
        round_type = params["round_type"]
        new_colname = params["new_colname"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, column, initial_freq, df_entry2, final_freq, round_type, new_colname,
                            new_var_name)

    def debug(self, df_entry, column, initial_freq, df_entry2, final_freq: List[str], round_type, new_colname,
              new_var_name):
        flog.debug("APPLY FREQUENCY ROUND")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMN = {column}")
        flog.debug(f"INIT FREQ = {initial_freq}")
        flog.debug(f"DF2 = {df_entry2}")
        flog.debug(f"FINAL FREQ = {final_freq}")
        flog.debug(f"ROUND TYPE = {round_type}")
        flog.debug(f"NEW COL = {new_colname}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def direct_execute(self, df_entry, column, initial_freq, df_entry2, final_freq: List[str], round_type, new_colname,
                       new_var_name):
        self.debug(df_entry, column, initial_freq, df_entry2, final_freq, round_type, new_colname, new_var_name)
        final_freq: str = final_freq[0]

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("round_column", column)
        inp.assign("lower_freq_col", initial_freq)
        inp.assign("df_entry2", df_entry2)
        inp.assign("higher_freq_col", final_freq)
        inp.assign("round_type", round_type)
        inp.assign("new_colname", new_colname)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = round_to_higher_frequency(inp("df_entry").copy(), inp("round_column"), inp("lower_freq_col"), inp("df_entry2"), inp("higher_freq_col"),
                                           inp("round_type"), inp("new_colname"))

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["higher_freq_col"]["value"] = node_detail_form.node_params["higher_freq_col"]["value"][0]

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = ["import pandas as pd",
                   self.export_internal_function(round_to_higher_frequency)]
        return imports


class CategorizeColumnHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'CategorizeColumn'
        self.fn_name = 'Categorize Column'

        # TODO: FILL IN
        # self.code_import_patterns = ['drop']
        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList()
        fdl.label("Categorize Column")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text=" ", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column")
        fdl.combobox(name="column_name", options=columns, row=2)
        fdl.label("Separator")
        fdl.comboentry(name="separator", text="",
                       options=["tab", "semicolon", "semicolon and space", "comma", "comma and space", "space"], row=3)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column_name = node_detail_form.get_chosen_value_by_name("column_name", variable_handler)
        separator = node_detail_form.get_chosen_value_by_name("separator", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, column_name, separator, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column_name = params["column_name"]
        separator = params["separator"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, column_name, separator, new_var_name)

    def debug(self, df_entry: pd.DataFrame, column_name: List[str], separator: List[str], new_var_name: str):
        flog.debug("APPLY categorize COLUMN")
        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS = {column_name}")
        flog.debug(f"separator = {separator}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, column_name: List[str], separator: List[str]):
        separator_dict = {
            "tab": "\t",
            "semicolon": ";",
            "semicolon and space": "; ",
            "comma": ",",
            "comma and space": ", ",
            "space": " "
        }

        if column_name:
            column_name = column_name[0]

        separator = separator[0]
        if separator in separator_dict:
            separator = separator_dict[separator]

        return column_name, separator

    def direct_execute(self, df_entry: pd.DataFrame, column_name: List[str], separator: List[str], new_var_name: str,
                       *args):
        self.debug(df_entry, column_name, separator, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")
        
        column_name: str
        separator: str
        column_name, separator = self.parse_input(column_name, separator)

        inp = Input()
        inp.assign("df_entry", df_entry)
        inp.assign("column_name", column_name)
        inp.assign("separator", separator)

        # TODO: should deal with not empty data only
        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            variable_handler.new_variable(new_var_name, df_entry.copy())
            raise SoftPipelineError("Unexpected internal error occured during execution.") from e

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        not_empty_data = inp("df_entry").loc[~inp("df_entry")[inp("column_name")].isna(), inp("column_name")]
        dummies = not_empty_data.astype(str).str.get_dummies(sep=inp("separator"))
        dummies.columns = [inp("column_name") + " " + column for column in dummies.columns]
        df_new = inp("df_entry").copy()
        df_new[dummies.columns] = dummies
        col_index = inp("df_entry").columns.get_loc(inp("column_name"))
        reordered_columns = df_new.columns[:col_index].tolist() + dummies.columns.tolist() + df_new.columns[
                                                                                             col_index + 1:len(
                                                                                                 df_new.columns) - len(
                                                                                                 dummies.columns)].tolist()
        df_new = df_new[reordered_columns]

        return df_new

    def export_code(self, node_detail_form):
        node_detail_form.node_params["column_name"]["value"], node_detail_form.node_params["separator"]["value"] = \
            self.parse_input(node_detail_form.node_params["column_name"]["value"], node_detail_form.node_params["separator"]["value"])

        code = self.export_code_with_node_params(node_detail_form.node_params)

        return code

    def export_imports(self, *args):
        imports = []
        return imports

class SimilarityMatchingHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'SimilarityMatching'
        self.fn_name = 'Similarity Matching'
        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning


    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        """
        to create form_dict_list when image becomes available
        """
        options = {} if options is None else options
        columns = options.get('columns', [])

        fdl = FormDictList()
        fdl.label("Similarity Matching")
        fdl.label("Similarity score for input")
        
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=2)
        fdl.label("Reference record index")
        fdl.entry(name="reference_record_index", text="0", row=3, input_types=["int"])
        
        fdl.label("Categorical columns")
        fdl.combobox(name="categorical_columns", options=columns, multiselect_indices={}, row=4)
        fdl.label("Text columns")
        fdl.combobox(name="text_columns", options=columns, multiselect_indices={}, row=5)
        fdl.label("Numerical columns")
        fdl.combobox(name="numerical_columns", options=columns, multiselect_indices={}, row=6)
        fdl.label("New column")
        fdl.entry(name="new_col_name", text="", input_types=["str"], row=7)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=8)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        reference_record_index = node_detail_form.get_chosen_value_by_name("reference_record_index", variable_handler)
        categorical_columns = node_detail_form.get_chosen_value_by_name("categorical_columns", variable_handler)
        text_columns = node_detail_form.get_chosen_value_by_name("text_columns", variable_handler)
        numerical_columns = node_detail_form.get_chosen_value_by_name("numerical_columns", variable_handler)
        new_col_name = node_detail_form.get_chosen_value_by_name("new_col_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(df_entry, reference_record_index, categorical_columns, text_columns, numerical_columns, new_col_name, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        
        df_entry = params["df_entry"]
        reference_record_index = params["reference_record_index"]
        categorical_columns = params["categorical_columns"]
        text_columns = params["text_columns"]
        numerical_columns = params["numerical_columns"]
        new_col_name = params["new_col_name"]
        new_var_name = params["new_var_name"]
        self.direct_execute(df_entry, reference_record_index, categorical_columns, text_columns, numerical_columns, new_col_name, new_var_name)

    # def debug(self, df_entry: Union[str, pd.DataFrame], column: str, split_on, select_index, keep_old: bool,
    #           new_col_name: str, new_var_name: str):
    #     flog.debug("APPLY DF JOIN")
    #     flog.debug(f"DF = {df_entry}")
    #     flog.debug(f"COLUMN = {column}")
    #     flog.debug(f"SPLIT ON = {split_on}")
    #     flog.debug(f"SELECT INDEX = {select_index}")
    #     flog.debug(f"KEEP OLD = {keep_old}")
    #     flog.debug(f"NEW COL = {new_col_name}")
    #     flog.debug(f"NEW VAR = {new_var_name}")

    def parse_input(self, select_index, new_col_name, column):
        select_index = int(select_index)
        # define augment column name if not available
        if not new_col_name:
            new_col_name = f"{column}_{select_index}"

        return select_index, new_col_name

    def direct_execute(self, df_entry, reference_record_index, categorical_columns, text_columns, numerical_columns, new_col_name, new_var_name, *args):
        """
        Split values in a given column by a given "split_on" string and take {select_index} th value or None
        Store new value into new_col_name column
        If keep old is True, keep old column, otherwise delete it
        """
        from thefuzz import fuzz
        #auxilliary function      
        def calculate_final_text_score(s1,s2):
            ratio=fuzz.ratio(s1,s2)
            partial_ratio=fuzz.partial_ratio(s1,s2)
            sort_ratio=fuzz.token_sort_ratio(s1,s2)
            set_ratio=fuzz.token_set_ratio(s1,s2)
            suma=ratio+partial_ratio+sort_ratio+set_ratio
            print(suma,":",ratio,partial_ratio,sort_ratio,set_ratio)
            return(suma)

        print("AAAAAAAAAAAAAAAAAAAA")        
        print(reference_record_index)
        print(categorical_columns)
        
        print("AAAAAAAAAAAAAAAAAAAA")
        
        similarity_dict={}
        for text_column in text_columns:
            df_text=list(df_entry[text_column])
            print("DF_TEXT",df_text)
            for i,text in enumerate(df_text):
                
                similarity_dict[i]=calculate_final_text_score(df_entry.iloc[int(reference_record_index)][text_column],text)
                print(similarity_dict[i],text)
        
        for categorical_column in categorical_columns:
            df_categorical=list(df_entry[categorical_column])
            print("DF_CATEGORY",df_categorical)
            for i,categorical in enumerate(df_categorical):
                
                similarity_dict[i]+=int(df_categorical[i]==df_entry.iloc[int(reference_record_index)][categorical_column])*100
                print(similarity_dict[i],text)
        
        similarity_dict={k:int(round(v/(len(categorical_columns)+4*len(text_columns)))) for k,v in similarity_dict.items()}
            
        
        
        # # df_names=list(df["name"])
        # # df_brands=list(df["brand"])
        # # df_categories=list(df["category"])
        # similarity_dict={}

        # for i,name in enumerate(df_names):
            
        #     similarity_dict[name]=calculate_final_text_score(first_item["name"],name)
        #     #print(i,name,similarity_dict[name])
            
            
        #     category_match=int(df_categories[i]==first_item["category"])*200
        #     if category_match>0:
        #         print("CATEGORY MATCH!",df_categories[i]) #df.iloc[i],
        #     similarity_dict[name]+=category_match
            
            
        #     brand_match=int(df_brands[i]==first_item["brand"])*100
        #     if brand_match>0:
        #         print("BRAND MATCH!",df_brands[i],df_names[i]) #df.iloc[i],
        #     similarity_dict[name]+=brand_match
            
            
            
            
            
            
        #import dbhydra.dbhydra_core as dh

        #name_url_dict=dh.df_to_dict(df, "name", "url")

        result_dict=dict(reversed(sorted(similarity_dict.items(), key=lambda x: x[1])))
        #new_df=dh.dict_to_df(result, "name", "score")
        #new_df["url"]=new_df["name"].map(name_url_dict)

        df_new=df_entry.copy(deep=True)
        df_new[new_col_name]=df_new.index.map(result_dict)
        df_new=df_new.sort_values(by=new_col_name,ascending=False)
        
        variable_handler.new_variable(new_var_name, df_new)
        # variable_handler.update_data_in_variable_explorer(glc)

        #self.debug(df_entry, column, split_on, select_index, keep_old, new_col_name, new_var_name)

        # if len(df_entry) > 0 and column and split_on and len(select_index) > 0:
        #     select_index, new_col_name = self.parse_input(select_index, new_col_name, column)

        #     inp = Input()
        #     inp.assign("df_entry", df_entry)
        #     inp.assign("column", column)
        #     inp.assign("split_on", split_on)
        #     inp.assign("select_index", select_index)
        #     inp.assign("keep_old", keep_old)
        #     inp.assign("new_col_name", new_col_name)

        #     try:
        #         df_new = self.input_execute(inp)
        #     except AttributeError as e:
        #         df_new = pd.DataFrame()
        #         flog.error(f"{e}")
        #     except ValueError as e:
        #         df_new = pd.DataFrame()
        #         flog.error(f"{e}")
        #     except Exception as e:
        #         df_new = df_entry.copy()
        #         flog.error(f"Undefined error ({e}) occurred")

   

    def export_code(self, *args):
        #TO BE IMPLEMENTED
        code = ""

        return code

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return imports




class CleanDataHandler(AbstractFunctionHandler):
    def __init__(self):
        super().__init__()
        self.icon_type = 'CleanData'
        self.fn_name = 'Clean Data'
        self.digit_extract_regex = r"(?P<digit>[+-]?\d+(?:\s*\d+)*(?:[\,\.](?:\d+))?)"
        self.digit_replace_regex = r"(?:[+-]?\d+(?:\s*\d+)*(?:[\,\.](?:\d+))?)"

        # TODO: FILL IN
        # self.code_import_patterns = ['drop']
        self.type_category = ntcm.categories.cleaning
        self.docs_category = DocsCategories.cleaning

    def make_form_dict_list(self, *args, options: Optional[dict] = None, node_detail_form=None) -> FormDictList:
        fdl = FormDictList()
        fdl.label("Clean Data")
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text=" ", input_types=["DataFrame"], required=True, row=1)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=2)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def debug(self, df_entry: pd.DataFrame, new_var_name: str):
        flog.debug("APPLY CLEAN DATA")
        flog.debug(f"DF = {df_entry.head()}")
        flog.debug(f"NEW VAR = {new_var_name}")

    def get_redundant_words(self, data, column):
        most_common_words = Counter(
            [c for b in data.apply(lambda x: list(set([x.strip(',') for x in str(x).split()]))).values for c in
             b]).most_common(10)
        id = 0
        guaranteed_words = []
        while id < len(most_common_words) and most_common_words[id][1] == len(data):
            print(column, most_common_words[id], most_common_words[id][1] / len(data))
            guaranteed_words.append(most_common_words[id][0])
            id += 1

        # sorted according to descending name length
        # example: if `in` is replaced earlier than `inside`, then `side` part will be left in the cell
        guaranteed_words = sorted(guaranteed_words, key=lambda x: len(x), reverse=True)
        return guaranteed_words

    def all_rows_containing_dash(self, data):
        return data.str.contains("-").mean() == 1.

    # TODO: max categories as user input
    def rows_containing_multiple_categorical_data(self, data_with_digits_placeholders, dummies, max_categories=20):
        return data_with_digits_placeholders.str.contains(",").mean() > 0. and len(dummies.columns) < max_categories

    def get_common_prefix(self, string1, string2):
        match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))

        print(match)  # -> Match(a=0, b=15, size=9)
        print(string1[match.a:match.a + match.size])  # -> apple pie
        print(string2[match.b:match.b + match.size])  # -> apple pie

        new_col_name = string1[match.a:match.a + match.size]

        return new_col_name

    def direct_execute(self, df_entry: pd.DataFrame, new_var_name: str, *args):
        self.debug(df_entry, new_var_name)
        
        if not isinstance(df_entry, pd.DataFrame):
            raise CriticalPipelineError("'Dataframe' argument must be of type 'DataFrame'.")

        df_new = df_entry.copy()
        try:
            for column in df_new.columns:
                not_empty_data = df_new.loc[~df_new[column].isna(), column]
                average_words_per_cell = not_empty_data.astype(str).str.split(' ').apply(lambda x: len(x)).mean()
                split_column_names = []
                dummy_columns = []

                print()
                print("column:", column)
                print("average words per cell:", average_words_per_cell)

                if len(not_empty_data) > 0 and 1 < average_words_per_cell < 10:
                    if self.all_rows_containing_dash(not_empty_data):
                        # split data on dash to two column containing main information and description
                        # TODO: probably use strip as well
                        split_columns = not_empty_data.str.split("-", expand=True, n=2)
                        split_columns.columns = [column, column + " description"]
                        split_column_names = split_columns.columns

                        print("column contains dash, spliting column into", split_column_names)

                        # nejsem si jistej jestli umime reorderovat sloupecky
                        df_new[split_column_names] = split_columns
                        column_index = df_new.columns.get_loc(column)
                        # only one new column to be added (the second one has the same name) (thats why only 1 is substracted in the last operator)
                        reordered_columns = df_new.columns[
                                            :column_index].tolist() + split_column_names.tolist() + df_new.columns[
                                                                                                    column_index + 1:len(
                                                                                                        df_new.columns) - 1].tolist()
                        df_new = df_new[reordered_columns]
                        print("5 columns starting from inserted column",
                              df_new.columns[column_index:column_index + 5])
                    else:
                        # neprazdna data umime, replace asi taky
                        digits_replaced = not_empty_data.astype(str).replace(self.digit_replace_regex, "digit",
                                                                             regex=True)
                        # add dummy columns to dataset if the dummy column are unique (there are less than xy of them)

                        # generate dummy categorical columns
                        dummies = digits_replaced.str.get_dummies(sep=', ')
                        max_digit_columns = 20
                        if self.rows_containing_multiple_categorical_data(digits_replaced, dummies, max_digit_columns):
                            dummies.columns = [column + " " + col.strip() for col in dummies.columns.values]
                            dummy_columns = dummies.columns
                            print("column contains commas dividing categorical features", dummy_columns)

                            # extract all digits from column
                            digits = not_empty_data.astype(str).str.extractall(self.digit_extract_regex).unstack()

                            # collapse columns if multiindex is created
                            digits.columns = [' '.join(map(str, col)).strip() for col in digits.columns.values]

                            # get dummy columns with 'digit' in their name
                            # and update column `digit` values with corresponding digits
                            # use number instead od `digit`
                            digit_columns = dummy_columns[dummy_columns.str.contains("digit") is True]

                            if len(digit_columns) == 0:
                                # DO NOT REMOVE
                                # categorical data with no digits, for example House, Garden in one row
                                # no need to do anything more
                                pass
                            elif len(digit_columns) == digits.shape[1]:
                                print("inserted digits to df")
                                dummies[digit_columns] = digits
                            elif digits.shape[1] == 1 and len(digit_columns) > 1:
                                print("inserted one digits column into common name column")
                                new_col_name = self.get_common_prefix(digit_columns[0], digit_columns[1])

                                if new_col_name in digit_columns[:2]:
                                    dummies[new_col_name] = digits
                                    print("Added ", new_col_name, "to new columns")
                                else:
                                    stack = dummies[digit_columns].stack()
                                    stack[stack is True] = digits.iloc[:, 0].values
                                    dummies = stack.unstack()
                                    dummies[column + " description"] = df_entry[column]
                                dummy_columns = dummies.columns

                            elif digits.shape[1] > len(digit_columns):
                                # one column containing possibly more than one digit for example 230V, 400V
                                # the problem is that with digits replaced the algorithm creates only one `digit V` column
                                try:
                                    print("Only used categorical features without inserting digits")
                                    digits_not_replaced = not_empty_data.astype(str)
                                    dummies = digits_not_replaced.str.get_dummies(sep=', ')
                                    dummies.columns = [column + " " + col.strip() for col in dummies.columns.values]
                                    dummy_columns = dummies.columns
                                except Exception as e:
                                    print("CANT INSERT DIGITS TO DF")
                                    print(e)
                            elif len(dummies.columns) > 10 and digit_columns < 10:
                                stack = dummies[digit_columns].stack()
                                stack[stack is True] = digits.iloc[:, 0].values
                                dummies = stack.unstack()
                                dummies[column] = df_entry[column]
                                dummy_columns = dummies.columns

                            else:
                                print("CANT INSERT DIGITS TO DF")

                            # add dummy columns to dataset
                            df_new[dummy_columns] = dummies
                            column_index = df_new.columns.get_loc(column)
                            print(df_new.columns[column_index + 1:len(df_new.columns) - len(dummy_columns)])
                            print(column_index + 1, len(df_new.columns) - len(dummy_columns))
                            df_new = df_new[
                                df_new.columns[
                                :column_index].tolist() + dummy_columns.tolist() + df_new.columns[
                                                                                   column_index + 1:len(
                                                                                       df_new.columns) - len(
                                                                                       dummy_columns)].tolist()]
                            print(df_new.columns[column_index:column_index + 5])
                    print(df_new.columns.tolist())

                    if len(split_column_names) > 0:
                        columns = split_column_names
                    elif len(dummy_columns) > 0:
                        columns = dummy_columns
                    else:
                        columns = [column]

                    for column in columns:
                        # TODO: do we have to use NOT EMPTY DATA??????
                        not_empty_data = df_new.loc[~df_new[column].isna(), column]
                        print("temporary column", column)
                        # get 10 most common words in a column
                        guaranteed_words = self.get_redundant_words(not_empty_data, column)

                        if guaranteed_words:
                            print("the most frequent words we got rid of")
                            # remove repeating words in a cell
                            name_suffix = ""
                            for word in guaranteed_words:
                                print(word)
                                # umime
                                not_empty_data = not_empty_data.astype(str).str.replace(word, '', regex=False)
                                # if word == "000":
                                #     name_suffix = "k"
                                # if word in ["000 000", "000000"]:  # TODO: "000 000" may not be identifiable, as the words are split by a space
                                #     name_suffix = "M"

                            # remove trailing white spaces
                            # TODO: PROBABLY REFACTOR CONDITION ROW WISE
                            # nejsem si jistej jestli umime takhle stripovat
                            # if average_words_per_cell - len(guaranteed_words) == 1:
                            #     not_empty_data = not_empty_data.str.strip()
                            # else:
                            #     # TODO: I TADY DOST MOZNA MUZEME POUZIT STRIP, ABY NEZACINAL A NEKONCIL MEZEROU< TOHLE TO NERESI
                            not_empty_data = not_empty_data.str.strip()
                            not_empty_data = not_empty_data.replace(r'\s+', ' ', regex=True)

                            # merge / join - spis nahrazeni
                            df_new[column] = not_empty_data

                            new_column_name = column + " " + " ".join(guaranteed_words) + " " + name_suffix
                            print("and renamed column to:", new_column_name)
                            print("data looks like following")
                            print(df_new.loc[~df_new[column].isna(), column].head())
                            # TODO: REPLACE 000 with thousands
                            # umime
                            df_new = df_new.rename(columns={column: new_column_name})
                            # column = new_column_name
        except Exception as e:
            flog.error(f"Undefined error {e} occurred")

        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        new_var_name = params["new_var_name"]

        self.direct_execute(df_entry, new_var_name)

    def execute(self, node_detail_form):
        """
        Execution of the drop column transformation
        """
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(df_entry, new_var_name)

        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)


    def export_code(self, node_detail_form):
        # TODO: Implement

        code = f"""
        """

        return code

    def export_imports(self, *args):
        imports = []
        return imports


cleaning_handlers_dict = {
    'NewDataFrame': NewDataFrameHandler(),
    'DropColumn': DropColumnHandler(),
    'RenameColumn': RenameColumnHandler(),
    'CastColumnType': CastColumnTypeHandler(),
    'ExplodeColumn': ExplodeColumnHandler(),
    'ConstantColumn': ConstantColumnHandler(),
    'SelectColumns': SelectColumnsHandler(),
    'RemoveEmptyRows': RemoveEmptyRowsHandler(),
    'RemoveDuplicates': RemoveDuplicatesHandler(),
    'KNNImputation': KNNImputationHandler(),
    'Imputation': ImputationHandler(),
    'Outliers': OutliersHandler(),
    'Replace': ReplaceHandler(),
    'Search': SearchHandler(),
    'StripColumn': StripColumnHandler(),
    'FilterString': FilterHandler(),
    'SplitString': SplitColumnHandler(),
    'Sort': SortHandler(),
    'ColumnWiseShift': ColumnWiseShiftHandler(),
    "DifferenceData": DifferenceDataHandler(),
    'AnalyzeDataFrame': AnalyzeDataFrameHandler(),
    'Concatenate': ConcatHandler(),
    'Join': JoinHandler(),
    'AggregateGroupedData': AggregateDataHandler(),
    'MathOperation': MathOperationHandler(),
    'FindJoinColumn': FindJoinColumnHandler(),
    'RoundToHigherFrequency': RoundToHigherFrequencyHandler(),
    'CategorizeColumn': CategorizeColumnHandler(),
    'CleanData': CleanDataHandler(),
    "ExtractString": ExtractStringHandler(),
    "SimilarityMatching": SimilarityMatchingHandler()
}
