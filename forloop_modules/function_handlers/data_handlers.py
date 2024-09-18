import json
import os
import pandas as pd
import ast

from tkinter.filedialog import askopenfile, asksaveasfile
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO
from fastapi import HTTPException

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.globals.docs_categories import DocsCategories

from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input
from forloop_modules.function_handlers.variable_handlers import variable_handlers_dict

from forloop_modules.utils.pandas_operations import read_spreadsheet

import forloop_modules.function_handlers.auxilliary.forloop_code_eval as fce
from forloop_modules.globals.dbtables_loader_popups import get_tables, get_specific_table
from forloop_modules.function_handlers.auxilliary.data_types_validation import validate_input_data_types


####### PROBLEMATIC IMPORTS TODO: REFACTOR #######
#from src.gui.gui_layout_context import glc
####### PROBLEMATIC IMPORTS TODO: REFACTOR #######





class DataFrameHandler(AbstractFunctionHandler):
    icon_type = "DataFrame"
    fn_name = "DataFrame"
    type_category = ntcm.categories.data

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl=FormDictList()
        fdl.label(self.fn_name)

        return fdl

    def direct_execute(self):
        # def __new__(cls, *args, **kwargs):
        """Do nothing"""
        pass

    def export_code(self, node_detail_form):
        code = """
        """
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)


class LoadTxtFileHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadTxtFile"
        self.fn_name = "Load txt file"

        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Load .TXT file")
        fdl.label("File name:")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=1)
        fdl.label("Variable name:")
        fdl.entry(name="varname", text="", input_types=["str"], row=2)
        fdl.button(function=self.open_list_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name='lookup_txt_file')
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_list_file(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('Text files', '*.txt'), ('All files', '*')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        varname = node_detail_form.get_chosen_value_by_name("varname", variable_handler)

        self.direct_execute(filename, varname)

    def execute_with_params(self, params):
        filename = params["filename"]
        varname = params["varname"]

        self.direct_execute(filename, varname)

    def direct_execute(self, filename, varname):
        if filename:
            inp = Input()
            inp.assign("filename", filename)

            try:
                rows = self.input_execute(inp)

                # df = pd.DataFrame(rows, columns=["Data"])

                if not varname:
                    short_filename = filename.split("/")[-1]
                    new_variable_name = short_filename.replace(".txt", "")
                else:
                    new_variable_name = varname

                params = {"variable_name": new_variable_name, "variable_value": str(rows)}  # rows str(rows)
                variable_handlers_dict["NewVariable"].execute_with_params(params)
                #variable_handler.update_data_in_variable_explorer(glc)

            except Exception as e:
                flog.error(f"Undefined error {e} occurred")
        else:
            flog.warning("Load txt missing filename input")

    def input_execute(self, inp):
        with open(inp("filename"), "r", encoding="utf8") as f:
            rows = f.readlines()
            rows = [x.replace("\n", "") for x in rows]  # get rid of \n at the line endings

        return rows

    def export_code(self, node_detail_form):
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        new_variable_name = filename.split("/")[-1].replace(".txt", "")

        code = f"""
        with open({filename}, "r") as f:
            rows = f.readlines()
            rows = [x.replace("\n", "") for x in rows]  

        {new_variable_name} = rows
        """

        ## TODO: make it to show \n as a string ---> now it creates a new line
        return (code.format(filename='"' + filename + '"', new_variable_name=new_variable_name, new_line="\n"))

    def export_imports(self, *args):
        imports = []
        return (imports)


class LoadFileHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadFile"
        self.fn_name = "Load File"
        self.type_category=ntcm.categories.data
        self.docs_category = DocsCategories.data_sources
        self.code_import_patterns = []  # ["read_excel", "read_csv"]
 
    def make_form_dict_list(self, *args, options: dict={}, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File name:")
        fdl.entry(name="file_name", text="", category="arguments", required=True, input_types=["str", "File"],show_info=True, row=1)
        fdl.button(name="load_file", function=self.open_file, function_args=node_detail_form, text="Look up file", enforce_required=False, frontend_implementation=True)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Upload", focused=True)

        return fdl

    def open_file(self, node_detail_form):
        file = askopenfile(mode='r')

        if file is not None:
            filename = file.name

            params_dict = node_detail_form.assign_value_by_name(name='file_name', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)


    def execute(self, node_detail_form):
        file_name = node_detail_form.get_chosen_value_by_name("file_name", variable_handler)

        self.direct_execute(file_name)

    def execute_with_params(self, params):
        filename = params["file_name"]

        self.direct_execute(filename)
    
    def parse_input(self, file_name):
        file_name = "" if file_name is None else file_name
        file_name = "./tmp/" + file_name
        return file_name 

    def direct_execute(self, file_name):
        file_name = self.parse_input(file_name)

        if file_name and os.path.isfile(file_name):
            variable_handler.new_file(file_name)

        else:
            flog.error(f"Given path `{file_name}` is not a file")

    def export_code(self, node_detail_form):
        code = ""

        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


class LoadExcelHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadExcel"
        self.fn_name = "Load Excel"
        self.type_category=ntcm.categories.data
        self.docs_category = DocsCategories.data_sources
        self.code_import_patterns = ["read_excel", "read_csv"]
 
    def make_form_dict_list(self, *args, options: dict={}, node_detail_form=None):
        # files = []
        #     files = [file["file_name"] for file in options["available_files"] if file["file_name"].split(".")[-1] in ["xlsx", "csv"]]
        file_types = [('Spreadsheets', '*.csv *.xlsx'), ('Excel files', '*.xlsx'), ('CSV files', '*.csv')]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File name:")
        # fdl.combobox(name="file_name", options=files, row=1)
        fdl.entry(name="file_name", text="", category="arguments", type='file', file_types=file_types, required=True, row=1)
        fdl.label("Load as:")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], required=True, row=2)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", frontend_implementation=True, focused=True)

        return fdl

    def load_df(self, filename):
        try:
            df = read_spreadsheet(filename)[0]
        except FileNotFoundError as e:
            print("File was not found")
            df = None
        return (df)

    def highlight_data_grid_view_button(self):
        #Tutorial highlighting disabled because of frontend dependency
        #for nav_menu_item in glc.nav_menu.nav_menu_items:
        #    if nav_menu_item.text == "DATA GRID VIEW" and not glc.table1.visible:
        #        nav_menu_item.is_highlighted = True
        pass

    def open_xlsx_file(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('Spreadsheets', '*.csv *.xlsx'), ('Excel files', '*.xlsx'),
                                                ('CSV files', '*.csv')])

        if file is not None:
            filename = file.name

            params_dict = node_detail_form.assign_value_by_name(name='file_name', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)


    def execute(self, node_detail_form):
        file_name = node_detail_form.get_chosen_value_by_name("file_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        
        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(file_name, new_var_name)
        
        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

        # TODO: WONT WORK IF VARIABLE WITH NEW VAR NAME ALREADY EXISTS AND LOAD EXCEL FAILED
        # HAD TO BE CHANGED BECAUSE DIRECT EXECUTE DOES NOT RETURN THE VALUE
        if new_var_name in variable_handler.variables:
            result = variable_handler.variables[new_var_name].value
        else:
            result = pd.DataFrame()

        return result  # used as data in pipeline_function - processing pipeline

    def execute_with_params(self, params):
        filename = params["file_name"]
        new_var_name = params["new_var_name"]

        self.direct_execute(filename, new_var_name)

        # TODO: WONT WORK IF VARIABLE WITH NEW VAR NAME ALREADY EXISTS AND LOAD EXCEL FAILED
        # HAD TO BE CHANGED BECAUSE DIRECT EXECUTE DOES NOT RETURN THE VALUE
        if new_var_name in variable_handler.variables:
            result = variable_handler.variables[new_var_name].value
        else:
            result = pd.DataFrame()

        return result  # used as data in pipeline_function - processing pipeline
    
    def parse_input(self, file_name):
        file_name = "" if file_name is None else file_name
        file_name = "./tmp/" + file_name
        return file_name 

    def direct_execute(self, file_name, new_var_name):
        # file_name = self.parse_input(file_name)

        if file_name and os.path.isfile(file_name):

            inp = Input()
            inp.assign("file_name", file_name)

            try:
                df_new = self.input_execute(inp)
                print(f"df: {df_new.head()}")
            except Exception as e:
                df_new = pd.DataFrame()
                flog.error(f"Undefined error {e} occurred")

            new_var_name = variable_handler._set_up_unique_varname(new_var_name)
            fields = self.generate_shown_dataframe_option_field(new_var_name)

            response = ncrb.new_node(pos=[500, 300], typ="DataFrame", fields=fields)
            if response.status_code in [200, 201]:
                result = json.loads(response.content.decode('utf-8'))
                node_uid = result["uid"]

                df_new = validate_input_data_types(df_new)
                variable_handler.new_variable(new_var_name, df_new)
                
                ncrb.update_last_active_dataframe_node_uid(node_uid)
                
                self.highlight_data_grid_view_button() #TODO: when executing in pipeline, the df does not automatically load to data grid view
            else:
                raise HTTPException(status_code=response.status_code, detail="Error requesting new node from api")

        else:
            flog.error(f"Given path `{file_name}` is not a file")

    def input_execute(self, inp):
        df = self.load_df(inp("file_name"))
        return df

    def export_code(self, node_detail_form):
        file_name = node_detail_form.get_variable_name_or_input_value_by_element_name("file_name")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)
            
        pandas_method = "pd.read_excel" if file_name.endswith(".xlsx") else "pd.read_csv"
        
        code = f"""
        {new_var_name} = {pandas_method}({file_name})
        """

        return code

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return (imports)


class SaveExcelHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = "SaveExcel"
        self.fn_name = "Save Excel"

        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ['w+', 'a+']

        fdl = FormDictList()
        fdl.label("Save Excel/CSV")
        fdl.label("Value")
        fdl.entry(name="value", text="", input_types=["list", "dict", "DataFrame"], required=True, row=1)
        fdl.label("Filename")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=2)
        fdl.button(function=self.open_xlsx_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name='lookup_excel_file')
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_xlsx_file(self, node_detail_form):
        file = asksaveasfile(mode='w', filetypes=[('Excel files', '*.xlsx'), ('CSV files', '*.csv')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def direct_execute(self, value, filename):
        if filename:
            inp = Input()
            inp.assign("value", value)
            inp.assign("filename", filename)

            try:
                self.input_execute(inp)
            except Exception as e:
                flog.error(f"Undefined error {e} occurred")

    def input_execute(self, inp):
        filename = inp("filename")
        input_value = inp("value")

        if isinstance(input_value, str):
            try:
                flog.warning(f'Provided value "{input_value}" has type string -> trying to load variable value from variable explorer.')
                input_value = variable_handler.variables[input_value].value
                flog.warning(f'Found value of type {type(input_value)}.')
            except:
                flog.error(f'Could not find variable "{input_value}" in variable explorer.')

        if not (filename.endswith('.xlsx') or filename.endswith('.csv')):
            filename += '.xlsx'

        if isinstance(input_value, dict):
            value = pd.DataFrame(input_value, index=[0])
        elif isinstance(input_value, list) and (all([isinstance(x, dict) for x in input_value]) or all([isinstance(x, list) for x in input_value])):
            value = pd.DataFrame(input_value)
        elif isinstance(input_value, pd.DataFrame):
            value = input_value
        else:
            flog.warning(f'Wrong value type: {type(inp("value"))}')
            return

        if filename.endswith('.xlsx'):
            value.to_excel(filename, index=False)
        elif filename.endswith('.csv'):
            value.to_csv(filename, index=False)

    def execute_with_params(self, params):
        value = params["value"]
        filename = params["filename"]

        self.direct_execute(value, filename)

    def execute(self, node_detail_form):
        value = node_detail_form.get_chosen_value_by_name("value", variable_handler)
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)

        self.direct_execute(value, filename)

    def export_code(self, node_detail_form):
        code = f"""
        df.to_excel(filename)
        """

        return code

    def export_imports(self, *args):
        imports = ["import pandas"]
        return (imports)


class DfToListHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "DfToList"
        self.fn_name = "Dataframe To List"        
        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources


    def make_form_dict_list(self, *args, options=[], node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Dataframe")
        fdl.entry(name="df_entry", text="", category="instance_var", input_types=["DataFrame"], required=True, row=1)
        fdl.label("Column name")
        fdl.entry(name="column_name", text="", input_types=["str", "list"], required=True, row=2)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        df_entry = node_detail_form.get_chosen_value_by_name("df_entry", variable_handler)
        column_names = node_detail_form.get_chosen_value_by_name("column_name", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        flog.debug(f"DF = {df_entry}")
        flog.debug(f"COLUMNS = {column_names}")

        self.direct_execute(df_entry, column_names, new_var_name)

    def execute_with_params(self, params):
        df_entry = params["df_entry"]
        column_names = params["column_name"]
        new_var_name = params["new_var_name"]
        flog.debug(f"DF = {df_entry}")
        self.direct_execute(df_entry, column_names, new_var_name)

    def direct_execute(self, df_entry, column_name, new_var_name):
        # df_entry[column_name] = df_entry[column_name].fillna("")
        inp = Input()

        inp.assign("df_entry", df_entry)
        inp.assign("column_name", column_name)

        try:
            new_value = self.input_execute(inp)
        except KeyError as e:
            new_value = []
            flog.error(e)
        except Exception as e:
            new_value = []
            flog.error(f"Undefined Error: {e}")

        variable_handler.new_variable(new_var_name, new_value)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        column_names=inp("column_name").split(",")
        new_value = inp("df_entry")[column_names].values.tolist()
        return new_value

    def export_code(self, node_detail_form):
        column_name = node_detail_form.get_variable_name_or_input_value_by_element_name("column_name")
        df_entry = node_detail_form.get_variable_name_or_input_value_by_element_name("df_entry")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        columns = column_name.split(",")
        if len(columns) == 1:
            columns = columns[0] # if only 1 columns selected, take just its name

        code = f"""
        {new_var_name} = {df_entry}[{columns}] # make sure {df_entry} is defined before
        """

        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


class ListToDfHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "ListToDf"
        self.fn_name = "List To Dataframe"

        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, options=[], node_detail_form=None):
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("List")
        fdl.entry(name="list_entry", text="", category="instance_var", input_types=["list"], required=True, row=1)
        fdl.label("Column names")
        fdl.entry(name="column_names", text="", input_types=["list"], required=True, row=2)
        fdl.label("New variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        list_entry = node_detail_form.get_chosen_value_by_name("list_entry", variable_handler)
        column_names = node_detail_form.get_chosen_value_by_name("column_names", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        new_var_name = self.update_node_fields_with_shown_dataframe(node_detail_form, new_var_name)

        self.direct_execute(list_entry, column_names, new_var_name)
        # glc.last_active_dataframe_icon = image
        # variable_handler.last_active_dataframe_node_uid = item_detail_form.node_detail_form.node_uid
        ncrb.update_last_active_dataframe_node_uid(node_detail_form.node_uid)

    def execute_with_params(self, params):
        list_entry = params["list_entry"]
        column_names = params["column_names"]
        new_var_name = params["new_var_name"]

        self.direct_execute(list_entry, column_names, new_var_name)

    def debug(self, list_entry, varname, new_variable_name):
        flog.debug("List to df")
        flog.debug(f"list_entry = {list_entry}")
        flog.debug(f"column_names = {varname}")
        flog.debug(f"new_variable_name = {new_variable_name}")

    def direct_execute(self, list_entry, column_names, new_var_name):
        self.debug(list_entry, column_names, new_var_name)

        inp = Input()
        inp.assign("list_entry", list_entry)
        inp.assign("column_names", column_names)

        try:
            new_df = self.input_execute(inp)
        except Exception as e:
            new_df = pd.DataFrame()
            flog.error(f"Undefined Error: {e}")


        variable_handler.new_variable(new_var_name, new_df)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):

        #new_value = pd.DataFrame(inp("list_entry"), columns=[inp("column_name")]) # is this correct? there was a merge conflict
        cols_data = ast.literal_eval(inp("list_entry"))
        cols_names = inp("column_names").split(",")
        new_value = pd.DataFrame(cols_data, columns=cols_names)
        return new_value

    def export_code(self, node_detail_form):
        column_names = node_detail_form.get_variable_name_or_input_value_by_element_name("column_names")
        list_entry = node_detail_form.get_variable_name_or_input_value_by_element_name("list_entry")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code = f"""
        {new_var_name} = pd.DataFrame({list_entry}, columns={column_names})
        """

        return code

    def export_imports(self, *args):
        imports = ["import pandas as pd"]
        return (imports)


class SaveTxtFileHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = "SaveTxtFile"
        self.fn_name = "Save Txt File"

        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ['w+', 'a+']

        fdl = FormDictList()
        fdl.label("Save .TXT file")
        fdl.label("Value")
        fdl.entry(name="value", text="", input_types=["str", "list", "dict", "int", "float"], required=True, row=1)
        fdl.label("Filename")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=2)
        fdl.label("Write in File Mode")
        fdl.combobox(name="write_mode", options=options, default="a+", row=3)
        fdl.button(function=self.open_list_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name='lookup_txt_file')
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_list_file(self, node_detail_form):
        file = asksaveasfile(mode='w', filetypes=[('Text files', '*.txt'), ('All files', '*')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def direct_execute(self, value, filename, write_mode):
        if value and filename:

            inp = Input()
            inp.assign("value", value)
            inp.assign("filename", filename)
            inp.assign("write_mode", write_mode)
            try:
                self.input_execute(inp)
            except Exception as e:
                flog.error(f"Undefined error {e} occurred")
        else:
            flog.warning("save txt missing filename and/or value input")

    def input_execute(self, inp):
        filename = inp("filename")
        value = inp("value")
        if not filename.endswith('.txt'):
            filename += '.txt'

        # TODO: DOMINIK FROM TOMAS - WHY IS EVAL HERE???
        try:
            value = fce.eval_expression(value, globals(), locals())
        except:
            pass

        with open(filename, inp("write_mode")) as file:
            if isinstance(value, list):
                for elem in value:
                    file.write(str(elem) + '\n')
            elif isinstance(value, str):
                file.write(value + '\n')
            elif isinstance(value, dict):
                for k, v in value.items():
                    file.write(f'{k}:{v}' + '\n')
            elif isinstance(value, (int, float)):
                file.write(str(value) + '\n')

    def execute_with_params(self, params, item_detail_form=None):
        value = item_detail_form.get_chosen_value_by_name("value", variable_handler)
        filename = item_detail_form.get_chosen_value_by_name("filename", variable_handler)
        write_mode = params["write_mode"]

        self.direct_execute(value, filename, write_mode)

    def execute(self, node_detail_form):
        value = node_detail_form.get_chosen_value_by_name("value", variable_handler)
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        write_mode = node_detail_form.get_chosen_value_by_name("write_mode", variable_handler)

        self.direct_execute(value, filename, write_mode)
        
    def export_code(self, node_detail_form):
        value = node_detail_form.get_variable_name_or_input_value_by_element_name("value")
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        write_mode = node_detail_form.get_variable_name_or_input_value_by_element_name("write_mode")
        
        code = f"""
        with open("{filename}", "{write_mode}") as file:
            file.write(str({value}))
        """
        
        return code

    def export_imports(self, *args):
        imports = []

        return imports


class LoadJsonFileHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "LoadJsonFile"
        self.fn_name = "Load Json File"
        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Load .JSON file")
        fdl.label("File name:")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=1)
        fdl.label("Variable name:")
        fdl.entry(name="varname", text="", input_types=["str"], row=2)
        fdl.button(function=self.open_list_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name='lookup_json_file')
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_list_file(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('JSON files', '*.json'), ('All files', '*')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)
            
    def open_list_file_execute(self, *args):
        flog.warning("Load json open list file execute method will be deprecated, added in 8.2")
        icon_image = args[0][0]

        varname = icon_image.item_detail_form.get_chosen_value_by_name("varname")

        element = icon_image.item_detail_form.get_element_by_name('filename')
        filename = element.text

        self.direct_execute(filename, varname)
        return (filename)

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        varname = node_detail_form.get_chosen_value_by_name("varname", variable_handler)

        self.direct_execute(filename, varname)

    def execute_with_params(self, params):
        filename = params["filename"]
        varname = params["varname"]

        self.direct_execute(filename, varname)

    def debug(self, filename, varname):
        flog.debug("Load Json")
        flog.debug(f"FILENAME = {filename}")
        flog.debug(f"VARIABLE NAME = {varname}")

    def direct_execute(self, filename, varname):
        self.debug(filename, varname)

        inp = Input()
        inp.assign("filename", filename)

        try:
            json_dict = self.input_execute(inp)

            if not varname:
                short_filename = filename.split("/")[-1]
                new_variable_name = short_filename.replace(".json", "")
            else:
                new_variable_name = varname

            variable_handler.new_variable(new_variable_name, json_dict)
            #variable_handler.update_data_in_variable_explorer(glc)

        except FileNotFoundError as e:
            flog.error(f"File {filename} was not found")
        except Exception as e:
            flog.error(f"Undefined Error: {e}")

    def input_execute(self, inp):
        with open(inp("filename"), "r", encoding="utf-8") as read_file:
            json_dict = json.load(read_file)

        return json_dict

    def export_imports(self, *args):
        imports = ["import json"]
        return (imports)


class SaveJsonFileHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = "SaveJsonFile"
        self.fn_name = "Save Json File"

        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ['w+', 'a+']

        fdl = FormDictList()
        fdl.label("Save .Json file")
        fdl.label("Dictionary")
        fdl.entry(name="value", text="", input_types=["dict"], required=True, row=1)
        fdl.label("Filename")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=2)
        fdl.label("Write in File Mode")
        fdl.combobox(name="write_mode", options=options, default="a+", row=3)
        fdl.button(function=self.open_list_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name='lookup_json_file')
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True, enforce_required=True)

        return fdl

    def open_list_file(self, node_detail_form):
        file = asksaveasfile(mode='w', filetypes=[('JSON files', '*.json'), ('All files', '*')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def execute(self, node_detail_form):
        value = node_detail_form.get_chosen_value_by_name("value", variable_handler)
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        write_mode = node_detail_form.get_chosen_value_by_name("write_mode", variable_handler)

        self.direct_execute(value, filename, write_mode)

    def execute_with_params(self, params, item_detail_form=None):
        value = item_detail_form.get_chosen_value_by_name("value", variable_handler)
        filename = item_detail_form.get_chosen_value_by_name("filename", variable_handler)
        write_mode = params["write_mode"]

        self.direct_execute(value, filename, write_mode)

    def debug(self, value, filename, write_mode):
        flog.debug("Save Json")
        flog.debug(f"VALUE = {value}")
        flog.debug(f"FILENAME = {filename}")
        flog.debug(f"WRITE MODE = {write_mode}")

    def direct_execute(self, value, filename, write_mode):
        self.debug(value, filename, write_mode)
        
        if not filename.endswith('.json'):
            filename += '.json'

        inp = Input()
        inp.assign("value", value)
        inp.assign("filename", filename)
        inp.assign("write_mode", write_mode)

        if value and filename:
            try:
                self.input_execute(inp)
            except Exception as e:
                flog.error(f"Undefined Error: {e}")
        else:
            flog.warning("save json missing filename and/or value input")

    def input_execute(self, inp):
        with open(inp("filename"), inp("write_mode")) as file:
            json.dump(inp("value"), file)

    def export_imports(self, *args):
        imports = ["import json"]

        return imports


class ReadPDFHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "ReadPDF"
        self.fn_name = "Read PDF"

        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()

        fdl.label("Read .PDF file")
        fdl.label("File name:")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=1)
        fdl.button(function=self.open_pdf_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name='lookup_txt_file')
        fdl.label("Variable name:")
        fdl.entry(name="varname", text="", input_types=["str"], row=3)
        fdl.label("Create .txt file")
        fdl.checkbox(name="create_txt_file", bool_value=True, row=4)
        fdl.button(function=self.open_pdf_file_execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_pdf_file(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('PDF files', '*.pdf')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)
            
    def open_pdf_file_execute(self, node_detail_form):
        varname = node_detail_form.get_chosen_value_by_name("varname", variable_handler)
        create_txt_file = node_detail_form.get_chosen_value_by_name("create_txt_file", variable_handler)
        filename = node_detail_form.get_chosen_value_by_name('filename', variable_handler)

        self.direct_execute(filename, varname, create_txt_file)

    def direct_execute(self, filename, varname, create_txt_file):

        parsed_pdf = self.convert_pdf_to_txt(filename) 

        if not varname:
            short_filename = filename.split("/")[-1]
            new_variable_name = short_filename.replace(".pdf", "")
        else:
            new_variable_name = varname

        if create_txt_file:
            self.create_txt_file(parsed_pdf, new_variable_name)
        
        variable_handler.new_variable(new_variable_name, parsed_pdf)
        #variable_handler.update_data_in_variable_explorer(glc)

    def convert_pdf_to_txt(self, filename: str) -> str:
       rsrcmgr = PDFResourceManager()
       retstr = StringIO()
       codec = 'utf-8'  # 'utf16','utf-8'
       laparams = LAParams()
       device = TextConverter(rsrcmgr, retstr, laparams=laparams) #codec=codec, 
       fp = open(filename, 'rb')
       interpreter = PDFPageInterpreter(rsrcmgr, device)
       password = ""
       maxpages = 1000 #0==infinite
       caching = False
       pagenos = set()
       i=0
       for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching, check_extractable=True):
           interpreter.process_page(page)
           i=i+1
           print(i)
       fp.close()
       device.close()
       parsed_file = retstr.getvalue()
       retstr.close()
       return parsed_file

    def create_txt_file(self, text: str, filename: str):

        if not ".txt" in filename:
            filename = f'{filename}.txt'

        with open(filename, 'wb') as file:
            file.write(text.encode('utf-8'))

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        varname = node_detail_form.get_chosen_value_by_name("varname", variable_handler)
        create_txt_file = node_detail_form.get_chosen_value_by_name("create_txt_file", variable_handler)

        self.direct_execute(filename, varname, create_txt_file)

    def export_code(self, node_detail_form):
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        varname = node_detail_form.get_variable_name_or_input_value_by_element_name("varname")
        create_txt_file = node_detail_form.get_chosen_valuget_variable_name_or_input_value_by_element_namee_by_name("create_txt_file")

        new_variable_name = filename.split("/")[-1].replace(".txt", "")

        code = """
        with open({filename}, "r") as f:
            rows = f.readlines()
            rows = [x.replace({new_line}, "") for x in rows]  

        {new_variable_name} = rows
        """
        ## TODO: make it to show \n as a string ---> now it creates a new line
        return (code.format(filename='"' + filename + '"', new_variable_name=new_variable_name, new_line="\n"))


class DictToDfHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "DictToDf"
        self.fn_name = "Normalize Dict To Dataframe"
        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Dictionary")
        fdl.entry(name="dict_entry", text="", category="instance_var", input_types=["dict"], required=True, row=1)
        fdl.label("Record Path")
        fdl.entry(name="record_path", text="", input_types=["str"], required=True, row=2)
        fdl.label("New Variable")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        dict_entry = node_detail_form.get_chosen_value_by_name("dict_entry", variable_handler)
        record_path = node_detail_form.get_chosen_value_by_name("record_path", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        
        new_var_name = variable_handler._set_up_unique_varname(new_var_name)
        fields = self.generate_shown_dataframe_option_field(new_var_name)

        response = ncrb.new_node(pos=[500, 300], typ="DataFrame", fields=fields)
        if response.status_code in [200, 201]:
            result = json.loads(response.content.decode('utf-8'))
            node_uid = result["uid"]

            self.direct_execute(dict_entry, record_path, new_var_name)

            ncrb.update_last_active_dataframe_node_uid(node_uid)
        else:
            raise HTTPException(status_code=response.status_code, detail="Error requesting new node from api")

    def execute_with_params(self, params):
        dict_entry = params["dict_entry"]
        record_path = params["record_path"]
        new_var_name = params["new_var_name"]

        self.direct_execute(dict_entry, record_path, new_var_name)

    def debug(self, record_path, new_var_name):
        flog.debug("Normalize Dict To Df")
        flog.debug(f"RECORD PATH = {record_path}")
        flog.debug(f"VARIABLE NAME = {new_var_name}")

    def direct_execute(self, dict_entry, record_path, new_var_name):
        self.debug(record_path, new_var_name)

        if not record_path or record_path == '""':
            record_path = None

        inp = Input()
        inp.assign("dict_entry", dict_entry)
        inp.assign("record_path", record_path)

        try:
            df_new = self.input_execute(inp)
        except Exception as e:
            df_new = pd.DataFrame()
            flog.error(f"Undefined Error: {e}")

        df_new = validate_input_data_types(df_new)
        variable_handler.new_variable(new_var_name, df_new)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):
        df_new = pd.json_normalize(inp("dict_entry"), record_path=inp("record_path"))
        return df_new

    def export_imports(self, *args):
        imports = []
        return (imports)


class DatabaseHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "Database"
        self.fn_name = "Database"
        self.type_category = ntcm.categories.data
        self.docs_category = DocsCategories.data_sources

    def make_form_dict_list(self, *args, options={}, node_detail_form=None):
        if options:
            self.dbtables_loader_popups = options["dbtables_loader_popups"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        
        fdl.button(function=get_tables, function_args=node_detail_form, text="Get all tables", frontend_implementation=True, name="get_all_tables")
        fdl.button(function=get_specific_table, function_args=node_detail_form, text="Get specific table", focused=True, frontend_implementation=True, name="get_specific_table")

        return fdl


    def direct_execute(self):
        pass

    def execute_with_params(self, params):
        self.direct_execute()

    def execute(self, node_detail_form):
        self.direct_execute()

    def export_code(self, node_detail_form):
        code = ""

        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


data_handlers_dict = {
    "LoadFile": LoadFileHandler(),
    "LoadExcel": LoadExcelHandler(),
    "SaveExcel": SaveExcelHandler(),
    "LoadTxtFile": LoadTxtFileHandler(),
    "SaveTxtFile": SaveTxtFileHandler(),
    "DfToList": DfToListHandler(),
    "DataFrame": DataFrameHandler(),
    "ListToDf": ListToDfHandler(),
    "LoadJsonFile": LoadJsonFileHandler(),
    "SaveJsonFile": SaveJsonFileHandler(),
    "DictToDf": DictToDfHandler(),
    "ReadPDF": ReadPDFHandler(),
    "Database": DatabaseHandler()
}
