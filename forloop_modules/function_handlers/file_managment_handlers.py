import os
import filehydra.filehydra_core as fh

from tkinter.filedialog import askopenfile, askdirectory

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.globals.docs_categories import DocsCategories

from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler


class DeleteFileHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = "DeleteFile"
        self.fn_name = "Delete File"

        self.type_category = ntcm.categories.file_management
        self.docs_category = DocsCategories.webscraping_and_rpa

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File name:")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=1)
        fdl.button(function=self.select_file, function_args=node_detail_form, text="Look up file", enforce_required=False, name="lookup_file")
        fdl.button(self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def select_file(self, node_detail_form):
        file = askopenfile(mode='r')  # ,filetypes=[('Spreadsheets', '*.csv *.xlsx'), ('Excel files', '*.xlsx'), ('CSV files', '*.csv')]

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)
                    
    def direct_execute(self, filename):
        filehydra = fh.FileHydra()
        print("WARNING: DANGREROUS - DELETING FILE", filename)

        if os.path.exists(filename):
            filehydra.delete_file(filename)
        else:
            flog.warning(f'File "{filename}" does not exist.')

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)

        self.direct_execute(filename)

    def export_code(self, node_detail_form):
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")

        code = """
        filehydra = fh.FileHydra()
        print("WARNING: DANGREROUS - DELETING FILE",{filename})

        filehydra.delete_file({filename})
        """

        return (code.format(filename=filename))

    def export_imports(self, *args):
        imports = ["import filehydra.filehydra_core as fh"]
        return (imports)


class CreateFolderHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = 'CreateFolder'
        self.fn_name = 'Create Folder'

        self.type_category = ntcm.categories.file_management
        self.docs_category = DocsCategories.webscraping_and_rpa

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Folder location:")
        fdl.entry(name="folder_loc", text="./", category="arguments", input_types=["str"], required=True, row=1)
        fdl.button(function=self.open_folder_location, function_args=node_detail_form, text="Load folder path", enforce_required=False, name="lookup_folder")
        fdl.label("Folder name:")
        fdl.entry(name="folder_name", text="My new folder", category="arguments", input_types=["str"], required=True, row=3)
        fdl.button(self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_folder_location(self, node_detail_form):
        folder_loc = askdirectory(initialdir=aet.home_folder)

        if folder_loc is not None:
            params_dict = node_detail_form.assign_value_by_name(name='folder_loc', value=folder_loc)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def execute(self, node_detail_form):
        folder_loc = node_detail_form.get_chosen_value_by_name("folder_loc", variable_handler)
        folder_name = node_detail_form.get_chosen_value_by_name("folder_name", variable_handler)

        self.direct_execute(folder_loc, folder_name)

    def execute_with_params(self, params):

        folder_loc = params["folder_loc"]
        folder_name = params["folder_name"]

        self.direct_execute(folder_loc, folder_name)

    def direct_execute(self, folder_loc, folder_name):

        folder_loc = os.path.normpath(folder_loc)  # str --> path format

        fh1 = fh.FileHydra()

        fh1.change_hydra_location(folder_loc)
        fh1.create_folder(folder_name)
        print(f'Created a new folder "{folder_name}". Full path = {os.path.join(folder_loc, folder_name)}')

    def export_code(self, node_detail_form):
        folder_loc = node_detail_form.get_variable_name_or_input_value_by_element_name("folder_loc")
        folder_name = node_detail_form.get_variable_name_or_input_value_by_element_name("folder_name")

        code = """
        fh1 = fh.FileHydra()

        fh1.change_hydra_location(folder_loc)
        fh1.create_folder(folder_name)
        """

        return code

    def export_imports(self, *args):
        imports = ["import filehydra.filehydra_core as fh"]
        return (imports)


class MoveFileHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = 'MoveFile'
        self.fn_name = 'Move File'

        self.type_category = ntcm.categories.file_management
        self.docs_category = DocsCategories.webscraping_and_rpa

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("File:")
        fdl.entry(name="filename", text="User\\Forloop\\file.txt", category="arguments", input_types=["str"], required=True, row=1)
        fdl.button(function=self.open_file_location, function_args=node_detail_form, text="Load file path", enforce_required=False, name="lookup_file")
        fdl.label("Move to:")
        fdl.entry(name="folder_name", text="User\\Forloop\\new_folder", category="arguments", input_types=["str"], required=True, row=3)
        fdl.button(function=self.open_folder_location, function_args=node_detail_form, text="Load folder path", enforce_required=False, name="lookup_destination_folder")
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_file_location(self, node_detail_form):
        file = askopenfile(mode='r', filetypes=[('All file types', '')])

        if file is not None:
            filename = file.name
            params_dict = node_detail_form.assign_value_by_name(name='filename', value=filename)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)
                    
    def open_folder_location(self, node_detail_form):
        folder_loc = askdirectory(initialdir=aet.home_folder)

        if folder_loc is not None:
            params_dict = node_detail_form.assign_value_by_name(name='folder_name', value=folder_loc)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        folder_name = node_detail_form.get_chosen_value_by_name("folder_name", variable_handler)

        self.direct_execute(filename, folder_name)

    def execute_with_params(self, params):

        filename = params["filename"]
        folder_name = params["folder_name"]

        self.direct_execute(filename, folder_name)

    def direct_execute(self, filename, folder_name):

        filename, folder_name = os.path.normpath(filename), os.path.normpath(folder_name)  # str --> path format

        fh1 = fh.FileHydra()

        fh1.move_file(filename, filename, folder_name)
        print(f'Moved file "{filename}" to "{folder_name}"')

    def export_code(self, node_detail_form):
        filename = node_detail_form.get_variable_name_or_input_value_by_element_name("filename")
        folder_name = node_detail_form.get_variable_name_or_input_value_by_element_name("folder_name")

        code = """
        fh1 = fh.FileHydra()

        fh1.move_file(filename, filename, folder_name)
        """

        return code

    def export_imports(self, *args):
        imports = ["import filehydra.filehydra_core as fh"]
        return (imports)


class CreateFileQueueHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = 'CreateFileQueue'
        self.fn_name = 'Create File Queue'

        self.type_category = ntcm.categories.file_management
        self.docs_category = DocsCategories.webscraping_and_rpa

    def make_form_dict_list(self, *args, node_detail_form=None):

        # TODO: Queue name could be an editable combobox with already created queue dirs instead of entry

        options = ["txt", "csv", "xlsx"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Root folder:")
        fdl.entry(name="folder_name", text="", category="arguments", input_types=["str"], required=True, row=1)
        fdl.button(function=self.open_folder_location, function_args=node_detail_form, text="Search directory", enforce_required=False, name="lookup_folder")
        fdl.label("Queue name:")
        fdl.entry(name="queue_name", text="My_queue", category="arguments", input_types=["str"], required=True, row=3)
        fdl.label("Suffix:")
        fdl.combobox(name="suffix", options=options, multiselect_indices=None, default=options[0], row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def open_folder_location(self, node_detail_form):
        folder_loc = askdirectory(initialdir="./file_queues")

        if folder_loc is not None:
            params_dict = node_detail_form.assign_value_by_name(name='folder_name', value=folder_loc)
            ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)

    def execute(self, node_detail_form):
        folder_name = node_detail_form.get_chosen_value_by_name("folder_name", variable_handler)
        queue_name = node_detail_form.get_chosen_value_by_name("queue_name", variable_handler)
        suffix = node_detail_form.get_chosen_value_by_name("suffix", variable_handler)

        self.direct_execute(folder_name, queue_name, suffix)

    def execute_with_params(self, params):

        folder_name = params["folder_name"]
        queue_name = params["queue_name"]
        suffix = params["suffix"]

        self.direct_execute(folder_name, queue_name, suffix)

    def direct_execute(self, folder_name, queue_name, suffix):

        queue_path = os.path.join(folder_name, queue_name)

        global fq, template

        fq = fh.FileQueue(folder_name, queue_name)

        # if the path to the directory does not exist or the object is not a directory --> create new directory with given name
        if not os.path.exists(queue_path) or not os.path.isdir(queue_path):
            fq.initialize_queue_folders()

        template = fh.FileTemplate("", suffix)

    def export_code(self, node_detail_form):
        folder_name = node_detail_form.get_variable_name_or_input_value_by_element_name("folder_name")
        queue_name = node_detail_form.get_variable_name_or_input_value_by_element_name("queue_name")
        suffix = node_detail_form.get_variable_name_or_input_value_by_element_name("suffix")

        code = f"""
        fq = fh.FileQueue({folder_name}, {queue_name})
        fq.initialize_queue_folders()
        template = fh.FileTemplate("", {suffix})
        """

        return code

    def export_imports(self, *args):
        imports = ["import filehydra.filehydra_core as fh"]
        return (imports)


class ProcessItemInQueueHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_cloud_compatible = False
        self.icon_type = 'ProcessItemInQueue'
        self.fn_name = 'Process Item In Queue'

        self.type_category = ntcm.categories.file_management
        self.docs_category = DocsCategories.webscraping_and_rpa

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("New variable name:")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(new_var_name)

    def execute_with_params(self, params):
        new_var_name = params["new_var_name"]

        self.direct_execute(new_var_name)

    def direct_execute(self, new_var_name):
        global fq, template

        filename, data = fq.process_next_file(template)

        if data is not None:
            variable_handler.new_variable(new_var_name, data)
            #variable_handler.update_data_in_variable_explorer(glc)

    def export_code(self, node_detail_form):
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code = f"""
        _, {new_var_name} = fq.process_next_file(template)
        """

        return code

    def export_imports(self, *args):
        imports = ["import filehydra.filehydra_core as fh"]
        return (imports)


file_managment_handlers_dict = {
    'DeleteFile': DeleteFileHandler(),
    'CreateFolder': CreateFolderHandler(),
    'MoveFile': MoveFileHandler(),
    'CreateFileQueue': CreateFileQueueHandler(),
    'ProcessItemInQueue': ProcessItemInQueueHandler()
}