from collections import UserList
from typing import Optional, Literal

import forloop_modules.flog as flog

from forloop_modules.function_handlers.auxilliary.docs import Docs


KeyLiteral = Literal["Label", "Combobox", "ComboEntry", "Button", "ButtonImage", "Checkbox"]
TypeLiteral = Literal["text", "file", "password"]

class FormDictList(UserList):
    """
    Class replacing initial way of definining form_dict_list (template for ItemDetailForm for each node)
    
    #Example of syntax until 0.6.1
    form_dict_list = [
    {"Label": self.fn_name},
    {"Label": "X:", "Entry": {"name": "x", "text": "0"}},
    {"Label": "Y:", "Entry": {"name": "y", "text": "0"}},
    {"Label": "Pipette current position (Press Enter Key)"},
    {"Label": str(glc.motion_mouse_pos)}
    ]   
    
    #Example of syntax after 0.6.1
    fdl=FormDictList()
    fdl.label(self.fn_name)
    fdl.label("X:")
    fdl.entry(name="x",text="0",row=1)
    fdl.label("Y:")
    fdl.entry(name="y",text="0",row=2)
    fdl.label("Pipette current position (Press Enter Key)")
    fdl.label(str(glc.motion_mouse_pos))
    """
    
    def __init__(self, initlist=None, docs: Optional[Docs] = None):
        super().__init__(initlist)
        self.docs = docs
    
    @property
    def form_dict_list(self):
        flog.warning("Forloop Deprecation Warning: Following FormDictList should be changed to return only fdl value, not fdl.form_dict_list:"+str(self))
        return(self)
    
    @form_dict_list.setter
    def form_dict_list(self,form_dict_list):
        self._form_dict_list=self
    
    def insert_element_to_form_dict_list(self, key, value, row=None):
        """Row parameter is specified only if it's not first element on the same row"""
        if isinstance(value, dict):
            value = {k: v for k, v in value.items() if v is not None}  # don't append None values in self
        if row is not None and row < len(self):
            self[row][key] = value
        else:
            self.append({key: value})

    def label(self, text, row=None):
        """
        Label is a simple text element
        args:
            text (str): text of the element
            row (int): row number of the element - specified only if it's not first element on the same row
        """
        key = "Label"
        value = text
        self.insert_element_to_form_dict_list(key, value, row)

    def entry(self, name, text, category=None, input_types=None, required=None, type='text', file_types=None, show_info=None, desktop_only=False, row=None):
        """
        Entry is a text element with a name and a text
        args:
            name (str): name of the element
            text (str): text of the element
            category (str): category of the element
            input_types (str): input types of the element
            required (bool): required to fill in on frontend
            type (str): One of 'text', 'file', 'password' - changes entry behaviour
            row (int): row number of the element - specified only if it's not first element on the same row
        """
        if file_types is None:
            file_types = [("all files", "*")]

        key = "Entry"
        value = {
            "name": name,
            "text": text,
            "category": category,
            "input_types": input_types,
            "required": required,
            "type": type,
            "file_types": file_types,
            "show_info": show_info,
            'desktop_only': desktop_only,
        }

        if required:
            if "Label" in self[-1].keys():
                self[-1]["Label"] = self[-1]["Label"] + "*"

        self.insert_element_to_form_dict_list(key, value, row)

    def combobox(self, name, options, multiselect_indices=None, default=None, show_info=None, desktop_only=False, row=None):
        """
        Combobox is a dropdown list with options

        Args:
            name (str): name of the element
            options (list): list of options
            multiselect_indices (dict, optional): Enables multiselect mode. Defaults to None.
            default (str, optional): Default option. Defaults to None.
            row (int, optional): Row number of the element - specified only if it's not first element on the same row. Defaults to None.
        """
        key = "Combobox"
        value = {"name": name, "options": options, "default": default, "multiselect_indices": multiselect_indices, "show_info":show_info, 'desktop_only': desktop_only}
        self.insert_element_to_form_dict_list(key, value, row)

    def comboentry(self, name, text, options, show_info=None, desktop_only=False, row=None):
        """
        Comboentry is a dropdown list with options and a text field
        
        Args:
            name (str): name of the element
            text (str): text of the element
            options (list): list of options
            row (int): row number of the element - specified only if it's not first element on the same row
        """
        key = "ComboEntry"
        value = {"name": name, "text": text, "options": options, "show_info": show_info, 'desktop_only': desktop_only}
        self.insert_element_to_form_dict_list(key, value, row)

    def button(self, function, function_args, text, focused: bool = False, enforce_required: bool = None, frontend_implementation=False, name=None, desktop_only=False, row=None):
        """
        Button is a button with a function and arguments

        Args:
            function (function): function to be called when button is pressed
            function_args (list): list of arguments for the function
            text (str): text of the button
            focused (bool, optional): If True, button will be executed after Enter push. Defaults to False.
            enforce_required (bool, optional): If True, button will check whether compulsory elements are filled. Defaults to True.
            row (int): row number of the element - specified only if it's not first element on the same row
        """    
        key = "Button"
        value = {"name": name, "function": function, "function_args": function_args, "text": text, "focused": focused, "enforce_required": enforce_required, "frontend_implementation": frontend_implementation, 'desktop_only': desktop_only}
        self.insert_element_to_form_dict_list(key, value, row)

    def button_image(self, image, x_size, y_size, x_offset, function, function_args=None, desktop_only=False, row=None):
        """
        Button is an image with or without function and arguments

        Args:
            image (str): path to image (relative to src/png)
            function (function): function to be called when button is pressed
            function_args (list): list of arguments for the function
            x_size (int): width of image
            y_size (int): height of the button
            x_offset (int): offset on x-axis
            row (int): row number of the element - specified only if it's not first element on the same row
        """

        key = "ButtonImage"
        value = {"image": image, "function": function, "function_args": function_args, "x_size": x_size, "y_size": y_size, "x_offset": x_offset, 'desktop_only': desktop_only}
        self.insert_element_to_form_dict_list(key, value, row)

    def checkbox(self, name, bool_value: bool = False, desktop_only=False, row=None):
        """
        CheckBox is an element for storing True/False values

        Args:
            name (str): name of the element
            bool_value (bool, optional): if True, checkbox is selected. Default False.
            row (int): row number of the element - specified only if it's not first element on the same row
        """
        key = "Checkbox"
        value = {"name": name, "bool_value": bool_value, 'desktop_only': desktop_only}
        self.insert_element_to_form_dict_list(key, value, row)



