from collections import UserList
from collections.abc import Callable
from typing import Optional, Literal

import forloop_modules.flog as flog

from forloop_modules.function_handlers.auxilliary.docs import Docs


KeyLiteral = Literal["Label", "Combobox", "ComboEntry", "Button", "ButtonImage", "Checkbox"]
TypeLiteral = Literal["text", "file", "password"]

class FormDictList(UserList):
    """
    Class replacing initial way of definining form_dict_list (template for ItemDetailForm for
    each node)
    
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
        flog.warning(
            f"""Forloop Deprecation Warning: Following FormDictList should be changed to return only
            fdl value, not fdl.form_dict_list: {str(self)}"""
        )

        return self

    @form_dict_list.setter
    def form_dict_list(self,form_dict_list):
        self._form_dict_list=self

    def insert_element_to_form_dict_list(self, key: KeyLiteral, value, row: Optional[int] = None):
        """Row parameter is specified only if it's not first element on the same row"""
        if isinstance(value, dict):
            value = {k: v for k, v in value.items() if v is not None}  # don't append None values in self
        if row is not None and row < len(self):
            self[row][key] = value
        else:
            self.append({key: value})

    def label(self, text: str, row: Optional[int] = None):
        """
        Label is a simple text element
        args:
            text (str): text of the element
            row (int): row number of the element - specified only if it's not first element on the
                same row
        """
        key = "Label"
        value = text
        self.insert_element_to_form_dict_list(key, value, row)

    def entry(self, name: str, text: str, category: Optional[str] = None,
        input_types: Optional[list[str]] = None, required: Optional[bool] = None,
        type: TypeLiteral = 'text', file_types: Optional[list[tuple[str]]] = None,
        show_info: Optional[bool] = None, desktop_only: bool = False, is_advanced: bool = False,
        row: Optional[int] = None
    ):
        """
        Entry is a text element with a name and a text.

        Args:
            name (str): Name of the element.
            text (str): Text of the element.
            category (Optional[str], optional): Category of the element. Defaults to None.
            input_types (Optional[list[str]], optional): Input types of the element. Defaults to None.
            required (Optional[bool], optional): Required to fill in on frontend. Defaults to None.
            type (TypeLiteral, optional): One of 'text', 'file', 'password' - changes entry
                behaviour. Defaults to 'text'.
            file_types (Optional[list[tuple[str]]], optional): Supported file file types in format
                [(<type label>, <file type>), ...], e.g.: [("Python files", "*.py")]. Defaults to None.
            show_info (Optional[bool], optional): Show node info switch. Defaults to None.
            desktop_only (bool, optional): If True, won't appear in cloud version. Defaults to False.
            is_advanced (bool, optional): If True, will be wrapped as an optional (not shown by
                default). Defaults to False.
            row (Optional[int], optional): Row number of the element - specified only if it's not
                first element on the same row. Defaults to None.
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
            "is_advanced": is_advanced,
        }

        if required:
            if "Label" in self[-1].keys():
                self[-1]["Label"] = self[-1]["Label"] + "*"

        self.insert_element_to_form_dict_list(key, value, row)

    def combobox(self, name: str, options: list, multiselect_indices: Optional[dict] = None,
        default: Optional[str] = None, show_info: Optional[bool] = None,
        desktop_only: bool = False, is_advanced: bool = False, row: Optional[int] = None
    ):
        """
        Combobox is a dropdown list with options.

        Args:
            name (str): Name of the element.
            options (list): List of options.
            multiselect_indices (Optional[dict], optional): Enables multiselect mode. Defaults to None.
            default (Optional[str], optional): Option selected by default. Defaults to None.
            show_info (Optional[bool], optional): Show node info switch. Defaults to None.
            desktop_only (bool, optional): If True, won't appear in cloud version. Defaults to False.
            is_advanced (bool, optional): If True, will be wrapped as an optional (not shown by
                default). Defaults to False.
            row (Optional[int], optional): Row number of the element - specified only if it's not
                first element on the same row. Defaults to None.
        """
        key = "Combobox"
        value = {
            "name": name,
            "options": options,
            "default": default,
            "multiselect_indices": multiselect_indices,
            "show_info": show_info,
            "desktop_only": desktop_only,
            "is_advanced": is_advanced,
        }
        self.insert_element_to_form_dict_list(key, value, row)

    def comboentry(self, name: str, text: str, options: list, show_info: Optional[bool] = None,
        desktop_only: bool = False, is_advanced: bool = False, row: Optional[int] = None
    ):
        """
        Comboentry is a dropdown list with options and a text field.

        Args:
            name (str): Name of the element.
            text (str): Text of the element.
            options (list): List of selectable options.
            show_info (Optional[bool], optional): Show node info switch. Defaults to None.
            desktop_only (bool, optional): If True, won't appear in cloud version. Defaults to False.
            is_advanced (bool, optional): If True, will be wrapped as an optional (not shown by
                default). Defaults to False.
            row (Optional[int], optional): Row number of the element - specified only if it's not
                first element on the same row. Defaults to None.
        """
        
        key = "ComboEntry"
        value = {
            "name": name,
            "text": text,
            "options": options,
            "show_info": show_info,
            "desktop_only": desktop_only,
            "is_advanced": is_advanced,
        }
        self.insert_element_to_form_dict_list(key, value, row)

    def button(self, function: Callable, function_args: list, text: str, focused: bool = False,
        enforce_required: bool = None, frontend_implementation: bool = False,
        name: Optional[str] = None, desktop_only: bool = False, is_advanced: bool = False,
        row: Optional[int] = None
    ):
        """
        Button is a button with a function and arguments.

        Args:
            function (Callable): A function to be called when button is pressed.
            function_args (list): A list of arguments for the function.
            text (str): Text of the button.
            focused (bool, optional): If True, button will be executed after Enter push. Defaults
                to False.
            enforce_required (bool, optional): If True, button will check whether compulsory elements
                are filled. Defaults to None.
            frontend_implementation (bool, optional): Marks FE only appearance. Defaults to False.
            name (Optional[str], optional): Button element name. Defaults to None.
            desktop_only (bool, optional): If True, won't appear in cloud version. Defaults to False.
            is_advanced (bool, optional): If True, will be wrapped as an optional (not shown by
                default). Defaults to False.
            row (Optional[int], optional): Row number of the element - specified only if it's not
                first element on the same row. Defaults to None.
        """
          
        key = "Button"
        value = {
            "name": name,
            "function": function,
            "function_args": function_args,
            "text": text,
            "focused": focused,
            "enforce_required": enforce_required,
            "frontend_implementation": frontend_implementation,
            "desktop_only": desktop_only,
            "is_advanced": is_advanced,
        }
        self.insert_element_to_form_dict_list(key, value, row)

    def button_image(self, image: str, x_size: int, y_size: int, x_offset: int, function: Callable,
        function_args: Optional[list] = None, desktop_only: bool = False, is_advanced: bool = False,
        row: Optional[int] = None
    ):
        """
        Button is an image with or without function and arguments.

        Args:
            image (str): A path to an image (relative to src/png).
            x_size (int): Image width.
            y_size (int): Image height.
            x_offset (int): Horizontal (x-axis) offset of the image.
            function (Callable): A function to be called when button is pressed.
            function_args (Optional[list], optional): A list of arguments for the function. Defaults
                to None.
            desktop_only (bool, optional): If True, won't appear in cloud version. Defaults to False.
            is_advanced (bool, optional): If True, will be wrapped as an optional (not shown by
                default). Defaults to False.
            row (Optional[int], optional): Row number of the element - specified only if it's not
                first element on the same row. Defaults to None.
        """

        key = "ButtonImage"
        value = {
            "image": image,
            "function": function,
            "function_args": function_args,
            "x_size": x_size,
            "y_size": y_size,
            "x_offset": x_offset,
            "desktop_only": desktop_only,
            "is_advanced": is_advanced,
        }
        self.insert_element_to_form_dict_list(key, value, row)

    def checkbox(self, name: str, bool_value: bool = False, desktop_only: bool = False,
        is_advanced: bool = False, row: Optional[int] = None
    ):
        """
        CheckBox is an element for storing True/False values.

        Args:
            name (str): Name of the element.
            bool_value (bool, optional): If True, checkbox is selected by default. Defaults to False.
            desktop_only (bool, optional): If True, won't appear in cloud version. Defaults to False.
            is_advanced (bool, optional): If True, will be wrapped as an optional (not shown by
                default). Defaults to False.
            row (Optional[int], optional): Row number of the element - specified only if it's not
                first element on the same row. Defaults to None.
        """
        
        key = "Checkbox"
        value = {
            "name": name,
            "bool_value": bool_value,
            "desktop_only": desktop_only,
            "is_advanced": is_advanced,
        }
        self.insert_element_to_form_dict_list(key, value, row)
