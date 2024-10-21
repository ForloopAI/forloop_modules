import ast
import math
from collections.abc import Iterable
from collections.abc import Hashable
from copy import deepcopy
from typing import Any

import forloop_modules.flog as flog
import forloop_modules.function_handlers.auxilliary.forloop_code_eval as fce
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.docs import Docs
from forloop_modules.globals.variable_handler import variable_handler, LocalVariable
from forloop_modules.globals.docs_categories import DocsCategories

from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input
from forloop_modules.errors.errors import CriticalPipelineError, SoftPipelineError


####### PROBLEMATIC IMPORTS TODO: REFACTOR #######
#from src.gui.gui_layout_context import glc
#import src.gui.components.gui_components as gc
#from src.gui.item_detail_form import ItemDetailForm #independent on GLC - but is Frontend -> Should separate to two classes
####### PROBLEMATIC IMPORTS TODO: REFACTOR #######

################################ VARIABLE HANDLERS #################################


class NewVariableHandler(AbstractFunctionHandler):
    """
    Add New Variable Node creates a new variable which gets stored in the variable explorer.
    """
    def __init__(self):
        self.icon_type = 'NewVariable'
        self.fn_name = 'New Variable'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = "Add New Variable Node requires 2 parameters for a succesful creation of a new variable."
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Variable name", name="variable_name",
                                          description="A name of the just defined variable under which will it be stored in the variable explorer.",
                                          typ="string", example="new_test_var")
        self.docs.add_parameter_table_row(title="Value", name="variable_value", description="Value of the new variable",
                                          example="123.4 | [1,2,3,4] | {'name': 'John'} | 'Hello world'")

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["int", "float", "str", "bool", "list", "dict"]

        fdl = FormDictList(docs=self.docs)
        fdl.label("Add New Variable")
        fdl.label("Variable name")
        fdl.entry(name="variable_name", text="", category="new_var", input_types=["str"], show_info=True, row=1)
        fdl.label("Value")
        fdl.entry(name="variable_value", text="", category="arguments", required=True, show_info=True, row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        variable_name = node_detail_form.get_chosen_value_by_name("variable_name", variable_handler)
        variable_value = node_detail_form.get_chosen_value_by_name("variable_value", variable_handler)

        self.direct_execute(variable_name, variable_value)

    def execute_with_params(self, params):
        variable_name = params["variable_name"]
        variable_value = params["variable_value"]
        
        self.direct_execute(variable_name, variable_value)

    def direct_execute(self, variable_name, variable_value):

        inp = Input()
        
        inp.assign("variable_name", variable_name)
        if type(variable_value)==str:
            try:
                variable_value=ast.literal_eval(variable_value) #works for integers, floats, ...
            except Exception: #Handling of this error ValueError: malformed node or string: <ast.Name object at 0x0000018DF791CC70> 22:12:53 NewVariableHandler: Error executing NewVariableHandler: malformed node or string: <ast.Name object at 0x0000018DF791CC70> 
                if "'" in variable_value:
                    variable_value=variable_value.replace("'",'"') #scraped text can contain apostrophe ' symbol - for example on news websites
                variable_value=ast.literal_eval("'"+variable_value+"'")
        #print("VARIABLE_VALUE",variable_value,type(variable_value))
        inp.assign("variable_value", variable_value)
        
        try:
            variable_value = self.input_execute(inp)
        except Exception as e:
            flog.error(f"Error executing NewVariableHandler: {e}")
            
        variable_handler.new_variable(variable_name, variable_value)  
        #variable_handler.update_data_in_variable_explorer(glc)

        """
        try:
            variable_value = ast.literal_eval(variable_value)
        except (ValueError, SyntaxError):
            variable_value = variable_value

        #response=ncrb.new_variable(variable_name, str(variable_value)) #TEMPORARY DISABLE - DO NOT ERASE
        #print("CONTENT",response.content)
        #print(variable,type(variable),variable.uid)
        variable_handler.new_variable(variable_name, variable_value)  
        ##variable_handler.update_data_in_variable_explorer(glc)
        """

    # TODO: Needs to deal with saving into "variable_name" from input
    # TODO: inp("var_name") = inp("var_value")
    def input_execute(self, inp): #ast.literal_eval(inp("variable_value")) was wrong
        variable_value = inp("variable_value")
        
        return variable_value

    def export_code(self, node_detail_form):
        variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_name", is_input_variable_name=True)
        variable_value = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_value")

        code = f"""
        {variable_name} = {variable_value}
        """

        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


class ConvertVariableTypeHandler(AbstractFunctionHandler):
    """
    Convert Variable Type Node converts the type of the selected variable to a different type if such conversion makes sense.
    """
    def __init__(self):
        self.icon_type = 'ConvertVariableType'
        self.fn_name = 'Convert Variable Type'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = "Convert Variable Type Node requires 2 parameters for a succesful conversion of the variable type."
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Variable name", name="variable_name",
                                          description="A name of the variable present in the variable explorer whose type is to be changed.",
                                          typ="string", example="test_float")
        self.docs.add_parameter_table_row(title="Convert to type", name="variable_type",
                                          description="A new type of the selected variable after the conversion. It can be selected as one of the options of the combobox."
                                          )
        self.docs.add_parameter_table_row(title="New variable name", name="new_variable_name",
                                          description="Name of the new variable containing the converted value. If left blank the initial variable will get overwritten.",
                                          typ="string", example="float_to_str")

    def make_form_dict_list(self, *args, node_detail_form=None):

        options = ["int", "float", "str", "bool", "list", "dict"]

        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Variable name")
        fdl.entry(name="variable_name", text="", input_types=["str", "var_name"], required=True, show_info=True, row=1)
        fdl.label("Convert to type")
        # TODO: Should be renamed to new_variable_type
        fdl.combobox(name="variable_type", options=options, show_info=True, row=2)
        fdl.label("New variable name")
        fdl.entry(name="new_variable_name", text="", category="new_var", input_types=["str"], row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    # TODO: Should be renamed to new_variable_type
    def direct_execute(self, variable_name, variable_type, new_variable_name):
        if variable_name in variable_handler.variables:
            variable = variable_handler.variables[variable_name]
            old_type = fce.eval_expression(variable.typ, globals(), locals())

            variable_value = variable.value
            variable_value = old_type(variable_value)

            variable_type = fce.eval_expression(variable_type, globals(), locals())

            inp = Input()
            inp.assign("variable_value", variable_value)
            inp.assign("variable_name", variable_name)
            # TODO: Should be renamed to new_variable_type
            inp.assign("variable_type", variable_type)

            try:
                new_value = self.input_execute(inp)
                #new_value = self.direct_execute_core(variable_name, variable_type) #bug - how can you assign name of variable to value?!
            except TypeError as e:
                flog.error("TypeError Exception Raised, undefined conversion called.")
                new_value = ""
            except ValueError as e:
                flog.error("ValueError Exception Raised, undefined conversion called.")
                new_value = ""

            if len(new_variable_name) == 0:
                new_variable_name = variable_name

            variable_handler.new_variable(new_variable_name, new_value)
            #variable_handler.update_data_in_variable_explorer(glc)

    # TODO: Cant do inp("variable_name") = inp("variable_value")
    #  probably should introduce complex input variable
    #  something like number =  1
    #  and then choose to show either var name or value in code export, while in input execute we only use value
    # TODO: value should be saved into new_variable_name instead of converted_value
    def input_execute(self, inp):
        if type(inp("variable_value")) == dict and inp("variable_type") == list:
            converted_value = list(list(pair) for pair in inp("variable_value").items())
        elif type(inp("variable_value")) == str and inp("variable_type") == dict:
            converted_value = ast.literal_eval(inp("variable_value"))
        
        else:
            converted_value = inp("variable_type")(inp("variable_value"))

        return converted_value
    
    #! Introduced for an experimental codeview approach -- functionality is the same, # No it is not the same functionality
    def direct_execute_core(self, variable_name, variable_type):
        new_variable_name = variable_type(variable_name)
        
        return new_variable_name

    def execute_with_params(self, params):
        variable_name = params["variable_name"]
        variable_type = params["variable_type"]
        new_variable_name = params["new_variable_name"]

        self.direct_execute(variable_name, variable_type, new_variable_name)

    def execute(self, node_detail_form):
        variable_name = node_detail_form.get_chosen_value_by_name("variable_name", variable_handler)
        variable_type = node_detail_form.get_chosen_value_by_name("variable_type", variable_handler)
        new_variable_name = node_detail_form.get_chosen_value_by_name("new_variable_name", variable_handler)

        self.direct_execute(variable_name, variable_type, new_variable_name)

    def export_code(self, node_detail_form):
        variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_name", is_input_variable_name=True)
        variable_type = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_type", is_input_variable_name=True)
        new_variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_variable_name", is_input_variable_name=True)

        code = f"""
        {new_variable_name} = {variable_type}({variable_name})
        """

        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


class MathModifyVariableHandler(AbstractFunctionHandler):
    """
    Math Modify Variable Node serves to perform a mathematical operations on variables in the variable explorer.
    """
    def __init__(self):
        self.icon_type = 'MathModifyVariable'
        self.fn_name = 'Math Modify Variable'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

        self.math_function_dict = {
            "+": lambda x, y: x + y,
            "-": lambda x, y: x - y,
            "*": lambda x, y: x * y,
            "/": lambda x, y: x / y,
            "^ (power)": lambda x, y: x ** y,
            "% (mod)": lambda x, y: x % y,
            "round": lambda x, y: round(x, ndigits=y),
            "floor": lambda x, y: math.floor(x)
        }
        
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """
        Math Modify Variable Node requires 3-4 parameters to succesfully perform a mathematical operation on a variable. 
        The last parameter, New variable name, is optional in a sense that if left blank the value of the chosen 
        variable will be rewritten adequately to the performed operation. However if a new name is inserted a new 
        variable bearing the new name with the value of the old one corrected by the mathematical operation will be 
        created while preserving the old variable.
        """
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Variable name", name="variable_name",
                                          description="A name of the variable present in the variable explorer which would be used for the operation.",
                                          typ="string", example="math_var")
        self.docs.add_parameter_table_row(title="Math operation", name="math_operation",
                                          description="A math operation to be perfomed on the selected variable. It can be selected as one of the options of the combobox.",
                                          )
        self.docs.add_parameter_table_row(title="Argument", name="argument",
                                          description="An argument of the selected mathematical operation, i.e. in case of e.g. division the argument is divisor.",
                                          typ="string", example="123 | 19.98")
        self.docs.add_parameter_table_row(title="New variable name", name="new_variable_name",
                                          description="Name of the new variable whose value will be equal to the old value modifed by the selected operation. If left blank the initial variable will get overwritten.",
                                          typ="string", example="sum_a_and_b")

    def make_form_dict_list(self, *args, node_detail_form=None):

        options = ["+", "-", "*", "/", "^ (power)", "% (mod)", "round", "floor"]

        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Variable name")
        fdl.entry(name="variable_name", text="", input_types=["str", "var_name"], required=True, show_info=True, row=1)
        fdl.label("Math operation")
        fdl.combobox(name="math_operation", options=options, row=2)
        fdl.label("Argument")
        fdl.entry(name="argument", text="", input_types=["int", "float"], required=True, row=3)
        fdl.label("New variable name")
        fdl.entry(name="new_variable_name", text="", category="new_var", input_types=["str"], row=4)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def resolve_type_error_list(self, stored_var, argument_var, math_operation):
        math_function = self.math_function_dict[math_operation]
        new_value = None
        try:
            new_value = math_function(stored_var, argument_var)

        except TypeError:
            if type(stored_var) == list:
                new_value = []
                for list_element in stored_var:
                    try:
                        new_value.append(eval(str(list_element) + math_operation + str(argument_var)))
                    except TypeError:
                        new_value.append(list_element)
                    except (ZeroDivisionError, OverflowError) as e:
                        flog.error(f"Error while resolving TypeError: {e}")
                        return None

        except (ZeroDivisionError, OverflowError) as e:
            flog.error(f"Error while resolving error type: {e}")
            return None

        return (new_value)

    def direct_execute(self, variable_name, math_operation, argument, new_variable_name):
        if variable_name in variable_handler.variables:
            variable = variable_handler.variables[variable_name]
            variable_value = variable.value

            inp = Input()
            inp.assign("variable_name", variable_name)
            inp.assign("variable_value", variable_value)
            inp.assign("math_operation", math_operation)
            inp.assign("argument", eval(argument))
            inp.assign("new_variable_name", new_variable_name)

            try:
                new_value = self.input_execute(inp)

                if new_value is None:
                    # break
                    pass
            except Exception as e:
                flog.error(message=f"{e}")

            if len(inp("new_variable_name")) == 0:
                new_variable_name = variable_name

            variable_handler.new_variable(new_variable_name, new_value)
            #variable_handler.update_data_in_variable_explorer(glc)
        
    def input_execute(self, inp):
        new_value = self.resolve_type_error_list(inp("variable_value"), inp("argument"), inp("math_operation"))

        return new_value

    def execute_with_params(self, params):
        variable_name = params["variable_name"]
        math_operation = params["math_operation"]
        argument = params["argument"]
        new_variable_name = params["new_variable_name"]

        self.direct_execute(variable_name, math_operation, argument, new_variable_name)

    def execute(self, node_detail_form):
        variable_name = node_detail_form.get_chosen_value_by_name("variable_name", variable_handler)
        math_operation = node_detail_form.get_chosen_value_by_name("math_operation", variable_handler)
        argument = node_detail_form.get_chosen_value_by_name("argument", variable_handler)
        new_variable_name = node_detail_form.get_chosen_value_by_name("new_variable_name", variable_handler)

        self.direct_execute(variable_name, math_operation, argument, new_variable_name)

    def export_code(self, node_detail_form):
        variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_name", is_input_variable_name=True)
        math_operation = node_detail_form.get_chosen_value_by_name("math_operation", variable_handler)
        argument = node_detail_form.get_variable_name_or_input_value_by_element_name("argument")
        new_variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_variable_name", is_input_variable_name=True)

        math_function_dict = dict([
            ("+", lambda x, y: f"{x} + {y}"),
            ("-", lambda x, y: f"{x} - {y}"),
            ("*", lambda x, y: f"{x}*{y}"),
            ("/", lambda x, y: f"{x}/{y}"),
            ("^ (power)", lambda x, y: f"{x}**{y}"),
            ("% (mod)", lambda x, y: f"{x}%{y}"),
            ("round", lambda x, y: f"round({x}, ndigits = {y})"),
            ("floor", lambda x, y: f"math.floor({x}, ndigits = {y})")
        ])

        code = f"""  
        try:      
            {new_variable_name} = {math_function_dict[math_operation](variable_name, argument)}
        except Exception as e:
            print(e)
        """

        # return(code.format(variable_name= '"' + variable_name + '"', math_operation= '"' + math_operation + '"', new_variable_name= '"' + new_variable_name + '"', argument= '"' + argument + '"', to_show = math_function_dict[math_operation](variable_handler.variables[variable_name].value, argument)))
        return code

    def export_imports(self, *args):
        imports = ["import math"]
        return (imports)

class StringModifyVariableHandler(AbstractFunctionHandler):
    """
    String Modify Variable Node serves to perform a various operations on variables of the string type.
    """
    def __init__(self):
        self.icon_type = 'StringModifyVariable'
        self.fn_name = 'String Modify Variable'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control
        self.operation_options = ["Concatenate", "Split", "Replace", "Strip", "Lower", "Upper", 'Endswith']
        
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """
        String Modify Variable Node requires 3-5 parameters to succesfully perform a string operation on a variable. 
        Argument 2 is in some cases optional, e.g. split(argument1) does not need a second argument. The last parameter, 
        New variable name, is optional in a sense that if left blank the value of the chosen variable will be rewritten 
        adequately to the performed operation. However if a new name is inserted a new variable bearing the new name 
        with the value of the old one modified by the selected operation will be created while preserving the old 
        variable.
        """
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Variable name", name="variable_name",
                                          description="A name of the variable present in the variable explorer which would be used for the operation.",
                                          typ="string", example="str_var")
        self.docs.add_parameter_table_row(title="String operation", name="string_operation",
                                          description="A string operation to be perfomed on the selected variable. It can be selected as one of the options of the combobox.",
                                          )
        self.docs.add_parameter_table_row(title="Argument", name="argument",
                                          description="The first argument of the selected string operation.",
                                          typ="string", example="'Hello world!'")
        self.docs.add_parameter_table_row(title="Argument 2", name="argument2",
                                          description="The second argument of the selected mathematical operation.",
                                          typ="string", example="'Some other string here'")
        self.docs.add_parameter_table_row(title="New variable name", name="new_variable_name",
                                          description="Name of the new variable whose value will be equal to the old value modifed by the selected operation. If left blank the initial variable will get overwritten.",
                                          typ="string", example="str_operation_result")

    def make_form_dict_list(self, *args, node_detail_form=None):        
        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Variable name")
        fdl.entry(name="variable_name", text="", input_types=["str", "var_name"], required=True, show_info=True, row=1)
        fdl.label("String operation")
        fdl.combobox(name="string_operation", options=self.operation_options, default=self.operation_options[0], row=2)
        fdl.label("Argument")
        fdl.entry(name="argument", text="", input_types=["str", "int", "float"], row=3)
        fdl.label("Argument #2")
        fdl.entry(name="argument2", text="", input_types=["str"], row=4)
        fdl.label("New variable name")
        fdl.entry(name="new_variable_name", text="", category="new_var", input_types=["str"], row=5)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl
    
    def _concatenate_argument_to_string_variable(self, string_variable:LocalVariable, argument, new_variable_name:str, convert_numerics:bool):
        variable_or_argument_is_a_number = type(string_variable.value) in [int, float] or type(argument) in [int, float]
        
        if variable_or_argument_is_a_number and not convert_numerics:
            flog.warning('Trying to concatenate numeric and non-numeric values')
            self._show_concatenate_warning_pop_up(string_variable.name, "Concatenate", argument, None, new_variable_name)
        else:
            new_value = str(string_variable.value) + str(argument)
            return new_value
                
    def _split_string_variable_by_argument(self, string_variable:LocalVariable, argument:str):
        try:
            if argument == "":
                new_value = string_variable.value.split()
            else:
                new_value = string_variable.value.split(argument)  # OUTPUT is list
            
            return new_value
        except AttributeError:
            flog.error("AttributeError Exception Raised")

    def str_modify_existing_variable(self, variable_name, str_operation, argument, argument2, new_variable_name, convert_numerics=False):
        assert str_operation in self.operation_options, f'Operation "{str_operation}" is not in operation options: {self.operation_options}'

        local_variable = variable_handler.get_local_variable_by_name(variable_name)
        
        if local_variable is None:
            #? Should we maybe notify the user as well (through a pop-up or something)?
            flog.warning(f'{self.icon_type}: non-existent variable name provided as an input.')
            return
        
        new_value = None # Just to be sure that new_value is always defined somehow
        
        if str_operation == "Concatenate":
            new_value = self._concatenate_argument_to_string_variable(local_variable, argument, new_variable_name, convert_numerics)
        elif str_operation == "Split":
            new_value = self._split_string_variable_by_argument(local_variable, argument)
        elif str_operation == "Replace":
            new_value = local_variable.value.replace(argument, argument2)
        elif str_operation == "Strip":
            new_value = local_variable.value.strip()
        elif str_operation == "Lower":
            new_value = local_variable.value.lower()
        elif str_operation == "Upper":
            new_value = local_variable.value.upper()
        elif str_operation == "Endswith":
            new_value = local_variable.value.endswith(argument)

        if len(new_variable_name) > 0:
            variable_handler.new_variable(new_variable_name, new_value)
        else:
            variable_handler.new_variable(local_variable.name, new_value)

    def _show_concatenate_warning_pop_up(self, variable_name, str_operation, argument, argument2, new_variable_name):
        params_dict = {
            'variable_name': {'variable': None, 'value': variable_name},
            'str_operation': {'variable': None, 'value': str_operation},
            'argument': {'variable': None, 'value': argument},
            'argument2': {'variable': None, 'value': argument2},
            'new_variable_name': {'variable': None, 'value': new_variable_name}
        }

        ncrb.new_popup([500, 400], "ConcatenationWarningPopup", params_dict)

    def direct_execute(self, variable_name, string_operation, argument, argument2, new_variable_name):

        self.str_modify_existing_variable(variable_name, string_operation, argument, argument2, new_variable_name)
    
    def input_execute(self, inp):

        for i, stored_variable in enumerate(variable_handler.variables.values()):
            if stored_variable.name == variable_handler:


                if inp("string_operation") == "Concatenate":
                    new_value = str(stored_variable.value) + str(inp("string_operation"))
                elif inp("string_operation") == "Split":
                    if inp("argument") == "":
                        new_value = stored_variable.value.split()
                    else:
                        new_value = stored_variable.value.split(inp("argument"))  # OUTPUT is list
                elif inp("string_operation") == "Replace":
                    new_value = stored_variable.value.replace(inp("argument"), inp("argument2"))
                elif inp("string_operation") == "Strip":
                    new_value = stored_variable.value.strip()
                elif inp("string_operation") == "Lower":
                    new_value = stored_variable.value.lower()
                elif inp("string_operation") == "Upper":
                    new_value = stored_variable.value.upper()
        
        return new_value


    def execute_with_params(self, params, item_detail_form=None):
        variable_name = item_detail_form.get_chosen_value_by_name("variable_name", variable_handler)
        string_operation = params["string_operation"]
        argument = item_detail_form.get_chosen_value_by_name("argument", variable_handler)
        argument2 = params["argument2"]
        new_variable_name = params["new_variable_name"]

        self.direct_execute(variable_name, string_operation, argument, argument2, new_variable_name)

    def execute(self, node_detail_form):
        variable_name = node_detail_form.get_chosen_value_by_name("variable_name", variable_handler)
        string_operation = node_detail_form.get_chosen_value_by_name("string_operation", variable_handler)
        argument = node_detail_form.get_chosen_value_by_name("argument", variable_handler)
        argument2 = node_detail_form.get_chosen_value_by_name("argument2", variable_handler)
        new_variable_name = node_detail_form.get_chosen_value_by_name("new_variable_name", variable_handler)

        self.direct_execute(variable_name, string_operation, argument, argument2, new_variable_name)

    def export_code(self, node_detail_form):
        variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_name", is_input_variable_name=True)
        string_operation = node_detail_form.get_chosen_value_by_name("string_operation", variable_handler)
        argument = node_detail_form.get_variable_name_or_input_value_by_element_name("argument")
        argument2 = node_detail_form.get_variable_name_or_input_value_by_element_name("argument2")
        new_variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_variable_name", is_input_variable_name=True)

        string_function_dict = {
            "Concatenate": lambda var, arg1, arg2: f"str({var}) + str({arg1})",
            "Split": lambda var, arg1, arg2: f"{var}.split()",
            "Replace": lambda var, arg1, arg2: f"{var}.replace({arg1}, {arg2})",
            "Strip": lambda var, arg1, arg2: f"{var}.strip()",
            "Lower": lambda var, arg1, arg2: f"{var}.lower()",
            "Upper": lambda var, arg1, arg2: f"{var}.upper()"
        }

        code = f"""
        {new_variable_name} = {string_function_dict[string_operation](variable_name, argument, argument2)}
        """

        # return(code.format(variable_name= '"' + variable_name + '"', string_operation= '"' + string_operation + '"', argument= '"' + argument + '"', argument2= '"' + argument2 + '"', new_variable_name= '"' + new_variable_name + '"'))
        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


class ListModifyVariableHandler(AbstractFunctionHandler):
    """
    List Modify Variable Node serves to perform a various operations on lists.
    """
    def __init__(self):
        self.icon_type = 'ListModifyVariable'
        self.fn_name = 'List Modify Variable'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

        self.list_operations = {
            "Get Element": lambda var, arg: var[arg],
            "Append": lambda var, arg: var.append(arg),
            "Remove": lambda var, arg: var.remove(arg),
            "Pop": lambda old_list, arg: old_list.pop(arg),
            "Index": lambda var, arg: var.index(arg),
            "Join Lists (Concat)": self._join_lists,
            "Difference Lists": self._difference_lists,
            "Find Duplicates": self._find_duplicates,
            "Deduplicate": lambda var, arg: list(set(var)),
            "Filter Substrings": self._filter_substring_occurences,
            "Join Elements": self._join_elements_in_string,
        }
        
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """
        List Modify Variable Node requires 3-4 parameters to succesfully perform an operation on a stored list. 
        The last parameter, New variable name, is optional in a sense that if left blank the value of the chosen 
        variable will be rewritten adequately to the performed operation. However if a new name is inserted a new 
        variable bearing the new name with the value of the old one modified by the selected operation will be created 
        while preserving the old variable.
        """
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Variable name", name="variable_name",
                                          description="A name of the variable (list) present in the variable explorer which would be used for the operation.",
                                          typ="string", example="str_var")
        self.docs.add_parameter_table_row(title="List operation", name="list_operation",
                                          description="A list operation to be perfomed on the selected variable. It can be selected as one of the options of the combobox.",
                                          )
        self.docs.add_parameter_table_row(title="Argument", name="argument",
                                          description="The first argument of the selected list operation.",
                                          typ="string", example="'Hello world!'")
        self.docs.add_parameter_table_row(title="New variable name", name="new_variable_name",
                                          description="Name of the new variable whose value will be equal to the old value modifed by the selected operation. If left blank the initial variable will get overwritten.",
                                          typ="string", example="str_operation_result")

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = list(self.list_operations.keys())

        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Variable name")
        fdl.entry(
            name="variable_name", text="", category="instance_var", input_types=["str", "var_name"],
            required=True, row=1
        )
        fdl.label("List operation")
        fdl.combobox(name="list_operation", options=options, row=2)
        fdl.label("Argument")
        fdl.entry(name="argument", text="", category="arguments", row=3)
        fdl.label("New variable name")
        fdl.entry(name="new_variable_name", text="", input_types=["str"], category="new_var", row=4)
        fdl.button(
            function=self.execute, function_args=node_detail_form, text="Execute", focused=True
        )

        return fdl

    def execute(self, node_detail_form):
        variable_name = node_detail_form.get_chosen_value_by_name("variable_name", variable_handler)
        list_operation = node_detail_form.get_chosen_value_by_name(
            "list_operation", variable_handler
        )
        argument = node_detail_form.get_chosen_value_by_name("argument", variable_handler)
        new_variable_name = node_detail_form.get_chosen_value_by_name(
            "new_variable_name", variable_handler
        )

        self.direct_execute(variable_name, list_operation, argument, new_variable_name)

    def execute_with_params(self, params, item_detail_form=None):
        variable_name = params["variable_name"]
        list_operation = params["list_operation"]
        argument = item_detail_form.get_chosen_value_by_name("argument", variable_handler)
        new_variable_name = params["new_variable_name"]

        self.direct_execute(variable_name, list_operation, argument, new_variable_name)

    def direct_execute(
        self, variable_name: str, list_operation: str, argument: str, new_variable_name: str
    ) -> None:
        if variable_name not in variable_handler.variables:
            raise CriticalPipelineError(f"Variable '{variable_name}' not found in Variables.")

        variable = variable_handler.variables[variable_name]
        variable_value = deepcopy(variable.value)
        argument = self._evaluate_argument(argument)

        inp = Input()
        inp.assign("list_variable", variable_value)
        inp.assign("list_operation", self.list_operations.get(list_operation))
        inp.assign("argument", argument)

        try:
            list_operation_result, updated_list = self.input_execute(inp)
        except Exception as e:
            raise CriticalPipelineError("ListModifyVariable handler failed to execute: "+str(e)) from e

        # 'updated_list' is always to be saved under 'variable_name' 
        if updated_list != variable.value:
            variable_handler.new_variable(variable_name, updated_list)
        # 'list_operation_result' is always to be saved under 'new_variable_name'
        if len(new_variable_name) != 0:
            variable_handler.new_variable(new_variable_name, list_operation_result)

    def input_execute(self, inp: Input) -> tuple[Any, list]:
        list_operation_result = inp("list_operation")(inp("list_variable"), inp("argument"))

        return list_operation_result, inp("list_variable")

    def _evaluate_argument(self, argument: str) -> Any:
        """Evaluate and cast if the 'argument' is of python type or return it without a change."""
        try:
            return ast.literal_eval(argument)
        except (ValueError, SyntaxError, TypeError):
            return argument

    def _join_lists(self, list_variable: list, argument: list) -> list:
        new_value = None

        if type(argument) == list:
            new_value = list_variable + argument
        else:
            for j, stored_variable in enumerate(variable_handler.variables.values()):
                if stored_variable.name == argument:
                    new_value = list_variable + stored_variable.value

        return new_value

    def _difference_lists(self, list_variable: list, argument: Any) -> list:
        new_value = None

        if type(argument) == list:
            new_value = list(set(list_variable) - set(argument))
        else:
            for j, stored_variable in enumerate(variable_handler.variables.values()):
                if stored_variable.name == argument:
                    new_value = list(set(list_variable) - set(stored_variable.value))

        return new_value

    def _find_duplicates(self, list_variable: list, *args: list) -> list:
        seen = set()
        duplicates = []
        for x in list_variable:
            if x in seen:
                duplicates.append(x)
            seen.add(x)
        duplicates = list(set(duplicates))  # deduplicate duplicates :)

        return duplicates

    def _filter_substring_occurences(self, list_variable: list, argument: list) -> list:
        """filters list based on occurence of a specific substring"""
        result = [x for x in list_variable if argument not in x]
        return result

    def _join_elements_in_string(self, list_variable: Iterable[str], argument: str) -> str:
        """joins elements in one particular string based on joining delimiter"""
        result = argument.join(list_variable)
        return result

    def export_code(self, node_detail_form):
        variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_name", is_input_variable_name=True)
        list_operation = node_detail_form.get_chosen_value_by_name("list_operation", variable_handler)
        argument = node_detail_form.get_variable_name_or_input_value_by_element_name("argument")
        new_variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_variable_name", is_input_variable_name=True)

        simple_list_function_dict = {
            "Get Element": lambda var, arg: f"{var}[int({arg})]",
            "Index": lambda var, arg: f"{var}.index(eval({arg}))",
            # "Concatenate": lambda var, arg: f"{var} + eval({arg}))",
            "Join Lists (Concat)": lambda var, arg: f"{var} + {arg})",
            "Difference Lists": lambda var, arg: f"list(set({var}) - set({arg}))"
        }

        list_function_dict = {
            "Append": lambda var, arg: f"{var}.append({arg})",
            "Remove": lambda var, arg: f"{var}.remove(int({arg}))",
            "Pop": lambda var, arg: f"{var}.remove(int({arg}))"
        }

        if list_operation in simple_list_function_dict.keys():
            code = f"""
        {new_variable_name} = {simple_list_function_dict[list_operation](variable_name, argument)}
        """
        elif list_operation in list_function_dict.keys():
            code = f"""
        init_list = {variable_name}.copy()
        {list_function_dict[list_operation]("init_list", argument)}
        {new_variable_name} = init_list.copy()
        """
        elif list_operation == "Find Duplicates":
            code = f"""
        seen = set()
        dupl = []
        for x in {variable_name}:
            if x in seen:
                dupl.append(x)
            seen.add(x)
        dupl = list(set(dupl)) 
        {new_variable_name} = dupl
        """
        elif list_operation == "Deduplicate":
            code = f"""
        seen = set()
        uniq = []
        for x in {variable_name}:
            if x not in seen:
                uniq.append(x)
                seen.add(x)
        {new_variable_name} = uniq
        """
        else:
            code = ""

            # return(code.format(variable_name= '"' + variable_name + '"', list_operation= '"' + list_operation + '"', argument= '"' + argument + '"', new_variable_name= '"' + new_variable_name + '"'))
        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


class DictionaryModifyVariableHandler(AbstractFunctionHandler):
    """
    Dictionary Modify Variable Node serves to perform a various operations on dictionaries.
    """
    def __init__(self):
        self.icon_type = 'DictionaryModifyVariable'
        self.fn_name = 'Dictionary Modify Variable'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """
        Dictionary Modify Variable Node requires 2-4 parameters to succesfully perform an operation on a stored dictionary. 
        *Argument* is required only for the *Get value by key* function. The last parameter, *New variable name*, is 
        optional in a sense that if left blank the value of the chosen variable will be rewritten adequately to the 
        performed operation. However if a new name is inserted a new variable bearing the new name with the value of 
        the old one modified by the selected operation will be created while preserving the old variable.
        """
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Variable name", name="variable_name",
                                          description="A name of the variable (dictionary) present in the variable explorer which would be used for the operation.",
                                          typ="string", example="dict_var")
        self.docs.add_parameter_table_row(title="List operation", name="dictionary_operation",
                                          description="A string operation to be perfomed on the selected variable. It can be selected as one of the options of the combobox.",
                                          )
        self.docs.add_parameter_table_row(title="Argument", name="argument",
                                          description="A first argument of a given operation (can be left blank - get keys, get values).",
                                          typ="Any", example="'name' | 'key_1'")
        self.docs.add_parameter_table_row(title="Argument 2", name="argument2",
                                          description="A second argument of agiven operation (can be left blank).",
                                          typ="Any", example="'new value' | [1,2,3] | {'name': 'John'}")
        self.docs.add_parameter_table_row(title="New variable name", name="new_variable_name",
                                          description="Name of the new variable whose value will be equal to the old value modifed by the selected operation. If left blank the initial variable will get overwritten.",
                                          typ="string", example="dict_operation_result")

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["Get Value By Key", "Keys", "Values", "Join Dictionaries", "Delete Value by Key", "Invert Dictionary", "Add key"]

        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Variable name")
        fdl.entry(name="variable_name", text="", input_types=["str", "var_name"], required=True, row=1)
        fdl.label("Dictionary operation")
        fdl.combobox(name="dictionary_operation", options=options, row=2)
        fdl.label("Argument 1")
        fdl.entry(name="argument", text="", row=3)
        fdl.label("Argument 2")
        fdl.entry(name="argument2", text="", row=4)
        fdl.label("New variable name")
        fdl.entry(name="new_variable_name", text="", category="new_var", input_types=["str"], row=5)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl
    
    def _get_value_by_key(self, d: dict, key: Hashable, *args: Any):        
        if not isinstance(key, Hashable):
            raise CriticalPipelineError(
                f"{self.icon_type}: provided dictionary key is unhashable."
            )

        return d.get(key)

    def _join_dictionaries(self, d_1: dict, d_2: dict, *args):
        if isinstance(d_2, dict):
            raise CriticalPipelineError(f"{self.icon_type}: Both arguments must of type 'dict'.")
        
        return {**d_1, **d_2}

    def _delete_dict_entry(self, d: dict, key: Hashable, *args):
        if not isinstance(key, Hashable):
            raise CriticalPipelineError(
                f"{self.icon_type}: provided dictionary key is unhashable."
            )
            
        pop_value = d.pop(key, None)

        return pop_value

    def _add_dict_entry(self, d: dict, key: Hashable, value: Any, *args):        
        if not isinstance(key, Hashable):
            raise CriticalPipelineError(
                f"{self.icon_type}: provided dictionary key is unhashable."
            )
        
        d[key] = value

        return d

    def _invert_dictionary(self, d: dict, *args):        
        has_unique_values = len(d) == len(set(d.values()))
        if not has_unique_values:
            raise SoftPipelineError(
                f"{self.icon_type}: provided dictionary must have unique values."
            )
        
        all_values_hashable = all(isinstance(x, Hashable) for x in d.values())
        if not all_values_hashable:
            raise SoftPipelineError(f"{self.icon_type}: all dictionary values must be hashable.")
        
        return {v: k for k, v in d.items()}   

    def dict_modify_existing_variable(self, variable_name, dict_operation, argument, argument2, new_variable_name):
        functions = {
            "Get Value By Key": self._get_value_by_key,
            "Keys": lambda variable, argument, argument2: list(variable.keys()),
            "Values": lambda variable, argument, argument2: list(variable.values()),
            "Join Dictionaries": self._join_dictionaries,
            "Delete Value by Key": self._delete_dict_entry,
            "Invert Dictionary": self._invert_dictionary,
            "Add key" : self._add_dict_entry
        }        

        for i, stored_variable in enumerate(variable_handler.variables.values()):
            if stored_variable.name == variable_name:

                dict_copy = stored_variable.value.copy()

                dict_function = functions[dict_operation]
                new_value = dict_function(dict_copy, argument, argument2)

                if len(new_variable_name) == 0:
                    new_variable_name = variable_name

                if new_value is not None:
                    variable_handler.new_variable(new_variable_name, new_value)

                break


    def direct_execute(self, variable_name, dictionary_operation, argument, argument2, new_variable_name):
        self.dict_modify_existing_variable(variable_name, dictionary_operation, argument, argument2, new_variable_name)
        #variable_handler.update_data_in_variable_explorer(glc)
    
    #? Why is this here if it's wrong?
    # TODO: Refactor or delete.
    # def input_execute_wrong(self, functions, inp): #probably wrong
    #     try:
    #         dict_operation_result, updated_variable_value = self.input_execute(inp)
    #     except Exception as e:
    #         flog.error("Error in Dict Modify Variable")
    #         return None

    #     if dict_operation_result:
    #         if len(new_variable_name) == 0:
    #             new_variable_name = "dict_operation_result"

    #         variable_handler.new_variable(new_variable_name, dict_operation_result)
    #     variable_handler.new_variable(inp("variable_name"), updated_variable_value)
    #     #variable_handler.update_data_in_variable_explorer(glc)
    #     """
    #     self.dict_modify_existing_variable(variable_name, dict_operation, argument, new_variable_name)
    #     #variable_handler.update_data_in_variable_explorer(glc)
    #     """
    
    def input_execute(self, inp):
        new_value = inp("dict_operation")(inp("variable_value"), inp("argument"))

        return new_value, inp("variable_value")


    def execute_with_params(self, params):
        variable_name = params["variable_name"]
        dictionary_operation = params["dictionary_operation"]
        argument = params["argument"]
        argument2 = params["argument2"]
        new_variable_name = params["new_variable_name"]

        self.direct_execute(variable_name, dictionary_operation, argument, argument2, new_variable_name)

    def execute(self, node_detail_form):
        variable_name = node_detail_form.get_chosen_value_by_name("variable_name", variable_handler)
        dictionary_operation = node_detail_form.get_chosen_value_by_name("dictionary_operation", variable_handler)
        argument = node_detail_form.get_chosen_value_by_name("argument", variable_handler)
        argument2 = node_detail_form.get_chosen_value_by_name("argument2", variable_handler)
        new_variable_name = node_detail_form.get_chosen_value_by_name("new_variable_name", variable_handler)

        self.direct_execute(variable_name, dictionary_operation, argument, argument2, new_variable_name)

    def export_code(self, node_detail_form):
        variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("variable_name", is_input_variable_name=True)
        dictionary_operation = node_detail_form.get_chosen_value_by_name("dictionary_operation", variable_handler)
        argument = node_detail_form.get_variable_name_or_input_value_by_element_name("argument")
        argument2 = node_detail_form.get_variable_name_or_input_value_by_element_name("argument2")
        new_variable_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_variable_name", is_input_variable_name=True)

        dict_function_dict = {
            "Get Value By Key": lambda var, arg, arg2: f"{var}[{arg}]",
            "Keys": lambda var, arg, arg2: f"list({var}.keys())",
            "Values": lambda var, arg, arg2: f"list({var}.values())",
            "Join Dictionaries": lambda var, arg, arg2: f"{{**{var}, **{arg}}}",
            "Delete Value by Key": lambda var, arg, arg2: f"{var}.pop('{arg}')",
            "Invert Dictionary": lambda var, arg, arg2: f"{{v: k for k, v in {var}.items()}}",
            "Add key" : lambda var, arg, arg2: f"{var}[{arg}] = {arg2}"
        }

        code = f"""
        {dict_function_dict[dictionary_operation](variable_name, argument, argument2)}
        """
        
        if dictionary_operation not in ["Delete Value by Key", "Add key"]:
            code = f"{new_variable_name} = {code.strip()}" # Add new variable initialization for the cases where it makes sense

        return code

    def export_imports(self, *args):
        imports = []
        return (imports)


class PrintVariableHandler(AbstractFunctionHandler):
    """
    Takes a variable as an input and prints it into a console.
    """

    def __init__(self):
        self.is_disabled = True # FIXME: No FE prepared for the user --> for the user it seems broken
        self.icon_type = "PrintVariable"
        self.fn_name = "Print Variable"

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control
        self._init_docs()

        super().__init__()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)
        self.docs.add_parameter_table_row(title="Variable name", name="variable_name",
                                          description="A name of the variable to be printed.",
                                          typ="string", example="my_awesome_var")

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Variable name")
        fdl.entry(name="variable_name", text="", input_types=["str", "var_name"], required=True, row=1)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def direct_execute(self, variable_name):

        inp = Input()
        inp.assign("variable_name", variable_name)

        self.input_execute(inp)

        """
        print(f'Variable value form PrintVariable icon: {variable_name}')
        """
    
    def input_execute(self, inp):
        print(f'Variable value form PrintVariable icon: {inp("variable_name")}')

    def execute_with_params(self, params, item_detail_form):
        variable_value = item_detail_form.get_chosen_value_by_name("variable_name", variable_handler)

        self.direct_execute(variable_value)

    def execute(self, node_detail_form):
        variable_name = node_detail_form.get_chosen_value_by_name("variable_name", variable_handler)

        self.direct_execute(variable_name)

    def export_code(self, *args):
        """TODO"""
        code = """  """

        return code

    def export_imports(self, *args):
        """TODO"""
        imports = []

        return imports



variable_handlers_dict = {
    "NewVariable": NewVariableHandler(),
    "ConvertVariableType": ConvertVariableTypeHandler(),
    "MathModifyVariable": MathModifyVariableHandler(),
    "StringModifyVariable": StringModifyVariableHandler(),
    "ListModifyVariable": ListModifyVariableHandler(),
    "DictionaryModifyVariable": DictionaryModifyVariableHandler(),
    "PrintVariable": PrintVariableHandler()
}
