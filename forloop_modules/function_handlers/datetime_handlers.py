import ast
import datetime

from dateutil.relativedelta import relativedelta
from dateutil import parser

import forloop_modules.flog as flog

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input
from forloop_modules.function_handlers.auxilliary.auxiliary_functions import parse_comboentry_input

####### PROBLEMATIC IMPORTS TODO: REFACTOR #######
#from src.gui.item_detail_form import ItemDetailForm #independent on GLC - but is Frontend -> Should separate to two classes
#from src.gui.gui_layout_context import glc
####### PROBLEMATIC IMPORTS TODO: REFACTOR #######


def convert_timestamp_to_datetime_if_needed(value):
    if type(value) == int or type(value) == float:
        value = datetime.datetime.fromtimestamp(value)

    return value


class DatetimeNowHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'DatetimeNow'
        self.fn_name = 'Datetime Now'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["datetime", "timestamp"]
        
        fdl = FormDictList()
        fdl.label("Datetime Now & Timestamp")
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=1)
        fdl.label("Format")
        fdl.combobox(name="datetime_format", options=options, default="timestamp", row=2)
        fdl.label("Add decimals")
        fdl.checkbox(name="add_decimal", bool_value=False, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        datetime_format = node_detail_form.get_chosen_value_by_name("datetime_format", variable_handler)
        add_decimal = node_detail_form.get_chosen_value_by_name("add_decimal", variable_handler)

        self.direct_execute(new_var_name, datetime_format, add_decimal)

    def direct_execute(self, new_var_name, datetime_format, add_decimal):

        inp = Input()
        inp.assign("datetime_format", datetime_format)
        inp.assign("add_decimal", add_decimal)

        now = self.input_execute(inp)

        variable_handler.new_variable(new_var_name, now)  
        #variable_handler.update_data_in_variable_explorer(glc)
        
    def input_execute(self, inp):
        now = datetime.datetime.now(datetime.timezone.utc)

        if inp("datetime_format") == "timestamp":
            if inp("add_decimal"):
                now = now.timestamp()
            else:
                now = int(now.timestamp())
        
        return now

    def export_code(self, node_detail_form):
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)
        datetime_format = node_detail_form.get_chosen_value_by_name("datetime_format", variable_handler)
        add_decimal = node_detail_form.get_chosen_value_by_name("add_decimal", variable_handler)

        if datetime_format == "datetime":
            code = f"""
            {new_var_name} = datetime.datetime.now()
            """
        else:
            code = f"""
            {new_var_name} = datetime.datetime.now().timestamp()
            """
            if not add_decimal:
                code = f'int({code})'

        return code

    def export_imports(self, *args):
        imports = ["import datetime"]
        return (imports)


class NewDatetimeHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'NewDatetime'
        self.fn_name = 'New Datetime'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        current_year = datetime.datetime.now().year
        hours_set = list(range(0, 23, 1))
        minutes_and_secons_set = list(range(0, 59, 1))
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=1)
        fdl.label("Year")
        fdl.entry(name="year", text=str(current_year), input_types=["int"], required=True, row=2)
        fdl.label("Month")
        fdl.entry(name="month", text="1", input_types=["int"], required=True, row=3)
        fdl.label("Day")
        fdl.entry(name="day", text="1", input_types=["int"], required=True, row=4)
        fdl.label("Hour")
        fdl.combobox(name="hour", options=hours_set, default=hours_set[0], row=5)
        fdl.label("Minute")
        fdl.combobox(name="minute", options=minutes_and_secons_set, default=minutes_and_secons_set[0], row=6)
        fdl.label("Second")
        fdl.combobox(name="second", options=minutes_and_secons_set, default=minutes_and_secons_set[0], row=7)
        fdl.button(name="execute", function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)
        year = node_detail_form.get_chosen_value_by_name("year", variable_handler)
        month = node_detail_form.get_chosen_value_by_name("month", variable_handler)
        day = node_detail_form.get_chosen_value_by_name("day", variable_handler)
        hour = node_detail_form.get_chosen_value_by_name("hour", variable_handler)
        minute = node_detail_form.get_chosen_value_by_name("minute", variable_handler)
        second = node_detail_form.get_chosen_value_by_name("second", variable_handler)

        self.direct_execute(new_var_name, year, month, day, hour, minute, second)

    def execute_with_params(self, params):
        
        new_var_name = params["new_var_name"]
        year = int(params["year"])
        month = int(params["month"])
        day = int(params["day"])

        time_keys = ["hour", "minute", "second", "microsecond"]
        time_entries = {}

        for time_key in time_keys:
            try:
                time_entries[time_key] = int(params[time_key])
            except:
                time_entries[time_key] = 0

        self.direct_execute(new_var_name, year, month, day, hour=time_entries["hour"], minute=time_entries["minute"], second=time_entries["second"], microsecond=time_entries["microsecond"])

    def direct_execute(self, new_var_name, year, month, day, hour, minute, second):
        hour = hour or 0
        minute = minute or 0
        second = second or 0

        inp = Input()
        inp.assign("year", int(year))
        inp.assign("month", int(month))
        inp.assign("day", int(day))
        inp.assign("hour", int(hour))
        inp.assign("minute", int(minute))
        inp.assign("second", int(second))

        new_datetime = self.input_execute(inp)
        
        variable_handler.new_variable(new_var_name, new_datetime)
        ##variable_handler.update_data_in_variable_explorer(glc)
    
    def input_execute(self, inp):
        new_datetime = datetime.datetime(inp("year"), inp("month"), inp("day"), inp("hour"), inp("minute"), 
                                         inp("second"), tzinfo=datetime.timezone.utc)

        return new_datetime

    def export_code(self, node_detail_form):
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)
        year = node_detail_form.get_variable_name_or_input_value_by_element_name("year")
        month = node_detail_form.get_variable_name_or_input_value_by_element_name("month")
        day = node_detail_form.get_variable_name_or_input_value_by_element_name("day")

        time_keys = ["hour", "minute", "second", "microsecond"]
        time_entries = {}

        for time_key in time_keys:
            try:
                time_entries[time_key] = node_detail_form.get_chosen_value_by_name(time_key, variable_handler)
            except:
                time_entries[time_key] = 0

        code = f"""
        {new_var_name} = datetime.datetime(year={year}, month={month}, day={day}, hour={time_entries['hour']}, minute={time_entries['minute']}, second={time_entries['second']}, microsecond={time_entries['microsecond']})
        """

        return code

    def export_imports(self, *args):
        imports = ["import datetime"]
        return (imports)
    
    # Now without use but could be useful in future when entries support additional value checking
    def _check_if_year_is_leap_year(self, year) -> bool:
        """
        To determine whether a year is a leap year, follow these steps:

            1. If the year is evenly divisible by 4, go to step 2. Otherwise, go to step 5.
            2. If the year is evenly divisible by 100, go to step 3. Otherwise, go to step 4.
            3. If the year is evenly divisible by 400, go to step 4. Otherwise, go to step 5.
            4. The year is a leap year (it has 366 days).
            5. The year is not a leap year (it has 365 days).

        """

        if year%4 == 0:
            if year%100 == 0:
                if year%400 == 0:
                    is_leap_year = True
                else:
                    is_leap_year = False
            else:
                is_leap_year = True
        else:
            is_leap_year = False

        return is_leap_year


class DatetimeDifferenceHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'DatetimeDifference'
        self.fn_name = 'Datetime Difference'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["years", "months", "days", "hours", "minutes", "seconds", "microseconds"]
        
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Datetime 1")
        fdl.entry(name="datetime1", text="", input_types=["datetime", "int", "float"], required=True, row=1)
        fdl.label("Datetime 2")
        fdl.entry(name="datetime2", text="", input_types=["datetime", "int", "float"], required=True, row=2)
        fdl.label("Format")
        fdl.combobox(name="difference_format", options=options, default="minutes", row=3)
        fdl.label("Add decimals")
        fdl.checkbox(name="add_decimal", bool_value=False, row=4)
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=5)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        datetime1 = node_detail_form.get_chosen_value_by_name("datetime1", variable_handler)
        datetime2 = node_detail_form.get_chosen_value_by_name("datetime2", variable_handler)
        difference_format = node_detail_form.get_chosen_value_by_name("difference_format", variable_handler)
        add_decimal = node_detail_form.get_chosen_value_by_name("add_decimal", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(new_var_name, datetime1, datetime2, difference_format, add_decimal)

    def direct_execute(self, new_var_name, datetime1, datetime2, difference_format, add_decimal):

        transformations = {
            "years": lambda x,y: relativedelta(y, x).years, 
            "months": lambda x,y: relativedelta(y, x).years*12 + relativedelta(y, x).months,  
            "days": lambda x,y: (y-x).days, 
            "hours": lambda x,y: (y-x).total_seconds()/3600, 
            "minutes": lambda x,y: (y-x).total_seconds()/60, 
            "seconds": lambda x,y: (y-x).total_seconds(), 
            "microseconds": lambda x,y: (y-x).total_seconds()*10**6
            }
        inp = Input()
        inp.assign("datetime1", datetime1)
        inp.assign("datetime2", datetime2)
        inp.assign("difference_format", difference_format)
        inp.assign("add_decimal", add_decimal)
        inp.assign("transformations", transformations)

        diff = self.input_execute(inp)
        
        variable_handler.new_variable(new_var_name, diff)
        #variable_handler.update_data_in_variable_explorer(glc)
    
    def input_execute(self, inp):
        datetime1 = convert_timestamp_to_datetime_if_needed(inp("datetime1"))
        datetime2 = convert_timestamp_to_datetime_if_needed(inp("datetime2"))

        diff = inp("transformations")[inp("difference_format")](datetime1, datetime2)

        if not inp("add_decimal"):
            diff = int(diff)
        
        return diff


    def export_code(self, node_detail_form):
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)
        datetime1 = node_detail_form.get_variable_name_or_input_value_by_element_name("datetime1")
        datetime2 = node_detail_form.get_variable_name_or_input_value_by_element_name("datetime2")
        difference_format = node_detail_form.get_chosen_value_by_name("difference_format", variable_handler)
        add_decimals = node_detail_form.get_chosen_value_by_name("add_decimal", variable_handler)
        
        transformation_strings = {
            "years": lambda x,y: f'relativedelta({y}, {x}).years', 
            "months": lambda x,y: f'relativedelta({y}, {x}).years*12 + relativedelta({y}, {x}).months',  
            "days": lambda x,y: f'({y}-{x}).days', 
            "hours": lambda x,y: f'({y}-{x}).total_seconds()/3600', 
            "minutes": lambda x,y: f'({y}-{x}).total_seconds()/60', 
            "seconds": lambda x,y: f'({y}-{x}).total_seconds()', 
            "microseconds": lambda x,y: f'({y}-{x}).total_seconds()*10**6'
            }

        code = f"""
        {new_var_name} = {transformation_strings[difference_format](datetime1, datetime2)}
        """
        
        if not add_decimals:
            code = f'int({code.strip()})'

        return code

    def export_imports(self, *args):
        imports = ["from dateutil import relativedelta"]
        return (imports)


class DatetimeValueHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'DatetimeValue'
        self.fn_name = 'Datetime Value'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["date", "time", "year", "month", "day", "hour", "minute", "second", "microsecond"]
        
        fdl = FormDictList()
        fdl.label("Get Date or Time Value from Datetime")
        fdl.label("Datetime variable")
        fdl.entry(name="datetime_var", text="", input_types=["datetime", "int", "float"], required=True, row=1)
        fdl.label("Return")
        fdl.combobox(name="value_to_get", options=options, default="time", row=2)
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)        
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        datetime_var = node_detail_form.get_chosen_value_by_name("datetime_var", variable_handler)
        value_to_get = node_detail_form.get_chosen_value_by_name("value_to_get", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(datetime_var, value_to_get, new_var_name)

    def direct_execute(self, datetime_var, value_to_get, new_var_name):
        apply_format = {
            "date": lambda x: x.date(),
            "time": lambda x: x.time(),
            "year": lambda x: x.year, 
            "month": lambda x: x.month, 
            "day": lambda x: x.day, 
            "hour": lambda x: x.hour, 
            "minute": lambda x: x.minute, 
            "second": lambda x: x.second, 
            "microsecond": lambda x: x.microsecond
        }

        inp = Input()
        inp.assign("datetime_var", datetime_var)
        inp.assign("value_to_get", value_to_get)
        inp.assign("apply_format", apply_format)

        try:
            extracted_value = self.input_execute(inp)
        except Exception as e:
            flog.error(message=f"{e}")
        
        variable_handler.new_variable(new_var_name, extracted_value)
        #variable_handler.update_data_in_variable_explorer(glc)

    def input_execute(self, inp):

        datetime_var = convert_timestamp_to_datetime_if_needed(inp("datetime_var"))

        extracted_value = inp("apply_format")[inp("value_to_get")](datetime_var)

        return extracted_value

    def export_code(self, node_detail_form):
        datetime_var = node_detail_form.get_variable_name_or_input_value_by_element_name("datetime_var")
        value_to_get = node_detail_form.get_chosen_value_by_name("value_to_get", variable_handler)
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        year = datetime_var.year
        month = datetime_var.month
        day = datetime_var.day
        hour = datetime_var.hour
        minute = datetime_var.minute
        second = datetime_var.second
        microsecond = datetime_var.microsecond

        if value_to_get in ["date", "time"]:
            code = f"""
            datetime_var = datetime.datetime({year}, {month}, {day}, {hour}, {minute}, {second}, {microsecond})
            {new_var_name} = datetime_var.{value_to_get}()
            """
        else:
            code = f"""
            datetime_var = datetime.datetime({year}, {month}, {day}, {hour}, {minute}, {second}, {microsecond})
            {new_var_name} = datetime_var.{value_to_get}
            """

        return code

    def export_imports(self, *args):
        imports = ["import datetime"]
        return (imports)


class DatetimeToStringHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'DatetimeToString'
        self.fn_name = 'Datetime To String'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["yyyy/mm/dd h:m:s", "dd.mm.yyyy h:m", "mm/dd/yy"]
        
        fdl = FormDictList()
        fdl.label("Datetime to String")
        fdl.label("Datetime variable")
        fdl.entry(name="datetime_var", text="", input_types=["datetime", "int", "float"], required=True, row=1)
        fdl.label("Return format")
        fdl.comboentry(name="return_format", text="", options=options, row=2)
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=3)        
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        datetime_var = node_detail_form.get_chosen_value_by_name("datetime_var", variable_handler)
        
        return_format = node_detail_form.get_chosen_value_by_name("return_format", variable_handler)
        return_format = parse_comboentry_input(return_format)
        
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(datetime_var, return_format, new_var_name)

    def direct_execute(self, datetime_var, return_format, new_var_name):
        inp = Input()
        inp.assign("datetime_var", datetime_var)
        inp.assign("return_format", return_format)

        formatted_datetime = self.input_execute(inp)
        
        variable_handler.new_variable(new_var_name, formatted_datetime)
        #variable_handler.update_data_in_variable_explorer(glc)
    
    def input_execute(self, inp):
        datetime_var = convert_timestamp_to_datetime_if_needed(inp("datetime_var"))
        
        date_formats = {
            "yyyy/mm/dd h:m:s": "%Y/%m/%d %H:%M:%S",
            "dd.mm.yyyy h:m": "%d.%m.%Y %H:%M",
            "mm/dd/yy": "%m/%d/%y"
        }

        formatted_datetime = datetime_var.strftime(date_formats[inp("return_format")]) #updated return_format to date_formats[return_format]
        """
        return_format = re.sub(4*'y', '%Y', return_format) # yyyy --> %Y
        return_format = re.sub(2*'y', '%y', return_format) # yy --> %y
        return_format = re.sub(2*'m', '%m', return_format)
        return_format = re.sub(2*'d', '%d', return_format)
        return_format = re.sub('h', '%H', return_format)
        return_format = re.sub('(?<!%)m', '%M', return_format)
        return_format = re.sub('s', '%S', return_format)

        formatted_datetime = datetime_var.strftime(return_format)
        """

        return formatted_datetime


    def export_code(self, node_detail_form):
        datetime_var = node_detail_form.get_chosen_value_by_name("datetime_var", variable_handler)
        return_format = node_detail_form.get_chosen_value_by_name("return_format", variable_handler)[0]
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        datetime_var = convert_timestamp_to_datetime_if_needed(datetime_var)
        year = datetime_var.year
        month = datetime_var.month
        day = datetime_var.day
        hour = datetime_var.hour
        minute = datetime_var.minute
        second = datetime_var.second
        microsecond = datetime_var.microsecond

        code = f"""
            datetime_var = datetime.datetime({year}, {month}, {day}, {hour}, {minute}, {second}, {microsecond})
            return_format = '{return_format}'

            return_format = re.sub(4*'y', '%Y', return_format) # yyyy --> %Y
            return_format = re.sub(2*'y', '%y', return_format) # yy --> %y
            return_format = re.sub(2*'m', '%m', return_format)
            return_format = re.sub(2*'d', '%d', return_format)
            return_format = re.sub('h', '%H', return_format)
            return_format = re.sub('(?<!%)m', '%M', return_format)
            return_format = re.sub('s', '%S', return_format)

            {new_var_name} = datetime_var.strftime(return_format)
        """

        return code

    def export_imports(self, *args):
        imports = ["import re", "import datetime"]
        return (imports)


class DatetimeFromStringHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'DatetimeFromString'
        self.fn_name = 'Datetime From String'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        
        fdl = FormDictList()
        fdl.label("Datetime from String")
        fdl.label("Datetime string")
        fdl.entry(name="datetime_str_var", text="", input_types=["str"], required=True, row=1)
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=2)        
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        datetime_str_var = node_detail_form.get_chosen_value_by_name("datetime_str_var", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(datetime_str_var, new_var_name)

    def direct_execute(self, datetime_str_var, new_var_name):
        inp = Input()
        inp.assign("datetime_str_var", datetime_str_var)

        parsed_datetime = self.input_execute(inp)

        variable_handler.new_variable(new_var_name, parsed_datetime)
        #variable_handler.update_data_in_variable_explorer(glc)
    
    def input_execute(self, inp):
            
        parsed_datetime = parser.parse(inp("datetime_str_var"))

        return parsed_datetime
    
    def export_code(self, node_detail_form):
        datetime_str_var = node_detail_form.get_variable_name_or_input_value_by_element_name("datetime_str_var")
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        code = f"""
            {new_var_name} = parser.parse("{datetime_str_var}")
        """

        return code

    def export_imports(self, *args):
        imports = ["from dateutil import parser"]
        return (imports)


class DatetimeAddDeltaHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = 'DatetimeAddDelta'
        self.fn_name = 'Datetime Add Delta'

        self.type_category = ntcm.categories.variable
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["years", "months", "weeks", "days", "hours", "minutes", "seconds", "microseconds"]
        
        fdl = FormDictList()
        fdl.label("Add Datetime Shift")
        fdl.label("Datetime variable")
        fdl.entry(name="datetime_var", text="", input_types=["datetime", "int", "float"], required=True, row=1)
        fdl.label("Shift to add")
        fdl.entry(name="datetime_delta", text="", input_types=["str", "int"], required=True, row=2)
        fdl.label("Unit")
        fdl.combobox(name="delta_unit", options=options, multiselect_indices={}, default="seconds", row=3)
        fdl.label("New variable name")
        fdl.entry(name="new_var_name", text="", category="new_var", input_types=["str"], row=4)        
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)

        return fdl

    def execute(self, node_detail_form):
        datetime_var = node_detail_form.get_chosen_value_by_name("datetime_var", variable_handler)
        datetime_delta = node_detail_form.get_chosen_value_by_name("datetime_delta", variable_handler)
        delta_unit = node_detail_form.get_chosen_value_by_name("delta_unit", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name("new_var_name", variable_handler)

        self.direct_execute(datetime_var, datetime_delta, delta_unit, new_var_name)

    def direct_execute(self, datetime_var, datetime_delta, delta_unit, new_var_name):
        inp = Input()
        inp.assign("datetime_var", datetime_var)
        inp.assign("datetime_delta", datetime_delta)
        inp.assign("delta_unit", delta_unit)

        datetime_plus_delta = self.input_execute(inp)

        variable_handler.new_variable(new_var_name, datetime_plus_delta)  
        #variable_handler.update_data_in_variable_explorer(glc)

        """
        datetime_var = convert_timestamp_to_datetime_if_needed(datetime_var)

        datetime_delta = f'[{datetime_delta}]'
        datetime_delta = ast.literal_eval(datetime_delta)

        if len(datetime_delta) != len(delta_unit) or type(datetime_delta) != list:
            flog.error("Wrong timedelta input")
            return None

        delta_dict = {}

        for i, unit in enumerate(delta_unit):
            delta_dict[unit] = datetime_delta[i]

        #delta = relativedelta(**{delta_unit: datetime_delta})
        delta = relativedelta(**delta_dict)

        try:
            new_var = datetime_var + delta
        except Exception as e:
            flog.error(message=f"{e}")
            return None
        

        
        
        variable_handler.new_variable(new_var_name, new_var)  
        #variable_handler.update_data_in_variable_explorer(glc)
        """

    def input_execute(self, inp):
        datetime_delta = ast.literal_eval(f'[{inp("datetime_delta")}]')

        delta_dict = {}

        for i, unit in enumerate(inp("delta_unit")):
            delta_dict[unit] = datetime_delta[i]

        delta = relativedelta(**delta_dict)
        datetime_plus_delta = inp("datetime_var") + delta
        
        return datetime_plus_delta

    def export_code(self, node_detail_form):
        datetime_var = node_detail_form.get_variable_name_or_input_value_by_element_name("datetime_var")
        datetime_delta = node_detail_form.get_variable_name_or_input_value_by_element_name("datetime_delta")
        delta_unit = node_detail_form.get_chosen_value_by_name("delta_unit", variable_handler)
        new_var_name = node_detail_form.get_variable_name_or_input_value_by_element_name("new_var_name", is_input_variable_name=True)

        year = datetime_var.year
        month = datetime_var.month
        day = datetime_var.day
        hour = datetime_var.hour
        minute = datetime_var.minute
        second = datetime_var.second
        microsecond = datetime_var.microsecond

        code = f"""
            datetime_var = datetime.datetime({year}, {month}, {day}, {hour}, {minute}, {second}, {microsecond})
            delta = relativedelta({delta_unit}={datetime_delta})

            {new_var_name} = datetime_var + delta
        """

        return code

    def export_imports(self, *args):
        imports = ["import datetime", "from dateutil.relativedelta import relativedelta"]
        return (imports)


datetime_handlers_dict = {
    "DatetimeNow": DatetimeNowHandler(),
    "NewDatetime": NewDatetimeHandler(),
    "DatetimeDifference": DatetimeDifferenceHandler(),
    "DatetimeValue": DatetimeValueHandler(),
    "DatetimeToString": DatetimeToStringHandler(),
    "DatetimeFromString": DatetimeFromStringHandler(),
    "DatetimeAddDelta": DatetimeAddDeltaHandler()
}