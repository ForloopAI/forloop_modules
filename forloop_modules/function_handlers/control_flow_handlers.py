import ast
import operator
import random
import time

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb
from forloop_modules.errors.errors import SoftPipelineError
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import (
    AbstractFunctionHandler,
)
from forloop_modules.function_handlers.auxilliary.docs import Docs
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.globals.variable_handler import variable_handler


class StartHandler(AbstractFunctionHandler):
    """
    Marks the starting point of a pipeline, i.e. it should be **the first** node in the pipeline.
    """
    
    def __init__(self):
        self.icon_type = "Start"
        self.fn_name = "Start"
        self.type_category = ntcm.categories.control_flow
        self.docs_category = DocsCategories.control
        self._init_docs()
        
    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)

    def make_form_dict_list(self, *args, node_detail_form=None):
        
        fdl=FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        return fdl

    def execute(self, node_detail_form):
        self.direct_execute()

    def direct_execute(self):
        """Do nothing"""
        pass

    def export_code(self, node_detail_form):
        code = """
        #start pipeline
        """
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)


class FinishHandler(AbstractFunctionHandler):
    """
    Marks the ending of a pipeline, i.e. it should be **the last** node in the pipeline.
    """
    def __init__(self):
        self.icon_type = "Finish"
        self.fn_name = "Finish"
        self.type_category = ntcm.categories.control_flow
        self.docs_category = DocsCategories.control

        self._init_docs()

    def _init_docs(self):
        self.docs = Docs(description=self.__doc__)

    def make_form_dict_list(self, *args, node_detail_form=None):
        
        fdl=FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        return fdl
        
    def direct_execute(self):
        # def __new__(cls, *args, **kwargs):
        """Finish - TODO: Refactor to not be dependent on ge"""
        # psm.reset(args)

    def export_code(self, node_detail_form):
        code = """
        #finish pipeline
        """
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)


class WaitHandler(AbstractFunctionHandler):
    """
    Wait Node creates a pauses between pipeline steps. It waits (and does nothing, obviously) for 
    a specified amount of ms and then lets the next step in a pipeline to proceed.
    """
    
    def __init__(self):
        self.icon_type = "Wait"
        self.fn_name = "Wait"

        self.type_category = ntcm.categories.control_flow
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

    def _init_docs(self):
        parameter_description = "Two parameters are required:"
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Miliseconds", name="milliseconds",
                                          description="Waiting time interval in miliseconds.",
                                          typ="int | float", example="1000 → 1000 ms (1 s) waiting time before another step")
        self.docs.add_parameter_table_row(title="Add random ms", name="rand_ms",
                                          description="Adds a random real picked from a uniform distribution defined on the interval (- entered value, + entered value).",
                                          typ="int | float"
                                          )

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl=FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Milliseconds:")
        fdl.entry(name="milliseconds",text="1000",row=1, input_types=["int", "float"], show_info=True, required=True)
        fdl.label("Add random ms:")
        fdl.entry(name="rand_ms",text="0",row=2, input_types=["int", "float"])
        return fdl

    def execute(self, node_detail_form):
        milliseconds = node_detail_form.get_chosen_value_by_name(
            "milliseconds", variable_handler
        )
        rand_ms = node_detail_form.get_chosen_value_by_name(
            "rand_ms", variable_handler
        )
        self.direct_execute(milliseconds, rand_ms)

    def direct_execute(self, milliseconds, rand_ms):
        milliseconds = int(milliseconds)

        if rand_ms != "":
            rand_ms = int(rand_ms)
            milliseconds += random.randint(0, rand_ms)

        time.sleep(milliseconds/1000)

    def export_code(self,node_detail_form):
        code="""
        time.sleep(milliseconds/1000)
        """
        #command = f'Wait({milliseconds})'
        #dc.execute_order(command)
        return (code)

    def export_imports(self, *args):
        imports = ["import time", "import random"]
        return (imports)


class RunPipelineHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "RunPipeline"
        self.fn_name = "Run Pipeline"
        self.type_category = ntcm.categories.control_flow

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Pipeline to trigger")
        fdl.combobox(name="pipeline_to_trigger", options=[], row=1)
        fdl.label("Variables to export")
        fdl.combobox(name="variable_uids", options=[], multiselect_indices={}, row=2)
        return fdl

    def execute(self, node_detail_form):
        pipeline_to_trigger = node_detail_form.get_chosen_value_by_name(
            "pipeline_to_trigger", variable_handler
        )
        variable_uids = node_detail_form.get_chosen_value_by_name(
            "variable_uids", variable_handler
        )
        self.direct_execute(pipeline_to_trigger, variable_uids)

    def direct_execute(self, pipeline_to_trigger, variable_uids):
        var_uids = []
        for var_uid in variable_uids:
            var = ncrb.get_variable(var_uid)
            if not var:
                raise SoftPipelineError(f"Variable {var_uid} was not found during exporting.")
            var_uids.append(var["uid"])

        ncrb.pipeline_direct_execute(
            pipeline_to_trigger,
            {
                "triggered_by": "pipeline_job",
                "uid": aet.active_pipeline_job_uid,
                "variable_uids": var_uids,
            },
        )


class IfConditionHandler(AbstractFunctionHandler):
    """
    Toggles one of the two channels depending on whether a condition defined by the user holds true or not.
    """
    
    def __init__(self):
        self.icon_type = 'IfCondition'
        self.fn_name = 'If Condition'
        self.type_category = ntcm.categories.control_flow
        self.docs_category = DocsCategories.control

        self.operators = {
            '==': operator.eq,
            '!=': operator.ne,
            '>=': operator.ge,
            '<=': operator.le,
            '>': operator.gt,
            '<': operator.lt,
            'and': operator.and_,
            'or': operator.or_,
            'contains': operator.contains,
            'isempty': None,
        }
        
        self._init_docs()
        
    def _init_docs(self):
        parameter_description = """
        If Condition Node requires 3 parameters. Together they form a condition, e.g. column_name == ‘city’ which can 
        be True or False. The Toggle button then switches the active channel, i.e. the user can choose if the “True 
        branch” will run or the “False branch”.
        """
        self.docs = Docs(description=self.__doc__, parameters_description=parameter_description)
        self.docs.add_parameter_table_row(title="Value 1", name="value_p",
                                          description="The first value used in the condition."
                                          , example="100 | True")
        self.docs.add_parameter_table_row(title="Operator", name="operator",
                                          description="The operator used used in the condition."
                                          )
        self.docs.add_parameter_table_row(title="Value 2", name="value_q",
                                          description="The second value used in the condition.", example="27 | ['a', 'b', 'c'] | {'name':'Sarah'}"
                                          )

    def make_form_dict_list(self, *args, node_detail_form=None):
    
        fdl=FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        
        fdl.label("Value 1")
        fdl.entry(name="value_p",text="",row=1)
        
        fdl.label("Operator")
        fdl.combobox(name="operator",options= list(self.operators.keys()),row=2)
        
        fdl.label("Value 2")
        fdl.entry(name="value_q",text="",row=3)
        
        fdl.label("Toggle channel")
        fdl.button(function=self.toggle_channel, function_args=args, text="Toggle",row=4, focused=True)
        
        fdl.label("Active channel: True")
        
        return fdl

    def toggle_channel(self, args): #Refactor to backend
        image = args[0]
        elements = image.item_detail_form.elements
        channel_str = elements[-1].text.split("Active channel: ")[1]
        channel = ast.literal_eval(channel_str)
        channel = not channel  # toggle
        elements[-1].text = "Active channel: " + str(channel)

        print(args)

    def direct_execute(self, value_p, operator, value_q):
        # def __new__(cls, value_p, operator, value_q, *args, **kwargs):
        try:
            if isinstance(value_p, list) and operator == 'isempty':
                if value_p:
                    result = False
                else:
                    result = True
            else:
                result = self.operators[operator](str(value_p), str(value_q))
            # result = self.operators[operator](ast.literal_eval(value_p), ast.literal_eval(value_q)) <- BEFORE (Doesn't work!!!)

        except Exception as e:
            flog.error(f'Error while running operation {value_p} {operator} {value_q}'+str(e),self)

            result = False
            flog.error('Error in if_condition:', e)
        return (result)


class WhileLoopHandler(AbstractFunctionHandler):
    icon_type = 'WhileLoop'
    fn_name = 'While Loop'
    type_category = ntcm.categories.control_flow

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl=FormDictList()
        fdl.label(self.fn_name)
        return fdl

    def direct_execute(self):
        pass


class ForLoopHandler(AbstractFunctionHandler):
    icon_type = 'ForLoop'
    fn_name = 'For Loop'
    type_category = ntcm.categories.control_flow

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl=FormDictList()
        fdl.label(self.fn_name)
        return fdl

    def direct_execute(self):
        pass




# TODO: Deprecated. Delete or substitute
class IterateHandler:
    def __init__(self):
        self.icon_type = "Iterate"
        self.fn_name = "Iterate"

        self.type_category = ntcm.categories.control_flow
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        options2 = ["Iterate whole list", "Infinite loop", "Custom"]

        form_dict_list = [
            {"Label": "Iterate"},
            {"Label": "Iteration variable", "Entry": "i"},
            {"Label": "Initial index value", "Entry": "0"},
            {"Label": "Iteration type", "Combobox": {"name": "iter_type", "options": options2}},
            {"Label": "End index value", "Entry": ""},
            {"Label": "Current iteration: 0"}
        ]
        
        return form_dict_list

    def execute(self, args):
        """TODO"""
        pass

# TODO: Deprecated. Substitute
class BatchHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "Batch"
        self.fn_name = "Batch"

        self.type_category = ntcm.categories.control_flow
        self.docs_category = DocsCategories.control

    def make_form_dict_list(self, *args, node_detail_form=None):
        form_dict_list = [
            {"Label": "Batch"},
            {"Label": "Selected Column", "Entry": {"name": "selected_column", "text": ""}},
            {"Label": "Batch size", "Entry": {"name": "size", "text": "0"}},
        ]
        return form_dict_list

    def execute(self, args):
        """TODO"""
        pass

    def direct_execute(self):
        """TODO"""
        pass


control_flow_handlers_dict = {
    'Start': StartHandler(), 
    'Finish': FinishHandler(), 
    'Wait': WaitHandler(), 
    'RunPipeline': RunPipelineHandler(), 
    'IfCondition': IfConditionHandler(), 
    'WhileLoop': WhileLoopHandler(), 
    'ForLoop': ForLoopHandler(), 
    # 'Iterate': IterateHandler(), 
    'Batch': BatchHandler()
}
