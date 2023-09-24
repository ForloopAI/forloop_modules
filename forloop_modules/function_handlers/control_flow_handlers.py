import ast
import operator
import random
import time
import forloop_modules.flog as flog

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList

from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler


class StartHandler(AbstractFunctionHandler):
    icon_type = "Start"
    fn_name = "Start"

    type_category = ntcm.categories.control_flow

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
        #start pipeline
        """
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)


class FinishHandler(AbstractFunctionHandler):
    icon_type = "Finish"
    fn_name = "Finish"

    type_category = ntcm.categories.control_flow

    def make_form_dict_list(self, *args, node_detail_form=None):
        
        fdl=FormDictList()
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
    icon_type = "Wait"
    fn_name = "Wait"

    type_category = ntcm.categories.control_flow

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl=FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Milliseconds:")
        fdl.entry(name="milliseconds",text="1000",row=1, input_types=["int", "float"], show_info=True, required=True)
        fdl.label("Add random ms:")
        fdl.entry(name="rand_ms",text="0",row=2, input_types=["int", "float"])        
        return fdl

    def direct_execute(self, milliseconds, rand_ms):
        # def __new__(cls, milliseconds, rand_ms, *args, **kwargs):
        milliseconds = int(milliseconds)
        rand_ms = int(rand_ms)
        milliseconds += random.randint(0, int(rand_ms))
        #milliseconds = str(milliseconds)

        #command = f'Wait({milliseconds})'
        
        time.sleep(milliseconds/1000)
        #dc.execute_order(command)

        
        
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
    
        fdl=FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Pipeline ID:")
        fdl.entry(name="pipeline_id",text="",row=1)
        fdl.label("Runs:")
        fdl.entry(name="runs",text="1",row=2)
        
        return fdl
    

    def direct_execute(self):
        """TODO"""
        pass

    def execute(self, args):
        """TODO"""
        pass


class IfConditionHandler(AbstractFunctionHandler):
    icon_type = 'IfCondition'
    fn_name = 'If Condition'
    type_category = ntcm.categories.control_flow

    operators = {
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

    def make_form_dict_list(self, *args, node_detail_form=None):
    
        fdl=FormDictList()
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
