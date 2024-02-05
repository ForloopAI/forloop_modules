from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList

from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler 


class TriggerHandler(AbstractFunctionHandler):
    icon_type = "Trigger"
    fn_name = "Trigger"
    type_category = ntcm.categories.data

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        
        
        fdl.label("Machine name")
        fdl.entry("machine","",row=1)
        
        fdl.label("Start time (HH:MM:SS)")
        fdl.entry("time","00:00:00",row=3)
        
        fdl.label("Frequency:")
        options=["Daily","Hourly","Every minute"]
        fdl.combobox("Frequency:",options,row=4)
        

        return fdl

    def direct_execute(self):
        # def __new__(cls, *args, **kwargs):
        """Do nothing"""
        pass

    def export_code(self, *args):
        image = args[0]

        try:
            filename = image.item_detail_form.elements[2].text
            with open(filename, 'r') as f:
                code = f.read()
        except:
            code = """
            """
        
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)



class RunForloopPipelineHandler(AbstractFunctionHandler):
    icon_type = "RunForloopPipeline"
    fn_name = "Run Forloop Pipeline"
    type_category = ntcm.categories.data

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Run pipeline:")
        options=["pipeline1","pipeline2","pipeline3"]
        fdl.combobox("pipeline",options,row=2)
        fdl.label("On machine:")
        options2=["machine1","machine2","machine3"]
        fdl.combobox("pipeline",options2,row=2)
        

        return fdl

    def direct_execute(self):
        # def __new__(cls, *args, **kwargs):
        """Do nothing"""
        pass

    def export_code(self, *args):
        image = args[0]

        try:
            filename = image.item_detail_form.elements[2].text
            with open(filename, 'r') as f:
                code = f.read()
        except:
            code = """
            """
        
        return (code)

    def export_imports(self, *args):
        imports = []
        return (imports)

orchestration_handlers_dict = {
    "Trigger": TriggerHandler(),
    "RunForloopPipeline": RunForloopPipelineHandler()    
    #"PredictModelValues": PredictModelValuesHandler()
}