#interactive_handlers.py
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.docs import Docs
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import (
    AbstractFunctionHandler
)
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.function_handlers.input_waiter import register_pending_input



class ConsoleEntryHandler(AbstractFunctionHandler):
    def __init__(self):
        self.is_disabled = False
        self.icon_type = "ConsoleEntry" 
        self.fn_name = "Console Entry"
        self.type_category = ntcm.categories.variable  
        self.docs_category = DocsCategories.control    
        self._init_docs()
        super().__init__()

    def _init_docs(self):
        self.docs = Docs(description="Prompt user via popup and wait for their input.")
        self.docs.add_parameter_table_row(
            title="Prompt",
            name="prompt",
            description="Text shown in console & popup",
            typ="string",
            example="Enter your name:"
        )
        self.docs.add_parameter_table_row(
            title="Output Var",
            name="output_variable",
            description="Variable name to store the response",
            typ="string",
            example="user_input"
        )

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.entry(
            name="prompt",
            text="Enter your name:",
            input_types=["str"],
            required=True,
            row=1
        )
        fdl.entry(
            name="output_variable",
            text="user_input",
            input_types=["str"],
            required=True,
            row=2
        )
        fdl.button(
            function=self.execute,
            function_args=node_detail_form,
            text="Execute",
            focused=True
        )
        return fdl

    def execute(self, node_detail_form):
        prompt     = node_detail_form.get_chosen_value_by_name("prompt")
        output_var = node_detail_form.get_chosen_value_by_name("output_variable")
        node_uid   = node_detail_form.node_uid

        register_pending_input(node_uid, output_var, prompt_text=prompt)

        try:
            ncrb.post_console_print_log(message=f">>> {prompt}", type="input_request")
        except Exception:
            pass

        ncrb.new_popup(
            pos=[400, 300],
            typ="ConsoleTextInput",
            params_dict={
                "prompt":         {"value": prompt},
                "output_variable":{"value": output_var},
                "parent_node_uid": {"value": node_uid},
            }
        )

        return "WAITING"

    def direct_execute(
        self,
        prompt: str,
        output_variable: str,
        node_uid: str    
    ):
        register_pending_input(node_uid, output_variable, prompt_text=prompt)

        try:
            ncrb.post_console_print_log(message=f">>> {prompt}", type="input_request")
        except:
            pass

        ncrb.new_popup(
            pos=[400, 300],
            typ="ConsoleTextInput",
            params_dict={
                "prompt":         {"value": prompt},
                "output_variable":{"value": output_variable},
                "node_uid":       {"value": node_uid},
            }
        )

        return "WAITING"
        

