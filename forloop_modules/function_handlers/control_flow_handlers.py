import ast
import os
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

from forloop_modules.queries.db_model_templates import JobStatusEnum
import forloop_modules.queries.node_context_requests_backend as ncrb


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


class HumanTaskHandler(AbstractFunctionHandler):
    """
    HumanTask pauses pipeline execution until the user submits an input.

    Edge control:
    - If the submitted input parses as `true` -> return True
    - otherwise -> return False
    """

    def __init__(self):
        self.icon_type = "HumanTask"
        self.fn_name = "Human task"
        self.type_category = ntcm.categories.control_flow
        self.docs_category = DocsCategories.control
        self._init_docs()

    def _init_docs(self):
        self.docs = Docs(
            description=self.__doc__,
            parameters_description="Human task waits for a decision from an assigned user.",
        )
        self.docs.add_parameter_table_row(
            title="Title",
            name="title",
            description="Task title shown in the Task Inbox.",
            typ="str",
            example="Approve request",
        )
        self.docs.add_parameter_table_row(
            title="Assignee",
            name="assignee_user_uid",
            description=(
                "Who can approve/reject this task. Assigning other users is temporarily "
                "disabled — only yourself is available."
            ),
            typ="str",
            example="Myself",
        )
        self.docs.add_parameter_table_row(
            title="Timeout (ms)",
            name="timeout_ms",
            description="If no decision arrives within this time, task expires.",
            typ="int",
            example="300000",
        )

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Title:")
        fdl.entry(name="title", text="", row=1, required=True, show_info=True)
        fdl.label("Assignee:")
        fdl.combobox(
            name="assignee_user_uid",
            options=["Myself"],
            default="Myself",
            row=2,
            show_info=True,
        )
        fdl.label("Timeout (ms):")
        fdl.entry(name="timeout_ms", text="300000", row=3, input_types=["int"], show_info=True)
        return fdl

    def execute(self, node_detail_form):
        title = node_detail_form.get_chosen_value_by_name(
            "title", variable_handler
        )
        timeout_ms = node_detail_form.get_chosen_value_by_name(
            "timeout_ms", variable_handler
        )
        node_uid = getattr(node_detail_form, "node_uid", None)
        # Assigning other users is temporarily disabled — always the current user.
        return self.direct_execute(title, "Myself", timeout_ms, node_uid=node_uid)

    def direct_execute(self, title, assignee_user_uid, timeout_ms, node_uid=None):
        """Pipeline calls this with resolved params; execute() passes node_uid from the form context."""
        pipeline_job_uid = aet.active_pipeline_job_uid
        assignee_user_uid = self._resolve_assignee_user_uid(assignee_user_uid)

        variable_handler.new_variable("human_task_title", str(title), is_result=True)
        variable_handler.new_variable(
            "human_task_assignee_user_uid", str(assignee_user_uid), is_result=True
        )
        variable_handler.new_variable("human_task_node_uid", str(node_uid), is_result=True)
        variable_handler.new_variable(
            "human_task_pipeline_job_uid", str(pipeline_job_uid), is_result=True
        )
        variable_handler.new_variable("human_task_status", "OPEN", is_result=True)

        variable_handler.new_variable("human_task_status", "IN_PROGRESS", is_result=True)

        self._set_pipeline_job_status(pipeline_job_uid, JobStatusEnum.WAITING_FOR_INPUT)

        decision = self._wait_for_user_decision(
            pipeline_job_uid=pipeline_job_uid,
            timeout_ms=int(timeout_ms) if timeout_ms else 300000,
        )
        return decision

    def _normalize_uid(self, value) -> str:
        if value is None:
            return ""
        if isinstance(value, (list, tuple)):
            if not value:
                return ""
            value = value[0]
        try:
            x = float(value)
            if x != x:
                return ""
            return str(int(x))
        except (ValueError, TypeError, OverflowError):
            return str(value).strip()

    def _resolve_assignee_user_uid(self, _assignee_user_uid) -> str:
        """Assigning other users is temporarily disabled — always the current user."""
        current_user_uid = self._get_current_user_uid()
        if not current_user_uid:
            raise SoftPipelineError(
                "Cannot determine current user for Human task assignee"
            )
        return current_user_uid

    def _get_current_user_uid(self) -> str:
        if aet.user_uid:
            uid = self._normalize_uid(aet.user_uid)
            if uid:
                return uid

        pipeline_job_uid = aet.active_pipeline_job_uid
        if pipeline_job_uid:
            uid = self._get_pipeline_job_created_by_user_uid(pipeline_job_uid)
            if uid:
                return uid
        return ""

    def _get_pipeline_job_created_by_user_uid(
        self, pipeline_job_uid: str, depth: int = 0
    ) -> str:
        if not pipeline_job_uid or depth > 10:
            return ""

        job_row = self._get_pipeline_job_row(pipeline_job_uid)
        if job_row is None:
            return ""

        created_by = job_row.get("created_by") or {}
        if isinstance(created_by, str):
            try:
                created_by = ast.literal_eval(created_by)
            except Exception:
                created_by = {}
        if not isinstance(created_by, dict):
            return ""

        created_by_type = created_by.get("type") or created_by.get("trigger_type")
        created_by_uid = created_by.get("uid")
        if created_by_type == "USER":
            return self._normalize_uid(created_by_uid)
        if created_by_type == "PIPELINE_JOB" and created_by_uid:
            return self._get_pipeline_job_created_by_user_uid(
                str(created_by_uid), depth=depth + 1
            )
        return ""

    def _get_pipeline_job_row(self, pipeline_job_uid: str):
        if not pipeline_job_uid:
            return None
        return ncrb.get_pipeline_job(str(pipeline_job_uid))

    def _set_pipeline_job_status(self, pipeline_job_uid: str, status: JobStatusEnum) -> None:
        if not pipeline_job_uid:
            return
        try:
            ncrb.update_pipeline_job_status(str(pipeline_job_uid), status)
        except Exception as e:
            flog.warning(
                f"Could not update pipeline job {pipeline_job_uid} status to {status}: {e}"
            )

    def _is_pipeline_job_canceled(self, pipeline_job_uid: str) -> bool:
        job_row = self._get_pipeline_job_row(pipeline_job_uid)
        if not job_row:
            return False
        return job_row.get("status") == JobStatusEnum.CANCELLING

    def _parse_user_input_decision(self, user_input: str) -> bool:
        v = (user_input or "").strip().lower()
        return v in ["true", "1", "approved", "approve", "yes", "y"]

    def _wait_for_user_decision(self, pipeline_job_uid: str, timeout_ms: int) -> bool:
        input_file = "/tmp/forloop_user_input.txt"
        start_ts = time.time()
        cancel_check_every_s = 5.0
        last_cancel_check = 0.0

        while True:
            elapsed_ms = int((time.time() - start_ts) * 1000)
            if elapsed_ms > timeout_ms:
                variable_handler.new_variable("human_task_status", "EXPIRED", is_result=True)
                # Prevent inbox from listing the task as still waiting.
                self._set_pipeline_job_status(pipeline_job_uid, JobStatusEnum.IN_PROGRESS)
                return False

            now = time.time()
            if now - last_cancel_check >= cancel_check_every_s:
                last_cancel_check = now
                if self._is_pipeline_job_canceled(pipeline_job_uid):
                    variable_handler.new_variable("human_task_status", "CANCELLED", is_result=True)
                    self._set_pipeline_job_status(
                        pipeline_job_uid, JobStatusEnum.IN_PROGRESS
                    )
                    return False

            if os.path.exists(input_file):
                try:
                    with open(input_file, "r", encoding="utf-8") as f:
                        user_input = f.read().strip()
                finally:
                    # Best-effort cleanup; if the file is re-created it will be read in the next loop.
                    try:
                        os.remove(input_file)
                    except Exception:
                        pass

                decision = self._parse_user_input_decision(user_input)

                variable_handler.new_variable(
                    "human_task_status",
                    "APPROVED" if decision else "REJECTED",
                    is_result=True,
                )
                variable_handler.new_variable(
                    "human_task_decision",
                    "true" if decision else "false",
                    is_result=True,
                )

                # Keep a final state for audit/history; inbox only lists WAITING_FOR_INPUT.
                variable_handler.new_variable("human_task_status", "DONE", is_result=True)

                return decision

            time.sleep(0.5)


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
                "triggered_by": "PIPELINE_JOB",
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
            'and': lambda a, b: bool(a and b),
            'or': lambda a, b: bool(a or b),
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
                                          description="The first value used in the condition. Use '$' for variable name. Example: $var1",
                                          example="100 | True",
                                          )
        self.docs.add_parameter_table_row(title="Operator", name="operator",
                                          description="The operator used used in the condition."
                                          )
        self.docs.add_parameter_table_row(title="Value 2", name="value_q",
                                          description="The second value used in the condition. Use '$' for variable name. Example: $var2", 
                                          example="27 | ['a', 'b', 'c'] | {'name':'Sarah'} $var2"
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
        
        fdl.label("Active channel")
        fdl.checkbox(name="active_channel", bool_value=True, row=4)
        
        #fdl.label(f"Current channel: ", row=5)
        
        return fdl


    # def toggle_channel(self, args): #Refactor to backend
    #     image = args[0]
    #     elements = image.item_detail_form.elements
    #     channel_str = elements[-1].text.split("Active channel: ")[1]
    #     channel = ast.literal_eval(channel_str)
    #     channel = not channel  # toggle
    #     elements[-1].text = "Active channel: " + str(channel)

    #     print(args)

    def direct_execute(self, value_p, operator, value_q, **kwargs):
        # Convert string values to appropriate types (int, float, bool, list, dict, etc.)
        # This handles cases where user types "1" in form which gets stored as string "1"
        if isinstance(value_p, str) and value_p:
            value_p = ast.literal_eval(value_p)
        
        if isinstance(value_q, str) and value_q:
            value_q = ast.literal_eval(value_q)

        
        # Handle isempty operator for any type
        if operator == 'isempty':
            if value_p is None:
                return True
            elif isinstance(value_p, (list, dict, str)):
                return len(value_p) == 0
            else:
                return not bool(value_p)
        
        # Use Python's native comparison behavior - let Python handle type checking
        try:
            result = self.operators[operator](value_p, value_q)
            
            # Ensure result is a boolean (Python allows some comparisons to return non-bool)
            # e.g., DataFrame == DataFrame returns DataFrame, not bool
            if not isinstance(result, bool):
                raise SoftPipelineError(
                    f"Comparison '{value_p} {operator} {value_q}' returned {type(result).__name__}, "
                    f"but a boolean value is required. This may occur with complex types like DataFrames."
                )
            
        except TypeError as e:
            # Python's TypeError for unsupported operations - re-raise with clearer message
            raise SoftPipelineError(
                f"Type mismatch in condition '{value_p} {operator} {value_q}': {str(e)}"
            ) from e
        except Exception as e:
            # Catch any other unexpected errors
            flog.error(f'Unexpected error while running operation {value_p} {operator} {value_q}: {str(e)}', self)
            raise SoftPipelineError(
                f"Error evaluating condition '{value_p} {operator} {value_q}': {str(e)}"
            ) from e
        
        return result
        
        return result


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
    'HumanTask': HumanTaskHandler(),
    'Wait': WaitHandler(), 
    'RunPipeline': RunPipelineHandler(), 
    'IfCondition': IfConditionHandler(), 
    'WhileLoop': WhileLoopHandler(), 
    'ForLoop': ForLoopHandler(), 
    # 'Iterate': IterateHandler(), 
    'Batch': BatchHandler()
}
