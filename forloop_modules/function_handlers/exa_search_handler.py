import pandas as pd

import forloop_modules.flog as flog
from forloop_modules.errors.errors import CriticalPipelineError
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import (
    AbstractFunctionHandler,
)
from forloop_modules.function_handlers.auxilliary.docs import Docs
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.integrations.exa_search import ExaSearchError, search_exa
from forloop_modules.redis.config.config import redis_config
from forloop_modules.redis.redis_connection import kv_redis



class ExaSearchHandler(AbstractFunctionHandler):
    """
    Run an Exa web search and store normalized results in a pipeline variable.
    """

    def __init__(self):
        self.icon_type = "ExaSearch"
        self.fn_name = "Exa search"
        self.type_category = ntcm.categories.api
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()
        super().__init__()

    def _init_docs(self):
        parameters_description = "ExaSearch Node takes 3 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)
        self.docs.add_parameter_table_row(
            title="Search query",
            name="query",
            description="Search query sent to Exa",
            typ="string",
        )
        self.docs.add_parameter_table_row(
            title="Number of results",
            name="num_results",
            description="Maximum number of search results to return",
            typ="int",
        )
        self.docs.add_parameter_table_row(
            title="Output variable",
            name="new_var_name",
            description="Variable name for the results DataFrame",
            typ="string",
        )

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label(self.fn_name)
        fdl.label("Search query")
        fdl.entry(name="query", text="", input_types=["str"], required=True, row=1)
        fdl.label("Number of results")
        fdl.entry(name="num_results", text="10", input_types=["int"], required=True, row=2)
        fdl.label("Output variable")
        fdl.entry(
            name="new_var_name",
            text="exa_search_results",
            input_types=["str"],
            required=True,
            row=3,
        )
        return fdl

    def execute(self, node_detail_form):
        query = node_detail_form.get_chosen_value_by_name("query", variable_handler)
        num_results = node_detail_form.get_chosen_value_by_name("num_results", variable_handler)
        new_var_name = node_detail_form.get_chosen_value_by_name(
            "new_var_name", variable_handler
        )
        self.direct_execute(query, num_results, new_var_name)

    def execute_with_params(self, params):
        self.direct_execute(
            params["query"],
            params["num_results"],
            params["new_var_name"],
        )

    def direct_execute(self, query, num_results, new_var_name):

        try:
            num_results = int(num_results)
        except (TypeError, ValueError) as error:
            raise CriticalPipelineError("Number of results must be an integer") from error

        try:
            response = search_exa(query, num_results)
        except ExaSearchError as error:
            raise CriticalPipelineError(error.detail) from error

        results = response.get("results") or []
        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(
            redis_action_key,
            {
                "query": response.get("query", query),
                "results": results,
            },
        )

        results_df = pd.DataFrame(results)

        if "highlights" in results_df.columns:
            results_df["highlights"] = results_df["highlights"].apply(
                lambda value: " | ".join(value) if isinstance(value, list) else value
            )

        variable_handler.new_variable(new_var_name, results_df)

        flog.info(
            f"[ExaSearch] Stored {len(results_df)} results in variable '{new_var_name}' "
            f"for query_length={len(str(response.get('query', '')))}"
        )


exa_search_handlers_dict = {
    "ExaSearch": ExaSearchHandler(),
}
