import difflib
import concurrent.futures
from typing import Union

import forloop_modules.queries.node_context_requests_backend as ncrb
from forloop_modules.errors.errors import CriticalPipelineError
from forloop_modules.flog import flog
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import (
    AbstractFunctionHandler,
)
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.globals.scraping_utilities_handler import suh
from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.redis.redis_connection import kv_redis, redis_config


class FindSimilarItemsHandler(AbstractFunctionHandler):
    """The FindSimilarItems node accepts a list of XPaths and determines their generalized XPath."""

    def __init__(self):
        self.icon_type = "FindSimilarItems"
        self.fn_name = "Find Similar Items"
        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        return fdl

    def direct_execute(self, xpath_elements: dict):
        # Get XPaths of selected elements
        xpaths = [x['xpath'] for x in xpath_elements]
        common_xpath_part, xpaths_leftovers = suh.cut_xpaths_to_common_part(xpaths)
        # Find optimal siblings level (index of XPath)
        generalized_common_xpath_parts, optimal_xpath_index = suh.get_generalized_xpaths(
            common_xpath_part
        )

        optimal_generalized_common_xpath_part = generalized_common_xpath_parts[optimal_xpath_index]
        flog.warning(optimal_generalized_common_xpath_part)

        elements_generalized_xpaths = [
            optimal_generalized_common_xpath_part + x + ';' for x in xpaths_leftovers
        ]
        flog.warning(f'GENERALIZED XPATHS {elements_generalized_xpaths}')

        suh.scan_web_page(
            incl_tables=False, incl_bullets=False, incl_texts=False, incl_headlines=False,
            incl_links=False, incl_images=False, incl_buttons=False,
            by_xpath=elements_generalized_xpaths,
            context_xpath=optimal_generalized_common_xpath_part
        )

        browser_view_elements = suh.get_webpage_elements()

        data = {
            'elements': browser_view_elements,
            'element_generalized_xpaths': elements_generalized_xpaths,
        }
        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(redis_action_key, data)


class ConvertToScrapingNodesHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "ConvertToScrapingNodes"
        self.fn_name = "Convert To Scraping Nodes"
        self.type_category = ntcm.categories.webscraping
        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        return fdl

    def direct_execute(self, xpaths: list[Union[str, list[str]]]):
        columns = [f"column_{i}" for i, _ in enumerate(xpaths)]

        variable_handler.create_variable("xpaths_ExtractXPathsToDf", xpaths)
        variable_handler.create_variable("columns_ExtractXPathsToDf", columns)
        ncrb.new_node(
            pos=[300, 300], typ="ExtractXPathsToDf", params_dict={
                "xpaths": {"variable": "xpaths", "value": ""},
                "columns": {"variable": "columns", "value": ""},
                "entry_df": {"variable": None, "value": ""},
                "write_mode": {"variable": None, "value": "Write"},
                "new_var_name": {"variable": None, "value": "scraped_results"},
            }, fields=[]
        )


class RefreshBrowserViewHandler(AbstractFunctionHandler):
    """The RefreshBrowserView node creates a screenshot of the currently opened webpage in BrowserView."""

    def __init__(self):
        self.icon_type = "RefreshBrowserView"
        self.fn_name = "Refresh Browser View"
        self.type_category = ntcm.categories.webscraping

        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        return fdl

    def direct_execute(self):
        suh.refresh_browser_view()
        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(redis_action_key, suh.screenshot_string)


class ScanWebPageHandler(AbstractFunctionHandler):
    """ScanWebPage Node scans for all specified elements in the currently opened webpage BrowserView."""

    def __init__(self):
        self.icon_type = "ScanWebPage"
        self.fn_name = "Scan web page"
        self.type_category = ntcm.categories.webscraping

        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        return fdl

    def direct_execute(
        self, incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images,
        incl_buttons, by_xpath, context_xpath=''
    ):
        suh.scan_web_page(
            incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images,
            incl_buttons, by_xpath, context_xpath, refresh_bv_elements=False
        )
        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(redis_action_key, suh.webpage_elements)


class ScanWebPageWithAIHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "ScanWebPageWithAI"
        self.fn_name = "Scan web page with AI"
        self.type_category = ntcm.categories.webscraping
        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        return FormDictList()

    def direct_execute():
        pass


browser_handlers_dict = {
    "FindSimilarItems": FindSimilarItemsHandler(),
    "ConvertToScrapingNodes": ConvertToScrapingNodesHandler(),
    "RefreshBrowserView": RefreshBrowserViewHandler(),
    "ScanWebPage": ScanWebPageHandler(),
    "ScanWebPageWithAI": ScanWebPageWithAIHandler(),
}
