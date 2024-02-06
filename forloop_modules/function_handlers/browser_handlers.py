import difflib
import concurrent.futures
from typing import Union

import forloop_modules.queries.node_context_requests_backend as ncrb
from forloop_modules.flog import flog
from forloop_modules.function_handlers.auxilliary import chatgpt_integration
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
from forloop_modules.errors.errors import CriticalPipelineError

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

    # def execute(self, node_detail_form):
    #     xpaths = node_detail_form.get_chosen_value_by_name("xpaths", variable_handler)
    #     self.direct_execute(xpaths)

    # def execute_with_params(self, params):
    #     xpaths = params["xpaths"]
    #     self.direct_execute(xpaths)

    def direct_execute(self, xpath_elements: dict):
        # selected_elements_xpaths_new = xpaths

        # elements_positions = kv_redis.get("elements_positions_temp")
        # # elements_positions = ncm.elements_positions_temp  #temp storage TODO: should be done for users or send via API
        # #for i,element_pos in enumerate(elements_positions):
        # #if element_pos["data"] is not None:
        # #    with open(element_pos["data"],"rb") as file:
        # #        new_data=pickle.load(file)
        # #else:
        # #    new_data=[]
        # #elements_positions[i]["data"]=new_data

        # selected_elements_referential_names = [
        #     x["name"] for x in selected_elements_xpaths_new
        # ]  #referential means the referential selected item by which other similar items are found

        # selected_elements_xpaths_new = [x["xpath"] for x in selected_elements_xpaths_new]  #temp
        # common_xpath_part, xpaths_leftovers = suh.cut_xpaths_to_common_part(
        #     selected_elements_xpaths_new
        # )
        # # Find optimal siblings level (index of XPath)
        # generalized_common_xpath_parts, optimal_xpath_index = suh.get_generalized_xpaths(
        #     common_xpath_part
        # )  #scans the website n+1 times - could be optimized?
        # optimal_generalized_common_xpath_part = generalized_common_xpath_parts[optimal_xpath_index]

        # elements_generalized_xpaths = [
        #     optimal_generalized_common_xpath_part + x for x in xpaths_leftovers
        # ]

        # xpath_leftover_referential_name_dict = {}
        # assert len(xpaths_leftovers) == len(
        #     selected_elements_referential_names
        # )  #to ensure that the following for loop will be well defined
        # for i, xpath_leftover in enumerate(xpaths_leftovers):
        #     xpath_leftover_referential_name_dict[xpath_leftover
        #                                         ] = selected_elements_referential_names[i]

        # # new_elements_positions after scanning of generalized elements in suh.get_generalized_xpaths
        # try:
        #     suh.update_webpage_elements(refresh_browser_view_elements=False)
        # except:
        #     flog.error('Error while loading elements positions!')

        # def extract_shared_prefix_and_suffix_from_strings_and_substring(string, substring):

        #     #string = "/html/body/div[4]/div/div/div/div/div[1]/a/div"
        #     #substring = "/html/body/div[4]/div/div/div/div/div/a/div"

        #     # Finding the differing part between the two strings
        #     differ = difflib.ndiff(string, substring)
        #     diff_indices = [i for i, d in enumerate(differ) if d.startswith('-')]

        #     # Splitting the string into three pieces
        #     before_piece = string[:diff_indices[0]]
        #     string[diff_indices[0]:diff_indices[-1] + 1]
        #     after_piece = string[diff_indices[-1] + 1:]

        #     #Before Piece: /html/body/div[4]/div/div/div/div/div
        #     #Middle Piece: [1]
        #     #After Piece: /a/div

        #     return (before_piece, after_piece)

        # try:
        #     before_piece, after_piece = extract_shared_prefix_and_suffix_from_strings_and_substring(
        #         common_xpath_part, optimal_generalized_common_xpath_part
        #     )

        #     #NOT IDEAL - problem if generalized xpath isnt done in on the last tag
        #     matching_elements = []

        #     matching_elements_groups = {}
        #     for _j, xpath_leftover in enumerate(
        #         xpaths_leftovers
        #     ):  #initialization of lists in groups
        #         referential_element_group = xpath_leftover_referential_name_dict[xpath_leftover]
        #         matching_elements_groups[referential_element_group] = []

        #     for i, element_position in enumerate(elements_positions):

        #         is_at_least_one_leftover_contained = False
        #         is_generalized_common_xpath_part_contained = False
        #         if before_piece in element_position["xpath"] and after_piece in element_position[
        #             "xpath"]:
        #             is_generalized_common_xpath_part_contained = True
        #         for _j, xpath_leftover in enumerate(xpaths_leftovers):
        #             if element_position["xpath"].endswith(xpath_leftover.replace(";", "")):
        #                 is_at_least_one_leftover_contained = True
        #                 referential_element_group = xpath_leftover_referential_name_dict[
        #                     xpath_leftover]

        #         if is_generalized_common_xpath_part_contained and is_at_least_one_leftover_contained:
        #             matching_elements.append(element_position["xpath"])
        #             matching_elements_groups[referential_element_group].append(
        #                 element_position
        #             )  #["xpath"]

        #     # for i,element_position in enumerate(elements_positions):

        #     #     is_at_least_one_leftover_contained=False
        #     #     is_generalized_common_xpath_part_contained=False
        #     #     if before_piece in element_position["xpath"] and after_piece in element_position["xpath"]:
        #     #         is_generalized_common_xpath_part_contained=True
        #     #     print(is_generalized_common_xpath_part_contained, element_position["xpath"])
        #     #     for j,xpath_leftover in enumerate(xpaths_leftovers):
        #     #         if xpath_leftover.replace(";","") in element_position["xpath"]:
        #     #             is_at_least_one_leftover_contained=True

        #     #     print(is_at_least_one_leftover_contained, xpaths_leftovers)

        #     #     if is_generalized_common_xpath_part_contained and is_at_least_one_leftover_contained:
        #     #         matching_elements.append(element_position["xpath"])
        #     kv_redis.set("matching_elements_groups", matching_elements_groups)
        #     # ncm.matching_elements_groups = matching_elements_groups
        #     message = "Selected elements XPATHs " + str(selected_elements_xpaths_new)
        # except Exception as e:
        #     print(e)
        #     matching_elements = []
        #     matching_elements_groups = {}
        #     kv_redis.set("matching_elements_groups", matching_elements_groups)
        #     # ncm.matching_elements_groups = matching_elements_groups
        #     message = "Selected elements XPATHs " + str(
        #         selected_elements_xpaths_new
        #     ) + ", Error occured in finding matching elements"

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
            'similar_item_elements': optimal_generalized_common_xpath_part,
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
    # TODO: To be provided by Daniel
    def __init__(self):
        self.icon_type = "ScanWebPageWithAI"
        self.fn_name = "Scan web page with AI"
        self.type_category = ntcm.categories.webscraping
        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        return FormDictList()

    def direct_execute(self, elements, objective):        
        client = chatgpt_integration.create_open_ai_client()
        
        try:
            filtered_elements, total_element_groups_count, timeouts_count = chatgpt_integration.filter_elements_with_timeout(client, elements, objective)
        except concurrent.futures.TimeoutError as e:
            raise CriticalPipelineError(f"OpenAI server response takes too long: {e}")
        
        result = {
            "elements": filtered_elements,
            "total_element_groups_count": total_element_groups_count,
            "timeouts_count": timeouts_count
        }
        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(pipeline_uid=aet.active_pipeline_uid)
        kv_redis.set(redis_action_key, result)
        
        return result


browser_handlers_dict = {
    "FindSimilarItems": FindSimilarItemsHandler(),
    "ConvertToScrapingNodes": ConvertToScrapingNodesHandler(),
    "RefreshBrowserView": RefreshBrowserViewHandler(),
    "ScanWebPage": ScanWebPageHandler(),
    "ScanWebPageWithAI": ScanWebPageWithAIHandler(),
}
