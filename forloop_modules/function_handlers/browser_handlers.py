import difflib

from forloop_modules.flog import flog
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import (
    AbstractFunctionHandler,
)
from forloop_modules.function_handlers.auxilliary.docs import Docs
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

        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ScanWebPage Node takes 8 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Tables",
            name="incl_tables",
            description="Whether to include tables in search",
        )

        self.docs.add_parameter_table_row(
            title="Bullet lists",
            name="incl_bullets",
            description="Whether to include bullet lists in search",
        )

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("Selected XPaths")
        fdl.entry(name="xpaths", text="", input_types=["list"], row=1)

        return fdl

    # def execute(self, node_detail_form):
    #     xpaths = node_detail_form.get_chosen_value_by_name("xpaths", variable_handler)
    #     self.direct_execute(xpaths)

    # def execute_with_params(self, params):
    #     xpaths = params["xpaths"]
    #     self.direct_execute(xpaths)

    def direct_execute(self, xpaths: list):
        # NOTE: Should input be received with the job or extracted from suh?
        xpaths = suh.get_browser_view_selected_elements()
        selected_elements_xpaths_new = xpaths

        elements_positions = kv_redis.get("elements_positions_temp")
        # elements_positions = ncm.elements_positions_temp  #temp storage TODO: should be done for users or send via API
        #for i,element_pos in enumerate(elements_positions):
        #if element_pos["data"] is not None:
        #    with open(element_pos["data"],"rb") as file:
        #        new_data=pickle.load(file)
        #else:
        #    new_data=[]
        #elements_positions[i]["data"]=new_data

        selected_elements_referential_names = [
            x["name"] for x in selected_elements_xpaths_new
        ]  #referential means the referential selected item by which other similar items are found

        selected_elements_xpaths_new = [x["xpath"] for x in selected_elements_xpaths_new]  #temp
        common_xpath_part, xpaths_leftovers = suh.cut_xpaths_to_common_part(
            selected_elements_xpaths_new
        )
        # Find optimal siblings level (index of XPath)
        generalized_common_xpath_parts, optimal_xpath_index = suh.get_generalized_xpaths(
            common_xpath_part
        )  #scans the website n+1 times - could be optimized?
        optimal_generalized_common_xpath_part = generalized_common_xpath_parts[optimal_xpath_index]

        elements_generalized_xpaths = [
            optimal_generalized_common_xpath_part + x for x in xpaths_leftovers
        ]

        xpath_leftover_referential_name_dict = {}
        assert len(xpaths_leftovers) == len(
            selected_elements_referential_names
        )  #to ensure that the following for loop will be well defined
        for i, xpath_leftover in enumerate(xpaths_leftovers):
            xpath_leftover_referential_name_dict[xpath_leftover
                                                ] = selected_elements_referential_names[i]

        # new_elements_positions after scanning of generalized elements in suh.get_generalized_xpaths
        try:
            suh.update_webpage_elements(refresh_browser_view_elements=False)
        except:
            flog.error('Error while loading elements positions!')

        def extract_shared_prefix_and_suffix_from_strings_and_substring(string, substring):

            #string = "/html/body/div[4]/div/div/div/div/div[1]/a/div"
            #substring = "/html/body/div[4]/div/div/div/div/div/a/div"

            # Finding the differing part between the two strings
            differ = difflib.ndiff(string, substring)
            diff_indices = [i for i, d in enumerate(differ) if d.startswith('-')]

            # Splitting the string into three pieces
            before_piece = string[:diff_indices[0]]
            string[diff_indices[0]:diff_indices[-1] + 1]
            after_piece = string[diff_indices[-1] + 1:]

            #Before Piece: /html/body/div[4]/div/div/div/div/div
            #Middle Piece: [1]
            #After Piece: /a/div

            return (before_piece, after_piece)

        try:
            before_piece, after_piece = extract_shared_prefix_and_suffix_from_strings_and_substring(
                common_xpath_part, optimal_generalized_common_xpath_part
            )

            #NOT IDEAL - problem if generalized xpath isnt done in on the last tag
            matching_elements = []

            matching_elements_groups = {}
            for _j, xpath_leftover in enumerate(
                xpaths_leftovers
            ):  #initialization of lists in groups
                referential_element_group = xpath_leftover_referential_name_dict[xpath_leftover]
                matching_elements_groups[referential_element_group] = []

            for i, element_position in enumerate(elements_positions):

                is_at_least_one_leftover_contained = False
                is_generalized_common_xpath_part_contained = False
                if before_piece in element_position["xpath"] and after_piece in element_position[
                    "xpath"]:
                    is_generalized_common_xpath_part_contained = True
                for _j, xpath_leftover in enumerate(xpaths_leftovers):
                    if element_position["xpath"].endswith(xpath_leftover.replace(";", "")):
                        is_at_least_one_leftover_contained = True
                        referential_element_group = xpath_leftover_referential_name_dict[
                            xpath_leftover]

                if is_generalized_common_xpath_part_contained and is_at_least_one_leftover_contained:
                    matching_elements.append(element_position["xpath"])
                    matching_elements_groups[referential_element_group].append(
                        element_position
                    )  #["xpath"]

            # for i,element_position in enumerate(elements_positions):

            #     is_at_least_one_leftover_contained=False
            #     is_generalized_common_xpath_part_contained=False
            #     if before_piece in element_position["xpath"] and after_piece in element_position["xpath"]:
            #         is_generalized_common_xpath_part_contained=True
            #     print(is_generalized_common_xpath_part_contained, element_position["xpath"])
            #     for j,xpath_leftover in enumerate(xpaths_leftovers):
            #         if xpath_leftover.replace(";","") in element_position["xpath"]:
            #             is_at_least_one_leftover_contained=True

            #     print(is_at_least_one_leftover_contained, xpaths_leftovers)

            #     if is_generalized_common_xpath_part_contained and is_at_least_one_leftover_contained:
            #         matching_elements.append(element_position["xpath"])
            kv_redis.set("matching_elements_groups", matching_elements_groups)
            # ncm.matching_elements_groups = matching_elements_groups
            message = "Selected elements XPATHs " + str(selected_elements_xpaths_new)
        except Exception as e:
            print(e)
            matching_elements = []
            matching_elements_groups = {}
            kv_redis.set("matching_elements_groups", matching_elements_groups)
            # ncm.matching_elements_groups = matching_elements_groups
            message = "Selected elements XPATHs " + str(
                selected_elements_xpaths_new
            ) + ", Error occured in finding matching elements"

        return {
            "message": message, "ok": True,
            "elements_generalized_xpaths": elements_generalized_xpaths,
            "similar_items_xpaths": matching_elements,
            "similar_items_xpath_groups": matching_elements_groups
        }


class PrepareIconsHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "PrepareIcons"
        self.fn_name = "Prepare Icons"
        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        return FormDictList()

    def direct_execute():
        pass


class RefreshBrowserViewHandler(AbstractFunctionHandler):
    """The RefreshBrowserView node refreshes the currently loaded webpage in the BrowserView."""

    def __init__(self):
        self.icon_type = "RefreshBrowserView"
        self.fn_name = "Refresh Browser View"
        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        self._init_docs()
        super().__init__()

    def _init_docs(self):
        parameters_description = ""
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        return fdl

    # def execute(self, node_detail_form):
    #     self.direct_execute()

    # def execute_with_params(self, params):
    #     self.direct_execute()

    def direct_execute(self, selected_xpaths: list):
        suh.refresh_browser_view()


class ScanWebPageHandler(AbstractFunctionHandler):
    """ScanWebPage Node looks for certain type of elements on web page and displays them in BrowserView."""

    def __init__(self):
        self.icon_type = "ScanWebPage"
        self.fn_name = "Scan web page"
        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ScanWebPage Node takes 8 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Tables",
            name="incl_tables",
            description="Whether to include tables in search",
        )

        self.docs.add_parameter_table_row(
            title="Bullet lists",
            name="incl_bullets",
            description="Whether to include bullet lists in search",
        )

        self.docs.add_parameter_table_row(
            title="Texts",
            name="incl_texts",
            description="Whether to include texts in search",
        )

        self.docs.add_parameter_table_row(
            title="Headlines",
            name="incl_headlines",
            description="Whether to include headlines in search",
        )

        self.docs.add_parameter_table_row(
            title="Links",
            name="incl_links",
            description="Whether to include links in search",
        )

        self.docs.add_parameter_table_row(
            title="Images",
            name="incl_images",
            description="Whether to include images in search",
        )

        self.docs.add_parameter_table_row(
            title="Buttons",
            name="incl_buttons",
            description="Whether to include buttons in search",
        )

        self.docs.add_parameter_table_row(
            title="By XPath", name="by_xpath", description="XPath of custom elements to search",
            typ="string", example=[
                '//div[@class="regular-price"]/text()', '//span[contains(text(), "Location")]'
            ]
        )

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("Tables")
        fdl.checkbox(name="incl_tables", bool_value=True, row=1)
        fdl.label("Bullet lists")
        fdl.checkbox(name="incl_bullets", bool_value=True, row=2)
        fdl.label("Texts")
        fdl.checkbox(name="incl_texts", bool_value=True, row=3)
        fdl.label("Headlines")
        fdl.checkbox(name="incl_headlines", bool_value=True, row=4)
        fdl.label("Links")
        fdl.checkbox(name="incl_links", bool_value=True, row=5)
        fdl.label("Images")
        fdl.checkbox(name="incl_images", bool_value=True, row=6)
        fdl.label("Buttons")
        fdl.checkbox(name="incl_buttons", bool_value=True, row=7)
        fdl.label("By XPath")
        fdl.entry(name="by_xpath", text="", input_types=["str"], row=8)

        return fdl

    def execute(self, node_detail_form):
        incl_tables = node_detail_form.get_chosen_value_by_name("incl_tables", variable_handler)
        incl_bullets = node_detail_form.get_chosen_value_by_name("incl_bullets", variable_handler)
        incl_texts = node_detail_form.get_chosen_value_by_name("incl_texts", variable_handler)
        incl_headlines = node_detail_form.get_chosen_value_by_name(
            "incl_headlines", variable_handler
        )
        incl_links = node_detail_form.get_chosen_value_by_name("incl_links", variable_handler)
        incl_images = node_detail_form.get_chosen_value_by_name("incl_images", variable_handler)
        incl_buttons = node_detail_form.get_chosen_value_by_name("incl_buttons", variable_handler)
        by_xpath = node_detail_form.get_chosen_value_by_name("by_xpath", variable_handler)

        self.direct_execute(
            incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images,
            incl_buttons, by_xpath
        )

    def execute_with_params(self, params):
        incl_tables = params["incl_tables"]
        incl_bullets = params["incl_bullets"]
        incl_texts = params["incl_texts"]
        incl_headlines = params["incl_headlines"]
        incl_links = params["incl_links"]
        incl_images = params["incl_images"]
        incl_buttons = params["incl_buttons"]
        by_xpath = params["by_xpath"]

        self.direct_execute(
            incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images,
            incl_buttons, by_xpath
        )

    def direct_execute(
        self, incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images,
        incl_buttons, by_xpath, context_xpath=''
    ):
        elements = suh.scan_web_page(
            incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images,
            incl_buttons, by_xpath, context_xpath
        )
        redis_prefix_key = redis_config.SCRAPING_KEY_PREFIX.format(
            project_uid=aet.project_uid, 
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(redis_prefix_key+ "scan_web_page", elements)


class ScanWebPageWithAIHandler(AbstractFunctionHandler):
    # TODO: To be provided by Daniel
    def __init__(self):
        self.icon_type = "ScanWebPageWithAI"
        self.fn_name = "Scan web page with AI"
        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        return FormDictList()

    def direct_execute():
        pass


browser_handlers_dict = {
    "FindSimilarItems": FindSimilarItemsHandler(),
    "PrepareIcons": PrepareIconsHandler(),
    "RefreshBrowserView": RefreshBrowserViewHandler(),
    "ScanWebPage": ScanWebPageHandler(),
    "ScanWebPageWithAI": ScanWebPageWithAIHandler(),
}
