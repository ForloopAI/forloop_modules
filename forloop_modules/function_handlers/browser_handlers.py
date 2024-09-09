import base64
from io import BytesIO
from pathlib import Path
from typing import Union, Optional

from PIL import Image

import forloop_modules.queries.node_context_requests_backend as ncrb
from docrawl.errors import SpiderFunctionError
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
        fdl.entry(name="elements", text="Elements", input_types=["list"], row=1)

        return fdl

    def execute(self, node_detail_form):
        elements = node_detail_form.get_chosen_value_by_name("elements", variable_handler)

        self.direct_execute(elements)

    def direct_execute_old(self, elements: list[dict]):
        # Get XPaths of selected elements
        xpaths = [x['xpath'] for x in elements]
        common_xpath_part, xpaths_leftovers = suh.cut_xpaths_to_common_part(xpaths)
        # Find optimal siblings level (index of XPath)
        generalized_common_xpath_parts, optimal_xpath_index = suh.get_generalized_xpaths(
            common_xpath_part
        )

        optimal_generalized_common_xpath_part = generalized_common_xpath_parts[optimal_xpath_index]

        elements_generalized_xpaths = [
            optimal_generalized_common_xpath_part + x + ';' for x in xpaths_leftovers
        ]

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

    def direct_execute(self, elements: list[dict]):
        # selected_elements_xpaths_new=selected_elements_xpaths.selected_elements_xpaths
        selected_elements_xpaths_new = elements
        # elements_positions=ncm.elements_positions_temp #temp storage TODO: should be done for users or send via API
        elements_positions = suh.elements_positions_temp  #temp storage TODO: should be done for users or send via API

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
            new_elements_positions = suh.update_webpage_elements(  # noqa
                refresh_browser_view_elements=False
            )
        except:
            flog.error('Error while loading elements positions!')
            # new_elements_positions = []  # noqa

        # api_user_flow_step=APIUserFlowStep(user_uid="1", step_identifier="WebExtractor_FindSimilarItems", step_data=str(len(new_elements_positions)), timestamp_utc=datetime.datetime.now())
        # acm.new_user_flow_step(api_user_flow_step)

        import difflib

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

            suh.matching_elements_groups = matching_elements_groups
            # message = "Selected elements XPATHs " + str(selected_elements_xpaths_new)
        except Exception as e:
            print(e)
            matching_elements = []
            matching_elements_groups = {}
            suh.matching_elements_groups = matching_elements_groups
            # message = "Selected elements XPATHs " + str(
            #     selected_elements_xpaths_new
            # ) + ", Error occured in finding matching elements"

        ##### FIND SIMILAR ITEMS X,Y POSITION ALGORITHMIC CORRECTION #####
        print("MATCHING GROUPS", matching_elements_groups)

        for k,v in matching_elements_groups.items():
            print("K,V", k,v)
            print(v[0])
            x_positions=[x["rect"]["x"] for x in v]
            y_positions=[x["rect"]["y"] for x in v]
            print("X POSITIONS", x_positions)
            print("Y POSITIONS", y_positions)
            print("ELEMENT_GROUP_LEN",len(v))

            x_positions_differences=[x_positions[i+1]-x_positions[i] for i,item in enumerate(x_positions) if i+1<len(x_positions)]
            y_positions_differences=[y_positions[i+1]-y_positions[i] for i,item in enumerate(y_positions) if i+1<len(y_positions)]
            print("X POSITIONS_DIFF", x_positions_differences)
            print("Y POSITIONS_DIFF", y_positions_differences)

            most_common_rounded_shift_in_x=[round(x/10)*10 for x in x_positions_differences]
            most_common_rounded_shift_in_y=[round(y/10)*10 for y in y_positions_differences]
            ##### TODO: Finish this!


        data = {
            # "message": message,
            # "ok": True,
            "elements_generalized_xpaths": elements_generalized_xpaths,
            "similar_items_xpaths": matching_elements,
            "similar_items_xpath_groups": matching_elements_groups,
        }
        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(redis_action_key, data)
        return data


class ConvertToScrapingNodesHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "ConvertToScrapingNodes"
        self.fn_name = "Convert To Scraping Nodes"
        self.type_category = ntcm.categories.webscraping
        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        fdl.entry(name="xpaths", text="", input_types=["str", "list"], row=1)

        return fdl

    def execute(self, node_detail_form):
        xpaths = node_detail_form.get_chosen_value_by_name("xpaths", variable_handler)

        self.direct_execute(xpaths)

    def direct_execute(self, xpaths: list[Union[str, list[str]]]):
        columns = [f"column_{i}" for i, _ in enumerate(xpaths)]

        variable_handler.create_variable("xpaths_ExtractXPathsToDf", xpaths)
        variable_handler.create_variable("columns_ExtractXPathsToDf", columns)
        ncrb.new_node(
            pos=[300, 300], typ="ExtractXPathsToDf", params_dict={
                "xpaths": {"variable": "xpaths_ExtractXPathsToDf", "value": ""},
                "columns": {"variable": "columns_ExtractXPathsToDf", "value": ""},
                "entry_df": {"variable": None, "value": ""},
                "write_mode": {"variable": None, "value": "Write"},
                "new_var_name": {"variable": None, "value": "results_ExtractXPathsToDf"},
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

    def execute(self, node_detail_form):
        self.direct_execute()

    def direct_execute(self):
        suh.refresh_browser_view()
        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(redis_action_key, suh.screenshot_string)


class ScanBrowserWebpageHandler(AbstractFunctionHandler):
    """ScanBrowserWebpage Node scans for all specified elements in the currently opened webpage BrowserView."""

    def __init__(self):
        self.icon_type = "ScanBrowserWebpage"
        self.fn_name = "Scan browser webpage"
        self.type_category = ntcm.categories.webscraping

        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        fdl.entry(name="url", text="", input_types=["str"], row=1)
        fdl.entry(
            name="incl_tables", text="Include tables", input_types=["bool"], row=2
        )
        fdl.entry(
            name="incl_bullets", text="Include bullets", input_types=["bool"], row=3
        )
        fdl.entry(
            name="incl_texts", text="Include texts", input_types=["bool"], row=4
        )
        fdl.entry(
            name="incl_headlines", text="Include headlines", input_types=["bool"], row=5
        )
        fdl.entry(
            name="incl_links", text="Include links", input_types=["bool"], row=6
        )
        fdl.entry(
            name="incl_images", text="Include images", input_types=["bool"], row=7
        )
        fdl.entry(
            name="incl_buttons", text="Include buttons", input_types=["bool"], row=8
        )
        fdl.entry(name="xpath", text="By XPath", input_types=["str"], row=9)

        return fdl

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        incl_tables = node_detail_form.get_chosen_value_by_name("incl_tables", variable_handler)
        incl_bullets = node_detail_form.get_chosen_value_by_name("incl_bullets", variable_handler)
        incl_texts = node_detail_form.get_chosen_value_by_name("incl_texts", variable_handler)
        incl_headlines = node_detail_form.get_chosen_value_by_name("incl_headlines", variable_handler)
        incl_links = node_detail_form.get_chosen_value_by_name("incl_links", variable_handler)
        incl_images = node_detail_form.get_chosen_value_by_name("incl_images", variable_handler)
        incl_buttons = node_detail_form.get_chosen_value_by_name("incl_buttons", variable_handler)
        xpath = node_detail_form.get_chosen_value_by_name("xpath", variable_handler)

        self.direct_execute(
            url, incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images, incl_buttons, xpath
        )

    def direct_execute(
        self, url: str, incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links,
        incl_images, incl_buttons, xpath
    ):
        scraping_options = {
            'incl_tables': incl_tables,
            'incl_bullets': incl_bullets,
            'incl_texts': incl_texts,
            'incl_headlines': incl_headlines,
            'incl_links': incl_links,
            'incl_images': incl_images,
            'incl_buttons': incl_buttons,
            'by_xpath': xpath
        }
        # start_time = time.perf_counter()
        # a_time = time.perf_counter() - start_time

        output_folder = Path.cwd() / 'tmp' / 'screenshots'

        # random_number = 50000000 + random.randint(1, 10000000)  # initialization

        # Websites where we want to handle blocking like Cloudflare - hardcoded for now

        # url = api_scan_webpage.url
        # api_user_flow_step=APIUserFlowStep(user_uid="1", step_identifier="WebExtractor_InputURL", step_data=url, timestamp_utc=datetime.datetime.utcnow())
        # acm.new_user_flow_step(api_user_flow_step)

        #API STATE UPDATE
        # matching_projects=[x for x in node_context_manager.projects if x.project_key==api_scan_webpage.email.replace("@","at")] #not ideal but easier than changing api_scan_webpage structure to pass project - in future could be refactored
        # if len(matching_projects)==1:
        #     project_uid=matching_projects[0].uid
        #     node_context_manager.project_uid_last_tutorial_scraped_url_dict[project_uid]=url
        # elif len(matching_projects)>1:
        #     raise Exception("There is more than 1 matching Forloop projects")
        # else:
        #     project_uid="0" #This shouldnt happen but is here for debug purposes

        # if any([x in url for x in websites_to_use_proxy_on]):
        #     flog.warning(f'Will use proxy for url {url}')
        #     # Scraper api proxy is too slow
        #     # ip = 'proxy-server.scraperapi.com'
        #     # port = '8001'
        #     # username = 'scraperapi'
        #     # password = scraping_config.SCRAPERAPI_KEY
        #     try:
        #         ip = 'proxy.scrapingbee.com'
        #         port = '8886'
        #         username = scraping_config.SCRPABINGBEE_KEY
        #         password = "render_js=False"

        #         # Save proxy to browser variables
        #         proxy = {'ip': ip, 'port': port, 'username': username, 'password': password}
        #     except Exception as e:
        #         flog.warning("Something went wrong with proxy setup, will not use proxy: "+str(e))
        #         proxy = None
        # else:
        #     proxy = None

        # b_time = time.perf_counter() - start_time

        #suh.webscraping_client.run_spider(driver='Firefox', in_browser=False, proxy=proxy)
        # suh.webscraping_client.acquire_browser(driver='Firefox', in_browser=False, proxy=proxy)

        # c_time = time.perf_counter() - start_time #0s

        # suh.webscraping_client.load_website(url, timeout=120)   # Increased timeout due to proxy usage
        # d_time = time.perf_counter() - start_time #10.5s

        # request_thread=slack_notif.UnblockingFunctionThread(slack_notif.send_slack_notification, api_scan_webpage.url, user_email=api_scan_webpage.email)
        # request_thread.start()

        #slack_notif.send_slack_notification(api_scan_webpage.url,user_email=api_scan_webpage.email)  #2 seconds
        # e_time = time.perf_counter() - start_time #12.5s
        if url:
            suh.webscraping_client.load_website(url, timeout=120)
        elements, screenshot_base64 = suh.scan_web_page_API(output_folder, scraping_options)  #9 seconds

        # # Convert PNG file to WEBP
        # img = Image.open(output_folder / "website.png")
        # img.save(output_folder / "website.webp", quality=85)
        # (output_folder / "website.png").unlink()
        # with open(output_folder / "website.webp", "rb") as file:
        #     screenshot = file.read()
        #     screenshot = base64.b64encode(screenshot).decode("utf-8")

        # Convert Base64 to WEBP
        png_img_data = BytesIO(base64.b64decode(screenshot_base64))
        webp_img_data = BytesIO()
        Image.open(png_img_data).save(webp_img_data, 'WEBP', quality=70)
        webp_img_data.seek(0)
        screenshot = base64.b64encode(webp_img_data.getvalue()).decode("utf-8")


        # f_time = time.perf_counter() - start_time #21.5s
        # request_thread.join()

        # i_time = time.perf_counter() - start_time #???s

        # ncm.elements_positions_temp=elements #temp storage TODO: should be done for users or send via API
        suh.elements_positions_temp = elements  #temp storage TODO: should be done for users or send via API

        # suh.webscraping_client.close_browser()      # Do not close browser, otherwise later refresh screenshot is not possible

        selected_elements_xpaths = []

        for i, element in enumerate(elements):
            # TODO handle link switch mode (extract link/text)
            elem_data = suh.extract_element_data(element)

            # elements[i]["data"] = elem_data

            if elem_data is not None:
                if "Mandalorian" in elem_data or "Real Estate Crowdfunding platform leads generation strategies" in elem_data:  #TEMP HERE, do not remove
                    selected_elements_xpaths += [elements[i]["xpath"]]

        selected_elements_xpaths = selected_elements_xpaths[:-1]  # temp

        def get_optimal_generalized_common_xpath_part():
            """Do not remove - important logic contained, this function takes roughly 9s because its scanning the website again with generalized xpath - could be potentially reused from previous scanning."""
            #Disabled for now because similar items are not found in scan now - but do not remove
            common_xpath_part, xpaths_leftovers = suh.cut_xpaths_to_common_part(
                selected_elements_xpaths
            )
            # g_time = time.perf_counter() - start_time #21.5s

            # Find optimal siblings level (index of XPath)
            generalized_common_xpath_parts, optimal_xpath_index = suh.get_generalized_xpaths(
                common_xpath_part
            )
            # h_time = time.perf_counter() - start_time #30.0s

            optimal_generalized_common_xpath_part = generalized_common_xpath_parts[
                optimal_xpath_index]

            optimal_generalized_common_xpath_part = None
            return (optimal_generalized_common_xpath_part)

        #optimal_generalized_common_xpath_part = get_optimal_generalized_common_xpath_part() #Disabled for now because similar items are not found in scan now - but do not remove

        # j_time = time.perf_counter() - start_time #???s

        # print("Scan and screenshot, Performance:"+"\n".join([str(x) for x in [a_time,b_time,c_time,d_time,e_time,f_time,i_time, j_time]])) #,g_time,h_time
        # elements_generalized_xpaths = [optimal_generalized_common_xpath_part + x + ';' for x in xpaths_leftovers]
        # print("ELEMENTS",elements_generalized_xpaths)
        # optimal_generalized_common_xpath_part = None
        data = {
            #'url': url,
            'elements': elements,
            # 'message': 'Page was successfully scanned.',
            # 'screenshot': f"website_{random_number}",
            'image': screenshot,
            # 'similar_item_elements': optimal_generalized_common_xpath_part,
            # 'total_pages': "???",
        }

        # node_context_manager.stored_xpaths = [x["xpath"] for x in elements]
        suh.stored_xpaths = [x["xpath"] for x in elements]

        # global scan_web_page_last_response_memory
        # scan_web_page_last_response_memory = response

        redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(
            pipeline_uid=aet.active_pipeline_uid
        )
        kv_redis.set(redis_action_key, data)
        return data


class FilterWebpageElementsWithAIHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "FilterWebpageElementsWithAI"
        self.fn_name = "Filter webpage elements with AI"
        self.type_category = ntcm.categories.webscraping
        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()
        fdl.entry(name="elements", text="Elements", input_types=["list"], row=1)
        fdl.entry(name="objective", text="Objective", input_types=["str"], row=2)

        return fdl

    def execute(self, node_detail_form):
        elements = node_detail_form.get_chosen_value_by_name("elements", variable_handler)
        objective = node_detail_form.get_chosen_value_by_name("objective", variable_handler)

        self.direct_execute(elements, objective)

    def direct_execute(self, elements, objective):
        response = ncrb.filter_webpage_elements_based_on_objective(elements=elements, objective=objective)

        if response.status_code in [200, 201]:
            result = response.json()
            redis_action_key = redis_config.SCRAPING_ACTION_KEY_TEMPLATE.format(pipeline_uid=aet.active_pipeline_uid)
            kv_redis.set(redis_action_key, result)
        else:
            raise CriticalPipelineError(response.reason)


browser_handlers_dict = {
    "FindSimilarItems": FindSimilarItemsHandler(),
    "ConvertToScrapingNodes": ConvertToScrapingNodesHandler(),
    "RefreshBrowserView": RefreshBrowserViewHandler(),
    "ScanBrowserWebpage": ScanBrowserWebpageHandler(),
    "FilterWebpageElementsWithAI": FilterWebpageElementsWithAIHandler(),
}
