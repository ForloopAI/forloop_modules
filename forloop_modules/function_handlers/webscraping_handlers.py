import os
import time
import requests
import io
import sys
if not "linux" in sys.platform:
    import pywinauto

from pathlib import Path
from keepvariable.keepvariable_core import Var, save_variables, kept_variables

import forloop_modules.flog as flog

from forloop_modules.globals.variable_handler import variable_handler
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_common_structures.core.variable import Variable

from forloop_modules.errors.errors import CriticalPipelineError
from forloop_modules.function_handlers.variable_handlers import variable_handlers_dict
from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.globals.scraping_utilities_handler import suh

from forloop_modules.redis.redis_connection import kv_redis

####################### SCRAPING HANDLERS ################################


# # # # # # # # # HANDLERS # # # # # # # # #


class OpenBrowserHandler:
    def __init__(self):
        self.icon_type = "OpenBrowser"
        self.fn_name = "Open Browser"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Show browser:")
        fdl.checkbox(name="in_browser", bool_value=True, row=1)
        fdl.label("Driver:")
        fdl.combobox(name="driver", options=['Firefox', 'Chrome'], default='Firefox', show_info=True, row=2)

        return fdl

    def execute(self, node_detail_form):
        in_browser = node_detail_form.get_chosen_value_by_name("in_browser", variable_handler)
        driver = node_detail_form.get_chosen_value_by_name("driver", variable_handler)

        self.direct_execute(in_browser, driver)

    def execute_with_params(self, params):
        in_browser = params["in_browser"]
        driver = params["driver"]

        self.direct_execute(in_browser, driver)

    def direct_execute(self, in_browser, driver):
        flog.info(f'ARGS OPEN BROWSER: {str([in_browser, driver])}')

        suh.webscraping_client.run_spider(driver=driver, in_browser=in_browser)

    def export_code(self, node_detail_form):
        """TODO"""

        in_browser = node_detail_form.get_chosen_value_by_name("in_browser", variable_handler)
        driver = node_detail_form.get_chosen_value_by_name("driver", variable_handler)
        
        code = f"""
        # Initialize the {driver} WebDriver
        driver = webdriver.{driver}()
        """
        
        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class LoadWebsiteHandler:
    def __init__(self):
        self.icon_type = "LoadWebsite"
        self.fn_name = "Load Website"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Url")
        fdl.entry(name="url", text="", input_types=["str"], required=True, show_info=True, row=1)
        fdl.label("Take screenshot")
        fdl.checkbox(name="take_screenshot", bool_value=True, row=2)
        fdl.label("Screenshot will be shown in Browser View", row=3)

        return fdl

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        take_screenshot = node_detail_form.get_chosen_value_by_name("take_screenshot", variable_handler)

        self.direct_execute(url, take_screenshot)

    def execute_with_params(self, params):
        url = params["url"]
        take_screenshot = params["take_screenshot"]

        self.direct_execute(url, take_screenshot)

    def direct_execute(self, url, take_screenshot):
        if url:
            browser_meta_data = suh.webscraping_client.get_browser_meta_data()

            # The following code is needed to be able to open page after loading another page from status bar
            try:
                spider_requests = browser_meta_data['request']
            except:
                spider_requests = {"url": url, "loaded": False}

            if spider_requests is None:
                spider_requests = {"url": url, "loaded": False}

            # If any page was loaded before -> close browser instance
            if spider_requests['loaded']:
                # Load last executed function
                try:
                    spider_functions = browser_meta_data['function']
                except:
                    spider_functions = None

                if spider_functions is not None:
                    if spider_functions['name'] != 'close_browser':
                        # suh.webscraping_client.close_browser()
                        pass

                    # If last executed function was close_browser -> do not close browser instance again (will close active browser)
                    else:
                        spider_functions = {"name": 'close_browser', "input": None, "done": True}

                        browser_meta_data['function'] = spider_functions

                        suh.webscraping_client.set_browser_meta_data(browser_meta_data)

            suh.webscraping_client.load_website(url)

            # Take screenshot of current page
            if take_screenshot:
                suh.take_and_load_screenshot()

                #suh.detect_cookies_popup()

    def load_website_from_status_bar(self, url, take_screenshot):

        suh.webscraping_client.load_website(url)

        # Take screenshot of current page
        if take_screenshot:
            suh.take_and_load_screenshot()

            suh.detect_cookies_popup()

    def export_code(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        
        code = f"""
        # Navigate to a website in the current window
        driver.get("{url}") # a "driver" is a selenium.webdriver object initialized by OpenBrowser node --> rename according to your taste
        """

        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class NextPageHandler:
    """
    Handler for NextPage icon. This icon is used to iterate through pages (performs pagination), using URL.
        Input:
            - string url_prefix - URL prefix, e.g https://mywebsite.com/listings?page=
            - string url_suffix - URL prefix, e.g. &filter=myFilter
            - string page_varname - variable name to be created in variable explorer
        Output:
            None
     """

    def __init__(self):
        self.icon_type = "NextPage"
        self.fn_name = "Next Page"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("URL prefix")
        fdl.entry(name="url_prefix", text="", required=True, row=1)
        fdl.label("URL suffix")
        fdl.entry(name="url_suffix", text="", row=2)
        fdl.label("Page variable name")
        fdl.entry(name="page_varname", text="", required=True, row=3)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True, row=4)

        return fdl

    def execute(self, node_detail_form):
        url_prefix = node_detail_form.get_chosen_value_by_name("url_prefix", variable_handler)
        url_suffix = node_detail_form.get_chosen_value_by_name("url_suffix", variable_handler)
        page_varname = node_detail_form.get_chosen_value_by_name("page_varname", variable_handler)

        self.direct_execute(url_prefix, url_suffix, page_varname)

    def execute_with_params(self, params):
        url_prefix = params["url_prefix"]
        url_suffix = params["url_suffix"]
        page_varname = params["page_varname"]

        self.direct_execute(url_prefix, url_suffix, page_varname)

    def direct_execute(self, url_prefix: str, url_suffix: str, page_varname: str) -> None:
        page_var = variable_handler.variables.get(page_varname)

        # Strip prefix/suffix from the URL - only a page_count variable must stay
        try:
            page_count_string = page_var.value[len(url_prefix):]
            # If 'url_suffix' is an empty list, slicing 'page_count_string' would return an empty list as well
            page_count_string = (
                page_count_string[:-len(url_suffix)] if len(url_suffix) != 0 else page_count_string
            )
            page_count = int(page_count_string)
            new_url = f"{url_prefix}{page_count + 1}{url_suffix}"
        except Exception as e: #(AttributeError, ValueError, TypeError, IndexError) as e:
            raise CriticalPipelineError("NextPage handler failed to execute") from e

        variable_handler.new_variable(page_varname, new_url)
        suh.webscraping_client.load_website(new_url)


    def export_code(self, node_detail_form):
        """TODO"""

        url_prefix = node_detail_form.get_chosen_value_by_name("url_prefix", variable_handler)
        url_suffix = node_detail_form.get_chosen_value_by_name("url_suffix", variable_handler)
        page_varname = node_detail_form.get_chosen_value_by_name("page_varname", variable_handler)

        code = ''
        return code

    def export_imports(self, *args):
        """TODO"""
        imports = ["import docrawl_launcher"]

        return imports


class ClickXPathHandler:
    def __init__(self):
        self.icon_type = "ClickXPath"
        self.fn_name = "Click XPath"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label("Click XPath element")
        fdl.label("XPath:")
        fdl.entry(name="xpath", text="", input_types=["str"], required=True, show_info=True, row=1)

        return fdl

    def execute(self, node_detail_form):
        xpath = node_detail_form.get_chosen_value_by_name("xpath", variable_handler)

        self.direct_execute(xpath)

    def execute_with_params(self, params):
        xpath = params["xpath"]

        self.direct_execute(xpath)

    def direct_execute(self, xpath):
        """
        Example: '//div[@class="media-select__input-content"]//button'
        """

        xpath = suh.check_xpath_apostrophes(xpath)
        suh.webscraping_client.click_xpath(xpath)

    def export_code(self, node_detail_form):
        xpath = node_detail_form.get_chosen_value_by_name("xpath", variable_handler)

        code = f"""
        # Find the element using XPath and click it
        element_xpath = "{xpath}"
        element = driver.find_element(By.XPATH, element_xpath) # a "driver" is a selenium.webdriver object initialized by OpenBrowser node --> rename according to your taste
        element.click()
        """
        
        return code

    def export_imports(self, *args):
        imports = [
            "from selenium import webdriver",
            "from selenium.webdriver.common.by import By"
            ]

        return imports


class ClickNameHandler:
    def __init__(self):
        self.icon_type = "ClickName"
        self.fn_name = "Click Name"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label("Click on element with given text")
        fdl.label("Text")
        fdl.entry(name="text", text="", input_types=["str"], required=True, row=1)

        return fdl

    def execute(self, node_detail_form):
        text = node_detail_form.get_chosen_value_by_name("text", variable_handler)

        self.direct_execute(text)

    def execute_with_params(self, params):
        text = params["text"]

        self.direct_execute(text)

    def direct_execute(self, text):
        suh.webscraping_client.click_name(text)

    def export_code(self, node_detail_form):
        text = node_detail_form.get_chosen_value_by_name("text", variable_handler)

        code = f"""
        # Specify the text you want to search for on the webpage
        target_text = "{text}"

        try:
            # Find the element with the specified text using XPath
            element = driver.find_element(By.XPATH, f'//*[contains(text(), target_text)]')

            # Click the element
            element.click()

        except Exception as e:
            print(f"Error: {{e}}")
        """

        return code

    def export_imports(self, *args):
        imports = [
            "from selenium import webdriver",
            "from selenium.webdriver.common.by import By"
            ]

        return imports


class ClickIdHandler:
    def __init__(self):
        self.icon_type = "ClickId"
        self.fn_name = "Click Id"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label("Click element by id")
        fdl.label("Id:")
        fdl.entry(name="id", text="", input_types=["str"], required=True, row=1)

        return fdl

    def execute(self, node_detail_form):
        click_id = node_detail_form.get_chosen_value_by_name("id", variable_handler)

        self.direct_execute(click_id)

    def execute_with_params(self, params):
        click_id = params["id"]

        self.direct_execute(click_id)

    def direct_execute(self, click_id):
        function = "exec"
        inp = "self.browser.find_element_by_id('" + click_id + "').click()"
        # docrawl_core.spider_functions={"function":function,"input":inp,"done":"False"}
        spider_functions = Var({"function": function, "input": inp, "done": False})
        save_variables(kept_variables)

    def export_code(self, node_detail_form):
        click_id = node_detail_form.get_chosen_value_by_name("id", variable_handler)

        code = f"""
        # Specify the ID of the element you want to click
        element_id = "{click_id}"

        try:
            # Find the element by its ID
            element = driver.find_element_by_id(element_id)

            # Click the element
            element.click()

        except Exception as e:
            print(f"Error: {{e}}")
        """

        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class CloseBrowserHandler:
    """
    Handler for CloseBrowser icon. This icon closes browser (deletes driver instance).
        Input: None
        Output: None
    """

    def __init__(self):
        self.icon_type = "CloseBrowser"
        self.fn_name = "Close browser after scraping"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label("Close browser after scraping")

        return fdl

    def execute(self, node_detail_form):
        self.direct_execute()

    def execute_with_params(self, params):
        self.direct_execute()

    def direct_execute(self):
        suh.webscraping_client.close_browser()

    def export_code(self, node_detail_form):
        code = """
        # Close the WebDriver
        driver.quit()
        """

        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class GetCurrentURLHandler:
    """
    Handler for GetCurrentURL icon. This icon exports the URL of the current opened website.
        Input: name for variable, that would be created within platform
        Output: new variable inside platform and .txt file

    Note: output file current_url.txt is used only for "internal" purposes, meaning for loading data from it,
    transferring to other icons etc.
    Therefore, this file may be deleted after icon's function was proceeded <- (NOT IMPLEMENTED YET)
    """

    def __init__(self):
        self.icon_type = "GetCurrentURL"
        self.fn_name = "Get current URL"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Output variable")
        fdl.entry(name="output", text="", input_types=["str"], required=True, row=1)

        return fdl

    def execute(self, node_detail_form):
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        self.direct_execute(output)

    def execute_with_params(self, params):
        output = params["output"]

        self.direct_execute(output)

    def direct_execute(self, output):
        filename = 'current_url.txt'

        time.sleep(10)  # !!! IMPORTANT !!! <- Necessary delay due to problems with crawler thread

        suh.webscraping_client.get_current_url(filename)

        data = suh.wait_until_data_is_extracted(filename, timeout=3)

        if data is not None:
            params = {"variable_name": output, "variable_value": str(data)}
            variable_handlers_dict["NewVariable"].direct_execute(params['variable_name'], params['variable_value'])
            ##variable_handler.update_data_in_variable_explorer(glc)

            # Delete file (this block can be removed if there is need to save current_url outside platform)
            try:
                os.remove(filename)
            except:
                flog.error('Error while deleting file!')

    def export_code(self, node_detail_form):
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        code = f"""
        # Get the current URL
        {output} = driver.current_url
        """
        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class WaitUntilElementIsLocatedHandler:
    """
    Handler for WaitUntilElementIsLocated. This icon waits until certain element appears and page and then clicks on it.
        Input: XPath of element to be located
        Output: None
    """

    def __init__(self):
        self.icon_type = "WaitUntilElementIsLocated"
        self.fn_name = "Wait until element is located"

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("XPath")
        fdl.entry(name="xpath", text="", input_types=["str"], required=True, row=1)

        return fdl

    def execute(self, node_detail_form):
        xpath = node_detail_form.get_chosen_value_by_name("xpath", variable_handler)

        self.direct_execute(xpath)

    def execute_with_params(self, params):
        xpath = params["xpath"]

        self.direct_execute(xpath)

    def direct_execute(self, xpath):
        xpath = suh.check_xpath_apostrophes(xpath)
        suh.webscraping_client.wait_until_element_is_located(xpath)

        # Take screenshot of current page
        suh.take_and_load_screenshot()

    def export_code(self, node_detail_form):
        xpath = node_detail_form.get_chosen_value_by_name("xpath", variable_handler)

        code = f"""
        # Specify the XPath of the element you want to wait for
        element_xpath = "{xpath}"

        try:
            # Wait for the element to be located
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, element_xpath))
            )

            # Element located, you can now interact with it
            # For example, you can click it: element.click()

        except Exception as e:
            print(f"Error: {{e}}")
        """
        return code

    def export_imports(self, *args):
        imports = [
            "from selenium import webdriver",
            "from selenium.webdriver.common.by import By",
            "from selenium.webdriver.support.ui import WebDriverWait",
            "from selenium.webdriver.support import expected_conditions as EC"
            ]

        return imports


class ScrollWebPageHandler:
    """
    Handler for ScrollWebPageHandler icon. This icon scrolls page up/down by n-pixels.
    Special option is "max" - scrolls page to the top or end of page
        Input:
            - combobox Scroll with directions (Up / Down)
            - entry By (px): scroll distance in pixels or "max"
            - checkbox scroll max: boolean, scroll to max
        Output: None
    """

    def __init__(self):
        self.icon_type = "ScrollWebPage"
        self.fn_name = "Scroll web page"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["Up", "Down"]

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Scroll")
        fdl.combobox(name="scroll_to", options=options, default="Down", row=1)
        fdl.label("By (px):")
        fdl.entry(name="scroll_by", text="", input_types=["int", "float"], row=2)
        fdl.label("Scroll max:")
        fdl.checkbox(name="scroll_max", bool_value=False, row=3)

        return fdl

    def execute(self, node_detail_form):
        scroll_to = node_detail_form.get_chosen_value_by_name("scroll_to", variable_handler)
        scroll_by = node_detail_form.get_chosen_value_by_name("scroll_by", variable_handler)
        scroll_max = node_detail_form.get_chosen_value_by_name("scroll_max", variable_handler)

        self.direct_execute(scroll_to, scroll_by, scroll_max)

    def execute_with_params(self, params):
        scroll_to = params["scroll_to"]
        scroll_by = params["scroll_by"]
        scroll_max = params["scroll_max"]

        self.direct_execute(scroll_to, scroll_by, scroll_max)

    def direct_execute(self, scroll_to, scroll_by, scroll_max):
        suh.webscraping_client.scroll_web_page(scroll_to, scroll_by, scroll_max)

    def export_code(self, node_detail_form):
        scroll_to = node_detail_form.get_chosen_value_by_name("scroll_to", variable_handler)
        scroll_by = node_detail_form.get_chosen_value_by_name("scroll_by", variable_handler)
        scroll_max = node_detail_form.get_chosen_value_by_name("scroll_max", variable_handler)
        
        sign = "+" if scroll_to == "Down" else "-"

        code = f"""
        # Scroll by a specified number of pixels
        scroll_up_pixels = {sign}{scroll_by}  # Negative value to scroll up, positive to scroll down
        driver.execute_script(f"window.scrollBy(0, {{scroll_up_pixels}});")
        """

        scroll_to_top_code = """
        # Scroll to the top of the page
        driver.execute_script("window.scrollTo(0, 0);")
        """

        scroll_to_bottom_code = """
        # Scroll to the bottom of the page
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        """
        
        if scroll_max:
            code += scroll_to_bottom_code if scroll_to == "Down" else scroll_to_top_code
        
        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class ScanWebPageHandler:
    """
    Handler for ScanWebPage icon. This icon finds tables and bullet lists on page, passes them
    to Browser View with coordinates on screenshot.
        Input:
            - checkbox incl_tables - find tables on page
            - checkbox incl_bullets - find bullet lists on page
            - checkbox incl_texts - find text elements on page
            - checkbox incl_headlines - find headlines on page
            - checkbox incl_links - find links on page
            - entry by_xpath - find elements by custom XPath
        Output:
            Coordinates of found elements along with page screenshot, passed to Browser View

    """

    def __init__(self):

        self.icon_type = "ScanWebPage"
        self.fn_name = "Scan web page"
        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Tables")
        fdl.checkbox(name="incl_tables", bool_value=False, row=1)
        fdl.label("Bullet lists")
        fdl.checkbox(name="incl_bullets", bool_value=False, row=2)
        fdl.label("Texts")
        fdl.checkbox(name="incl_texts", bool_value=False, row=3)
        fdl.label("Headlines")
        fdl.checkbox(name="incl_headlines", bool_value=False, row=4)
        fdl.label("Links")
        fdl.checkbox(name="incl_links", bool_value=False, row=5)
        fdl.label("Images")
        fdl.checkbox(name="incl_images", bool_value=False, row=6)
        fdl.label("Buttons")
        fdl.checkbox(name="incl_buttons", bool_value=False, row=7)
        fdl.label("By XPath")
        fdl.entry(name="by_xpath", text="", input_types=["str"], row=8)

        return fdl

    def execute(self, node_detail_form):
        incl_tables = node_detail_form.get_chosen_value_by_name("incl_tables", variable_handler)
        incl_bullets = node_detail_form.get_chosen_value_by_name("incl_bullets", variable_handler)
        incl_texts = node_detail_form.get_chosen_value_by_name("incl_texts", variable_handler)
        incl_headlines = node_detail_form.get_chosen_value_by_name("incl_headlines", variable_handler)
        incl_links = node_detail_form.get_chosen_value_by_name("incl_links", variable_handler)
        incl_images = node_detail_form.get_chosen_value_by_name("incl_images", variable_handler)
        incl_buttons = node_detail_form.get_chosen_value_by_name("incl_buttons", variable_handler)
        by_xpath = node_detail_form.get_chosen_value_by_name("by_xpath", variable_handler)

        self.direct_execute(incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images, incl_buttons, by_xpath)

    def execute_with_params(self, params):
        incl_tables = params["incl_tables"]
        incl_bullets = params["incl_bullets"]
        incl_texts = params["incl_texts"]
        incl_headlines = params["incl_headlines"]
        incl_links = params["incl_links"]
        incl_images = params["incl_images"]
        incl_buttons = params["incl_buttons"]
        by_xpath = params["by_xpath"]

        self.direct_execute(incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images, incl_buttons, by_xpath)


    def direct_execute(self, incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images, incl_buttons, by_xpath, context_xpath=''):
        suh.scan_web_page(incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images, incl_buttons, by_xpath, context_xpath)


    def export_code(self, node_detail_form):
        incl_tables = node_detail_form.get_chosen_value_by_name("incl_tables", variable_handler)
        incl_bullets = node_detail_form.get_chosen_value_by_name("incl_bullets", variable_handler)
        incl_texts = node_detail_form.get_chosen_value_by_name("incl_texts", variable_handler)
        incl_headlines = node_detail_form.get_chosen_value_by_name("incl_headlines", variable_handler)
        incl_links = node_detail_form.get_chosen_value_by_name("incl_links", variable_handler)
        incl_images = node_detail_form.get_chosen_value_by_name("incl_images", variable_handler)
        incl_buttons = node_detail_form.get_chosen_value_by_name("incl_buttons", variable_handler)
        by_xpath = node_detail_form.get_chosen_value_by_name("by_xpath", variable_handler)

        code = f"""
        webpage_elements = {{}}
        """
        
        if incl_tables:
            tables_codelines = [
            '# Find and process tables',
            'tables = driver.find_elements_by_tag_name("table")',
            'webpage_elements["tables"] = tables',
            ''
            ]
            tables_code = "\n".join(tables_codelines)
            code += tables_code
            
        if incl_bullets:
            bullets_codelines = [
            '# Find and process bullet lists',
            'lists = driver.find_elements_by_tag_name("ul")',
            'webpage_elements["lists"] = lists',
            ''
            ]
            bullets_code = "\n".join(bullets_codelines)
            code += bullets_code
            
        if incl_texts:
            texts_codelines = [
            '# Find and process paragraphs',
            'texts = driver.find_elements_by_tag_name("p")',
            'webpage_elements["texts"] = texts',
            ''
            ]
            texts_code = "\n".join(texts_codelines)
            code += texts_code
            
        if incl_headlines:
            headlines_codelines = [
            '# Find and process headlines',
            'headlines = driver.find_elements_by_xpath("//h1 | //h2 | //h3 | //h4 | //h5 | //h6")',
            'webpage_elements["headlines"] = headlines',
            ''
            ]
            headlines_code = "\n".join(headlines_codelines)
            code += headlines_code
            
        if incl_links:
            links_codelines = [
            '# Find and process links',
            'links = driver.find_elements_by_tag_name("a")',
            'webpage_elements["links"] = links',
            ''
            ]
            links_code = "\n".join(links_codelines)
            code += links_code
            
        if incl_images:
            images_codelines = [
            '# Find and process images',
            'images = driver.find_elements_by_tag_name("img")',
            'webpage_elements["images"] = images',
            ''
            ]
            images_code = "\n".join(images_codelines)
            code += images_code
            
        if incl_buttons:
            buttons_codelines = [
            '# Find and process buttons',
            'buttons = driver.find_elements_by_tag_name("button")',
            'webpage_elements["buttons"] = buttons',
            ''
            ]
            buttons_code = "\n".join(buttons_codelines)
            code += buttons_code
            
        if by_xpath:
            xpath_codelines = [
            '# Find elements by XPath',
            f'xpath_elements = driver.find_elements_by_xpath("{by_xpath}")',
            'webpage_elements["xpath_elements"] = xpath_elements',
            ''
            ]
            xpath_code = "\n".join(xpath_codelines)
            code += xpath_code
            
        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class ExtractXPathHandler:
    def __init__(self):
        self.icon_type = "ExtractXPath"
        self.fn_name = "Extract XPath"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        options = ["w+", "a+"]
        options_2 = ['DataFrame']

        fdl = FormDictList()
        fdl.label("Extract HTML element by XPath")
        fdl.label("XPath")
        fdl.entry(name="xpath", text="", input_types=["str"], required=True, row=1)
        fdl.label("Output variable")
        fdl.entry(name="output", text="", input_types=["str"], required=True, row=2)
        fdl.label("Write in file mode")
        fdl.combobox(name="write_mode", options=options, default="w+", row=3)
        fdl.label("Save to")
        fdl.combobox(name="save_to", options=options_2, default="DataFrame", row=4)
        # Should be dynamically appended when Save to DataFrame selected
        fdl.label("Column name")
        fdl.entry(name="column", text="", input_types=["str"], required=False, show_info=True, row=5)
        fdl.label('Store list as string')
        fdl.checkbox(name="list_as_str", bool_value=True, row=6)

        return fdl

    def execute(self, node_detail_form):
        xpath = node_detail_form.get_chosen_value_by_name("xpath", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)
        write_mode = node_detail_form.get_chosen_value_by_name("write_mode", variable_handler)
        save_to = node_detail_form.get_chosen_value_by_name("save_to", variable_handler)
        column = node_detail_form.get_chosen_value_by_name("column", variable_handler)
        list_as_str = node_detail_form.get_chosen_value_by_name("list_as_str", variable_handler)

        self.direct_execute(xpath, output, write_mode, save_to, column, list_as_str)

    def execute_with_params(self, params):
        xpath = params["xpath"]
        output = params["output"]
        write_mode = params["write_mode"]
        save_to = params["save_to"]
        column = params["column"]
        list_as_str = params["list_as_str"]

        self.direct_execute(xpath, output, write_mode, save_to, column, list_as_str)

    def direct_execute(self, xpath, output, write_mode, save_to, column, list_as_str):
        xpath = suh.check_xpath_apostrophes(xpath)
        filename = f'{output}.txt'

        #if not glc.table1.visible: #temporary disabled - should be rather in psm
        #    glc.toggle_grid_view([glc.table1])

        suh.webscraping_client.extract_xpath(xpath, filename, write_mode)

        data = suh.wait_until_data_is_extracted(filename, timeout=3, xpath_func=True)
        flog.warning(data)

        if data is not None and column:
            params = {"variable_name": output, "variable_value": str(data)}  # rows str(rows)
            variable_handlers_dict["NewVariable"].execute_with_params(params)
            ##variable_handler.update_data_in_variable_explorer(glc)


            # TODO Ilya: Better separation of frontend and backend - teporarily disabled
            # if save_to == 'DataFrame':
            #     if isinstance(data, list):
            #         if list_as_str:
            #             new_data = ', '.join(data)
            #         else:
            #             new_data = data
            #     else:
            #         new_data = str(data)

            #     old_df = glc.tables.elements[0].df

            #     if old_df is None:
            #         if isinstance(new_data, list):
            #             new_df = pd.DataFrame({column: new_data})
            #         else:
            #             new_df = pd.DataFrame([{column: new_data}])
            #     else:
            #         if column in old_df.columns:
            #             # Get position of column
            #             pos = list(old_df.columns).index(column)

            #             if pos == 0:
            #                 if isinstance(new_data, list):
            #                     new_df = pd.concat([old_df, pd.DataFrame({column: new_data})])
            #                     """
            #                                                     for i, elem in enumerate(new_data):
            #                         flog.error('INSERTING ELEMENT')
            #                         old_df = glc.tables.elements[0].df
            #                         part_df = pd.DataFrame([{column: elem}])
            #                         flog.error(str(part_df.shape))
            #                         new_df = pd.concat([old_df, part_df])

            #                         glc.populate_table_with_df(new_df, 0)
            #                         time.sleep(0.2)
            #                     """

            #                 else:
            #                     new_df = pd.concat([old_df, pd.DataFrame({column: [new_data]})])

            #             # Value first be nan, when replaced with actual data
            #             else:
            #                 new_df = old_df.copy()

            #                 if isinstance(new_data, list):
            #                     pass
            #                 else:
            #                     new_df.iloc[-1, pos] = new_data

            #         else:
            #             if isinstance(new_data, list):
            #                 new_df = pd.concat([old_df, pd.DataFrame({column: new_data})], axis=1)
            #             else:
            #                 new_df = pd.concat([old_df, pd.DataFrame({column: [new_data]})], axis=1)

            #     glc.populate_table_with_df(new_df, 0)
            #     variable_handler.new_variable("scraped_df", new_df)
            #     #variable_handler.update_data_in_variable_explorer(glc)


    def export_code(self, node_detail_form):
        xpath = node_detail_form.get_chosen_value_by_name("xpath", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)
        write_mode = node_detail_form.get_chosen_value_by_name("write_mode", variable_handler)
        save_to = node_detail_form.get_chosen_value_by_name("save_to", variable_handler)
        column = node_detail_form.get_chosen_value_by_name("column", variable_handler)
        list_as_str = node_detail_form.get_chosen_value_by_name("list_as_str", variable_handler)

        try:
            filename = node_detail_form.get_chosen_value_by_name('filename', variable_handler)
        except TypeError:
            filename = ""
            
        code = f"""
        # Specify the XPath of the element you want to extract
        element_xpath = "{xpath}"

        # Find the element using its XPath
        target_element = driver.find_element(By.XPATH, element_xpath)

        # Extract the text or other attributes of the element
        {output} = target_element.text
        
        with open("{output}.txt", "{write_mode}") as file:
            file.write({output})
        """
        
        return code

    def export_imports(self, *args):
        imports = [
            "from selenium import webdriver",
            "from selenium.webdriver.common.by import By"
            ]

        return imports


class ExtractMultipleXPathHandler:
    def __init__(self):
        self.icon_type = "ExtractMultipleXPath"
        self.fn_name = "Extract Multiple XPath"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Extract multiple HTML elements by XPath")
        fdl.label("Extraction setup file")
        fdl.entry(name="filename", text="", input_types=["str"], required=True, row=1)
        fdl.label("Output variable")
        fdl.entry(name="output", text="", input_types=["str"], required=True, row=2)

        return fdl

    def execute(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        self.direct_execute(filename, output)

    def execute_with_params(self, params):
        filename = params["filename"]
        output = params["output"]

        self.direct_execute(filename, output)

    def direct_execute(self, filename, output):
        with Path(aet.home_folder, filename).open(mode='r', encoding="utf-8") as f:
            xpaths = f.readlines()

        xpaths = [suh.check_xpath_apostrophes(x.replace("\n", "")) for x in xpaths]

        output_filename = output + ".txt"
        flog.info(f"XPATHS: {xpaths}")

        suh.webscraping_client.extract_multiple_xpath(xpaths, output_filename)

        data = suh.wait_until_data_is_extracted(output_filename, timeout=3, xpath_func=True)

        if data is not None:
            params = {"variable_name": output, "variable_value": str(data)}  # rows str(rows)
            variable_handlers_dict["NewVariable"].execute_with_params(params)
            ##variable_handler.update_data_in_variable_explorer(glc)

    def export_code(self, node_detail_form):
        filename = node_detail_form.get_chosen_value_by_name("filename", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        code = f"""
        filename = "{filename}"
        if not os.path.exists(filename):
            raise FileNotFoundError(f'File "{{filename}}" does not exist.')
            
        {output} = []
        with open(filename, "r") as file:
            xpaths = file.readlines()
            
        for xpath in xpaths:
            # Find the element using its XPath
            target_element = driver.find_element(By.XPATH, xpath)

            # Extract the text or other attributes of the element
            element_text = target_element.text
            
            {output}.append(element_text)
        """
        
        return code

    def export_imports(self, *args):
        imports = [
            "from selenium import webdriver",
            "from selenium.webdriver.common.by import By"
            ]

        return imports


class ExtractTableXPathHandler:
    def __init__(self):
        self.icon_type = "ExtractTableXPath"
        self.fn_name = "Extract Table XPath"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Extract HTML table element by XPath")
        fdl.label("XPath Rows")
        fdl.entry(name="xpath_row", text="", required=True, row=1)
        fdl.label("XPath Columns")
        fdl.entry(name="xpath_col", text="", required=True, row=2)
        fdl.label("Output variable")
        fdl.entry(name="output", text="", input_types=["str"], required=True, row=3)
        fdl.label("Use first row as header")
        fdl.checkbox(name="first_row_header", bool_value=False, row=4)

        return fdl

    def execute(self, node_detail_form):
        xpath_row = node_detail_form.get_chosen_value_by_name("xpath_row", variable_handler)
        xpath_col = node_detail_form.get_chosen_value_by_name("xpath_col", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)
        first_row_header = node_detail_form.get_chosen_value_by_name("first_row_header", variable_handler)

        self.direct_execute(xpath_row, xpath_col, output, first_row_header)

    def execute_with_params(self, params):
        xpath_row = params["xpath_row"]
        xpath_col = params["xpath_col"]
        output = params["output"]
        first_row_header = params["first_row_header"]

        self.direct_execute(xpath_row, xpath_col, output, first_row_header)

    def direct_execute(self, xpath_row, xpath_col, output, first_row_header):
        xpath_row = suh.check_xpath_apostrophes(xpath_row)
        xpath_col = suh.check_xpath_apostrophes(xpath_col)

        filename = output + '.pickle'
        suh.webscraping_client.extract_table_xpath(xpath_row, xpath_col, first_row_header, filename)

        # TODO: change redis key
        redis_key = 'test_user:test_project:test_pipeline:scraping:extracted_table'
        data = suh.wait_until_data_is_extracted_redis(redis_key, timeout=3, xpath_func=True)

        if data is not None and data:
            params = {"variable_name": output, "variable_value": data}  # rows str(rows)
            variable_handlers_dict["NewVariable"].execute_with_params(params)

            kv_redis.set(key=redis_key, value="")

            ##variable_handler.update_data_in_variable_explorer(glc)

    def export_code(self, node_detail_form):
        """TODO"""

        xpath_row = node_detail_form.get_chosen_value_by_name("xpath_row", variable_handler)
        xpath_col = node_detail_form.get_chosen_value_by_name("xpath_col", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)
        first_row_header = node_detail_form.get_chosen_value_by_name("first_row_header", variable_handler)

        code = """
           suh.webscraping_client.extract_table_xpath()
           """

        return code

    def export_imports(self, *args):
        """TODO"""

        imports = ["import docrawl_launcher"]

        return imports


class ExtractPageSourceHandler:
    """
    Handler for ExtractPageSource icon. This icon exports the source of currently scraped page.
       Input: Name of variable to be created inside platform
       Output: Variable with page source
    """

    def __init__(self):
        self.icon_type = "ExtractPageSource"
        self.fn_name = "Extract Page Source"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Output variable")
        fdl.entry(name="output", text="", input_types=["str"], required=True, row=1)

        return fdl

    def execute(self, node_detail_form):
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        self.direct_execute(output)

    def execute_with_params(self, params):
        output = params["output"]

        self.direct_execute(output)

    def direct_execute(self, output):
        filename = 'page_source.txt'

        # time.sleep(10)  # !!! IMPORTANT !!! <- Necessary delay due to problems with crawler thread

        suh.webscraping_client.extract_page_source(filename)

        data = suh.wait_until_data_is_extracted(filename, timeout=3)

        if data is not None:
            # Delete file (this block can be removed if there is need to save page source outside platform)
            try:
                os.remove(filename)
            except:
                flog.error('Error while deleting file!')

            params = {"variable_name": output, "variable_value": str(data)}
            variable_handlers_dict["NewVariable"].direct_execute(params['variable_name'], params['variable_value'])
            ##variable_handler.update_data_in_variable_explorer(glc)

    def export_code(self, node_detail_form):
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        code = f"""
        # Get the HTML source of the current webpage
        {output} = driver.page_source    
        """
        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class RefreshPageSourceHandler:
    def __init__(self):
        self.icon_type = "RefreshPageSource"
        self.fn_name = "Refresh Page Source"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()
        fdl.label(self.fn_name)

        return fdl

    def execute(self, node_detail_form):
        self.direct_execute()

    def execute_with_params(self, params):

        self.direct_execute()

    def direct_execute(self):
        function = "exec"
        inp = "self.browser.page_source"

        spider_functions = Var({"function": function, "input": inp, "done": False})
        save_variables(kept_variables)

    def export_code(self, node_detail_form):
        code = f"""
        # Refresh the page source
        driver.refresh() # a "driver" is a selenium.webdriver object initialized by OpenBrowser node --> rename according to your taste
        """

        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class DownloadImageHandler:
    """
    Handler for DownloadImageIcon. This icon downloads image from url.
    """

    def __init__(self):
        self.icon_type = 'DownloadImage'
        self.fn_name = 'Download Image'

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("Image URL")
        fdl.entry(name="image_url", text="", input_types=["str"], required=True, row=1)
        fdl.label("Output filename")
        fdl.entry(name="output", text="", input_types=["str"], required=True, row=2)

        return fdl

    def execute(self, node_detail_form):
        image_url = node_detail_form.get_chosen_value_by_name("image_url", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        self.direct_execute(image_url, output)

    def execute_with_params(self, params):
        image_url = params["image_url"]
        output = params["output"]

        self.direct_execute(image_url, output)

    def direct_execute(self, image_url, output):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}

        # If entered filename contains extension -> drop extension
        if '.' in output:
            output = output.split('.')[0]

        image_extension = image_url.split('.')[-1]
        filename = f'{output}.{image_extension}'

        r = requests.get(image_url, headers=headers)
        with Path(aet.home_folder, filename).open(mode='wb') as outfile:
            outfile.write(r.content)
        # urllib.request.urlretrieve(image_url, f'{filename}.{image_extension}')

        # suh.webscraping_client.download_image(image_url, filename)

    def export_code(self, node_detail_form):
        """TODO"""

        image_url = node_detail_form.get_chosen_value_by_name("image_url", variable_handler)
        output_filename = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        code = f"""
        # URL of the image you want to download
        image_url = "{image_url}"

        # Get the image element based on its URL
        image_element = driver.find_element(By.XPATH, f'//img[@src="{{image_url}}"]')

        # Get the image source URL from the element
        image_source_url = image_element.get_attribute('src')

        # Download the image using requests
        response = requests.get(image_source_url)

        # Specify the local path where you want to save the image
        local_image_path = "{output_filename}"

        # Write the image data to the local file
        with open(local_image_path, 'wb') as image_file:
            image_file.write(response.content)
        """

        return code

    def export_imports(self, *args):
        imports = [
            "import requests",
            "from selenium import webdriver",
            "from selenium.webdriver.common.by import By"
                   ]

        return imports


class DownloadImagesXPathHandler:
    """
    Handler for DownloadImagesXPath icon. This icon downloads image using XPath.
    """

    def __init__(self):
        self.icon_type = 'DownloadImagesXPath'
        self.fn_name = 'Download Images XPath'

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("XPath")
        fdl.entry(name="image_xpath", text="", input_types=["str"], required=True, row=1)
        fdl.label("Output name")
        fdl.entry(name="output", text="", input_types=["str"], required=True, row=2)
        fdl.label("Multiple images can be downloaded", row=3)

        return fdl

    def execute(self, node_detail_form):
        image_xpath = node_detail_form.get_chosen_value_by_name("image_xpath", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        self.direct_execute(image_xpath, output)

    def execute_with_params(self, params):
        image_xpath = params["image_xpath"]
        output = params["output"]

        self.direct_execute(image_xpath, output)

    def direct_execute(self, image_xpath, output):
        image_xpath = suh.check_xpath_apostrophes(image_xpath)
        suh.webscraping_client.download_images(image_xpath, output)

    def export_code(self, node_detail_form):
        image_xpath = node_detail_form.get_chosen_value_by_name("image_xpath", variable_handler)
        output = node_detail_form.get_chosen_value_by_name("output", variable_handler)

        code = f"""
        image_xpath = "{image_xpath}"
        
        # Get the image element based on its URL
        image_element = driver.find_element(By.XPATH, image_xpath)

        # Get the image source URL from the element
        image_source_url = image_element.get_attribute('src')

        response = requests.get(image_source_url)
        {output} = response.content
        """

        return code

    def export_imports(self, *args):
        imports = [
            "import requests",
            "from selenium import webdriver",
            "from selenium.webdriver.common.by import By"
            ]

        return imports


class SetProxyHandler:
    """
    Handler for SetProxy icon. This icon changes the IP via proxy.
    """

    def __init__(self):
        self.icon_type = 'SetProxy'
        self.fn_name = 'Set Proxy'

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("IP")
        fdl.entry(name="ip", text="", input_types=["str"], required=True, row=1)
        fdl.label("Port")
        fdl.entry(name="port", text="", input_types=["int"], required=True, row=2)
        fdl.label("Username")
        fdl.entry(name="username", text="", input_types=["str"], required=False, row=3)
        fdl.label("Password")
        fdl.entry(name="password", text="", input_types=["str"], required=False, row=4)

        return fdl

    def execute(self, node_detail_form):
        ip = node_detail_form.get_chosen_value_by_name("ip", variable_handler)
        port = node_detail_form.get_chosen_value_by_name("port", variable_handler)
        username = node_detail_form.get_chosen_value_by_name("username", variable_handler)
        password = node_detail_form.get_chosen_value_by_name("password", variable_handler)

        self.direct_execute(ip, port, username, password)

    def execute_with_params(self, params):
        ip = params["ip"]
        port = params["port"]
        username = params["username"]
        password = params["password"]

        self.direct_execute(ip, port, username, password)

    def direct_execute(self, ip, port, username, password):
        proxy = {'ip': '', 'port': '', 'username': '', 'password': ''}

        browser_meta_data = suh.webscraping_client.get_browser_meta_data()

        # Save proxy to browser variables
        proxy = {'ip': ip, 'port': port, 'username': username, 'password': password}
        browser_meta_data['browser']['proxy'] = proxy
        suh.webscraping_client.set_browser_meta_data(browser_meta_data)

    def export_code(self, node_detail_form):
        """TODO"""

        ip = node_detail_form.get_chosen_value_by_name("ip", variable_handler)
        port = node_detail_form.get_chosen_value_by_name("port", variable_handler)
        username = node_detail_form.get_chosen_value_by_name("username", variable_handler)
        password = node_detail_form.get_chosen_value_by_name("password", variable_handler)

        general_code = f"""
        proxy_ip = '{ip}'
        proxy_port = '{port}'
        proxy_username = '{username}'
        proxy_password = '{password}'
        
        """

        chrome_code = f"""
        # Create a Proxy object and set its type to MANUAL
        proxy = Proxy()
        proxy.proxy_type = ProxyType.MANUAL
        proxy.http_proxy = f"{{proxy_ip}}:{{proxy_port}}"
        proxy.ssl_proxy = f"{{proxy_ip}}:{{proxy_port}}"

        # If your proxy requires authentication
        proxy.add_argument(f"--proxy-auth={{proxy_username}}:{{proxy_password}}")

        # Create Chrome options and add the proxy settings
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--headless')  # Run headless if desired
        chrome_options.add_argument('--proxy-server=http://{{}}'.format(proxy_ip))

        # Create the WebDriver with Chrome options and the proxy
        driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options, proxy=proxy)
        """
        
        firefox_code = f"""
        # Create Firefox profile with proxy settings
        firefox_profile = webdriver.FirefoxProfile()
        firefox_profile.set_preference('network.proxy.type', 1)
        firefox_profile.set_preference('network.proxy.http', proxy_ip)
        firefox_profile.set_preference('network.proxy.http_port', int(proxy_port))
        firefox_profile.set_preference('network.proxy.ssl', proxy_ip)
        firefox_profile.set_preference('network.proxy.ssl_port', int(proxy_port))

        # If your proxy requires authentication
        firefox_profile.set_preference('network.proxy.socks_username', proxy_username)
        firefox_profile.set_preference('network.proxy.socks_password', proxy_password)

        # Create the WebDriver with Firefox profile
        driver = webdriver.Firefox(executable_path=driver_path, firefox_profile=firefox_profile)
        """

        # code = """
        #    suh.webscraping_client.extract_table_xpath()
        #    """

        # return code

    def export_imports(self, *args):
        """TODO"""

        imports = ["import docrawl_launcher"]

        return imports


"""TODO - OLD IMPLEMENTATION - REFACTOR"""


class ScrapeSendKeysHandler:
    def __init__(self):
        self.icon_type = "ScrapeSendKeys"
        self.fn_name = "Scrape Send Keys"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label("Send Keys During Scraping (blocking)")

        return fdl

    def execute(self, node_detail_form):

        self.direct_execute()

    def execute_with_params(self, params):

        self.direct_execute()

    def direct_execute(self):
        global counter  # counter obsolete?
        pid = self.get_pid()
        app = pywinauto.application.Application().connect(process=pid)
        app_dialog = app.top_window()
        app_dialog.set_focus()
        # Commented because of dependency on glc, to be refactored
        # pywinauto.keyboard.send_keys(glc.entries[counter].label.text)
        counter += 1

    @staticmethod
    def get_pid():
        """TODO - OLD IMPLEMENTATION - REFACTOR"""
        # if pid_entry.text.get()=="browser":
        try:
            browser_pid = suh.webscraping_client.get_browser_meta_data()['browser']['pid']
        except:
            browser_pid = 0
        pid = browser_pid

        return pid


class ClickHTMLTagHandler:
    def __init__(self):
        self.icon_type = "ClickHTMLTag"
        self.fn_name = "Click HTML Tag Send Keys"

        self.type_category = ntcm.categories.webscraping

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("HTML tag")
        fdl.entry(name="tag", text="", input_types=["str"], required=True, row=1)
        fdl.label("Index")
        fdl.entry(name="index", text="", input_types=["str"], required=True, row=2)
        fdl.label("Class input")
        fdl.entry(name="class_input", text="", input_types=["str"], required=True, row=3)

        return fdl

    def execute(self, node_detail_form):
        tag = node_detail_form.get_chosen_value_by_name("tag", variable_handler)
        index = node_detail_form.get_chosen_value_by_name("index", variable_handler)
        class_input = node_detail_form.get_chosen_value_by_name("class_input", variable_handler)

        self.direct_execute(tag, index, class_input)

    def execute_with_params(self, params):
        tag = params["tag"]
        index = params["index"]
        class_input = params["class_input"]

        self.direct_execute(tag, index, class_input)

    def direct_execute(self, tag, index, class_input):
        function = "exec"

        inp = "click_class(self.browser,'" + class_input + "'," + index + ",tag='" + tag + "')"
        spider_functions = Var({"function": function, "input": inp, "done": False})
        save_variables(kept_variables)


webscraping_handlers_dict = {
    "OpenBrowser": OpenBrowserHandler(),
    "ExtractPageSource": ExtractPageSourceHandler(),
    "RefreshPageSource": RefreshPageSourceHandler(),
    "ScrapeSendKeys": ScrapeSendKeysHandler(),
    "LoadWebsite": LoadWebsiteHandler(),
    "ClickXPath": ClickXPathHandler(),
    "ClickName": ClickNameHandler(),
    "ClickId": ClickIdHandler(),
    "GetCurrentURL": GetCurrentURLHandler(),
    "WaitUntilElementIsLocated": WaitUntilElementIsLocatedHandler(),
    "CloseBrowser": CloseBrowserHandler(),
    "ScrollWebPage": ScrollWebPageHandler(),
    "ScanWebPage": ScanWebPageHandler(),
    "ExtractXPath": ExtractXPathHandler(),
    "ExtractMultipleXPath": ExtractMultipleXPathHandler(),
    "ExtractTableXPath": ExtractTableXPathHandler(),
    "DownloadImage": DownloadImageHandler(),
    "DownloadImagesXPath": DownloadImagesXPathHandler(),
    "NextPage": NextPageHandler(),
    "SetProxy": SetProxyHandler()
}