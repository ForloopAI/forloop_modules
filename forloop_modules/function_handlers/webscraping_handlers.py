import os
import time
import requests
import sys

if "linux" not in sys.platform:
    import pywinauto

from pathlib import Path
from bs4 import BeautifulSoup
from keepvariable.keepvariable_core import Var, save_variables, kept_variables

import forloop_modules.flog as flog

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.docs import Docs
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler
from forloop_modules.function_handlers.variable_handlers import variable_handlers_dict

from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.globals.scraping_utilities_handler import suh
from forloop_modules.globals.docs_categories import DocsCategories
from forloop_modules.globals.variable_handler import variable_handler

from forloop_modules.errors.errors import CriticalPipelineError
from forloop_modules.redis.redis_connection import kv_redis

#from src.gui.gui_layout_context import glc
####################### SCRAPING HANDLERS ################################


# # # # # # # # # HANDLERS # # # # # # # # #



class OpenBrowserHandler(AbstractFunctionHandler):
    """
    Opens the system default web browser (for further usage such as Load Website etc.). Allows to choose driver to use.
    For now available: Firefox (Geckodriver), Chrome.

    Note: To use a driver an appropriate web browser should be installed. Driver itself will be installed automatically.
    """

    def __init__(self):
        self.icon_type = "OpenBrowser"
        self.fn_name = "Open Browser"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "OpenBrowser Node takes 2 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)


        self.docs.add_parameter_table_row(
            title="Show browser",
            name="in_browser",
            description="Whether to show browser instance GUI",
            typ="boolean",
        )

        self.docs.add_parameter_table_row(
            title="Driver",
            name="driver",
            description="Browser to use: Firefox (Geckodriver), Chrome"
        )

    def make_form_dict_list(self, node_detail_form=None):
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
        
        code = """
        # Initialize the {driver} WebDriver
                
        class Spider(scrapy.spiders.CrawlSpider):
            name = "forloop"
        
            custom_settings = {
                'LOG_LEVEL': 'ERROR',
                'USER_AGENT': "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
                'DEFAULT_REQUEST_HEADERS': {
                    'Referer': 'https://forloop.ai'
                }
                #   'CONCURRENT_REQUESTS' : '20',
            }
        
            def __init__(self, *a, **kw):
                self.docrawl_client = kw['docrawl_client']
        
                self.kv_redis_key_screenshot = self.docrawl_client.kv_redis_keys.get('screenshot', 'screenshot')
                self.kv_redis_key_elements = self.docrawl_client.kv_redis_keys.get('elements', 'elements')
        
                self.browser = self._initialise_browser()
                browser_meta_data = self.docrawl_client.get_browser_meta_data()
                browser_meta_data['browser']['pid'] = self._determine_browser_pid()
        
                self.docrawl_client.set_browser_meta_data(browser_meta_data)
        
                self.start_requests()
        
            def _initialise_browser(self):
                browser_meta_data = self.docrawl_client.get_browser_meta_data()
        
                docrawl_logger.info(f'Browser settings: {browser_meta_data}')
        
                try:
                    self.driver_type = browser_meta_data['browser']['driver']
                except Exception as e:
                    docrawl_logger.error(f'Error while loading driver type information: {e}')
                    self.driver_type = 'Firefox'
        
                try:
                    self.headless = browser_meta_data['browser']['headless']
                except Exception as e:
                    docrawl_logger.error(f'Error while loading headless mode information: {e}')
                    self.headless = False
        
                try:
                    proxy_info = browser_meta_data['browser']['proxy']
                except Exception as e:
                    docrawl_logger.warning(f'Error while loading proxy information: {e}')
                    proxy_info = None
        
                if self.driver_type == 'Firefox':
                    self.options = FirefoxOptions()
                    self.options.set_preference("marionette", True)
        
                    sw_options = self._set_proxy(proxy_info)
        
                    if self.headless:
                        self.options.add_argument("--headless")
        
                        # For headless mode different width of window is needed
                        window_size_x = 1450
        
                    try:
                        self.browser = webdriver.Firefox(options=self.options, service=Service(GeckoDriverManager().install()), seleniumwire_options=sw_options)
                    except Exception as e:
                        docrawl_logger.error(f'Error while creating Firefox instance {e}')
                        self.browser = webdriver.Firefox(options=self.options)
        
                elif self.driver_type == 'Chrome':
                    self.options = ChromeOptions()
        
                    sw_options = self._set_proxy(proxy_info)
        
                    if self.headless:
                        self.options.add_argument("--headless")
        
                        # For headless mode different width of window is needed
                        window_size_x = 1450
        
                    try:
                        self.browser = webdriver.Chrome(options=self.options, service=Service(ChromeDriverManager().install()), seleniumwire_options=sw_options)
                    except Exception as e:
                        docrawl_logger.error(f'Error while creating Chrome instance {e}')
                        self.browser = webdriver.Chrome(options=self.options)
        
                window_size_x = 1820
        
                self.browser.set_window_size(window_size_x, 980)
        
                return self.browser

        
        """
        
        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class LoadWebsiteHandler(AbstractFunctionHandler):
    """
    Loads website with a given URL.
    """

    def __init__(self):
        self.icon_type = "LoadWebsite"
        self.fn_name = "Load Website"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "LoadWebsite Node takes 2 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="URL",
            name="url",
            description="URL of website to load",
            typ="string",
            example=['https://forloop.ai', 'forloop.ai/blog', 'www.forloop.ai/pricing']
        )

        self.docs.add_parameter_table_row(
            title="Take screenshot",
            name="take_screenshot",
            description="Whether to take screenshot of loaded page (will be displayed in BrowserView)"
        )

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("URL")
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


class DismissCookiesHandler(AbstractFunctionHandler):
    """
    DismissCookies automatically detects cookies panel on page.
    Note: this node might not work on all webpages
    """

    def __init__(self):
        self.icon_type = "DismissCookies"
        self.fn_name = "Dismiss Cookies"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "DismissCookies Node takes no parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("This node automatically detects cookies")
        fdl.label("It might not work on all websites")
        fdl.label("See our documentation for more information")
        
        return fdl

    def execute(self, node_detail_form):
        
        self.direct_execute()

    def execute_with_params(self, params):
        
        self.direct_execute()

    def direct_execute(self):
        suh.detect_cookies_popup()

    def export_code(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        
        code = f"""
        # Export code is not yet available for DismissCookies node.
        """

        return code

    def export_imports(self, *args):
        imports = ["from selenium import webdriver"]

        return imports


class NextPageHandler(AbstractFunctionHandler):
    """
    NextPage Node allows to iterate over pages (performs pagination) by constructing page URL
    """

    def __init__(self):
        self.icon_type = "NextPage"
        self.fn_name = "Next Page"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "NextPage Node takes 3 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="URL prefix",
            name="url_prefix",
            description="Part of URL before page number",
            typ="string",
            example=['https://mywebsite.com/listings?page=']
        )

        self.docs.add_parameter_table_row(
            title="URL suffix",
            name="url_suffix",
            description="Part of URL after page number",
            typ="string",
            example=['&priceMin=10000']
        )

        self.docs.add_parameter_table_row(
            title="Page variable name",
            name="page_varname",
            description="Forloop variable which stores page number",
            typ="string"
        )

    def make_form_dict_list(self, node_detail_form=None):
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
        except Exception as e:
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


class ClickXPathHandler(AbstractFunctionHandler):
    """
    ClickXPath Node clicks on web page element with given XPath
    """

    def __init__(self):
        self.icon_type = "ClickXPath"
        self.fn_name = "Click XPath"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ClickXPath Node takes 1 parameter"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="XPath",
            name="xpath",
            description="XPath of web page element to click on",
            typ="string",
            example=['/html/body/header/div[3]/div[1]/div[2]/section/nav/a', '//button[@class="page-link"]']
        )

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label("Click XPath element")
        fdl.label("XPath")
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


class ClickNameHandler(AbstractFunctionHandler):
    """
    ClickName Node clicks on web page element with given text
    """
    
    def __init__(self):
        self.icon_type = "ClickName"
        self.fn_name = "Click Name"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ClickName Node takes 1 parameter"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Text",
            name="text",
            description="Text of web page element to click on",
            typ="string",
            example=['Show details', 'Open article']
        )
    
    def make_form_dict_list(self, node_detail_form=None):
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


class ClickIdHandler(AbstractFunctionHandler):
    """
    ClickName Node clicks on web page element with given ID
    """

    def __init__(self):
        self.icon_type = "ClickId"
        self.fn_name = "Click Id"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ClickId Node takes 1 parameter"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="ID",
            name="id",
            description="ID of web page element to click on",
            typ="string",
            example=['listing-details', 'pagination-elem']
        )

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label("Click element by id")
        fdl.label("Id")
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


class CloseBrowserHandler(AbstractFunctionHandler):
    """
    CloseBrowser Node closes browser instance.
    """

    def __init__(self):
        self.icon_type = "CloseBrowser"
        self.fn_name = "Close browser after scraping"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "CloseBrowser Node takes no parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

    def make_form_dict_list(self, node_detail_form=None):
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


class GetCurrentURLHandler(AbstractFunctionHandler):
    """
    GetCurrentURL Node saves the URL of currently opened web pag to a new variable inside platform.
    """

    def __init__(self):
        self.icon_type = "GetCurrentURL"
        self.fn_name = "Get current URL"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "GetCurrentURL Node takes 1 parameter"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Output variable",
            name="output",
            description="Name of variable to save current URL to",
            typ="string",
            example=['current_url', 'scraped_page']
        )

    def make_form_dict_list(self, node_detail_form=None):
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
        """
        Note: output file current_url.txt is used only for "internal" purposes, meaning for loading data from it,
        transferring to other icons etc.
        TODO: Therefore, this file may be deleted after icon's function was proceeded <- (NOT IMPLEMENTED YET)
        """

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


class WaitUntilElementIsLocatedHandler(AbstractFunctionHandler):
    """
    WaitUntilElementIsLocated Node waits until certain element appears and page and then clicks on it.
    """

    def __init__(self):
        self.icon_type = "WaitUntilElementIsLocated"
        self.fn_name = "Wait until element is located"

        self.type_category = ntcm.categories.webscraping
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "WaitUntilElementIsLocated Node takes 1 parameter"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="XPath",
            name="xpath",
            description="XPath of web page element to wait and click on",
            typ="string",
            example=['//button[@class="load-more"]', '//*[@id="map-search-results"]/div[1]/div/div[25]/div[2]/a']
        )

    def make_form_dict_list(self, node_detail_form=None):
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


class ScrollWebPageHandler(AbstractFunctionHandler):
    """
    ScrollWebPage Node scrolls page up/down by n-pixels.
    Special option is "max" - scrolls page to the top or end of page
    """

    def __init__(self):
        self.icon_type = "ScrollWebPage"
        self.fn_name = "Scroll web page"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ScrollWebPage Node takes 3 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Scroll direction",
            name="scroll_to",
            description="The direction of scrolling (Up or Down)",
        )

        self.docs.add_parameter_table_row(
            title="Scroll by",
            name="scroll_by",
            description="How big should the scroll be (in pixels)",
            typ=["float", "integer"],
            example=['300', '250.5']
        )

        self.docs.add_parameter_table_row(
            title="Scroll max",
            name="scroll_max",
            description="Whether to scroll by maximum possible pixels (to the header / bottom)",
            typ="boolean"
        )

    def make_form_dict_list(self, node_detail_form=None):
        options = ["Up", "Down"]

        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("Scroll direction")
        fdl.combobox(name="scroll_to", options=options, default="Down", row=1)
        fdl.label("By (px)")
        fdl.entry(name="scroll_by", text="", required=True, input_types=["int", "float"], row=2)
        fdl.label("Scroll max")
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


class ScanWebPageHandler(AbstractFunctionHandler):
    """
    ScanWebPage Node looks for certain type of elements on web page and displays them in BrowserView
    """

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
            title="By XPath",
            name="by_xpath",
            description="XPath of custom elements to search",
            typ="string",
            example=['//div[@class="regular-price"]/text()', '//span[contains(text(), "Location")]']
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


class ExtractXPathHandler(AbstractFunctionHandler):
    """
    ExtractXPath Node looks for web page element with given XPath and extracts its content
    """

    def __init__(self):
        self.icon_type = "ExtractXPath"
        self.fn_name = "Extract XPath"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ExtractXPath Node takes 6 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="XPath",
            name="xpath",
            description="XPath of page element to be extracted",
            typ="string",
            example=['//*[@id="next"]/main/section/article[15]/div[2]/h2/span[2]', '/html/body/div[4]/div[1]/a/div[2]']
        )

        self.docs.add_parameter_table_row(
            title="Output variable",
            name="output",
            description="Name of variable to be created inside the platform",
            typ="string",
            example=['price', 'publish_date']
        )

        self.docs.add_parameter_table_row(
            title="Write in file mode",
            name="write_mode",
            description="What write mode to use (a+ for appending to file, w+ for rewriting previous file content)",
        )

        self.docs.add_parameter_table_row(
            title="Save to",
            name="save_to",
            description="Where to save Node output to. Possible options: GridView inside platform",
        )

        self.docs.add_parameter_table_row(
            title="Column name",
            name="column",
            description="Column name for stored data",
            typ="string",
            example=['price', 'publish_date']
        )

        self.docs.add_parameter_table_row(
            title="Store list as string",
            name="list_as_str",
            description="Whether to store list of elements as string. E.g ['2-bedrooms', '1 bathroom', '3rd floor'] will be transformed into '2-bedrooms, 1 bathroom, 3rd floor'",
        )

    def make_form_dict_list(self, node_detail_form=None):
        options = ["w+", "a+"]
        options_2 = ['GridView']

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
        # TODO: Should be dynamically appended when Save to DataFrame selected
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

            
            # #TODO Dominik + Ilya: Better separation of frontend and backend - teporarily disabled - DO NOT ERASE THE WHOLE SECTION!
            # if save_to == 'GridView':
            #     if isinstance(data, list):
            #         if list_as_str:
            #             new_data = ', '.join(data)
            #         else:
            #             new_data = data
            #     else:
            #         new_data = str(data)

            #     old_df = glc.tables.elements[0].df
            #     #old_df = None
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


class ExtractMultipleXPathHandler(AbstractFunctionHandler):
    """
    ExtractMultipleXPath Node looks for multiple web page elements with given XPaths and extracts theirs content
    """

    def __init__(self):
        self.icon_type = "ExtractMultipleXPath"
        self.fn_name = "Extract Multiple XPath"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ExtractMultipleXPath Node takes 2 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Extraction setup file",
            name="filename",
            description="Path to file with list of XPaths, one XPath per line",
            typ="string",
            example=['/Users/admin/Desktop/xpaths.txt']
        )

        self.docs.add_parameter_table_row(
            title="Output variable",
            name="output",
            description="Name of variable to be created inside the platform",
            typ="string",
            example=['listing_details', 'product_info']
        )

    def make_form_dict_list(self, node_detail_form=None):
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


class ExtractTableXPathHandler(AbstractFunctionHandler):
    """
    ExtractTableXPath Node finds table using given XPath and extracts its content.
    """

    def __init__(self):
        self.icon_type = "ExtractTableXPath"
        self.fn_name = "Extract Table XPath"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ExtractTableXPath Node takes 4 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="XPath Rows",
            name="xpath_row",
            description="XPath of row elements",
            typ="string",
            example=["//table[@class='my-table']//tr"]
        )

        self.docs.add_parameter_table_row(
            title="XPath Columns",
            name="xpath_col",
            description="XPath of column elements",
            typ="string",
            example=["//td//a[@class='listing-info']//@href"]
        )

        self.docs.add_parameter_table_row(
            title="Output variable",
            name="output",
            description="Name of variable to be created inside the platform",
            typ="string",
            example=['listings']
        )

        self.docs.add_parameter_table_row(
            title="Use first row as header",
            name="first_row_header",
            description="Whether first row should be used as header",
        )

    def make_form_dict_list(self, node_detail_form=None):
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


class ExtractPageSourceHandler(AbstractFunctionHandler):
    """
    ExtractPageSource Node extracts the HTML source of currently loaded page.
    """

    def __init__(self):
        self.icon_type = "ExtractPageSource"
        self.fn_name = "Extract Page Source"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "ExtractPageSource Node takes 1 parameter"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Output variable",
            name="output",
            description="Name of variable to be created inside the platform",
            typ="string",
            example=['current_page_html']
        )

    def make_form_dict_list(self, node_detail_form=None):
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


class RefreshPageSourceHandler(AbstractFunctionHandler):
    """
    RefreshPageSource Node refreshes HTML source currently loaded page.
    """

    def __init__(self):
        self.icon_type = "RefreshPageSource"
        self.fn_name = "Refresh Page Source"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "RefreshPageSource Node takes no parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

    def make_form_dict_list(self, node_detail_form=None):
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


class DownloadImageHandler(AbstractFunctionHandler):
    """
    DownloadImage Node downloads image from given URL.
    """

    def __init__(self):
        self.icon_type = 'DownloadImage'
        self.fn_name = 'Download Image'

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "DownloadImage Node takes 2 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Image URL",
            name="image_url",
            description="URL of image to download",
            typ="string",
            example=['https://forloop.ai/logo.png']
        )

        self.docs.add_parameter_table_row(
            title="Output filename",
            name="output",
            description="Name of filename to save image to",
            typ="string",
            example=['logo.png', 'product_main_photo.jpg']
        )

    def make_form_dict_list(self, node_detail_form=None):
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


class DownloadImagesXPathHandler(AbstractFunctionHandler):
    """
    DownloadImagesXPath Node downloads image using given XPath. Allows to download multiple images at once.
    """

    def __init__(self):
        self.icon_type = 'DownloadImagesXPath'
        self.fn_name = 'Download Images XPath'

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "DownloadImagesXPath Node takes 2 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="XPath",
            name="image_xpath",
            description="XPath of image element(s)",
            typ="string",
            example=['/html/body/header/div[3]/div[1]/div[2]/section//picture/img']
        )

        self.docs.add_parameter_table_row(
            title="Output filename",
            name="output",
            description="Name of filename to save image to",
            typ="string",
            example=['logo.png', 'product_main_photo.jpg']
        )

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("XPath")
        fdl.entry(name="image_xpath", text="", input_types=["str"], required=True, row=1)
        fdl.label("Output filename")
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


class SetProxyHandler(AbstractFunctionHandler):
    """
    SetProxy Node sets up the proxy for sending scraping requests.
    """

    def __init__(self):
        self.icon_type = 'SetProxy'
        self.fn_name = 'Set Proxy'

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "SetProxy Node takes 4 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="IP",
            name="ip",
            description="Proxy IP",
            typ="string",
            example=['99.129.22.4']
        )

        self.docs.add_parameter_table_row(
            title="Port",
            name="port",
            description="Proxy port",
            typ="integer",
            example=['7784']
        )

        self.docs.add_parameter_table_row(
            title="Username",
            name="username",
            description="Username (if proxy with authentication is used)",
            typ="string",
            example=['user1']
        )

        self.docs.add_parameter_table_row(
            title="Password",
            name="password",
            description="Password (if proxy with authentication is used)",
            typ="string",
            example=['mypassword123']
        )

    def make_form_dict_list(self, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("IP")
        fdl.entry(name="ip", text="", input_types=["str"], required=True, row=1)
        fdl.label("Port")
        fdl.entry(name="port", text="", input_types=["int"], required=True, row=2)
        fdl.label("Username")
        fdl.entry(name="username", text="", input_types=["str"], required=False, row=3)
        fdl.label("Password")
        fdl.entry(name="password", text="", input_types=["str"], type='password', required=False, row=4)

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


class ScrapeSendKeysHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "ScrapeSendKeys"
        self.fn_name = "Scrape Send Keys"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
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


class ClickHTMLTagHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "ClickHTMLTag"
        self.fn_name = "Click HTML Tag Send Keys"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa

        super().__init__()

    def make_form_dict_list(self, node_detail_form=None):
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


class GetPageSourceHandler(AbstractFunctionHandler):
    """
    GetPageSource Node retrieves HTML source of provided page.
    """

    def __init__(self):
        self.icon_type = "GetPageSource"
        self.fn_name = "Get Page Source"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "GetPageSource Node takes 2 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="URL",
            name="url",
            description="URL of web page to extract HTML source of",
            typ="string",
        )

        self.docs.add_parameter_table_row(
            title="Output variable",
            name="output_variable",
            description="Name of variable to store HTML source in"
        )

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("URL")
        fdl.entry(name="url", text="", required=True, input_types=["str"], row=1)
        fdl.label("Output variable")
        fdl.entry(name="output_variable", text="", required=True, input_types=["str"], row=2)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True, row=3)

        return fdl

    def execute(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        output_variable = node_detail_form.get_chosen_value_by_name("output_variable", variable_handler)

        self.direct_execute(url, output_variable)

    def execute_with_params(self, params):
        url = params["url"]
        output_variable = params["output_variable"]

        self.direct_execute(url, output_variable)

    def direct_execute(self, url, output_variable):
        if "http" not in url:
            url = "http://" + url

        r = requests.get(url)

        variable_handler.new_variable(output_variable, r.text)

    def export_code(self, node_detail_form):
        url = node_detail_form.get_chosen_value_by_name("url", variable_handler)
        output_variable = node_detail_form.get_chosen_value_by_name("output_variable", variable_handler)

        if 'http://' not in url:
            url = f'http://{url}'

        code = f"""
        # Get page source
        r = requests.get(url="{url}")

        {output_variable} = r.text
        """

        return code

    def export_imports(self, *args):
        imports = ["import requests"]

        return imports


class FindPageElementsHandler(AbstractFunctionHandler):
    """
    FindPageElements Node searches for web page elements with provided attributes
    """

    def __init__(self):
        self.icon_type = "FindPageElements"
        self.fn_name = "Find Page Elements"

        self.type_category = ntcm.categories.webscraping
        self.docs_category = DocsCategories.webscraping_and_rpa
        self._init_docs()

        super().__init__()

    def _init_docs(self):
        parameters_description = "FindPageElements Node takes 7 parameters"
        self.docs = Docs(description=self.__doc__, parameters_description=parameters_description)

        self.docs.add_parameter_table_row(
            title="Page source",
            name="page_source",
            description="HTML source of page to parse",
            typ="string",
        )

        self.docs.add_parameter_table_row(
            title="Tag",
            name="tag",
            description="HTML tag name to search for",
            typ="string",
        )

        self.docs.add_parameter_table_row(
            title="Class",
            name="class_",
            description="HTML tag class to search for",
            typ="string",
        )

        self.docs.add_parameter_table_row(
            title="Attributes",
            name="attributes",
            description="Additional attributes to search element(s) by",
            typ="dictionary",
        )

        self.docs.add_parameter_table_row(
            title="Get",
            name="get",
            description="What data to extract from found element(s). Choose from predefined or add your own one. "
                        "Special option is '-' which will not extract any data and will store element itself "
                        "(useful for further parsing of elements)",
            typ="string",
        )

        self.docs.add_parameter_table_row(
            title="Find all",
            name="find_all",
            description="Whether to search for all possible elements, not only for the first one",
            typ="boolean",
        )

        self.docs.add_parameter_table_row(
            title="Output variable",
            name="output_variable",
            description="Name of variable to store found element(s) in"
        )

    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList()

        fdl.label(self.fn_name)
        fdl.label("Page source")
        fdl.entry(name="page_source", text="", required=True, input_types=["str"], row=1)
        fdl.label("Tag")
        fdl.entry(name="tag", text="", required=True, input_types=["str"], row=2)
        fdl.label("Class")
        fdl.entry(name="class_", text="", required=False, input_types=["str"], row=3)
        fdl.label("Attributes")
        fdl.entry(name="attributes", text="", required=False, input_types=["dict"], row=4)
        fdl.label("Get")
        fdl.comboentry(name="get", text="text", options=['text', 'href', '-'], row=5)
        fdl.label("Find all")
        fdl.checkbox(name="find_all", bool_value=False, row=6)
        fdl.label("Output variable")
        fdl.entry(name="output_variable", text="", required=True, input_types=["str"], row=7)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True, row=8)

        return fdl

    def execute(self, node_detail_form):
        page_source = node_detail_form.get_chosen_value_by_name("page_source", variable_handler)
        tag = node_detail_form.get_chosen_value_by_name("tag", variable_handler)
        class_ = node_detail_form.get_chosen_value_by_name("class_", variable_handler)
        attributes = node_detail_form.get_chosen_value_by_name("attributes", variable_handler)
        get = node_detail_form.get_chosen_value_by_name("get", variable_handler)
        find_all = node_detail_form.get_chosen_value_by_name("find_all", variable_handler)
        output_variable = node_detail_form.get_chosen_value_by_name("output_variable", variable_handler)

        self.direct_execute(page_source, tag, class_, attributes, get, find_all, output_variable)

    def execute_with_params(self, params):
        page_source = params["page_source"]
        tag = params["tag"]
        class_ = params["class_"]
        attributes = params["attributes"]
        get = params["get"]
        find_all = params["find_all"]
        output_variable = params["output_variable"]

        self.direct_execute(page_source, tag, class_, attributes, get, find_all, output_variable)

    def direct_execute(self, page_source, tag, class_, attributes, get, find_all, output_variable):
        # '-' means not to extract any attribute from element and store element itself instead
        get = get[0] if get else '-'

        if not attributes:
            attributes = dict()
        else:
            # FIXME: why 'attributes' param (defined with type dict in FDL) is stored as string?
            attributes = eval(attributes)

        if class_:
            attributes['class'] = class_

        soup = BeautifulSoup(page_source, 'html.parser')

        # TODO: handle not found elements
        if find_all:
            elements = soup.find_all(tag, attrs=attributes)
        else:
            elements = [soup.find(tag, attrs=attributes)]

        if get == 'text':
            elements_data = [x.text for x in elements]
        elif get == '-':
            elements_data = elements
        else:
            elements_data = [x.get(get) for x in elements]

        if not find_all:
            elements_data = elements_data[0]

        variable_handler.new_variable(output_variable, elements_data)

    def export_code(self, node_detail_form):
        page_source = node_detail_form.get_chosen_value_by_name("page_source", variable_handler)
        tag = node_detail_form.get_chosen_value_by_name("tag", variable_handler)
        class_ = node_detail_form.get_chosen_value_by_name("class_", variable_handler)
        attributes = node_detail_form.get_chosen_value_by_name("attributes", variable_handler)
        get = node_detail_form.get_chosen_value_by_name("get", variable_handler)
        find_all = node_detail_form.get_chosen_value_by_name("find_all", variable_handler)
        output_variable = node_detail_form.get_chosen_value_by_name("output_variable", variable_handler)

        if not attributes:
            attributes = dict()

        if class_:
            attributes['class'] = class_

        if find_all:
            code_res = f"res = soup.find_all('{tag}', attrs={attributes})"
        else:
            code_res = f"res = soup.find('{tag}', attrs={attributes})"

        code = f"""
        # Prepare parser object
        soup = BeautifulSoup(r.text, 'html.parser')

        {code_res}
        """

        return code

    def export_imports(self, *args):
        imports = ["from bs4 import BeautifulSoup"]

        return imports


webscraping_handlers_dict = {
    "OpenBrowser": OpenBrowserHandler(),
    "ExtractPageSource": ExtractPageSourceHandler(),
    "RefreshPageSource": RefreshPageSourceHandler(),
    "ScrapeSendKeys": ScrapeSendKeysHandler(),
    "LoadWebsite": LoadWebsiteHandler(),
    "DismissCookies": DismissCookiesHandler(),
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
    "SetProxy": SetProxyHandler(),
    "GetPageSource": GetPageSourceHandler(),
    "FindPageElements": FindPageElementsHandler()
}
