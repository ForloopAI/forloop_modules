# Used in handlers but also in API calls from tutorial flow

#WARNING!
#It is forbidden to add imports to popup handlers, pipeline function handlers, or any gui components

import os
import sys
import re
import forloop_modules.flog as flog
import numpy as np
import time
import configparser
from contextlib import suppress
from itertools import product
import uuid
from typing import Optional

import forloop_modules.queries.node_context_requests_backend as ncrb #questionable import - potential problems - should be rather called from pfh

from pathlib import Path
from docrawl.docrawl_client import DocrawlClient
from docrawl.elements import ElementType

from forloop_modules.globals.active_entity_tracker import aet
from forloop_modules.redis.redis_connection import kv_redis

from forloop_modules.utils import synchronization_flags
#WARNING!
#It is forbidden to add imports to popup handlers, pipeline function handlers, or any gui components



class ScrapingUtilitiesHandler:
    """
    Backend class for handling of browser view events and store its state


    Webpage elements structure:

    webpage_elements (list): elements loaded from docrawl (not scaled for platform)
    browser_view_elements (list): webpage_elements (rescaled for platform)
    browser_view_selected_elements (list): selected elements (subset of browser_view_elements)

    Each element in webpage_elements is represented as dict (~ Element class instance in docrawl) with the following structure:

    webpage_element = {
        'name': 'link_17',
        'type': 'link',
        'rect': {'x': 0.0, 'y': 0.0, 'width': 0.0, 'height': 0.0},
        'xpath': '/html/body/div[1]/div/div/div[2]/ul[2]/li[1]/a',
        'data': {
            'tag_name': 'a',
            'text': 'Absolventi',
            'attributes': {'class': 'nav-link', 'href': 'https://www.vse.cz/absolventi/'}
        }
    }

    After that, each element is transformed as follows and propagated to browser_view_elements:

    browser_view_element = {
        'name': 'link_17',
        'type': 'link',
        'pos': [124, 90],       # rescale for platform
        'size': [20, 30],       # rescale for platform
        'xpath': '/html/body/div[1]/div/div/div[2]/ul[2]/li[1]/a',
        'data': {
            'tag_name': 'a',
            'text': 'Absolventi',
            'attributes': {'class': 'nav-link', 'href': 'https://www.vse.cz/absolventi/'}
        }
    }

    Ideally webpage_elements should be only used in SUH (BE), while browser_view_elements and
    browser_view_selected_elements should be used in BrowserView (FE)

    Also note, that redis communication should be performed here, not in BrowserView

    """

    def __init__(self, uid: Optional[str] = None, init_docrawl = False):
        if not init_docrawl:
            return

        self.webpage_elements = []
        self.browser_view_elements = []
        self.browser_view_selected_elements = []
        self.browser_view_selected_element_groups = []

        # Used for refreshing image in BrowserView
        self.is_screenshot_updated = False
        self.screenshot_string = None

        # Used for refreshing highlighted elements in BrowserView
        self.are_elements_updated = False
        self.are_all_elements_selected = False

        self._is_browser_active = None

        # TODO: for now hardcoded, user_id, project_id and pipeline_id should be passed later
        # All kv_redis scraping keys should be stored only here to avoid
        
        # if synchronization_flags.IS_MODULE_MAIN_INITIALIZED:
        #     redis_key_prefix="FORLOOP_MAIN_DOCRAWL_"
        # elif synchronization_flags.IS_MODULE_FORLOOP_FASTAPI_INITIALIZED:
        #     redis_key_prefix="FORLOOP_FASTAPI_DOCRAWL_"
        # elif synchronization_flags.IS_MODULE_EXECUTION_CORE_INITIALIZED:
        #     redis_key_prefix="FORLOOP_EXECUTION_CORE_DOCRAWL_"
        # else:
        #     redis_key_prefix = None

        if uid is not None:
            redis_key_prefix = f'docrawl:{uid}'
        else:
            redis_key_prefix = f'docrawl:{str(uuid.uuid4())[:8]}'

        self._scraping_data_redis_key_prefix = redis_key_prefix
        self._kv_redis_key_browser_meta_data = f'{self._scraping_data_redis_key_prefix}:browser_meta_data'
        self._kv_redis_key_screenshot = f'{self._scraping_data_redis_key_prefix}:screenshot'
        self._kv_redis_key_elements = f'{self._scraping_data_redis_key_prefix}:elements'

        kv_redis_keys = {
            'browser_meta_data': self._kv_redis_key_browser_meta_data,
            'screenshot': self._kv_redis_key_screenshot,
            'elements': self._kv_redis_key_elements,
        }

        # For now DocrawlClient should follow singleton pattern, meaning it should be initialised only once and here
        self.webscraping_client = DocrawlClient(kv_redis=kv_redis, kv_redis_keys=kv_redis_keys, number_of_spawn_browsers=1, redis_key_prefix=redis_key_prefix)

        try:
            config = configparser.ConfigParser()
            config.read(Path(__file__).parent.parent.parent.absolute() / 'config' / 'scraping_conf.ini')
    
            self.scraperapi_key = config['PROXY']['SCRAPERAPI_KEY']
            self.scrapingbee_key = config['PROXY']['SCRPABINGBEE_KEY']
        except KeyError:
            flog.warning("PROXY KEY WASNT FOUND",self)
            self.scraperapi_key = None
            self.scrapingbee_key = None
            
    @property
    def is_browser_active(self) -> bool:
        # TODO: replace with webscraping_client.is_browser_active()

        """
        Defines, whether browser instance is currently active (browser was not closed)
        """

        last_function = self.webscraping_client.get_browser_meta_data()['function']

        function = last_function['name']
        is_function_done = last_function['done']
        self._is_browser_active = not (function == 'close_browser' and is_function_done)

        return self._is_browser_active

    def update_webpage_elements(self, refresh_browser_view_elements: bool = True):
        """
        :param refresh_browser_view_elements: whether to update browser view FE elements
            (sometime it's just not needed when, e.g. when calling from web app)
        """

        try:
            flog.warning('Refreshing elements')

            # Reset (remove) highlighted and selected rectangles
            self.reset_browser_view_elements()

            webpage_elements = kv_redis.get(self._kv_redis_key_elements)

            if webpage_elements is None:
                webpage_elements = []
            self.are_elements_updated = refresh_browser_view_elements
        except Exception as e:
            flog.error(f'Error while loading webpage elements: {e}')
            webpage_elements = []

        self.webpage_elements = webpage_elements

        return self.webpage_elements
    
    def rescale_webpage_elements(self, webpage_elements, scaling_factor_x):
        """
        webpage_elements -> browser_view_elements
        """

        # Loading mode of browser (normal/headless)
        try:
            browser_info = self.webscraping_client.get_browser_meta_data()['browser']
            headless = browser_info['headless']
            driver_type = browser_info['driver']

            # Due to some issues when rerunning pipeline
            if headless is None:
                headless = False
        except Exception as e:
            flog.error(f'Error while loading bool_scrape_in_browser / driver type!: {e}')
            headless = True
            driver_type = 'Firefox'

        bool_scrape_in_browser = not headless

        # TODO: ScanWebPage icon was not tested on Linux platform
        # Coordinates / size of elements (rectangles) vary on different platforms/drivers, so need to be scaled by coefficient
        # scaling_coef - scaling coefficient depending on OS
        # x, y, height, width - padding
        # [coefficient in headless mode, coefficient with GUI]
        coefs_by_platform_and_driver = {
            'Firefox':
                {
                    'scaling_coef':
                        {'darwin': [1, 2], 'win32': [1, 1], 'linux2': [1, 1], 'linux': [1, 1]},
                    'x': [3, 3],
                    'y': [1, 2],
                    'height': [4, 4],
                    'width': [6, 4],
                },
            'Chrome':
                {
                    'scaling_coef':
                        {'darwin': [2, 2], 'win32': [1, 1], 'linux2': [1, 1], 'linux': [1, 1]},
                    'x': [3, 3],
                    'y': [1, 2],
                    'height': [4, 4],
                    'width': [6, 4],
                },
        }

        padding_x = coefs_by_platform_and_driver[driver_type]['x'][bool_scrape_in_browser]
        padding_y = coefs_by_platform_and_driver[driver_type]['y'][bool_scrape_in_browser]
        padding_height = coefs_by_platform_and_driver[driver_type]['height'][bool_scrape_in_browser]
        padding_width = coefs_by_platform_and_driver[driver_type]['width'][bool_scrape_in_browser]

        scaling_coef = coefs_by_platform_and_driver[driver_type]['scaling_coef'][sys.platform][bool_scrape_in_browser]

        browser_view_elements = []

        # Transfer coordinates of elements to Browser Editor (Browser View)
        for element in webpage_elements:
            # +/- X at the end of line creates padding

            x = element['rect']['x'] * scaling_factor_x * scaling_coef - padding_x
            y = element['rect']['y'] * scaling_factor_x * scaling_coef - padding_y
            h = element['rect']['height'] * scaling_factor_x * scaling_coef + padding_height
            w = element['rect']['width'] * scaling_factor_x * scaling_coef + padding_width

            name = element['name']
            xpath = element['xpath']
            data = element['data']
            type_ = element['type']

            browser_view_elements.append({"pos": [x, y], "size": [w, h], 'name': name, 'type': type_, 'xpath': xpath, 'data': data})

        self.append_browser_view_elements(browser_view_elements)

    def get_webpage_elements(self):
        """
        Note: this method is supposed to be call only from SUH and BrowserView,
        otherwise (e.g. from BrowserViewPopupHandlers) it can return empty list because elements are not yet updated
        """

        return self.webpage_elements

    def get_browser_view_elements(self):
        return self.browser_view_elements

    def get_browser_view_selected_elements(self):
        return self.browser_view_selected_elements
    
    def get_group_index_of_webpage_element(self, element):
        element_group_index = None
        
        for i, group in enumerate(suh.browser_view_selected_element_groups):
            if element in group:
                element_group_index = i
                break
            
        return element_group_index
    
    def delete_empty_groups(self):
        for i, element_group in reversed(list(enumerate(self.browser_view_selected_element_groups))):
            if len(element_group) == 0:
                self.browser_view_selected_element_groups.pop(i)

    def reset_browser_view_elements(self):
        self.browser_view_elements = []
        self.browser_view_selected_elements = []
        self.browser_view_selected_element_groups = []

    def reset_browser_view_selected_elements(self):
        self.browser_view_selected_elements = []

    def append_browser_view_elements(self, browser_view_elements):
        self.browser_view_elements.extend(browser_view_elements)

    def append_browser_view_selected_elements(self, selected_elements_elements):
        self.browser_view_selected_elements.extend(selected_elements_elements)

    def check_xpath_apostrophes(self, xpath):
        """
        Replaces "wrong" apostrophes and quotation marks with the usual one.
    
        :param xpath: XPath to check
        """
    
        apostrophes_transform = {
            '”': '"',
            '“': '"',
            "’": "'",
            "‘": "'",
        }
        if xpath is not None:
            xpath_transformed = [apostrophes_transform[x] if x in apostrophes_transform.keys() else x for x in list(xpath)]
    
            checked_xpath = ''.join(xpath_transformed)
        else:
            checked_xpath = None
        return checked_xpath

    def take_and_load_screenshot(self):
        # Default timeout
        timeout = 160

        with suppress(Exception):
            # Increase timeout when using proxy
            proxy_info = self.webscraping_client.get_browser_meta_data()['browser']['proxy']

            if proxy_info['ip']:
                timeout = 30

        current_screenshot = kv_redis.get(self._kv_redis_key_screenshot)

        if current_screenshot:
            # Remove old screenshot
            kv_redis.set(key=self._kv_redis_key_screenshot, value='')
            flog.warning('Old screenshot was removed')

        # Make screenshot of current page
        self.webscraping_client.take_screenshot()

        timeout_start = time.time()
        while not self.is_screenshot_updated:
            # Load screenshot as base64 string
            screenshot = kv_redis.get(self._kv_redis_key_screenshot)

            if screenshot:
                self.screenshot_string = screenshot
                self.is_screenshot_updated = True
            
            time.sleep(0.1)
            flog.info('Screenshot of page was not found! Waiting 0.1 sec ...')

            if time.time() > timeout_start + timeout:
                flog.warning('Timeout for taking screenshot was exceeded.')

                break

        self.reset_browser_view_elements()

    def refresh_browser_view(self):
        """
        Refreshed the Browser View window (loads new screenshot)
        """

        if self.is_browser_active:
            self.take_and_load_screenshot()
        else:
            ncrb.new_popup([500, 400], "InactiveBrowserPopup")

    def refresh_browser_view_api(self, output_folder):
        """
        Refreshed the Browser View window (loads new screenshot)
        """

        if self.is_browser_active:
            self.webscraping_client.take_png_screenshot(str(Path(output_folder, 'website.png')))

    def wait_until_data_is_extracted_redis(self, redis_key: str, timeout: int = 10, xpath_func=False):
        is_data_extracted = False
        data = None
        timeout_start = time.time()

        while not is_data_extracted and time.time() < timeout_start + timeout:
            data = kv_redis.get(redis_key)

            if data:
                is_data_extracted = True

        if is_data_extracted:
            if len(data) == 1:
                data = data[0].strip()

        # Data was not extracted, e.g. due to invalid XPath or timeout
        else:
            if xpath_func:
                ncrb.new_popup([500, 400], "InvalidXPathPopup")

        return data

    def wait_until_data_is_extracted(self, filename: str, timeout: int = 10, xpath_func=False):  # -> str | list | None
        """
        Waits until icon where data is being saved to file is finished.

        :param filename: name of file that should be created
        :param timeout: default timeout (in seconds), after which loop is ended
        :param xpath_func: does function contains XPath argument
        """
    
        is_file_created = False
        timeout_start = time.time()
    
        while not is_file_created and time.time() < timeout_start + timeout:
            try:
                with Path(filename).open(mode='r', encoding="utf-8") as _:
                    pass
                is_file_created = True
    
            except FileNotFoundError:
                flog.info("File not found, waiting 1 s...")
                time.sleep(1)
    
        # In case file was successfully created
        if is_file_created:
            with Path(filename).open(mode='r', encoding="utf-8") as f:
                data = f.readlines()
    
            if len(data) == 1:
                data = data[0].strip()
    
        # File was not created, e.g. due to invalid XPath or timeout
        else:
            if xpath_func:
                ncrb.new_popup([500, 400], "InvalidXPathPopup")
    
            data = None
    
        return data

    def detect_cookies_xpath_preparation(self):
    
        #if glc.browser_view1.img is None:
        #    return
    
        # flog.warning('Trying to detect cookies popup window')
    
        # In which attributes to search in
        attributes = ('class', 'id', 'name', 'value', 'href')
    
        # What values to search for
        values = ('accept-none', 'accept-all', 'acceptAll', 'AcceptAll', 'acceptall', 'acceptAllCookies'
                  'deny-all', 'DenyAll', 'denyAll', 'denyall',
                  'reject-all', 'RejectAll', 'rejectAll', 'rejectall',
                  'allowall', 'allowAll', 'AllowAll', 'allowAllCookies'
                  'Allow All', 'Allow all', 'allow all', 'Accept All', 'Accept all', 'accept all')
                  #'Cookie', 'cookie', 'Cookies', 'cookies')
    
        # Possible button text
        text_options = ('Accept all', 'Accept All', 'Accept', 'Accept cookies', 'Accept Cookies', 'Accept all cookies',
                        'Allow all', 'Allow All', 'Allow', 'Allow cookies', 'Allow Cookies',
                        'Agree', 'I agree', 'I Agree',
                        'Consent', 'consent', 'Přijmout vše', 'Souhlasím', 'Prijať všetko')
    
        attributes_value_combinations = list(product(attributes, values))
    
        # TODO: not always button is as "button" tag -> need to extend
        xpath = '//*[('
    
        # First part of XPath (searching for mark words in attributes)
        for combination in attributes_value_combinations:
            attribute, value = combination
            xpath += f'contains(@{attribute}, "{value}") or '
    
        # Second part of XPath (searching for button text)
        for text_option in text_options[:-1]:
            xpath += f'*[contains(text(), "{text_option}")] or '
    
        xpath += f'*[contains(text(), "{text_options[-1]}")]) and '
    
        xpath += '(self::button or self::a)]'
    
        # flog.warning(xpath)
        return(xpath)
    
    def detect_cookies_popup(self):
        """
        Detects Cookies Popup on web page using predefined marks.
        # TODO: Can be extended for not cookies only
        """
        xpath = self.detect_cookies_xpath_preparation()
    
        self.webscraping_client.scan_web_page(incl_tables=False, incl_bullets=False, incl_texts=False,
                                       incl_headlines=False, incl_links=False, incl_images=False,
                                       incl_buttons=False, by_xpath=xpath)
    
        webpage_elements = self.update_webpage_elements(refresh_browser_view_elements=False)
    
        # If at least one element was found -> raise popup
        if webpage_elements:
            flog.warning('Cookies popup was found')
            button_xpath = webpage_elements[0]['xpath']
    
            params_dict = {
               'xpath': {'variable': None, 'value': button_xpath}
            }
    
            ncrb.new_popup([500, 400], "CookiesDetectedPopup", params_dict)

    def cut_xpaths_to_common_part(self, xpaths:list):
        # Get common part of XPaths
        common_xpath_part = str(os.path.commonprefix(list(xpaths))).rstrip('/')
    
        # Special case: last element in common XPaths is ordered. For example:
        # .../div[4]/div/div[3] - XPath 1
        # .../div[4]/div/div[4] - XPath 2
        # .../div[4]/div/div[   - common XPaths part -> need to handle unclosed "["
        if common_xpath_part.endswith('['):
            common_xpath_part_split = common_xpath_part.split('/')
            common_xpath_part_split_last_tag = common_xpath_part_split[-1]
    
            # The rest parts of XPaths after removing common part
            xpaths_leftovers = ['/' + common_xpath_part_split_last_tag + re.sub(f"^{re.escape(common_xpath_part)}", "", xp) for xp in xpaths]
    
            # Reconstruct common XPaths part (without unclosed "[")
            common_xpath_part = '/'.join(common_xpath_part_split[:-1])
        else:
            # The rest parts of XPaths after removing common part
            xpaths_leftovers = [re.sub(f"^{re.escape(common_xpath_part)}", "", xp) for xp in xpaths]
    
        flog.warning(common_xpath_part)
        flog.warning(f'LEFTOVERS: {xpaths_leftovers}')
        
        return(common_xpath_part, xpaths_leftovers)
    
    def get_generalized_xpaths(self, xpath):
        """
        Defines the optimal generalized XPath.
        The optimal XPath is XPath that outputs the closest to 20 number of elements (assuming 20 is average number of
        the same elements on page, e.g. real estate listings on one page, product items in eshop etc.).
    
        Returns all generalized XPaths along with optimal XPath index.
        """
    
        flog.warning(f'Original XPath: {xpath}')
    
        # /html/body/div[2]/div/div[4]/img -> ['html', 'body', 'div[2]', 'div', 'div[4]', 'img']
        xpath_split = xpath.split('/')
        tag_order_pattern = re.compile('.{1,10}[[0-9]]')

        # ['html', 'body', 'div[2]', 'div', 'div[4]', 'img'] -> ['img', 'div[4]', 'div, 'div[2]', 'body', 'html']
        xpath_split_reversed = xpath_split[::-1]

        # Used as levels of siblings
        ordered_tags_pos = [i for i, tag in enumerate(xpath_split_reversed) if tag_order_pattern.match(tag)]
    
        generalised_xpaths = [xpath]
    
        # Prepare XPaths (consequently remove index from every ordered tag)
        # Original XPath: /html/body/div[2]/div[3]/div/p[4]
        # New XPaths:
        #   - /html/body/div[2]/div[3]/div/p
        #   - /html/body/div[2]/div/div/p[4]
        #   - /html/body/div/div[3]/div/p[4]
    
        for i, ordered_tag_pos in enumerate(ordered_tags_pos):
            xpath_temp = xpath_split_reversed.copy()
            xpath_temp[ordered_tag_pos] = xpath_temp[ordered_tag_pos].split('[')[0]
    
            xpath_new = '/'.join(xpath_temp[::-1])
            generalised_xpaths.append(xpath_new)
    
        flog.warning('\n'.join(generalised_xpaths))
    
        results = []
        expected_optimal = np.log(20)

        webpage_elements_history = []
    
        for i, generalised_xpath in enumerate(generalised_xpaths):
            if aet.home_folder is None:
                aet.set_home_folder()
            
            webpage_elements = self.scan_web_page(incl_tables=False, incl_bullets=False, incl_texts=False,
                                                  incl_headlines=False, incl_links=False, incl_images=False,
                                                  incl_buttons=False, by_xpath=generalised_xpath, refresh_bv_elements=False)

            num_of_elements = len(webpage_elements)

            flog.warning(f'Using XPath: {generalised_xpath}')
            flog.warning(f'Num of elements found: {num_of_elements}')
    
            # How far is point from expected optimal
            distance_from_optimal = abs(expected_optimal - np.log(num_of_elements))
            flog.warning(f'Distance from optimal: {distance_from_optimal}')
            results.append(distance_from_optimal)
            webpage_elements_history.append(webpage_elements)

            # If it's already the best possible XPath (number of elements is 20) or very close to optimum -> break
            if distance_from_optimal <= 0.3:
                flog.warning('Found most probable optimum, exiting cycle')
                break

        # Get the index of the best result
        optimal_xpath_index = results.index(min(results))
        webpage_elements = webpage_elements_history[optimal_xpath_index]

        flog.warning(f'Optimal XPath {generalised_xpaths[optimal_xpath_index]} yields {len(webpage_elements)} elements')

        self.webscraping_client.set_browser_scanned_elements(webpage_elements)
        self.webpage_elements = webpage_elements
        self.are_elements_updated = True
    
        return generalised_xpaths, optimal_xpath_index

    def scan_web_page_API(self, output_folder, scraping_options: dict):
        """
        Function only to use in "Getting Started" tutorial on web app !!!
        Combines ScanWebPage (all elements) with Cookies Detection
        """
        def generate_folder_structure(folder_name):
            try:
                os.mkdir(folder_name)
            except:
                print("skipping - "+folder_name+" folder exists already")
        
        generate_folder_structure("tmp")
        generate_folder_structure("tmp/screenshots")
        generate_folder_structure("tmp/scraped_data")

        xpath = self.detect_cookies_xpath_preparation()
    
        # self.webscraping_client.take_png_screenshot(str(Path(output_folder, 'website.png'))) #needs to run before the scanner so there is enough time for the parallel thread
        self.webscraping_client.take_screenshot()
        self.webscraping_client.scan_web_page(**scraping_options, timeout = 60) #Duration: ~3s
        
    
        webpage_elements = self.update_webpage_elements(refresh_browser_view_elements=False) #Duration: ~ 0s

        cookies_elements, rest_elements = [], []

        # Split elements
        for x in webpage_elements:
            if 'cookies' in x['name']:
                cookies_elements.append(x)
            else:
                rest_elements.append(x)

        if cookies_elements:
            flog.warning('Cookies popup was found')
            button_xpath = cookies_elements[0]['xpath']

            # Close cookies popup
            self.webscraping_client.click_xpath(button_xpath)
            # self.webscraping_client.take_png_screenshot(str(Path(output_folder, 'website.png'))) #TODO: The scanning finishes before the screenshot thread - need to either 1) refresh screenshot multiple times in FE (optimal), or 2) run this not in thread when cookies detected
            self.webscraping_client.take_screenshot()


        # [::-1] needed to ensure that FE rectangles are not overlapped (bigger elements do not cover smaller)
        return rest_elements[::-1], self.webscraping_client.get_browser_screenshot()

    def scan_web_page(self, incl_tables, incl_bullets, incl_texts, incl_headlines, incl_links, incl_images,
                      incl_buttons, by_xpath, context_xpath='', refresh_bv_elements=True):
        by_xpath = self.check_xpath_apostrophes(by_xpath)

        self.webscraping_client.scan_web_page(incl_tables, incl_bullets, incl_texts, incl_headlines,
                                       incl_links, incl_images, incl_buttons, by_xpath, context_xpath=context_xpath)

        # Load coordinates of elements on page
        webpage_elements = self.update_webpage_elements(refresh_browser_view_elements=refresh_bv_elements)

        return webpage_elements

    def send_text(self, xpath, text):
        xpath = self.check_xpath_apostrophes(xpath)
        self.webscraping_client.send_text(xpath, text)

    def find_nth_substring_occurence(self, string, substring, n):
        #TODO: potentially could hit recursive limit
        if (n == 1) or n == 0:
            return string.find(substring)
        else:
            return string.find(substring, self.find_nth_substring_occurence(string, substring, n - 1) + 1)

    def extract_element_data(self, element: dict, extract_text_from_link: bool = False):
        element_type = element['type']
        
        if type(element["data"])==str:
            #old approach but still might be used when find similar items not called - requires edits on Frontend
            flog.warning('Deprecation warning: Extracted element is not in correct format, it should be dictionary with "tag_name", "text" and "attributes" key', class_instance=self)
            element_data = element['data']
        else:
            #in future this is preferred            
            assert "text" in element["data"].keys()
            assert "attributes" in element["data"].keys()
            assert "tag_name" in element["data"].keys()
            
            if element_type in [ElementType.LINK, ElementType.BUTTON]:
                element_data = element['data']['text'] if extract_text_from_link else element['data']['attributes'].get('href')
            elif element_type == ElementType.IMAGE:
                element_data = element['data']['attributes'].get('src')
            else:
                element_data = element['data']['text']

        return element_data
    
    def extract_data_from_xpath(self, xpath: str, filename: str, write_mode: str = "w+", timeout: int = 3):
        xpath = self.check_xpath_apostrophes(xpath)
        self.webscraping_client.extract_xpath(xpath, filename, write_mode)
        data = self.wait_until_data_is_extracted(filename, timeout=timeout, xpath_func=True)
        
        return data
    
    def extract_data_from_list_of_xpaths(self, xpaths: list[str], filename: str, timeout: int = 3):
        xpaths = [self.check_xpath_apostrophes(xpath) for xpath in xpaths]
        self.webscraping_client.extract_multiple_xpath(xpaths, filename)
        data = self.wait_until_data_is_extracted(filename, timeout=timeout, xpath_func=True)
        
        return data
    
    def find_content_on_page(self):
        self.are_all_elements_selected = True
        self.scan_web_page(by_xpath='//div[count(p) > 4]/*//text()')
        
    def move_webpage_element_from_current_group_to_previous_group(self, element):
        element_group_index = self.get_group_index_of_webpage_element(element)
        self.browser_view_selected_element_groups[element_group_index].remove(element)
                                
        previous_group_index = element_group_index - 1
        
        if previous_group_index < 0:
            new_group = [element]
            self.browser_view_selected_element_groups.append(new_group)
        else:
            self.browser_view_selected_element_groups[previous_group_index].append(element)
            
        self.delete_empty_groups()
    
    def group_elements(self):
        selected_elements = self.get_browser_view_selected_elements()
    
        if len(selected_elements) == 0:
            flog.warning('No elements selected')
        elif len(selected_elements) == 1:
            flog.warning('At least 2 elements should be selected to create a group')
        else:
            xpaths = [x['xpath'] for x in selected_elements]
            names = [x['name'] for x in selected_elements]
            common_xpath_part = str(os.path.commonprefix(list(xpaths))).rstrip('/')
    
            first_elem_pos, last_elem_pos = selected_elements[0]['pos'], selected_elements[-1]['pos']
            first_elem_size = selected_elements[0]['size']
            size = [first_elem_size[0], abs(last_elem_pos[1] - first_elem_pos[1])]
    
            rest_of_browser_view_elements = [x for x in self.browser_view_elements if x['name'] not in names]
    
            new_elem = {
                'name': 'group',
                'type': 'group',
                'pos': first_elem_pos,
                'size': size,
                'xpath': common_xpath_part,
                'data': {
                    'text': '\n'.join([x['data'].get('text', '') for x in selected_elements])
                }
            }
    
            self.browser_view_elements = rest_of_browser_view_elements + [new_elem]
            self.reset_browser_view_selected_elements()
    
            flog.warning(self.browser_view_elements)

    def refresh_page_source(self):
        self.webscraping_client.refresh_page_source()

# HACK: Without passing `init_docrawl=True`, `suh` variable acts as uninitialized, empty proxy.
# This is necessary as the whole codebase imports `suh` from this file.

# On the other hand, if `docrawl` gets initialized here, ExecCore workers are failing to spin up
# their own event loops on which their `docrawl` instances relies. This is due to the fact that when
# workers are spawned from the parent process, `docrawl` is already initialized (alongside with it's
# event loop). The spawned process inherits `docrawls` event loop state without the actual event
# loop thread running. This leads to the inconsistent state in every worker.

# `suh` must only be used inside of processes which have `docrawl` event loop initialized -
# currently it means only  `ExecutionCore` workers. As long as the process WILL NOT spawn child
# processes, it is safe to initialize `docrawl` in it.
if synchronization_flags.IS_MODULE_MAIN_INITIALIZED:
    suh = ScrapingUtilitiesHandler(init_docrawl=True)
else:
    suh = ScrapingUtilitiesHandler()

