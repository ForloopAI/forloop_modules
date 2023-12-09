import pandas as pd
import datetime

# Standard icons - on_click creates an icon
STANDARD_ICONS = ["Wait", "Write", "SaveExcel", "SaveList",
                  "IfCondition", "OpenBrowser", "ClickId", "ClickXPath", "LoadWebsite", "PageSource",
                  "PrintXPath", "Start", "Finish", "RunPipeline", "ModifyInput", "Transform", "ClickName",
                  "DfToList", "ListToDf", "Iterate", "PostRequest", "GetRequest", "Trigger"
                  ]

PIPELINE_CLEANING_ICONS = ["NewDataFrame", "DropColumn", "RenameColumn", "ConstantColumn", "SelectColumns", "RemoveEmptyRows",
                           "RemoveDuplicates", "KNNImputation", "Imputation", "Outliers", "Replace", "StripColumn",
                           "Search", "FilterString", "ApplyMapping",
                           "SplitString", "Sort", "ColumnWiseShift", "DifferenceData", "Concatenate",
                           "Join", "AggregateGroupedData", "MathOperation", "FindJoinColumn", "RoundToHigherFrequency",
                           "CategorizeColumn", "SimilarityMatching", "CleanData", "ExtractString"]

SCRAPING_ICONS = ["OpenBrowser", "LoadWebsite", "RefreshPageSource", "ClickXPath", "ScanWebPage", "ExtractXPath",
                  "ExtractMultipleXPath", "ExtractTableXPath", "ClickName", "ClickId", "PrintXPath", "GetCurrentURL",
                  "CloseBrowser", "WaitUntilElementIsLocated", "ExtractPageSource", "ScrollWebPage", "DownloadImage",
                  "DownloadImagesXPath", "SetProxy", "FindPageElements", "GetPageSource"]

ROLLABLE_ICONS = ["DefineFunction"] #* Can be unrolled into sub-block of nodes (functions, classes, loops etc.)
DIRECT_EXECUTE_CORE_HANDLERS = ["ConvertVariableType", "NewVariable"] #! Temporary information holder for testing of an experimental approach in codeview

# Folders with icons
WEBSCRAPING_RPA_ICONS_FOLDER = 'web_scraping_and_rpa'
DATA_SOURCES_ICONS_FOLDER = 'data_sources'
CONTROL_ICONS_FOLDER = 'control'
INTEGRATIONS_ICONS_FOLDER = 'integrations'
CUSTOM_ICONS_FOLDER = 'custom'
SCHEDULE_ICONS_FOLDER = 'schedule'


PLOT_ICONS = ["Graph", "Histogram"]

DATAFRAME_SOURCE_ICONS = ["LoadExcel", "LoadGoogleSheet", "DictToDf", "DBQuery", "DBSelect"]


BACKWARD_COMPATIBILITY_ICON_TYPE_MAPPINGS = {
    ("Scrape"): "OpenBrowser"
}


FL_DATA_TYPES = ["text", "number", "date", "money", "phone number", "address", "domain", "email", "name", "gps", "binary", "unknown"]



# Color mapping for variable type {type:(r,g,b)}

# VARIABLE_TYPE_COLOR_MAPPING = {
#     "DataFrame":(180, 255, 180),
#     "int":(180, 180, 255),
#     "str":(255, 180, 180),
#     "dict":(180, 240, 255),
#     "list":(255, 255, 180),
#     "float":(220, 180, 220),
#     "bool":(180, 240, 240),
#     "function":(255, 130, 170),
#     "datetime": (239,190,125),
#     "date": (101, 163, 152),
#     "time": (128, 160, 214)
# }

# VARIABLE_TYPE_COLOR_MAPPING = {
#     "DataFrame":(190, 255, 190),
#     "int":(190, 190, 255),
#     "str":(255, 190, 190),
#     "dict":(190, 250, 255),
#     "list":(255, 255, 190),
#     "float":(230, 190, 230),
#     "bool":(190, 250, 250),
#     "function":(255, 140, 180),
#     "datetime": (249,200,135),
#     "date": (111, 173, 162),
#     "time": (138, 170, 224)
# }



VARIABLE_TYPE_COLOR_MAPPING = {
    "DataFrame":(200, 255, 200),
    "int":(200, 200, 255),
    "str":(255, 200, 200),
    "dict":(200, 255, 255),
    "list":(255, 255, 200),
    "float":(240, 200, 240),
    "bool":(200, 255, 255),
    "function":(255, 150, 190),
    "datetime": (255,210,145),
    "date": (121, 183, 172),
    "time": (148, 180, 234)
}





"""
#More saturated
self.variable_type_color_mapping={
    "DataFrame":(120, 200, 120),
    "int":(120, 120, 200),
    "str":(200, 120, 120),
    "dict":(120, 190, 230),
    "list":(220, 220, 120),
    "float":(160, 120, 160),
    "bool":(120, 180, 180),
    "function":(128, 0, 0)
    }
"""



ALLOWED_ENTRY_INPUT_TYPES = list(VARIABLE_TYPE_COLOR_MAPPING.keys())

CUSTOM_NODES_FAVOURITE_ARGS_MAPPING = {
    "numpy.zeros": ["dtype"]
}

GOOGLE_API_SERVICES = ["gmail", "drive", "sheets"]
GOOGLE_API_SERVICE_INFO = {
    "gmail": {
        "scopes": ["https://www.googleapis.com/auth/gmail.compose"], 
        "version": "v1"},
    "sheets": {
        "scopes": ["https://www.googleapis.com/auth/spreadsheets"], 
        "version": "v4"},
    "drive": {
        "scopes": ["https://www.googleapis.com/auth/drive"], 
        "version": "v3"}
}

JSON_SERIALIZABLE_TYPES = [str, int, bool, list, dict, float, tuple]
JSON_SERIALIZABLE_TYPES_AS_STRINGS = [typ.__name__ for typ in JSON_SERIALIZABLE_TYPES]
REDIS_STORED_TYPES = [pd.DataFrame, datetime.datetime]
REDIS_STORED_TYPES_AS_STRINGS = [typ.__name__ for typ in REDIS_STORED_TYPES] + ["function", "class"]
