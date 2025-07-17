import ast
from forloop_modules.flog import flog

def parse_comboentry_input(input_value):
    """
    Parse comboentry input to extract the actual value.
    Handles various input formats: lists, strings, and string representations 
    of lists that occur during pipeline serialization.
    """
    if isinstance(input_value, list) and len(input_value) > 0:
        return input_value[0]
    
    if isinstance(input_value, str) and input_value.startswith('[') and input_value.endswith(']'):
        try:
            parsed_value = ast.literal_eval(input_value)
            if isinstance(parsed_value, list) and len(parsed_value) > 0:
                return parsed_value[0]
        except (ValueError, SyntaxError) as e:
            flog.warning(f"Could not parse comboentry input string '{input_value}'. Error: {e}")
            pass
    
        return input_value

def parse_google_sheet_id_from_url(sheet_url: str):
    """
    Extracts and returns the Google Sheet ID from a given Google Sheets URL.

    Args:
    sheet_url (str): The full URL of the Google Sheet.

    Returns:
    str: The extracted Google Sheet ID.

    Example:
    >>> sheet_url = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0"
    >>> parse_google_sheet_id_from_url(sheet_url)
    '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
    """
    
    google_file_id = sheet_url.split("/d/")[1].split("/")[0]

    return google_file_id

