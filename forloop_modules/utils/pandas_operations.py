import pandas as pd
import numpy as np
import re


# keep it here for future use
# pattern = re.compile("^([A-Z][0-9]+)+$")
# pattern.match(string)


def ensure_list(variable):
    if not isinstance(variable, list):
        variable = [variable]
    return variable


def read_spreadsheet(path):
    try:
        df = pd.read_excel(path)
        filetype = 'EXCEL'
    except Exception:
        df = pd.read_csv(path)
        filetype = 'CSV'
    return df, filetype


def write_spreadsheet(data, path):
    if re.compile('.*\\.csv$').search(path):
        data.to_csv(path, index=False)
    elif re.compile('.*\\.xlsx$').search(path):
        data.to_excel(path, index=False)
    return 0


def df_difference(data, subtract_df):
    """
    Dataframe difference, returns all rows in data that are not in subtract dataframe
    https://stackoverflow.com/questions/48647534/python-pandas-find-difference-between-two-data-frames
    ---inputs---
    data - pandas data frame
    subtract_df - pandas data frame to be subtracted from data; has a same schema as data.
    """
    return data[~data.fillna(0).apply(tuple, 1).isin(subtract_df.fillna(0).apply(tuple, 1))]


def sort(data, col_names, ascending=True):
    """
    sort pandas dataframe based on columns provided in column_names (str or list)
    use order based on ascending param (str or list)
    """
    return data.sort_values(by=col_names, ascending=ascending)


# keep for now, may be used in new pivot table icon
def dcast(data, value_variable=None, index=None, columns=None, fun_aggregate=np.mean, fill_value=None):
    if fun_aggregate is None:
        out = data.pivot(index=index, columns=columns, values=value_variable)
        if fill_value is not None:
            out.fillna(fill_value, inplace=True)
    else:
        out = data.pivot_table(values=value_variable, index=index, columns=columns, aggfunc=fun_aggregate,
                               fill_value=fill_value, dropna=False)
    return out  # .pivot_table(values = value_variable, index = index, columns = columns,
    #              aggfunc = fun_aggregate, fill_value = fill_value, dropna = False)



def find_value(data, value, search_cols=None, match='exact'):
    if search_cols is None:
        search_cols = data.columns
    else:
        search_cols = ensure_list(search_cols)
    if match == 'pattern':
        regex = True
    else:
        regex = False

    indices = data[search_cols].apply(lambda x: x.astype('str').str.contains(value, regex=regex)).any(axis=1)
    df_new = data.copy()
    df_new = df_new[indices]
    return df_new


def find_replace(data, pattern: str, replacement: str, search_cols=None, match='exact'):
    if search_cols is None:
        search_cols = data.columns
    else:
        search_cols = ensure_list(search_cols)

    old_dtypes = data.dtypes
    stack = data[search_cols].stack()
    if match == 'pattern':
        replaced_data = stack.astype('str').str.replace(pattern, replacement, regex=True).unstack()
    else:
        replaced_data = stack.astype('str').replace(pattern, replacement).unstack()
    out = data.copy()
    out[replaced_data.columns] = replaced_data

    for x in out.columns:
        try:
            out[x] = out[x].astype(old_dtypes[x])
        except:
            pass
    return out
