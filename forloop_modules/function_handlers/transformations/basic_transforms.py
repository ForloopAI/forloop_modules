import pandas as pd
import numpy as np

from dateutil import parser

import forloop_modules.flog as flog

""" rename, drop and select columns """


def column_rename(data, old_colname: str, new_colname: str):
    """
    Renames a column

    Keyword arguments:
    data -- data in the column
    old_colname
    """
    return data.rename(columns={old_colname: new_colname})


def column_drop(data, colname: str):
    return data.drop(colname, axis=1)


def columns_select(data, columns: list):
    return data[columns]


def add_column_constant(data, value, new_colname: str):
    new_data = data.copy()
    new_data[new_colname] = value
    return new_data


def get_valid_columns(data: pd.DataFrame, columns: list):
    return [column for column in columns if column in data.columns]


""" DATE FORMATTING HANDLER FUNCTIONS """


def change_date_format(data, value: str, colname: str):
    """
    Date formatting handler - takes a date and returns it in a specified format.

    Keyword arguments:
    data -- data to be reformatted, type: pandas.DataFrame (expected)
    value -- new date format, type: str
    colname -- name of the column storing the data to be reformatted, type: str
    """

    delimiters = get_value_delimiters(value)
    new_value = simplify_datetime_input(value, delimiters)

    date_formats_dict = {
        "yyyymmdd": "%Y" + delimiters[0] + "%m" + delimiters[0] + "%d",
        "ddmmyyyy": "%d" + delimiters[0] + "%m" + delimiters[0] + "%Y",
        "mmddyyyy": "%m" + delimiters[0] + "%d" + delimiters[0] + "%Y",
        "yyyymmddhms": "%Y" + delimiters[0] + "%m" + delimiters[0] + "%d" + " " + "%H" + delimiters[1] + "%M" +
                       delimiters[1] + "%S",
        "ddmmyyyyhms": "%d" + delimiters[0] + "%m" + delimiters[0] + "%Y" + " " + "%H" + delimiters[1] + "%M" +
                       delimiters[1] + "%S",
        "mmddyyyyhms": "%m" + delimiters[0] + "%d" + delimiters[0] + "%Y" + " " + "%H" + delimiters[1] + "%M" +
                       delimiters[1] + "%S"
    }

    # data[colname]=data[colname].apply(lambda x:"-".join(x.split("-")[-1:-1:-1]))
    data[colname] = data[colname].dropna().apply(lambda x: parser.parse(str(x)).strftime(date_formats_dict[new_value]))

    return data


def get_value_delimiters(value: str) -> list:
    """
    Retrieves delimiters for datetime format -- helper function for change_date_format function
    """
    date_delimiters = ["/", ":", "-", "."]
    time_delimiters = ["-", "/", ":"]
    date = str(value).split(" ")
    date_delim = ""
    time_delim = ":"

    for delimiter in date_delimiters:
        if delimiter in date[0]:
            date_delim = delimiter
            break

    if len(date) > 1:
        for delimiter in time_delimiters:
            if delimiter in date[1]:
                time_delim = delimiter
                break

    return [date_delim, time_delim]


def simplify_datetime_input(datetime_value: str, delimiters: list) -> str:
    new_value = datetime_value.replace(' ',
                                       '').lower()  # removes spaces and lower letters of the new datetime format string

    for delim in delimiters:  # removes all the delimiters -- as a result e.g. YYYY/MM/DD --> yyyymmdd
        new_value = new_value.replace(delim, '')

    return new_value


""" DATE FORMATTING HANDLER FUNCTIONS END """


# date_index2 = pd.date_range('12/29/2009', periods=10, freq='D')
# date_index = pd.date_range('12/29/2009 12:01:00', periods=240, freq='H')
# df1 = pd.concat([pd.Series(date_index2), pd.Series(np.random.randint(100, size=10))], axis=1)
# df2 = pd.concat([pd.Series(date_index), pd.Series(np.random.randint(100, size=240))], axis=1)
# init_index = pd.DatetimeIndex([parser.parse(str(x)) for x in df1[0].dropna()]).sort_values()


def round_to_higher_frequency(df1, data_column_name, low_freq_dt_column_name, df2, high_freq_dt_column_name,
                              round_type: str, new_colname: str):
    low_freq_dt_column = df1[low_freq_dt_column_name].dropna()
    high_freq_dt_column = df2[high_freq_dt_column_name].dropna()
    low_freq_indices = low_freq_dt_column.index

    low_freq_dt_index = pd.DatetimeIndex([parser.parse(str(x)) for x in low_freq_dt_column]).sort_values()
    high_freq_dt_index = pd.DatetimeIndex([parser.parse(str(x)) for x in high_freq_dt_column]).sort_values()

    data_column = df1.loc[low_freq_indices, data_column_name]
    df = pd.DataFrame(data_column.tolist(), index=low_freq_dt_index)
    df.name = new_colname

    if round_type == "Fill forward":
        df = df.reindex(high_freq_dt_index, method="ffill")
    elif round_type == "Fill backward":
        df = df.reindex(high_freq_dt_index, method="bfill")
    elif round_type == "Fill nearest":
        df = df.reindex(high_freq_dt_index, method="nearest")

    df_new = df.reset_index()
    return df_new


""" basic math operations """


def sum_columns(data, columns: list, new_colname: str = 'total'):
    data[new_colname] = data[columns].sum(axis=1)
    return data


def subtract_columns(data, subtract_from: str, subtract_what: list, new_colname: str = 'difference'):
    data[new_colname] = data[subtract_from] - data[subtract_what].sum(axis=1)
    return data


def divide_columns(data, numerator: str, denominator: str, new_colname: str = 'ratio'):
    data[new_colname] = data[numerator] / data[denominator]
    return data


def multiply_columns(data, columns: list, new_colname: str = 'product'):
    data[new_colname] = data[columns].prod(axis=1)
    return data


""" basic math transformations """


def exp_column(data, column: str, new_colname: str = None):
    """
    Exponential function
    """
    if new_colname is None:
        new_colname = 'exp_' + column
    data[new_colname] = np.exp(data[column])
    return data


def log_column(data, column: str, new_colname: str = None, base: float = np.e):
    """
    Logarithm, natural by default, otherwise determined by base parameter
    """
    if new_colname is None:
        new_colname = 'log_' + column
    data[new_colname] = np.log(data[column]) / np.log(base)
    return data


def power_column(data, column: str, new_colname: str = None, power: float = 2):
    """
    take a power of column, 2nd power by default
    """
    if new_colname is None:
        new_colname = 'pow' + str(power) + '_' + column
    data[new_colname] = np.power(data[column], power)
    return data


def root_column(data, column: str, new_colname: str = None, root: float = 2):
    """
    take a root of a column, squareroot by default
    """
    if new_colname is None:
        new_colname = 'root' + str(root) + '_' + column
    data[new_colname] = np.power(data[column], 1 / root)
    return data


def absolute_column(data, column: str, new_colname: str = None):
    if new_colname is None:
        new_colname = 'abs_' + column
    data[new_colname] = np.abs(data[column])
    return data


""" describe """


def describe(data):
    return data.describe()


""" cut numeric column into groups """


def cut_numerics(data, columns: list, bins: int = 10, suffix: str = '_cut'):
    for i in columns:
        data[str(i) + suffix] = pd.cut(data[i], bins=bins)
    return data


""" string operations """


def extract_pattern(data, column: str, new_colname: str = "extracted_pattern", pattern: str = '(.*)'):
    data[new_colname] = data[column].str.extract(pattern, expand=True)
    return data


def match_pattern(data, column: str, new_colname: str = "matched_pattern", pattern: str = '(.*)'):
    data[new_colname] = data[column].str.match(pattern)
    return data


def replace_pattern(data, column: str, new_colname: str = 'replaced_pattern', pattern: str = '(.*)',
                    replacement: str = 'nooo'):
    data[new_colname] = data[column].str.replace(pat=pattern, repl=replacement)
    return data


def convert_case(data, column: str, new_colname: str = None, case: str = 'casefold'):
    if new_colname is None:
        new_colname = case + '_' + column
    if case == 'lower':
        data[new_colname] = data[column].str.lower()
    if case == 'upper':
        data[new_colname] = data[column].str.upper()
    if case == 'title':
        data[new_colname] = data[column].str.title()
    if case == 'capitalize':
        data[new_colname] = data[column].str.capitalize()
    if case == 'swapcase':
        data[new_colname] = data[column].str.swapcase()
    if case == 'casefold':
        data[new_colname] = data[column].str.casefold()
    return data


""" string split and concat """


def column_concat(data, columns: list, new_colname: str = 'new_col', sep: str = "_"):
    data[new_colname] = data[columns[0]]
    if len(columns) > 1:
        for i in columns[1:]:
            data[new_colname] = data[new_colname].astype('str') + sep + data[i].astype('str')
    return data


def column_split(data, colname: str, split_on: str = "_"):
    split = data[colname].astype('str').str.split(split_on, expand=True)
    split.columns = [colname + '_split_' + str(i) for i in split.columns]
    return data.join(split)


""" Column Wise Data Shift and missing data imputation/removal """


def one_way_fix(complete_column, missing_column, behaviour):
    # metoda pocita s tim, ze complete sloupec ma vice zaznamu a do missing sloupce budu pridavat prazdne hodnoty
    row = 0
    increment = 0

    # iterate over all rows from complete column
    while row < complete_column.shape[0]:
        a = complete_column.iloc[row]
        b = missing_column.iloc[row]

        print(row, increment, complete_column.shape[0])
        print(a, b)
        while b not in a:
            increment += 1
            if row + increment + 1 >= complete_column.shape[0]:
                break

            # if b is not in a, get next value from complete column
            a = complete_column.iloc[row + increment]
            print(row, increment, complete_column.shape[0])
            print(a, b)

        if row + increment + 1 >= complete_column.shape[0]:
            break

        if increment > 0:
            # insert increment * NA between previous and current row
            if behaviour == "keep":
                missing_column = pd.concat(
                    [missing_column.iloc[:row], pd.Series(increment * [pd.NA], name=missing_column.name),
                     missing_column.iloc[row:]])
            # remove increment (number of removals) values - from current row (inclusive) to row + increment (exclusive)
            elif behaviour == "remove":
                complete_column = complete_column.drop(complete_column.index[row: row + increment])

        if behaviour == "keep":
            row += increment + 1

        elif behaviour == "remove" and increment == 0:
            row += 1
        increment = 0

    return complete_column.reset_index(drop=True), missing_column.reset_index(drop=True)


def detect_and_fix_data_shift(complete_column_name, incomplete_column_name, data, behaviour="keep"):
    complete_column = data[complete_column_name]
    incomplete_column = data[incomplete_column_name]
    print("First step")
    complete_column, incomplete_column = one_way_fix(complete_column, incomplete_column, behaviour)

    print(complete_column.shape)
    print(incomplete_column.shape)

    complete_column_length = complete_column.shape[0]
    incomplete_column_length = incomplete_column.shape[0]

    if complete_column_length != incomplete_column_length:
        print("bad lengths")
        if complete_column_length < incomplete_column_length:
            less, more = complete_column_length, incomplete_column_length
            smaller_df, bigger_df = complete_column, incomplete_column
        else:
            less, more = incomplete_column_length, complete_column_length
            smaller_df, bigger_df = incomplete_column, complete_column

        diff = more - less
        print("diff", diff)
        if behaviour == "keep":
            smaller_df = pd.concat([smaller_df, pd.Series(diff * [pd.NA], name=smaller_df.attrs["name"])]).reset_index(
                drop=True)
        elif behaviour == "remove":
            bigger_df = bigger_df.drop(bigger_df.index[less:more]).reset_index(drop=True)

        if complete_column_length < incomplete_column_length:
            complete_column, incomplete_column = smaller_df, bigger_df
        else:
            incomplete_column, complete_column = smaller_df, bigger_df

    return pd.concat([complete_column, incomplete_column], axis=1)


""" Empty row detection and removal"""


def find_empty_rows_indices(data, id_columns):
    data_without_id = data.loc[:, data.columns.difference(id_columns)]
    empty_rows = data_without_id.isnull().all(axis=1)
    empty_rows_indices = empty_rows.index[empty_rows].tolist()
    return empty_rows_indices


def find_empty_rows(data, id_columns):
    empty_rows_indices = find_empty_rows_indices(data, id_columns)
    result = data.loc[empty_rows_indices]
    return result


def remove_empty_rows(data, id_columns):
    empty_rows_indices = find_empty_rows_indices(data, id_columns)
    flog.debug(message=f"empty indices: {empty_rows_indices}")
    flog.debug(message=f"row difference: {data.index.difference(empty_rows_indices)}")
    result = data.loc[data.index.difference(empty_rows_indices)]
    return result


""" input parsing """


def cast_str_to_numeric(value):
    value = float(value)
    if abs(value) % 1 < 1e-8:
        value = int(value)

    return value


def cast_user_input_to_proper_type(value):
    """
    Cast input value to float / integer if possible. Otherwise keep string.
    """
    try:
        value = cast_str_to_numeric(value)
    except:
        value = str(value)
    return value
