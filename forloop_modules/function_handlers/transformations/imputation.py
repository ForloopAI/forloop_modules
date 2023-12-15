import numpy as np
import pandas as pd
import warnings
import os
import pickle

from collections import Counter
from sklearn.impute import KNNImputer

import forloop_modules.flog as flog

from forloop_modules.redis.redis_connection import kv_redis, redis_config
import forloop_modules.queries.node_context_requests_backend as ncrb

def ensure_list(variable):
    if not isinstance(variable, list):
        variable = [variable]
    return variable


def most_frequent(x):
    return Counter(x).most_common(1)[0][0]


def dtype_apply(x, function_num=np.mean, function_cat=most_frequent):
    if isinstance(x, pd.DataFrame):
        return x.apply(lambda y: dtype_apply(y))
    else:
        if np.issubdtype(x.dtype, np.number):
            return function_num(x)
        else:
            try:
                return function_cat(x[~pd.isnull(x)])
            except IndexError as e:
                warnings.warn(str(e))
                return function_cat(x)


class TransferImputation:
    def __init__(self, model_directory='.', function_cat=most_frequent, function_num=np.mean, iterate=False,
                 coltype=np.number, save=True):
        self.function_num = function_num
        self.function_cat = function_cat
        self.iterate = iterate
        self.coltype = coltype
        self.index_cols = None
        self.cols = None
        self.fillings = None
        if save:
            if (model_directory == '') or (not model_directory):
                model_directory = '.'
            self.output_path = model_directory

    def save(self):
        # if self.output_path == '.':
        label_names = [c.lower().replace(' ', '_') for c in self.cols]
        self.output_path = self.output_path + '/' + label_names[0]

        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        pickle.dump(self, open(os.path.join(self.output_path, "transfer_imputer.pickle"), "wb"))

    @staticmethod
    def load(output_path: str):
        self = pickle.load(open(os.path.join(output_path + '/', "transfer_imputer.pickle"), "rb"))
        return self

    def _learn_agg(self, data, group_cols, cols):
        self.cols = ensure_list(cols)
        df = data.copy()
        group_cols = ensure_list(group_cols)
        group_cols = [i for i in group_cols if i not in self.cols]
        self.fillings = df.groupby(group_cols)[self.cols].apply(
            lambda x: dtype_apply(x[~x.isnull()], function_num=self.function_num, function_cat=self.function_cat)
        )
        self.index_cols = group_cols
        return self.fillings

    def _apply_agg(self, data, cols=None, fillings=None):
        df = data.copy()
        index = df.index
        df['row_id'] = np.arange(len(df))
        if fillings is None:
            fillings = self.fillings
        if cols is None:
            cols = [i for i in fillings.columns]
        cols = ensure_list(cols)
        df = df.set_index(fillings.index.names)
        df.update(fillings, overwrite=False)
        df.sort_values('row_id', ascending=True, inplace=True)
        df.index = index

        return df[cols]

    def _learn_noagg(self, data, cols):
        self.cols = ensure_list(cols)
        df = data.copy()
        self.fillings = dtype_apply(df[cols], function_num=self.function_num,
                                    function_cat=self.function_cat)  # df[cols].agg(lambda x: self.function(x))
        return self.fillings

    def _apply_noagg(self, data, cols=None, fillings=None):
        if fillings is None:
            fillings = self.fillings
        if cols is None:
            cols = fillings.index
        return data[cols].fillna(fillings)

    def learn(self, data, cols, group_cols=None):
        if group_cols is None:
            self._learn_noagg(data, cols)
        else:
            self._learn_agg(data=data, cols=cols, group_cols=group_cols)
        return self.fillings

    def impute(self, data, cols=None, fillings=None):
        if self.index_cols is None:
            out = self._apply_noagg(data=data, cols=cols, fillings=fillings)
        else:
            out = self._apply_agg(data=data, cols=cols, fillings=fillings)
        return out


class KNNImputation:
    def __init__(self):
        pass

    def clean_data(self, entry_data): ##### NOT PROPERLY TESTED, TODO: When LoadExcel works
        """
        Clean data from empty rows, columns and duplicates.
        Only includes numerical columns
        
        This node has dependency to Scanner which should be available through RecommendationEngine API
        """
        
        kv_redis.set("stored_special_cleaned_dataframe", entry_data)
        result=ncrb.clean_data("stored_special_cleaned_dataframe")
        print("RESULT",result)
        
        if result.ok:
            data=kv_redis.get("stored_special_cleaned_dataframe")
        else:
            data=entry_data
            print("Entry data was not modified")
        return data

    def fit_transform(self, entry_data, imputation_columns=None, is_cleaned: bool = False, return_valid_only: bool = False):
        flog.debug("data before cleaning")
        flog.debug(entry_data)
        if not is_cleaned:
            clean_data = self.clean_data(entry_data)
        else:
            clean_data = entry_data

        if not imputation_columns:
            imputation_columns = clean_data.columns.to_list()
        else:
            imputation_columns = [column for column in imputation_columns if column in clean_data]

        sklearn_knn = KNNImputer(n_neighbors=5, weights="distance").fit_transform(clean_data)

        # round data to integers if column is integer
        integer_columns_indices = [clean_data.columns.get_loc(column) for column in imputation_columns if
                           pd.api.types.is_integer_dtype(entry_data[column].dtype)]
        if integer_columns_indices:
            sklearn_knn[:, integer_columns_indices] = np.round_(sklearn_knn[:, integer_columns_indices])

        imputation_column_indices = [clean_data.columns.get_loc(column) for column in imputation_columns]
        # create resulting dataframe
        if return_valid_only:
            resulting_df = pd.DataFrame(sklearn_knn[:, imputation_column_indices], index=clean_data.index, columns=imputation_columns)
        else:
            resulting_df = entry_data.copy()
            # TODO: PROBABLY CAN BE REPLACED WITH DF.UPDATE(DF2)
            resulting_df.loc[clean_data.index, imputation_columns] = sklearn_knn[:, imputation_column_indices]

        flog.debug("dataframe after imputer")
        flog.debug(resulting_df)

        return resulting_df

# class Imputation:
#     def __init__(self, function = np.mean, iterate = False, coltype = np.number):
#         self.function = function
#         self.iterate = iterate # if low memory, then fill missing values one by one
#         self.coltype = coltype # if no columns provided, all columns of given type will be chosen.
#
#     def _fill_missing_by_group(self, data, columns, group_cols):
#         df = data[columns + group_cols].copy()
#         if not self.iterate:
#             df[columns] = df.groupby(group_cols)[columns].transform(lambda x: x.fillna(self.function(x)))
#         else:
#             grouping = df.groupby(group_cols)
#             for i in columns:
#                 df[i] = grouping[i].transform(lambda x: x.fillna(self.function(x)))
#         return df
#
#     def _fill_missing_noagg(self, data, columns):
#         df = data[columns].copy()
#         if not self.iterate:
#             df[columns] = df[columns].transform(lambda x: x.fillna(self.function(x)))
#         else:
#             for i in columns:
#                 df[i] = df[i].transform(lambda x: x.fillna(self.function(x)))
#         return df
#
#     def fill_missing(self, data, columns = None, group_cols = None):
#         # If no columns to fill provided, all default
#         if columns is None:
#             columns = [i for i in data.select_dtypes(include = self.coltype).columns]
#         else:
#             columns = ensure_list(columns)
#         if group_cols is not None:
#             group_cols = ensure_list(group_cols)
#             df = self._fill_missing_by_group(data = data, columns = columns, group_cols = group_cols)
#         else:
#             df = self._fill_missing_noagg(data = data, columns = columns)
#         return df[columns]


# class DeepImputation:
#     def __init__(self, model_directory = '.'):
#         self.imputer = None
#         self.input_columns = None
#         self.output_column = None
#         if model_directory is None:
#             model_directory = '.'
#         self.model_directory = model_directory
#
#     def learn(self, data, input_columns, output_column: str):
#         if input_columns is None:
#             input_columns = [i for i in data.columns if i not in ensure_list(output_column)]
#         else:
#             input_columns = [i for i in ensure_list(input_columns) if i not in ensure_list(output_column)]
#
#
#
#         #TEMPORARY PACKAGING DISABLE
#
#         self.imputer = dtwg.SimpleImputer(
#             input_columns = input_columns,
#             output_column = output_column,
#             output_path = self.model_directory + '/' + output_column.lower().replace(" ", "_")
#         )
#         self.imputer.model_directory = self.model_directory
#         self.imputer.fit(train_df = data)
#
#
#         self.input_columns = input_columns
#         self.output_column = output_column
#         return self
#
#     def impute(self, data):
#         imputed = self.imputer.predict(data)
#         out = imputed[self.imputer.output_column]
#         out[out.isna()] = imputed[f'{self.imputer.output_column}_imputed'][out.isna()]
#         return out
#
#     def learn_impute(self, data, input_columns, output_column: str):
#         self.learn(data = data, input_columns = input_columns, output_column = output_column)
#         return self.impute(data)
#
#     def load(self, model_path):
#         #self.imputer = dtwg.SimpleImputer.load(model_path) #Temporary packaging disable
#         self.model_directory = self.imputer.model_directory
#         return self
