import numpy as np
import pandas as pd
import rrcf

from sklearn.preprocessing import MinMaxScaler

import forloop_modules.flog as flog

from forloop_modules.utils.pandas_operations import df_difference


# based on: https://github.com/kLabUM/rrcf

def ensure_list(variable):
    if not isinstance(variable, list):
        variable = [variable]
    return variable


def remove_outliers(data, outliers):
    return df_difference(data, outliers)


def detect_numeric_outliers(data, cols=None, num_trees=100, tree_size=128, scaler=MinMaxScaler().fit_transform,
                            top_n=None, top_n_percent=None, seed=42):
    """
    Implements outlier detection with Robust Random Cut Forest (https://github.com/kLabUM/rrcf) algorithm.

    The likelihood that a point is an outlier is measured by its collusive displacement (CoDisp):
        if including a new point significantly changes the model complexity (i.e. bit depth),
        then that point is more likely to be an outlier.

    returns a dataframe of the same structure as data containing observations most likely to be outliers.
    Number of returned observations is controled by top_n and top_n_percent parameters
    --- inputs ---
    data - pandas dataframe without missing values in analyzed columns
    cols - names of columns to be analyzed. If None (default), then uses all numeric columns.
    scaler - the data scaling to be applied. Defaults to MinMax, but optimal scaler depends on data
    top_n or top_n_percent - how many values to return, defaults to 2.5% of observations most likely to be outliers
    --- result ---
    pandas dataframe of the same schema as data
    """
    # by default mark top 2.5% likeliest to be outliers as outliers
    if top_n_percent is None and top_n is None:
        top_n_percent = 0.025

    forest = []
    n = data.shape[0]
    d = data.shape[1]

    # print("numeric outliers")
    # print("input Data:", data)

    sample_size_range = (n // tree_size, tree_size)

    if cols is None or len(cols) == 0:
        X = data.select_dtypes(include=np.number)
    else:
        cols = ensure_list(cols)
        X = data[cols]
    if scaler is not None:
        X = scaler(X)
    # print("Data:", X)
    np.random.seed(seed=seed)
    while len(forest) < num_trees:
        # Select random subsets of points uniformly from point set

        ixs = np.random.choice(n, size=sample_size_range,
                               replace=False)
        # Add sampled trees to forest
        trees = []
        for ix in ixs:
            try:
                Xa = np.array(X)[ix]
                if np.sum(Xa) != 0:
                    trees.append(rrcf.RCTree(Xa, index_labels=ix))
                else:
                    print("Outliers array containing only zeros")
            except Exception as e:

                cls = rrcf.RCTree()
                flog.warning("Outliers nan warning", cls)
                flog.warning(e, cls)
                flog.warning("data", cls)
                flog.warning(np.array(X), cls)
                flog.warning("data indexed", cls)
                flog.warning(np.array(X[ix]), cls)

        # trees = [rrcf.RCTree(np.array(X)[ix], index_labels=ix)
        #          for ix in ixs]
        forest.extend(trees)

    avg_codisp = pd.Series(0.0, index=np.arange(n))
    index = np.zeros(n)
    for tree in forest:
        codisp = pd.Series({leaf: tree.codisp(leaf)
                            for leaf in tree.leaves})
        avg_codisp[codisp.index] += codisp
        np.add.at(index, codisp.index.values, 1)
    avg_codisp /= index

    # align indexes
    avg_codisp.index = data.index
    if top_n_percent is not None:

        out = data[avg_codisp > avg_codisp.quantile(1 - top_n_percent)]
    else:
        out = data[avg_codisp >= np.array(avg_codisp)[
            np.array(avg_codisp).argpartition(kth=range((len(avg_codisp) - top_n), len(avg_codisp)))[
            (len(avg_codisp) - top_n):]].min()]
    return out
