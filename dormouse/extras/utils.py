import pandas as pd
import numpy as np

import time
import datetime

from sqlalchemy.sql import func
from sqlalchemy import Integer


def clean_db_col_names(name: str, rule_set={".": "_"}, replace_set=None):
    """
    Clean column names for the master player lookup table
    :param name: The column/parameter name to be cleaned
    :type str, required
    :param rule_set: The rules defining what will be substituted in the string
    :type dict, optional
    :param replace_set: The rules defining what WHOLE words will be repplaced
    :type dict, optional
    """
    # Get rid of invalid characters that will result in syntax errors
    if type(name) is not str:
        # print(name)
        return name

    # repalace names if necessary
    new_name = name
    if replace_set is not None:
        for key, value in replace_set.items():
            if new_name == key:
                new_name = value

    for key, value in rule_set.items():
        new_name = new_name.replace(key, value)

    return new_name


def get_col_min_max(session, table, column):
    """
    Given a session, table, and target column, get the earliest and latest dates the column contains
    :param session: Sqlalchemy session
    :type class: 'sqlalchemy.orm.Session', required
    :param table: Database table object
    :type class: 'sqlalchemy.ext.declarative.delarative_base', required
    :param column: Name of the target column
    :type str, required
    """
    qry = session.query(
        func.min(getattr(table, column)).label("start"),
        func.max(getattr(table, column)).label("end"),
    )
    res = qry.one()

    return res.start, res.end


def native_dtype(np_value: np.ndarray):
    """
    Converts a given numpy scalar to a native python type
    :param np_value: The numpy scalar to be converted
    :type class: '{numpy.ndarray}', required
    """
    try:
        val = np_value.item()
    except AttributeError:
        # Happens when passed a pandas string object or a pandas timestamp object
        try:
            val = np_value.date()
        except AttributeError:
            val = str(np_value)

    return val


def space_out_req(func):
    """
    Ensures that requests to websites are properly spaced out to not make any admins angry
    :param func: The function making the outside requests
    :type function, required
    """

    def wrap(*args, **kwargs):
        time_start = time.time()
        while time.time() - time_start < 0.5:
            time.sleep(0.1)
        return func(*args, **kwargs)

    return wrap


def cast_fiel_dtypes(df: pd.DataFrame, tbl) -> pd.DataFrame:
    """
    Cast fields that map to an int to numeric ints
    """
    col_names = df.columns
    clean_names = [clean_db_col_names(x) for x in col_names]

    for cname, rname in zip(clean_names, col_names):
        try:
            if isinstance(getattr(tbl, cname).type, Integer):
                df[rname] = (
                    pd.to_numeric(df[rname], errors="coerce")
                    .fillna(0)
                    .astype(np.int64)
                )

        except AttributeError:
            pass

    return df
