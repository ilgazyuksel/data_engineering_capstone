"""
Helper methods.
"""
import logging
from itertools import chain
from typing import Iterable
from typing import List

from pyspark.sql import DataFrame, functions as F


def uppercase_columns(df, col_list: List):
    """
    Rewrite the selected columns with upper cases
    :param df: dataframe
    :param col_list: string array of columns to be upper-cased
    :return: dataframe
    """
    for col in col_list:
        df = df.withColumn(col, F.upper(F.col(col)))
        df = df.withColumn(col, F.regexp_replace(F.col(col), 'İ', 'I'))
        df = df.withColumn(col, F.trim(F.col(col)))
    logging.info(f"{col_list} columns are converted to uppercase")
    return df


def melt(df: DataFrame, key_cols: Iterable[str], value_cols: Iterable[str],
         var_name: str = "variable", value_name: str = "value") -> DataFrame:
    """
    Convert wide dataframe format into long format
    :param df: Wide dataframe
    :param key_cols: Key columns to be remained after conversion.
    :param value_cols: Value columns to be converted into variable and value columns.
    :param var_name: Column name of variable.
    :param value_name: Column name of value.
    :return: Long dataframe
    """
    # Create map<key: value>
    vars_and_vals = F.create_map(
        list(chain.from_iterable([
            [F.lit(c), F.col(c)] for c in value_cols]
        ))
    )

    df = (
        df
            .select(*key_cols, F.explode(vars_and_vals))
            .withColumnRenamed('key', var_name)
            .withColumnRenamed('value', value_name)
            .filter(F.col(value_name).isNotNull())
    )
    logging.info(f"Wide dataframe converted to long dataframe")
    return df
