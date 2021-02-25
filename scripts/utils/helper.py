"""
Doc
"""
from itertools import chain
from typing import Iterable
from typing import List

from pyspark.sql import DataFrame, functions as F


def uppercase_columns(df, col_list: List):
    """
    Rewrite the selected columns with upper cases
    :param df: dataframe
    :param col_list: List
        string array of columns to be upper-cased
    :return: df
        dataframe
    """
    for col in col_list:
        df = df.withColumn(col, F.upper(F.col(col)))
        df = df.withColumn(col, F.regexp_replace(F.col(col), 'İ', 'I'))
        df = df.withColumn(col, F.trim(F.col(col)))
    return df


def melt(df: DataFrame, key_cols: Iterable[str], value_cols: Iterable[str],
         var_name: str = "variable", value_name: str = "value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

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

    return df
