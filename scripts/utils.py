"""
Doc
"""
from typing import Dict, List

from omegaconf import OmegaConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session() -> SparkSession:
    """Get or create a spark session"""
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def provide_config(path):
    conf = OmegaConf.load(path)
    resolved = OmegaConf.to_container(conf, resolve=True)
    return resolved


def apply_schema(df: DataFrame, schema: Dict) -> DataFrame:
    """
    """
    select_cols = []
    for col_name, col_type in schema.items():
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        select_cols.append(col_name)
    df = df.select(select_cols)
    return df


def read_with_meta(spark, df_meta: dict, *args) -> DataFrame:
    """
    """
    path = df_meta["path"]
    schema = df_meta["schema"]
    data_format = df_meta["data_format"]
    if data_format == "parquet":
        df = spark.read.parquet(path)
    elif data_format == "csv":
        df = spark.read.csv(path, *args)
    else:
        raise AttributeError("Only csv or parquet data formats are readable")
    df = apply_schema(df, schema=schema)
    return df


def write_with_meta(df, df_meta: dict):
    """
    """
    path = df_meta["path"]
    schema = df_meta["schema"]
    try:
        partition_cols = df_meta["partition_cols"]
    except:
        partition_cols = None
        repartition_number = 3
        df = df.repartition(repartition_number)
    df = apply_schema(df, schema=schema)
    df.write.parquet(path=path, mode='overwrite', partitionBy=partition_cols)


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
