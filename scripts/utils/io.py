"""
Input & output methods.
"""
import logging
from typing import Dict

from omegaconf import OmegaConf
from pyspark.sql import DataFrame, SparkSession, functions as F


def create_spark_session() -> SparkSession:
    """Get or create a spark session"""
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.info("SparkSession created")
    return spark


def provide_config(path) -> Dict:
    """Get config from path with OmegaConf resolver"""
    conf = OmegaConf.load(path)
    resolved = OmegaConf.to_container(conf, resolve=True)
    logging.info("Config provided")
    return resolved


def apply_schema(df: DataFrame, schema: Dict) -> DataFrame:
    """
    Cast columns with schema dictionary.
    :param df: Dataframe
    :param schema: Dictionary with column name as key, column type as value.
    :return: dataframe
    """
    select_cols = []
    for col_name, col_type in schema.items():
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        logging.info(f"{col_name} column cast as {col_type}")
        select_cols.append(col_name)
    df = df.select(select_cols)
    return df


def read_with_meta(spark, df_meta: dict, **kwargs) -> DataFrame:
    """
    Read data with meta dictionary.
    :param spark: Spark session
    :param df_meta: Meta dictionary including path, schema and data format.
    :return: Dataframe
    """
    path = df_meta["path"]
    schema = df_meta["schema"]
    data_format = df_meta["data_format"]
    if data_format == "parquet":
        df = spark.read.parquet(path)
    elif data_format == "csv":
        df = spark.read.csv(path, **kwargs)
    else:
        raise AttributeError("Only csv or parquet data formats are readable")
    df = apply_schema(df, schema=schema)
    logging.info("Dataframe was read successfully with meta")
    return df


def write_with_meta(df, df_meta: dict):
    """
    Write data with meta dictionary.
    :param df: Dataframe
    :param df_meta: Meta dictionary including path, schema and partition columns (optional).
    :return: None
    """
    path = df_meta["path"]
    schema = df_meta["schema"]
    try:
        partition_cols = df_meta["partition_cols"]
        if len(partition_cols) == 1:
            df = df.repartition(partition_cols[0])
            logging.info(f'Dataframe was repartitioned with {partition_cols} column')
    except:
        partition_cols = None
        repartition_number = 3
        df = df.repartition(repartition_number)
    df = apply_schema(df, schema=schema)
    df.write.parquet(path=path, mode='overwrite', partitionBy=partition_cols)
    logging.info("Dataframe was written successfully with meta")
