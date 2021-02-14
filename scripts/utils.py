"""
Doc
"""
from typing import Dict

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
