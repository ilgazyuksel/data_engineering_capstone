"""
Doc
"""

from omegaconf import OmegaConf
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Get or create a spark session"""
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def provide_config(path):
    conf = OmegaConf.load(path)
    resolved = OmegaConf.to_container(conf, resolve=True)
    return resolved
