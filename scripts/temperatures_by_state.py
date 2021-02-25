"""
Temperatures by state etl script.
"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from scripts.utils.helper import uppercase_columns
from scripts.utils.io import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def rename(df: DataFrame) -> DataFrame:
    """
    Rename dataframe columns
    :param df: Temperatures by state dataframe
    :return: Temperatures by state dataframe
    """
    df = (
        df
        .withColumnRenamed("dt", "date")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("AverageTemperature", "avg_temperature")
        .withColumnRenamed("AverageTemperatureUncertainty", "avg_temperature_uncertainty")
    )
    return df


def control_input(df: DataFrame) -> DataFrame:
    """
    Remove rows with null avg temperature
    Drop duplicates on unique keys
    :param df: Temperatures by state dataframe
    :return: Temperatures by state dataframe
    """
    df = df.filter(F.col('avg_temperature').isNotNull())
    df = df.drop_duplicates(['date', 'country', 'state'])
    logging.info("Input controls completed")
    return df


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read with meta
    - Uppercase columns
    - Rename dataframe
    - Control input
    - Add year column
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('temperatures_by_state')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df, ['Country', 'State'])
    df = rename(df)
    df = control_input(df)

    df = df.withColumn('year', F.year('date'))
    write_with_meta(df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
