"""
Temperatures by country etl script.
"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.helper import (
    add_decade_column,
    correct_country_names,
    get_country_id,
    uppercase_columns
)
from utils.io import (
    create_spark_session,
    get_config_path_from_cli,
    provide_config,
    read_with_meta,
    write_with_meta
)


def rename(df: DataFrame) -> DataFrame:
    """
    Rename dataframe columns
    :param df: Temperatures by country dataframe
    :return: Temperatures by country dataframe
    """
    df = (
        df
        .withColumnRenamed("dt", "date")
        .withColumnRenamed("Country", "country_name")
        .withColumnRenamed("AverageTemperature", "avg_temperature")
    )
    return df


def control_input(df: DataFrame) -> DataFrame:
    """
    Remove rows with null avg temperature
    Drop duplicates on unique keys
    :param df: Temperatures by country dataframe
    :return: Temperatures by country dataframe
    """
    df = df.filter(F.col('avg_temperature').isNotNull())
    df = df.drop_duplicates(['date', 'country_name'])
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
    - Correct country names
    - Get country id
    - Control input
    - Add decade column
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = get_config_path_from_cli()
    config = provide_config(config_path).get('scripts').get('temperatures_by_country')
    country_mapping_path = config.get('country_mapping_path')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df=df, col_list=['Country'])
    df = rename(df=df)
    df = control_input(df=df)
    df = correct_country_names(df=df, country_col='country_name',
                               country_mapping_path=country_mapping_path)
    df = get_country_id(spark, df=df, config=config)
    df = add_decade_column(df=df, date_col='date')

    write_with_meta(df=df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
