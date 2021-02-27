"""
Country etl script.
"""
import logging
from itertools import chain

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, lit

from scripts.utils.helper import uppercase_columns
from scripts.utils.io import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def fix_corrupted_country_names(df: DataFrame, mapping_config_path: str) -> DataFrame:
    """
    Replace corrupted country values with true ones.

    :param df: dataframe including country_name column
    :param mapping_config_path: Path of mapping config
    :return: dataframe including country_name columns
    """
    column = "country_name"
    replace_dict = provide_config(mapping_config_path)
    corrupted_values = list(replace_dict.keys())
    map_col = create_map([lit(x) for x in chain(*replace_dict.items())])
    df = df.withColumn(column, F.regexp_replace(column, '"', ''))
    df = df.withColumn(column, F.when(F.col(column).isin(corrupted_values), map_col[df[column]]
                                      ).otherwise(F.col(column)))
    df = df.filter(F.col(column).isNotNull())
    df = df.drop_duplicates()
    logging.info("Corrupted country columns are replaced with true values")
    return df


def read_data(spark, config: dict) -> tuple:
    """
    Read all dataframes that include country columns

    :param spark: Spark session
    :param config: config including input meta
    :return: dataframe tuple
    """
    gdp_per_capita = read_with_meta(
        spark,
        df_meta=config['gdp_per_capita_meta'],
        header=True
    )
    human_capital_index = read_with_meta(
        spark,
        df_meta=config['human_capital_index_meta'],
        header=True
    )
    press_freedom_index = read_with_meta(
        spark,
        df_meta=config['press_freedom_index_meta'],
        header=True
    )
    temperatures_by_country = read_with_meta(
        spark,
        df_meta=config['temperatures_by_country_meta'],
        header=True
    )
    return gdp_per_capita, human_capital_index, press_freedom_index, temperatures_by_country


def get_country_names(df: DataFrame, col: str) -> DataFrame:
    """
    Get unique country values as a dataframe

    :param df: Dataframe including country column
    :param col: column name of country column
    :return: Dataframe
    """
    df = uppercase_columns(df, [col])
    df = df.select(col).drop_duplicates().withColumnRenamed(col, 'country_name')
    return df


def merge_country_names(gdp_per_capita: DataFrame, human_capital_index: DataFrame,
                        press_freedom_index: DataFrame, temperatures_by_country: DataFrame
                        ) -> DataFrame:
    """
    Get unique country values of each dataframe
    Merge all unique values to single dimension data

    :param gdp_per_capita: gdp_per_capita dataframe
    :param human_capital_index: human_capital_index dataframe
    :param press_freedom_index: press_freedom_index dataframe
    :param temperatures_by_country: temperatures_by_country dataframe
    :return:
    """
    gdp_per_capita = get_country_names(gdp_per_capita, 'Country Name')
    human_capital_index = get_country_names(human_capital_index, 'Country Name')
    press_freedom_index = get_country_names(press_freedom_index, 'Country Name')
    temperatures_by_country = get_country_names(temperatures_by_country, 'Country')

    country = (
        gdp_per_capita
        .unionByName(human_capital_index)
        .unionByName(press_freedom_index)
        .unionByName(temperatures_by_country)
        .drop_duplicates()
    )

    return country


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read all dataframes with meta
    - Merge country names
    - Fix corrupted values
    - Generate an id column
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('scripts').get('country')

    (
        gdp_per_capita, human_capital_index, press_freedom_index, temperatures_by_country
    ) = read_data(spark, config=config)
    df = merge_country_names(
        gdp_per_capita, human_capital_index, press_freedom_index, temperatures_by_country
    )

    mapping_config_path = "scripts/country_correction.yaml"
    df = fix_corrupted_country_names(df=df, mapping_config_path=mapping_config_path)
    df = df.withColumn('country_id', F.row_number().over(Window.orderBy('country_name')))

    write_with_meta(df=df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
