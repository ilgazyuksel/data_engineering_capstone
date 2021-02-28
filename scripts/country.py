"""
Country etl script.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.helper import correct_country_names, uppercase_columns
from utils.io import (
    create_spark_session,
    get_config_path_from_cli,
    provide_config,
    read_with_meta,
    write_with_meta
)


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
    immigration = read_with_meta(
        spark,
        df_meta=config['immigration_meta'],
        header=True
    )
    return (gdp_per_capita, human_capital_index, press_freedom_index, temperatures_by_country,
            immigration)


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
                        press_freedom_index: DataFrame, temperatures_by_country: DataFrame,
                        immigration: DataFrame
                        ) -> DataFrame:
    """
    Get unique country values of each dataframe
    Merge all unique values to single dimension data

    :param gdp_per_capita: gdp_per_capita dataframe
    :param human_capital_index: human_capital_index dataframe
    :param press_freedom_index: press_freedom_index dataframe
    :param temperatures_by_country: temperatures_by_country dataframe
    :param immigration: immigration dataframe
    :return:
    """
    gdp_per_capita = get_country_names(gdp_per_capita, 'Country Name')
    human_capital_index = get_country_names(human_capital_index, 'Country Name')
    press_freedom_index = get_country_names(press_freedom_index, 'Country Name')
    temperatures_by_country = get_country_names(temperatures_by_country, 'Country')
    immigration = get_country_names(immigration, 'i94res')

    country = (
        gdp_per_capita
        .unionByName(human_capital_index)
        .unionByName(press_freedom_index)
        .unionByName(temperatures_by_country)
        .unionByName(immigration)
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
    - Correct country names
    - Generate an id column
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = get_config_path_from_cli()
    config = provide_config(config_path).get('scripts').get('country')
    country_mapping_path = config.get('country_mapping_path')

    (
        gdp_per_capita, human_capital_index, press_freedom_index, temperatures_by_country,
        immigration
    ) = read_data(spark, config=config)
    df = merge_country_names(
        gdp_per_capita, human_capital_index, press_freedom_index, temperatures_by_country,
        immigration
    )

    df = correct_country_names(df=df, country_col='country_name',
                               country_mapping_path=country_mapping_path)
    df = df.withColumn('country_id', F.row_number().over(Window.orderBy('country_name')))

    write_with_meta(df=df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
