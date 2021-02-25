"""
US cities demographics etl script.
"""
from pyspark.sql import DataFrame

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
    :param df: us cities demographics dataframe
    :return: us cities demographics dataframe
    """
    df = (
        df
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Median Age", "median_age")
        .withColumnRenamed("Male Population", "male_population")
        .withColumnRenamed("Female Population", "female_population")
        .withColumnRenamed("Total Population", "total_population")
        .withColumnRenamed("Number of Veterans", "veteran_population")
        .withColumnRenamed("Foreign-born", "foreign_born_population")
        .withColumnRenamed("Average Household Size", "average_household_size")
        .withColumnRenamed("Race", "race")
        .withColumnRenamed("Count", "count")
    )
    return df


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read with meta
    - Uppercase columns
    - Rename dataframe
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('us_cities_demographics')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True, sep=';')
    df = uppercase_columns(df, ['City', 'State', 'Race'])
    df = rename(df)

    write_with_meta(df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
