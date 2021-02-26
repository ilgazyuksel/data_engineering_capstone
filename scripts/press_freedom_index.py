"""
Press freedom index etl script.
"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from scripts.utils.helper import uppercase_columns, melt
from scripts.utils.io import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def add_rank_column(df: DataFrame) -> DataFrame:
    """
    Calculate country's rank in terms of press freedom index
    :param df: press freedom index dataframe
    :return: press freedom index dataframe
    """
    w = Window.partitionBy('year').orderBy(F.col('press_freedom_index'))
    df = df.withColumn('press_freedom_rank', F.row_number().over(w))
    logging.info("Press freedom rank calculated")
    return df


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read with meta
    - Filter indicator column with press freedom index
    - Uppercase columns
    - Rename dataframe
    - Convert wide dataframe to long
    - Add rank column
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('scripts').get('press_freedom_index')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = df.filter(F.col('Indicator') == 'Press Freedom Index').drop('Indicator')
    df = uppercase_columns(df, ['Country Name'])
    df = df.withColumnRenamed("Country Name", "country")

    df_long = melt(
        df=df,
        key_cols=['country'],
        value_cols=[str(i) for i in list(set(range(2001, 2020)) - {2010, 2011})],
        var_name='year',
        value_name='press_freedom_index'
    )
    df_long = add_rank_column(df_long)

    write_with_meta(df_long, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
