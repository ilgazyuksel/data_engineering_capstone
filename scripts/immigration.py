"""
Immigration etl script.
"""
import logging
from datetime import datetime, timedelta
from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map, lit, udf
from pyspark.sql.types import DateType

from scripts.utils.helper import get_country_id, uppercase_columns
from scripts.utils.io import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def replace_ids_with_values(df: DataFrame, mapping_config_path: str) -> DataFrame:
    """
    Replace ids with values in order to faster analytic processes.
    :param df: immigration dataframe
    :param mapping_config_path: Path of id-value mapping config
    :return: immigration dataframe
    """
    mapping = provide_config(mapping_config_path)
    for column in mapping.keys():
        replace_dict = mapping.get(column)
        map_col = create_map([lit(x) for x in chain(*replace_dict.items())])
        df = df.withColumn(column, map_col[df[column]])
        df = df.fillna('UNKNOWN', column)
    logging.info("ID columns are replaced with values")
    return df


def rename(df: DataFrame) -> DataFrame:
    """
    Rename dataframe columns
    :param df: immigration dataframe
    :return: immigration dataframe
    """
    df = (
        df
        .withColumnRenamed("cicid", "immigration_id")
        .withColumnRenamed("biryear", "birth_year")
        .withColumnRenamed("i94res", "country_name")
        .withColumnRenamed("arrdate", "arrival_date")
        .withColumnRenamed("i94mode", "transportation_type")
        .withColumnRenamed("i94visa", "visa_type")
        .withColumnRenamed("i94cit", "origin_city")
        .withColumnRenamed("i94port", "destination_city")
        .withColumnRenamed("i94addr", "residence_city")
        .withColumnRenamed("occup", "job")
        .withColumnRenamed("depdate", "departure_date")
        .withColumnRenamed("i94yr", "year")
        .withColumnRenamed("i94mon", "month")
    )
    return df


def control_input(df: DataFrame) -> DataFrame:
    """
    Get data only from 2016
    Drop duplicates on unique keys
    :param df: immigration dataframe
    :return: immigration dataframe
    """
    df = df.filter(F.col('year') == 2016)
    df = df.drop_duplicates(['immigration_id'])
    logging.info("Input controls completed")
    return df


datetime_from_sas = udf(lambda x: datetime(1960, 1, 1) + timedelta(days=int(x)), DateType())


def convert_sas_to_date(df: DataFrame) -> DataFrame:
    """
    Convert dates from sas format to datetime.
    :param df: immigration dataframe
    :return: immigration dataframe
    """
    df = (
        df
        .fillna(0, ['arrdate', 'depdate'])
        .withColumn('arrdate', datetime_from_sas('arrdate'))
        .withColumn('depdate', datetime_from_sas('depdate'))
        .withColumn('arrdate',
                    F.when(F.col('arrdate') == '1960-01-01', None)
                    .otherwise(F.col('arrdate')))
        .withColumn('depdate',
                    F.when(F.col('depdate') == '1960-01-01', None)
                    .otherwise(F.col('depdate')))
    )
    logging.info("SAS date formats converted to datetime")
    return df


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read with meta
    - Convert dates from sas format to datetime
    - Replace ids with values
    - Uppercase columns
    - Rename dataframe
    - Get origin country id
    - Control input
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    mapping_config_path = "scripts/immigration_data_map.yaml"
    config = provide_config(config_path).get('scripts').get('immigration')

    df = read_with_meta(spark, df_meta=config['input_meta'])
    df = convert_sas_to_date(df=df)
    df = replace_ids_with_values(df=df, mapping_config_path=mapping_config_path)
    df = uppercase_columns(df=df, col_list=['i94port', 'i94addr', 'occup', 'gender'])
    df = rename(df=df)
    df = get_country_id(spark, df=df, config=config)
    df = control_input(df=df)
    df = df.withColumnRenamed('country_id', 'origin_country_id')

    write_with_meta(df=df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
