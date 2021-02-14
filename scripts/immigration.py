from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql.functions import create_map, lit

from scripts.utils import (
    create_spark_session,
    provide_config,
    read_with_meta,
    uppercase_columns,
    write_with_meta
)


def replace_ids_with_values(df: DataFrame, mapping_config_path: str):
    mapping = provide_config(mapping_config_path)
    for column in mapping.keys():
        replace_dict = mapping.get(column)
        map_col = create_map([lit(x) for x in chain(*replace_dict.items())])
        df = df.withColumn(column, map_col[df[column]])
    return df


def rename(df):
    df = (
        df
            .withColumnRenamed("cicid", "imigrant_id")
            .withColumnRenamed("i94yr", "year")
            .withColumnRenamed("i94mon", "month")
            .withColumnRenamed("i94cit", "origin_city")
            .withColumnRenamed("i94res", "nationality")
            .withColumnRenamed("i94port", "destination_city")
            .withColumnRenamed("arrdate", "arrival_date")
            .withColumnRenamed("i94mode", "transportation_type")
            .withColumnRenamed("i94addr", "residence_city")
            .withColumnRenamed("i94visa", "visa_type")
            .withColumnRenamed("occup", "job")
            .withColumnRenamed("gender", "gender")
    )
    return df


spark = create_spark_session()

config_path = "scripts/config.yaml"
mapping_config_path = "scripts/immigration_data_map.yaml"
config = provide_config(config_path).get('data-transfer').get('immigration')

df = read_with_meta(spark, df_meta=config['input_meta'])
df = replace_ids_with_values(df, mapping_config_path=mapping_config_path)
df = uppercase_columns(df, ['i94port', 'i94addr', 'occup', 'gender'])
df = rename(df)

write_with_meta(df, df_meta=config['immigration_meta'])
write_with_meta(df, df_meta=config['immigrant_meta'])
