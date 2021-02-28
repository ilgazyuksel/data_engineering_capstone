"""
Immigration etl script.
"""
import logging
from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql.functions import create_map, lit

from utils.io import (
    create_spark_session,
    get_config_path_from_cli,
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


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read with meta
    - Replace ids with values
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = get_config_path_from_cli()
    config_path = 'scripts/config.yaml'
    config = provide_config(config_path).get('scripts').get('immigration_mapping')
    mapping_config_path = config.get('mapping_config_path')

    df = read_with_meta(spark, df_meta=config['input_meta'])
    df = replace_ids_with_values(df=df, mapping_config_path=mapping_config_path)

    write_with_meta(df=df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
