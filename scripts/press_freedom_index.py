"""
Press freedom index etl script.
"""
from pyspark.sql import functions as F

from utils.helper import add_rank_column, get_country_id, uppercase_columns, melt
from utils.io import (
    create_spark_session,
    get_config_path_from_cli,
    provide_config,
    read_with_meta,
    write_with_meta
)


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read with meta
    - Filter indicator column with press freedom index
    - Uppercase columns
    - Rename dataframe
    - Get country id
    - Convert wide dataframe to long
    - Add rank column
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = get_config_path_from_cli()
    config = provide_config(config_path).get('scripts').get('press_freedom_index')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = df.filter(F.col('Indicator') == 'Press Freedom Index').drop('Indicator')
    df = uppercase_columns(df=df, col_list=['Country Name'])
    df = df.withColumnRenamed("Country Name", "country_name")
    df = get_country_id(spark, df=df, config=config)

    df_long = melt(
        df=df,
        key_cols=['country_id'],
        value_cols=[str(i) for i in list(set(range(2001, 2020)) - {2010, 2011})],
        var_name='year',
        value_name='press_freedom_index'
    )
    df_long = add_rank_column(df=df_long, partition_col='year', order_by_col='press_freedom_index',
                              rank_col='press_freedom_rank', ascending=True)

    write_with_meta(df=df_long, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
