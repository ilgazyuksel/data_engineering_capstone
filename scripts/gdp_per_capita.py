"""
Gdp per capita etl script.
"""
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
    config = provide_config(config_path).get('scripts').get('gdp_per_capita')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df=df, col_list=['Country Name'])
    df = df.withColumnRenamed("Country Name", "country_name")
    df = get_country_id(spark, df=df, config=config)

    df_long = melt(
        df=df,
        key_cols=['country_id'],
        value_cols=[str(i) for i in list(range(1960, 2021))],
        var_name='year',
        value_name='gdp_per_capita'
    )
    df_long = add_rank_column(df=df_long, partition_col='year', order_by_col='gdp_per_capita',
                              rank_col='gdp_per_capita_rank', ascending=False)

    write_with_meta(df=df_long, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
