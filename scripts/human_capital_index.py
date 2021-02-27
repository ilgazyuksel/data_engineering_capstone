"""
Human capital index etl script.
"""
from scripts.utils.helper import add_rank_column, get_country_id, uppercase_columns, melt
from scripts.utils.io import (
    create_spark_session,
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

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('scripts').get('human_capital_index')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df=df, col_list=['Country Name'])
    df = df.withColumnRenamed("Country Name", "country_name")
    df = get_country_id(spark, df=df, config=config)

    df_long = melt(
        df=df,
        key_cols=['country_id'],
        value_cols=[str(i) for i in list(range(2010, 2021))],
        var_name='year',
        value_name='human_capital_index'
    )
    df_long = add_rank_column(df=df_long, partition_col='year', order_by_col='human_capital_index',
                              rank_col='human_capital_rank', ascending=False)

    write_with_meta(df=df_long, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
