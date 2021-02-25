from scripts.utils.helper import uppercase_columns, melt
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
    - Convert wide dataframe to long
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('gdp_per_capita')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df, ['Country Name'])
    df = df.withColumnRenamed("Country Name", "country")

    df_long = melt(
        df=df,
        key_cols=['country'],
        value_cols=[str(i) for i in list(range(1960, 2021))],
        var_name='year',
        value_name='gdp_per_capita'
    )

    write_with_meta(df_long, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
