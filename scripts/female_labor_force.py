from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from scripts.utils.helper import uppercase_columns, melt
from scripts.utils.io import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def rename(df: DataFrame) -> DataFrame:
    """
    Rename dataframe columns
    :param df: female labor force dataframe
    :return: female labor force dataframe
    """
    df = df.withColumnRenamed("Country Name", "country")
    for year in range(2011, 2021):
        df = df.withColumnRenamed(f"{year} [YR{year}]", str(year))
    return df


def main():
    """
    Run pipeline:
    - Create spark session
    - Get config
    - Read with meta
    - Filter series name column with female labor force
    - Uppercase columns
    - Rename dataframe
    - Convert wide dataframe to long
    - Write with meta
    :return: None
    """
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('female_labor_force')
    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = df.filter(F.col('Series Name') == 'Labor force, female (% of total labor force)')
    df = uppercase_columns(df, ['Country Name'])
    df = rename(df)

    df_long = melt(
        df=df,
        key_cols=['country'],
        value_cols=[str(i) for i in list(range(2011, 2021))],
        var_name='year',
        value_name='female_labor_force_ratio'
    )

    write_with_meta(df_long, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
