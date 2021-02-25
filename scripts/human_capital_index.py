from pyspark.sql import Window
from pyspark.sql import functions as F

from scripts.utils.helper import uppercase_columns, melt
from scripts.utils.io import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def add_rank_column(df):
    w = Window.partitionBy('year').orderBy(F.col('human_capital_index').desc())
    df = df.withColumn('human_capital_rank', F.row_number().over(w))
    return df


def main():
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('human_capital_index')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df, ['Country Name'])
    df = df.withColumnRenamed("Country Name", "country")

    df_long = melt(
        df=df,
        key_cols=['country'],
        value_cols=[str(i) for i in list(range(2010, 2021))],
        var_name='year',
        value_name='human_capital_index'
    )
    df_long = add_rank_column(df_long)

    write_with_meta(df_long, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
