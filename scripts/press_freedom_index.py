from pyspark.sql import Window
from pyspark.sql import functions as F

from scripts.utils import (
    create_spark_session,
    provide_config,
    read_with_meta,
    uppercase_columns,
    write_with_meta,
    melt
)


def add_rank_column(df):
    w = Window.partitionBy('year').orderBy(F.col('press_freedom_index').desc())
    df = df.withColumn('press_freedom_rank', F.row_number().over(w))
    return df


def main():
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('press_freedom_index')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df, ['Country Name'])
    df = df.withColumnRenamed("Country Name", "country")
    df = df.filter(F.col('Indicator') == 'Press Freedom Index').drop('Indicator')

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
