from pyspark.sql import functions as F

from scripts.utils.helper import uppercase_columns
from scripts.utils.io import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def rename(df):
    df = (
        df
        .withColumnRenamed("dt", "date")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Latitude", "latitude")
        .withColumnRenamed("Longitude", "longitude")
        .withColumnRenamed("AverageTemperature", "avg_temperature")
        .withColumnRenamed("AverageTemperatureUncertainty", "avg_temperature_uncertainty")
    )
    return df


def control_input(df):
    df = df.filter(F.col('avg_temperature').isNotNull())
    df = df.drop_duplicates(['date', 'country', 'city'])
    return df


def main():
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('temperatures_by_city')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df, ['Country', 'City'])
    df = rename(df)
    df = control_input(df)

    df = df.withColumn('year', F.year('date'))
    write_with_meta(df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
