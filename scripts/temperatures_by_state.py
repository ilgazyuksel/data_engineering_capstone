from scripts.utils import (
    create_spark_session,
    provide_config,
    read_with_meta,
    uppercase_columns,
    write_with_meta
)


def rename(df):
    df = (
        df
        .withColumnRenamed("dt", "date")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("AverageTemperature", "avg_temperature")
        .withColumnRenamed("AverageTemperatureUncertainty", "avg_temperature_uncertainty")
    )
    return df


def main():
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('temperatures_by_state')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = uppercase_columns(df, ['Country', 'State'])
    df = rename(df)

    write_with_meta(df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()
