from scripts.utils import (
    create_spark_session,
    provide_config,
    read_with_meta,
    write_with_meta
)


def rename(df):
    df = (
        df
        .withColumnRenamed("dt", "date")
        .withColumnRenamed("LandAverageTemperature", "land_avg_temperature")
        .withColumnRenamed("LandAverageTemperatureUncertainty", "land_avg_temperature_uncertainty")
        .withColumnRenamed("LandMaxTemperature", "land_max_temperature")
        .withColumnRenamed("LandMaxTemperatureUncertainty", "land_max_temperature_uncertainty")
        .withColumnRenamed("LandMinTemperature", "land_min_temperature")
        .withColumnRenamed("LandMinTemperatureUncertainty", "land_min_temperature_uncertainty")
        .withColumnRenamed("LandAndOceanAverageTemperature", "land_ocean_avg_temperature")
        .withColumnRenamed("LandAndOceanAverageTemperatureUncertainty",
                           "land_ocean_avg_temperature_uncertainty")
    )
    return df


def main():
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('global_temperatures')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
    df = rename(df)

    write_with_meta(df, df_meta=config['output_meta'])


if __name__ == "__main__":
    main()