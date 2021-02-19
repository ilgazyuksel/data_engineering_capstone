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
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Median Age", "median_age")
        .withColumnRenamed("Male Population", "male_population")
        .withColumnRenamed("Female Population", "female_population")
        .withColumnRenamed("Total Population", "total_population")
        .withColumnRenamed("Number of Veterans", "veteran_population")
        .withColumnRenamed("Foreign-born", "foreign_population")
        .withColumnRenamed("Average Household Size", "average_household_size")
        .withColumnRenamed("Race", "race")
        .withColumnRenamed("Count", "count")
    )
    return df


def main():
    spark = create_spark_session()

    config_path = "scripts/config.yaml"
    config = provide_config(config_path).get('data-transfer').get('us_cities_demographics')

    df = read_with_meta(spark, df_meta=config['input_meta'], header=True, sep=';')
    df = uppercase_columns(df, ['City', 'State', 'Race'])
    df = df.withColumnRenamed("Country Name", "country")
    df = rename(df)

    write_with_meta(df, df_meta=config['us_cities_demographics_meta'])


if __name__ == "__main__":
    main()
