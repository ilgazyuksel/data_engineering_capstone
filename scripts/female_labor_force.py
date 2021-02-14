from scripts.utils import (
    create_spark_session,
    provide_config,
    read_with_meta,
    uppercase_columns,
    write_with_meta,
    melt
)


def rename(df):
    df = df.withColumnRenamed("Country Name", "country")
    for year in range(2011, 2021):
        df = df.withColumnRenamed(f"{year} [YR{year}]", str(year))
    return df


spark = create_spark_session()

config_path = "scripts/config.yaml"
config = provide_config(config_path).get('data-transfer').get('female_labor_force')
from pyspark.sql import functions as F

df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
df = (
    df
    .filter(F.col('Series Name') == 'Labor force, female (% of total labor force)')
    .drop('Series Name')
)
df = uppercase_columns(df, ['Country Name'])
df = rename(df)

df_long = melt(
    df=df,
    key_cols=['country'],
    value_cols=[str(i) for i in list(range(2011, 2021))],
    var_name='year',
    value_name='female_labor_force_ratio'
)

write_with_meta(df_long, df_meta=config['female_labor_force_meta'])
