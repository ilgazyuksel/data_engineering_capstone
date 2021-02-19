from scripts.utils import (
    create_spark_session,
    provide_config,
    read_with_meta,
    uppercase_columns,
    write_with_meta,
    melt
)

spark = create_spark_session()

config_path = "scripts/config.yaml"
config = provide_config(config_path).get('data-transfer').get('press_freedom_index')

df = read_with_meta(spark, df_meta=config['input_meta'], header=True)
df = uppercase_columns(df, ['Country Name'])
df = df.withColumnRenamed("Country Name", "country")

df_long = melt(
    df=df,
    key_cols=['country'],
    value_cols=[str(i) for i in list(set(range(2001, 2020)) -
                                     set(range(2001, 2020)) - {2010, 2011})],
    var_name='year',
    value_name='press_freedom_index'
)

write_with_meta(df_long, df_meta=config['press_freedom_index_meta'])
