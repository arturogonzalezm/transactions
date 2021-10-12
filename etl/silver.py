from pyspark.shell import spark

from misc.constants import OUTPUT_PATH
from schema_definition.schema import bronze_schema


def silver():
    bronze_df = spark.read.option("mode", "DROPMALFORMED").csv(OUTPUT_PATH + 'bronze/*.csv', sep='\t', header=True, schema=bronze_schema, enforceSchema=True)
    drop_nulls = bronze_df.na.drop(subset=["AccountID"])
    # filter_out = drop_nulls.filter(drop_nulls.Fibre.startswith('E')).show()
    return drop_nulls.repartition(1).write.csv(OUTPUT_PATH + 'silver', sep='\t', mode='overwrite')

