from pyspark.shell import spark

from misc.constants import OUTPUT_PATH
from schema_definition.schema import bronze_schema


def silver():
    bronze_df = spark.read.csv(OUTPUT_PATH + 'bronze/*.csv',
                               sep='|',
                               header=True,
                               schema=bronze_schema,
                               enforceSchema=True)
    drop_nulls = bronze_df.na.drop(subset=["AccountID"])
    filter_out = drop_nulls.filter(drop_nulls.Fibre.startswith('E'))
    # hash_and_substract = filter_out_integers.withColumn("Response", F.datediff(F.col("RequestDate"), F.col("ImplementedDate")))
    return filter_out.repartition(1).write.json(OUTPUT_PATH + 'silver', mode='overwrite')
