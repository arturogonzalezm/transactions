from pyspark.shell import spark

from misc.constants import OUTPUT_PATH
from schema_definition.schema import gold_schema

silver_df = spark.read.csv(OUTPUT_PATH + 'silver/*.csv', sep='|', header=True, schema=gold_schema, enforceSchema=True)
silver_df.repartition(1).write.option("maxRecordsPerFile", 1000).json(OUTPUT_PATH + 'gold', mode='overwrite')
