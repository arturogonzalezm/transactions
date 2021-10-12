from pyspark.shell import spark
from pyspark.sql import functions as F

from misc.constants import RAW_FILE_PATH, OUTPUT_PATH
from pyspark.sql.types import TimestampType
from schema_definition.schema import bronze_schema


raw_df = spark.read.csv(RAW_FILE_PATH, sep=',', header=True, schema=bronze_schema, enforceSchema=True)
Augment = raw_df.withColumn("HashKey", F.sha2(F.concat_ws("||", *raw_df.columns), 256))
Augment.repartition(1).write.csv(OUTPUT_PATH + 'bronze', header=True, sep=',', mode='overwrite')
