from pyspark.shell import spark
from pyspark.sql import functions as F

from misc.constants import RAW_FILE_PATH, OUTPUT_PATH
from schema_definition.schema import bronze_schema

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


def bronze():
    raw_df = spark.read.csv(RAW_FILE_PATH, sep=',', header=True, schema=bronze_schema, enforceSchema=True)
    convert_to_timestamp = raw_df \
        .withColumn("ImplementedDate", F.to_timestamp("ImplementedDate", 'dd/MM/yyyy HH:mm')) \
        .withColumn("RequestDate", F.to_timestamp("RequestDate", 'dd/MM/yyyy HH:mm')) \
        .withColumn("LastUpdatedDate", F.to_timestamp("LastUpdatedDate", 'dd/MM/yyyy HH:mm')) \
        .withColumn("HashKey", F.sha2(F.concat_ws("||", *raw_df.columns), 256))
    return convert_to_timestamp.repartition(1).write.csv(OUTPUT_PATH + 'bronze', header=True, sep='\t', mode='overwrite')
