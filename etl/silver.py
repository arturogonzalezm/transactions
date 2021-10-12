from datetime import datetime

from pyspark.shell import spark
from pyspark.sql import functions as F

from misc.constants import OUTPUT_PATH
from pyspark.sql.types import TimestampType
from schema_definition.schema import bronze_schema, silver_schema

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


def parse_date(date):
    return datetime.strptime(date, "%d/%m/%Y %H:%M:%S %p")


convert_date = F.udf(parse_date, TimestampType())

bronze_df = spark.read.csv(OUTPUT_PATH + 'bronze/*.csv', sep=',', header=True, schema=silver_schema, enforceSchema=True)
convert_to_timestamp = bronze_df \
    .withColumn("ImplementedDate", convert_date(F.from_utc_timestamp(F.col("ImplementedDate"), "PST"))) \
    .withColumn("RequestDate", F.to_timestamp("RequestDate", 'd/M/yyyy H:mm')) \
    .withColumn("LastUpdatedDate", F.to_timestamp("LastUpdatedDate", 'd/M/yyyy H:mm'))
drop_nulls = convert_to_timestamp.na.drop(subset=["AccountID"])
drop_nulls.show()
# filter_out = drop_nulls.filter(drop_nulls.Fibre.startswith('E')).show()
# drop_nulls.repartition(1).write.csv(OUTPUT_PATH + 'silver', sep=',', header=True, mode='overwrite')
