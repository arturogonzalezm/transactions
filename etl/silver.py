from datetime import datetime

from pyspark.shell import spark
from pyspark.sql import functions as F

from misc.constants import OUTPUT_PATH
from pyspark.sql.types import TimestampType
from schema_definition.schema import silver_schema

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


def convert_date(date):
    return datetime.strptime(str(date), "%d/%m/%Y %H:%M")


spark.udf.register("convert_date", convert_date, TimestampType())


bronze_df = spark.read.csv(OUTPUT_PATH + 'bronze/*.csv', sep=',', header=True, schema=silver_schema, enforceSchema=True)
convert_to_timestamp = bronze_df \
    .withColumn("ImplementedDate", F.unix_timestamp("ImplementedDate", "d/MM/yyyy HH:mm").cast(TimestampType())) \
    .withColumn("RequestDate", F.to_timestamp("RequestDate", 'dd/MM/yyyy HH:mm').cast(TimestampType())) \
    .withColumn("LastUpdatedDate", F.to_timestamp("LastUpdatedDate", 'dd/MM/yyyy HH:mm').cast(TimestampType()))


calculate_response = convert_to_timestamp.withColumn("Response", F.datediff(F.col("RequestDate"), F.col("ImplementedDate")))
calculate_response.createOrReplaceTempView("fastest_response")

fastest_response_query = """
                        SELECT * FROM fastest_response ORDER BY Response DESC
                        """
fastest_response_df = spark.sql(fastest_response_query)

drop_nulls = fastest_response_df.na.drop(subset=["AccountID"])
filter_out = drop_nulls.filter(drop_nulls.Fibre.startswith('E'))
filter_out.repartition(1).write.csv(OUTPUT_PATH + 'silver', sep=',', header=True, mode='overwrite')
