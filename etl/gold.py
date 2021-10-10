from pyspark.shell import spark
from pyspark.sql import functions as F

from misc.constants import OUTPUT_PATH
from schema_definition.schema import silver_schema


def gold():
    silver_df = spark.read.csv(OUTPUT_PATH + 'silver/*.csv',
                               sep='|',
                               header=True,
                               schema=silver_schema,
                               enforceSchema=True)
    response = silver_df.withColumn("Response", F.datediff(F.col("RequestDate"), F.col("ImplementedDate")))
    # top_amount = response.groupBy("AgentID").max("Amount").show()
    response.createOrReplaceTempView("fastest_response")
    fastest_response_query = """
                        SELECT * FROM fastest_response ORDER BY Response DESC
                        """
    fastest_response_df = spark.sql(fastest_response_query)
    return fastest_response_df.write.option("maxRecordsPerFile", 1000).json(OUTPUT_PATH + 'gold', mode='overwrite')
