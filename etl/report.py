from pyspark.shell import spark
from pyspark.sql import functions as F

from misc.constants import OUTPUT_PATH
from schema_definition.schema import gold_schema

gold_df = spark.read.json(OUTPUT_PATH + 'gold/*.json', schema=gold_schema)

gold_df.createOrReplaceTempView("top_agents")
top_agents_query = """
                    SELECT AgentID, PostCode, max(Amount) AS MaxAmount 
                    FROM top_agents 
                    GROUP BY AgentID, PostCode 
                    ORDER BY MaxAmount DESC;
                   """
top_agents_df = spark.sql(top_agents_query)
top_agents_df.show(200, truncate=False)
top_agents_df.repartition(1).write.parquet(OUTPUT_PATH + 'report', mode='overwrite')
