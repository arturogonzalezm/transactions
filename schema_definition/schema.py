from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ShortType, TimestampType

bronze_schema = StructType([ \
    StructField("AccountID", StringType(), True), \
    StructField("CODE", StringType(), True), \
    StructField("ImplementedDate", StringType(), True), \
    StructField("ActiveIndicator", StringType(), True), \
    StructField("AccountType", StringType(), True), \
    StructField("Service", StringType(), True), \
    StructField("BU", StringType(), True), \
    StructField("RequestDate", StringType(), True), \
    StructField("AccountStatus", StringType(), True), \
    StructField("StatusCode", StringType(), True), \
    StructField("Amount", StringType(), True), \
    StructField("Version", StringType(), True), \
    StructField("AgentID", StringType(), True), \
    StructField("Fibre", StringType(), True), \
    StructField("LastUpdatedDate", StringType(), True), \
    StructField("PropertyType", StringType(), True), \
    StructField("PostCode", StringType(), True)

])

silver_schema = StructType([ \
    StructField("AccountID", IntegerType(), True), \
    StructField("CODE", IntegerType(), True), \
    StructField("ImplementedDate", StringType(), True), \
    StructField("ActiveIndicator", IntegerType(), True), \
    StructField("AccountType", StringType(), True), \
    StructField("Service", StringType(), True), \
    StructField("BU", StringType(), True), \
    StructField("RequestDate", StringType(), True), \
    StructField("AccountStatus", StringType(), True), \
    StructField("StatusCode", IntegerType(), True), \
    StructField("Amount", DoubleType(), True), \
    StructField("Version", StringType(), True), \
    StructField("AgentID", IntegerType(), True), \
    StructField("Fibre", StringType(), True), \
    StructField("LastUpdatedDate", StringType(), True), \
    StructField("PropertyType", StringType(), True), \
    StructField("PostCode", IntegerType(), True), \
    StructField("HashKey", StringType(), True), \
    StructField("Response", ShortType(), True)

])

gold_schema = StructType([ \
    StructField("AccountID", IntegerType(), True), \
    StructField("CODE", IntegerType(), True), \
    StructField("ImplementedDate", TimestampType(), True), \
    StructField("ActiveIndicator", IntegerType(), True), \
    StructField("AccountType", StringType(), True), \
    StructField("Service", StringType(), True), \
    StructField("BU", StringType(), True), \
    StructField("RequestDate", TimestampType(), True), \
    StructField("AccountStatus", StringType(), True), \
    StructField("StatusCode", IntegerType(), True), \
    StructField("Amount", DoubleType(), True), \
    StructField("Version", StringType(), True), \
    StructField("AgentID", IntegerType(), True), \
    StructField("Fibre", StringType(), True), \
    StructField("LastUpdatedDate", TimestampType(), True), \
    StructField("PropertyType", StringType(), True), \
    StructField("PostCode", IntegerType(), True), \
    StructField("HashKey", StringType(), True), \
    StructField("Response", ShortType(), True)

])
