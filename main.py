# from datetime import datetime
# #
# from pyspark.shell import spark
from pyspark.sql import functions as F
# #
# # dt_string = "7/9/2017  12:00:00 am"
# #
# #
# # def convert_date(date):
# #     return datetime.strptime(str(date), "%d/%m/%Y %H:%M:%S %p")
# #
# #
# # if __name__ == '__main__':
# #     print(convert_date(dt_string))
#
from pyspark.shell import spark

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

from pyspark.sql.functions import to_timestamp
df = spark.createDataFrame([('7/09/2017 0:00',)], ['t'])
# df.withColumn("t", F.to_timestamp(df.t, 'd/m/yyyy HH:mm:ss').alias('dt')).show()
df.withColumn("t", F.to_timestamp("t", 'd/m/yyyy HH:mm')).show()

import datetime

strdate = "7/09/2017 0:00"
datetimeobj = datetime.datetime.strptime(strdate, "%d/%m/%Y %H:%M")
print(datetimeobj)
