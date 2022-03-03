
import pyspark
from spark3 import Spark3


sc = pyspark.SparkContext()
spark = pyspark.SQLContext(sc)


spark3 = Spark3(spark.sparkSession, Spark3.EtherscanABIProvider())

contract = spark3.contract("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b")
df = contract.get_event_by_name('OrdersMatched')
df.printSchema()

df.select("event_parameter.*", "dt").createOrReplaceTempView("opensea_order_matched")
spark.sql('select dt,count(1),sum(price) from opensea_order_matched where dt > "2022-01-01" group by dt order by dt ').show()







