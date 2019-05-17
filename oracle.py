# coding: utf-8

"""
- https://www.oracle.com/technetwork/apps-tech/jdbc-112010-090769.html
- ojdbc jar file download
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("oracle test") \
                    .config("spark.jars", "/home/pjh0347/work/spark/ojdbc6.jar") \
                    .getOrCreate()
#                    .config("spark.driver.extraClassPath", "/home/pjh0347/work/spark/*") \
#                    .config("spark.driver.extraClassPath", "/home/pjh0347/work/spark/ojdbc6.jar") \
# for standalone mode
#                    .config("spark.executor.extraClassPath", "/home/pjh0347/work/spark/ojdbc6.jar") \

# Create the JDBC URL
host = "192.168.100.180"
port = 1521
db = "xe"
table = "HR.EMPLOYEES"
username = "system"
password = "oracle"
jdbcUrl = "jdbc:oracle:thin:{}/{}@{}:{}/{}".format(username, password, host, port, db)
df = spark.read.format("jdbc") \
               .option("url", jdbcUrl) \
               .option("dbtable", table) \
               .option("driver", "oracle.jdbc.driver.OracleDriver") \
               .load()
df.printSchema()
df.show()

