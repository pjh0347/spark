# coding: utf-8

"""
- https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
- jdbc jar file download (https://jdbc.postgresql.org/download.html)
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("postgresql test") \
                    .config("spark.jars", "/root/spark/postgresql-42.2.8.jar") \
                    .config("spark.driver.extraClassPath", "/root/spark/postgresql-42.2.8.jar") \
                    .getOrCreate()

# Create the JDBC URL
host = "192.168.102.182"
port = 5432
db = "postgres"
table = "(select * from gen_100 limit 10) as tmp"
username = "postgres"
password = "ahqlwps12#$"
jdbcUrl = "jdbc:postgresql://{}:{}/{}?&characterEncoding=UTF-8".format(host, port, db)
df = spark.read.format("jdbc") \
               .option("url", jdbcUrl) \
               .option("dbtable", table) \
               .option("user", username) \
               .option("password", password) \
               .option("fetchsize", 100000) \
               .option("driver", "org.postgresql.Driver") \
               .load()
df.printSchema()
print df.collect()

