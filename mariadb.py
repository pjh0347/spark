# coding: utf-8

"""
wget https://downloads.mariadb.com/Connectors/java/connector-java-2.4.1/mariadb-java-client-2.4.1.jar
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("mariadb test") \
                    .config("spark.jars", "/home/pjh0347/work/spark/mariadb-java-client-2.4.1.jar") \
                    .getOrCreate()
#                    .config("spark.driver.extraClassPath", "/home/pjh0347/work/spark/*") \
#                    .config("spark.driver.extraClassPath", "/home/pjh0347/work/spark/mariadb-java-client-2.4.1.jar") \
# for standalone mode
#                    .config("spark.executor.extraClassPath", "/home/pjh0347/work/spark/mariadb-java-client-2.4.1.jar") \

# Create the JDBC URL
host = "192.168.101.50"
port = 3306
db = "test"
username = "metatron"
password = "metatron123"
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(host, port, db, username, password)

# query to the database engine
query = "(select * from user) as test"
df = spark.read.jdbc(url=jdbcUrl, table=query)
df.printSchema()
df.show()

'''
# Read from JDBC connections across multiple workers
df = spark.read.jdbc(url=jdbcUrl, table="user", column="employee_id", lowerBound=1, upperBound=100000, numPartitions=100)
df.printSchema()
df.show()


# another way to create df
df = spark.read.format("jdbc") \
               .option("url", "jdbc:mysql://{0}:{1}/{2}".format(host, port, db)) \
               .option("dbtable", "test.user") \
               .option("user", username) \
               .option("password", password) \
               .option("driver", "org.mariadb.jdbc.Driver") \
               .load()
df.printSchema()
df.show()

# https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html
create = """
CREATE TABLE test_table
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:mysql://192.168.101.50:3306",
  user "{0}",
  password "{1}",
  dbtable "test.user"
)""".format(username, password)
#dbtable "(SELECT * FROM test.user) as test"
df = spark.sql(create)

# https://docs.databricks.com/spark/latest/spark-sql/language-manual/insert.html
insert = """
INSERT INTO test_table VALUES (100, 100, "NORMAL", "test", "test")
"""
df = spark.sql(insert)

select = """
SELECT count(*) record_count FROM test_table
"""
df = spark.sql(select)
df.show()
'''

