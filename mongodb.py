# coding: utf-8

'''
- https://docs.mongodb.com/spark-connector/master/python-api/ 참고
- Mongo Spark Connector
  spark-shell --version 명령어로 scala 버전 확인해서 동일한 버전으로 다운로드.
  https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector
- MongoDB Java Driver
  The MongoDB Java Driver uber-artifact, containing the legacy driver, the mongodb-driver, mongodb-driver-core, and bson
  https://docs.mongodb.com/ecosystem/drivers/driver-compatibility-reference/ 에서 버전 호환성 참고.
  https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver
'''

from pyspark.sql import SparkSession

host = "192.168.101.45"
port = 27017
db = "test_angora"
collection = "test_angora_mongo"
uri = "mongodb://{0}:{1}/{2}.{3}".format(host, port, db, collection)

spark = SparkSession.builder \
                    .appName("mongodb test") \
                    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.0") \
                    .config("spark.mongodb.input.uri", uri) \
                    .config("spark.mongodb.output.uri", uri) \
                    .getOrCreate()
#                    .config("spark.driver.extraClassPath", "/home/pjh0347/work/spark/*") \
#                    .config("spark.driver.extraClassPath", "/home/pjh0347/work/spark/mongo-java-driver-3.9.1.jar:/home/pjh0347/work/spark/mongo-spark-connector_2.11-2.4.0.jar") \
#                    .config("spark.jars", "/home/pjh0347/work/spark/mongo-java-driver-3.9.1.jar,/home/pjh0347/work/spark/mongo-spark-connector_2.11-2.4.0.jar") \
# for standalone mode
#                    .config("spark.executor.extraClassPath", "/home/pjh0347/work/spark/mongo-java-driver-3.9.1.jar:/home/pjh0347/work/spark/mongo-spark-connector_2.11-2.4.0.jar") \

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

df.printSchema()
df.show()

