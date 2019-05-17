# coding: utf-8

"""
- https://diogoalexandrefranco.github.io/interacting-with-hbase-from-pyspark/ 참고
- HADOOP_CONF_DIR 에 hbase-site.xml 파일이 존재하는 경로 추가해 줘야함.
- HBase table key, values 는 raw byte strings 으로 처리됨.
"""

"""
- hbase shell
create 'test_pjh0347','rowkey', 'syslog'
list
describe 'test_pjh0347'
put 'test_pjh0347', 1, 'rowkey:key', 1
put 'test_pjh0347', 1, 'syslog:DATETIME', '20190507100101'
put 'test_pjh0347', 1, 'syslog:RAW', '20190507100101 | platform2 | daemon | info | info | 1e | systemd | Removed slice User Slice of root.'
put 'test_pjh0347', 1, 'syslog:HOST', 'platform2'
put 'test_pjh0347', 1, 'syslog:FACILITY', 'daemon'
put 'test_pjh0347', 1, 'syslog:PRIORITY', 'info'
put 'test_pjh0347', 1, 'syslog:LEVEL', 'info'
put 'test_pjh0347', 1, 'syslog:LEVEL_INT', 7.0
put 'test_pjh0347', 1, 'syslog:TAG', '1e'
put 'test_pjh0347', 1, 'syslog:PROGRAM', 'systemd'
put 'test_pjh0347', 1, 'syslog:MSG', 'Removed slice User Slice of root.'
scan 'test_pjh0347'
get 'test_pjh0347', 1
put 'test_pjh0347', 1, 'syslog:TAG', 'new value'
delete 'test_pjh0347', '<row>', '<column name >', '<time stamp>'
disable 'test_pjh0347'
drop 'test_pjh0347'
"""

from pyspark.sql import SparkSession

catalog = ''.join("""
    {
        "table": {
            "namespace": "default",
            "name": "test_pjh0347"
        },
        "rowkey": "key",
        "columns": {
            "key":      {"cf": "rowkey", "col": "key",       "type": "string"},
            "DATETIME": {"cf": "syslog", "col": "DATETIME",  "type": "string"},
            "RAW":      {"cf": "syslog", "col": "RAW",       "type": "string"},
            "HOST":     {"cf": "syslog", "col": "HOST",      "type": "string"},
            "FACILITY": {"cf": "syslog", "col": "FACILITY",  "type": "string"},
            "PRIORITY": {"cf": "syslog", "col": "PRIORITY",  "type": "string"},
            "LEVEL":    {"cf": "syslog", "col": "LEVEL",     "type": "string"},
            "LEVEL_INT":{"cf": "syslog", "col": "LEVEL_INT", "type": "string"},
            "TAG":      {"cf": "syslog", "col": "TAG",       "type": "string"},
            "PROGRAM":  {"cf": "syslog", "col": "PROGRAM",   "type": "string"},
            "MSG":      {"cf": "syslog", "col": "MSG",       "type": "string"}
        }
    }
""".split())

print catalog

spark = SparkSession.builder \
                    .appName("hbase test") \
                    .config("spark.jars.packages", "com.hortonworks.shc:shc-core:1.1.0.3.1.0.30-1") \
                    .config("spark.jars.repositories", "https://repo.hortonworks.com/content/groups/public/") \
                    .getOrCreate()

df = spark.read \
               .options(catalog=catalog) \
               .format("org.apache.spark.sql.execution.datasources.hbase.DefaultSource") \
               .load()

df.printSchema()
df.show()

