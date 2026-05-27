from pyspark.sql import SparkSession

HDFS_INPUT = "hdfs:///data/taobao/input/UserBehavior.csv"
HDFS_OUTPUT = "hdfs:///data/taobao/output"

def get_spark(app_name="TaobaoUserBehavior"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("yarn") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "2") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
