from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, when, dayofweek
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType


SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("behavior_type", StringType(), True),
    StructField("timestamp", LongType(), True),
])

def load_and_clean(spark, path: str) -> DataFrame:

    df = spark.read \
        .option("header", "false") \
        .schema(SCHEMA) \
        .csv(path) \
        .repartition(24)  # 打散分区，防止单分区过大数据倾斜

    # 过滤脏时间戳
    ts_min, ts_max = 1509465600, 1514764800
    df = df.filter((col("timestamp") >= ts_min) & (col("timestamp") <= ts_max))

    # 时间解析
    df = df.withColumn("datetime", from_unixtime("timestamp"))
    df = df.withColumn("date", col("datetime").substr(1, 10))
    df = df.withColumn("hour", col("datetime").substr(12, 2).cast("int"))

    # 星期几
    dow_map = {1: "周日", 2: "周一", 3: "周二", 4: "周三",
               5: "周四", 6: "周五", 7: "周六"}
    df = df.withColumn("dow_num", dayofweek("datetime"))
    dow_expr = None
    for num, name in dow_map.items():
        expr = when(col("dow_num") == num, name)
        dow_expr = expr if dow_expr is None else dow_expr.when(col("dow_num") == num, name)
    df = df.withColumn("day_of_week", dow_expr).drop("dow_num")

    df = df.filter(col("behavior_type").isin("pv", "cart", "fav", "buy"))

    total = df.count()
    users = df.select("user_id").distinct().count()
    items = df.select("item_id").distinct().count()
    cats = df.select("category_id").distinct().count()
    dates = df.select("date").distinct().orderBy("date").collect()

    print("=" * 60)
    print("数据预处理完成")
    print(f"  总记录: {total:,}")
    print(f"  用户数: {users:,}")
    print(f"  商品数: {items:,}")
    print(f"  品类数: {cats:,}")
    print(f"  时间: {dates[0][0]} ~ {dates[-1][0]}")
    print("=" * 60)

    return df
