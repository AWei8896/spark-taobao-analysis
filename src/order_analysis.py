"""
24小时购买分布、每日转化率趋势、周内购买分布、用户购买频次、分时段热门品类
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, hour, when

def hourly_pattern(df: DataFrame) -> None:
    print("\n" + "=" * 60)
    print("九、24小时购买行为分布")
    print("=" * 60)

    df.groupBy("hour").agg(
        count("*").alias("total_actions"),
        count(when(col("behavior_type") == "buy", 1)).alias("buy_count"),
    ).withColumn(
        "conversion", col("buy_count") / col("total_actions") * 100
    ).orderBy("hour").show(24, truncate=False)


def daily_conversion_trend(df: DataFrame) -> None:
    print("\n" + "=" * 60)
    print("十、每日购买转化率趋势")
    print("=" * 60)

    df.groupBy("date").agg(
        count("*").alias("total"),
        count(when(col("behavior_type") == "pv", 1)).alias("pv"),
        count(when(col("behavior_type") == "buy", 1)).alias("buy"),
    ).withColumn(
        "pv_to_buy", col("buy") / col("pv") * 100
    ).orderBy("date").show(20, truncate=False)


def weekday_pattern(df: DataFrame) -> None:
    print("\n" + "=" * 60)
    print("十一、周内购买分布")
    print("=" * 60)

    df.filter(col("behavior_type") == "buy") \
      .groupBy("day_of_week") \
      .agg(count("*").alias("buy_count")) \
      .orderBy(col("buy_count").desc()) \
      .show()


def user_purchase_frequency(df: DataFrame) -> None:
    """用户购买频次分布：买1次的用户占比 vs 多次购买"""
    print("\n" + "=" * 60)
    print("十二、用户购买频次分布")
    print("=" * 60)

    from pyspark.sql.functions import sum as spark_sum

    user_buy_counts = df.filter(col("behavior_type") == "buy") \
                        .groupBy("user_id") \
                        .agg(count("*").alias("buy_times"))

    total_buyers = user_buy_counts.count()

    freq_stats = user_buy_counts.groupBy("buy_times") \
                                .agg(count("*").alias("user_count")) \
                                .withColumn("pct", col("user_count") / total_buyers * 100) \
                                .orderBy("buy_times")

    freq_stats.show(20, truncate=False)

    # 只买1次 vs 多次
    once = freq_stats.filter(col("buy_times") == 1).agg(spark_sum("user_count")).first()[0] or 0
    print(f"\n  购买用户总数: {total_buyers:,}")
    print(f"  仅购买1次: {once:,} ({once/total_buyers*100:.1f}%)")
    print(f"  购买≥2次: {total_buyers - once:,} ({(total_buyers-once)/total_buyers*100:.1f}%)")


def top_category_by_hour(df: DataFrame, n: int = 10) -> None:
    print("\n" + "=" * 60)
    print(f"十三、分时段热门品类 Top {n}")
    print("=" * 60)

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    hourly_cat = df.filter(col("behavior_type") == "buy") \
                   .groupBy("hour", "category_id") \
                   .agg(count("*").alias("buy_count"))

    w = Window.partitionBy("hour").orderBy(col("buy_count").desc())
    result = hourly_cat.withColumn("rank", row_number().over(w)) \
                       .filter(col("rank") <= n) \
                       .orderBy("hour", "rank")

    result.show(30, truncate=False)


def run_order_analysis(df: DataFrame) -> None:
    """一键执行购买全部分析"""
    hourly_pattern(df)
    daily_conversion_trend(df)
    weekday_pattern(df)
    user_purchase_frequency(df)
    top_category_by_hour(df)
