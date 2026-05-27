"""
	热销商品排行、品类渗透率、商品复购率、长尾效应
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, when


def top_products(df: DataFrame, n: int = 20) -> None:
    print("\n" + "=" * 60)
    print(f"五、热销商品 Top {n}")
    print("=" * 60)

    df.filter(col("behavior_type") == "buy") \
      .groupBy("item_id") \
      .agg(count("*").alias("buy_count")) \
      .orderBy(col("buy_count").desc()) \
      .show(n, truncate=False)


def category_penetration(df: DataFrame, n: int = 20) -> None:
    print("\n" + "=" * 60)
    print(f"六、品类渗透率 Top {n}")
    print("=" * 60)

    total_buyers = df.filter(col("behavior_type") == "buy") \
                     .select("user_id").distinct().count()

    df.filter(col("behavior_type") == "buy") \
      .groupBy("category_id") \
      .agg(
          countDistinct("user_id").alias("buyer_count"),
          count("*").alias("buy_times"),
      ) \
      .withColumn("penetration", col("buyer_count") / total_buyers * 100) \
      .orderBy(col("buyer_count").desc()) \
      .show(n, truncate=False)


def repurchase_rate(df: DataFrame, n: int = 20) -> None:
    """商品复购率"""
    print("\n" + "=" * 60)
    print(f"七、商品复购率分析 Top {n}")
    print("=" * 60)

    # 每用户-每商品的购买次数
    user_item_buy = df.filter(col("behavior_type") == "buy") \
                      .groupBy("item_id", "user_id") \
                      .agg(count("*").alias("buy_times"))

    # 按商品聚合：总购买用户数、复购用户数（买>=2次）
    item_stats = user_item_buy.groupBy("item_id").agg(
        countDistinct("user_id").alias("total_buyers"),
        spark_sum(when(col("buy_times") >= 2, 1).otherwise(0)).alias("repeat_buyers"),
        spark_sum("buy_times").alias("total_sales"),
    ).filter(col("total_buyers") >= 10)

    item_stats = item_stats.withColumn(
        "repurchase_rate", col("repeat_buyers") / col("total_buyers") * 100
    )

    item_stats.orderBy(col("repurchase_rate").desc()).show(n, truncate=False)


def long_tail_analysis(df: DataFrame) -> None:
    """长尾效应：Top 20% 商品贡献了多少销量"""
    print("\n" + "=" * 60)
    print("八、商品长尾效应分析")
    print("=" * 60)

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, percent_rank

    item_sales = df.filter(col("behavior_type") == "buy") \
                   .groupBy("item_id") \
                   .agg(count("*").alias("sales")) \
                   .orderBy(col("sales").desc())

    total_items = item_sales.count()
    total_sales = item_sales.agg(spark_sum("sales")).first()[0]

    # 累加销量占比
    w = Window.orderBy(col("sales").desc())
    item_sales = item_sales.withColumn("cum_pct",
        spark_sum("sales").over(w) / total_sales * 100)

    top_20_count = int(total_items * 0.2)
    sales_top20 = item_sales.limit(top_20_count).agg(spark_sum("sales")).first()[0]

    print(f"  总商品数: {total_items:,}")
    print(f"  Top 20% 商品 ({top_20_count:,} 个) 贡献销量: {sales_top20:,}")
    print(f"  销量占比: {sales_top20 / total_sales * 100:.1f}%")


def run_product_analysis(df: DataFrame) -> None:
    """一键执行商品全部分析"""
    top_products(df)
    category_penetration(df)
    repurchase_rate(df)
    long_tail_analysis(df)
