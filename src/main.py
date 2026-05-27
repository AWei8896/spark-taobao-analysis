import time
from pyspark.sql.functions import col
from config import get_spark, HDFS_INPUT
from data_preprocess import load_and_clean
from user_behavior import run_behavior_analysis
from product_analysis import run_product_analysis
from order_analysis import run_order_analysis

def main():
    start = time.time()

    print("=" * 60)
    print("淘宝用户行为分析 — Spark 集群版")
    print("数据: UserBehavior.csv (HDFS)")
    print("=" * 60)

    # ---- 0. 创建 SparkSession ----
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    #  数据预处理
    print("\n[Step 1/5] 加载 & 预处理数据...")
    df = load_and_clean(spark, HDFS_INPUT)
    df.createOrReplaceTempView("user_behavior")

    #  用户行为分析
    print("\n[Step 2/5] 用户行为分析...")
    run_behavior_analysis(df)

    # 商品分析
    print("\n[Step 3/5] 商品销售分析...")
    run_product_analysis(df)

    # 购买分析
    print("\n[Step 4/5] 购买行为分析...")
    run_order_analysis(df)

    # 可视化
    print("\n[Step 5/5] 生成可视化图表...")
    try:
        from visualization import (
            plot_behavior_pie, plot_daily_trend, plot_hourly_pattern,
            plot_weekday_purchase, plot_conversion_trend,
        )
        from pyspark.sql.functions import count, when, countDistinct

        # 提取聚合数据用于画图
        behavior_df = df.groupBy("behavior_type").agg(count("*").alias("cnt"))
        daily_df = df.groupBy("date").agg(
            countDistinct("user_id").alias("DAU"),
        )
        hourly_df = df.groupBy("hour").agg(
            count("*").alias("total_actions"),
            count(when(col("behavior_type") == "buy", 1)).alias("buy_count"),
        ).withColumn("conversion", col("buy_count") / col("total_actions") * 100)
        weekday_df = df.filter(col("behavior_type") == "buy") \
                       .groupBy("day_of_week").agg(count("*").alias("buy_count"))
        conversion_df = df.groupBy("date").agg(
            count(when(col("behavior_type") == "pv", 1)).alias("pv"),
            count(when(col("behavior_type") == "buy", 1)).alias("buy"),
        ).withColumn("pv_to_buy", col("buy") / col("pv") * 100)

        plot_behavior_pie(behavior_df)
        plot_daily_trend(daily_df)
        plot_hourly_pattern(hourly_df)
        plot_weekday_purchase(weekday_df)
        plot_conversion_trend(conversion_df)

        print("  可视化完成: /root/spark-taobao/output/")
    except Exception as e:
        print(f"  [WARN] 可视化跳过: {e}")

    # 代码运行完成时间
    elapsed = time.time() - start
    print("\n" + "=" * 60)
    print(f"  全部分析完成! 耗时 {elapsed:.1f} 秒")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
