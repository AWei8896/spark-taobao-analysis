"""
用户行为分析
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, round as spark_round


def traffic_overview(df: DataFrame) -> None:
    """整体流量概览：PV / UV / 人均浏览深度"""
    print("\n" + "=" * 60)
    print("一、整体流量概览")
    print("=" * 60)

    total_pv = df.filter(col("behavior_type") == "pv").count()
    total_uv = df.select("user_id").distinct().count()
    avg_depth = total_pv / total_uv if total_uv > 0 else 0

    print(f"总 PV (浏览量): {total_pv:>15,}")
    print(f"总 UV (独立访客): {total_uv:>13,}")
    print(f"人均浏览深度:    {avg_depth:>15.1f} 页/人")


def behavior_distribution(df: DataFrame) -> None:
    print("\n" + "=" * 60)
    print("二、行为类型分布")
    print("=" * 60)

    df.groupBy("behavior_type").agg(count("*").alias("cnt")) \
        .withColumn("pct", spark_round(col("cnt") / df.count() * 100, 2)) \
        .orderBy(col("cnt").desc()) \
        .show()


def funnel_analysis(df: DataFrame) -> None:
    print("\n" + "=" * 60)
    print("三、用户行为转化漏斗")
    print("=" * 60)

    def _count_users(behavior: str) -> int:
        return df.filter(col("behavior_type") == behavior) \
                 .select("user_id").distinct().count()

    pv_users = _count_users("pv")
    cart_users = _count_users("cart")
    fav_users = _count_users("fav")
    buy_users = _count_users("buy")

    steps = [
        ("浏览 (pv)", pv_users),
        ("加购 (cart)", cart_users),
        ("收藏 (fav)", fav_users),
        ("购买 (buy)", buy_users),
    ]

    print(f"{'环节':<16s} {'用户数':>12s} {'整体转化率':>12s} {'环节转化率':>12s}")
    print(f"{'-' * 52}")
    for i, (name, cnt) in enumerate(steps):
        overall_rate = f"{cnt / pv_users * 100:.2f}%" if pv_users > 0 else "-"
        step_rate = f"{cnt / steps[i-1][1] * 100:.2f}%" if i > 0 and steps[i-1][1] > 0 else "-"
        print(f"  {name:<16s} {cnt:>12,d} {overall_rate:>12s} {step_rate:>12s}")


def daily_active_analysis(df: DataFrame) -> None:
    """每日活跃趋势：DAU+各行为日趋势"""
    print("\n" + "=" * 60)
    print("四、每日活跃趋势 (DAU)")
    print("=" * 60)

    df.groupBy("date").agg(
        countDistinct("user_id").alias("DAU"),
        count("*").alias("total_actions"),
        countDistinct("item_id").alias("active_items"),
    ).orderBy("date").show(20, truncate=False)


def run_behavior_analysis(df: DataFrame) -> None:
    """一键执行用户行为全部分析"""
    traffic_overview(df)
    behavior_distribution(df)
    funnel_analysis(df)
    daily_active_analysis(df)
