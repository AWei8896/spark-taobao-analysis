"""
可视化
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd

# 中文字体
_cjk = ["SimHei", "Microsoft YaHei", "KaiTi", "SimSun"]
_avail = {f.name for f in fm.fontManager.ttflist}
_found = next((f for f in _cjk if f in _avail), None)
if _found:
    plt.rcParams["font.sans-serif"] = [_found] + plt.rcParams["font.sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

OUTPUT_DIR = "/root/spark-taobao/output"


def save(fig, name: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = f"{OUTPUT_DIR}/{name}"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"  [CHART] {path}")
    plt.close(fig)


def plot_behavior_pie(spark_df):
    """行为类型占比饼图"""
    data = spark_df.toPandas()
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(data["cnt"], labels=data["behavior_type"], autopct="%1.1f%%",
           colors=["#4C72B0", "#DD8452", "#55A868", "#C44E52"],
           startangle=90)
    ax.set_title("User behavior distribution", fontsize=14, fontweight="bold")
    save(fig, "01_behavior_pie.png")


def plot_daily_trend(spark_df):
    """日活跃趋势折线图"""
    data = spark_df.orderBy("date").toPandas()
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(data["date"], data["DAU"], "o-", color="#4C72B0", linewidth=2, label="DAU")
    ax.set_xlabel("Date")
    ax.set_ylabel("DAU")
    ax.set_title("Daily Active Users trend", fontsize=14, fontweight="bold")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.xticks(rotation=45)
    save(fig, "02_daily_trend.png")


def plot_hourly_pattern(spark_df):
    """24小时购买分布图"""
    data = spark_df.orderBy("hour").toPandas()
    fig, ax1 = plt.subplots(figsize=(14, 5))
    ax1.bar(data["hour"], data["total_actions"], color="#4C72B0", alpha=0.6, label="Total actions")
    ax2 = ax1.twinx()
    ax2.plot(data["hour"], data["conversion"], "o-", color="#C44E52", linewidth=2, label="Conversion %")
    ax1.set_xlabel("Hour")
    ax1.set_ylabel("Actions")
    ax2.set_ylabel("Conversion (%)")
    ax1.set_title("Hourly activity & conversion rate", fontsize=14, fontweight="bold")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    ax1.grid(alpha=0.3)
    save(fig, "03_hourly_pattern.png")


def plot_weekday_purchase(spark_df):
    """周内购买分布柱状图"""
    data = spark_df.toPandas()
    order = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    data["day_of_week"] = pd.Categorical(data["day_of_week"], categories=order, ordered=True)
    data = data.sort_values("day_of_week")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(data["day_of_week"], data["buy_count"], color="#55A868", alpha=0.85)
    ax.set_xlabel("Day of week")
    ax.set_ylabel("Purchase count")
    ax.set_title("Weekly purchase distribution", fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    save(fig, "04_weekday_purchase.png")


def plot_conversion_trend(spark_df):
    """每日转化率趋势"""
    data = spark_df.orderBy("date").toPandas()
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(data["date"], data["pv_to_buy"], "o-", color="#C44E52", linewidth=2)
    ax.set_xlabel("Date")
    ax.set_ylabel("PV → Buy Conversion (%)")
    ax.set_title("Daily purchase conversion rate trend", fontsize=14, fontweight="bold")
    ax.grid(alpha=0.3)
    plt.xticks(rotation=45)
    save(fig, "05_conversion_trend.png")
