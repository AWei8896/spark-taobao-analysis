import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示中文
plt.rcParams['axes.unicode_minus'] = False

data = [
    ("周日", 463401),
    ("周六", 459052),
    ("周一", 226835),
    ("周三", 223072),
    ("周四", 221463),
    ("周二", 212000),
    ("周五", 210016),
]

order = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
data.sort(key=lambda x: order.index(x[0]))
labels = [d[0] for d in data]
values = [d[1] for d in data]

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(labels, values, color="#55A868", alpha=0.85, edgecolor="white")

for bar, val in zip(bars, values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5000,
            f"{val:,}", ha="center", va="bottom", fontsize=11)

ax.set_xlabel("星期", fontsize=13)
ax.set_ylabel("购买量", fontsize=13)
ax.set_title("周内购买行为分布（淘宝用户行为 2017.11-12）", fontsize=15, fontweight="bold")
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig("weekday_purchase_cn.png", dpi=200, bbox_inches="tight", facecolor="white")
print("已保存 weekday_purchase_cn.png")
