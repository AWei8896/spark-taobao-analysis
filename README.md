# 基于 Spark 的电商用户行为分析

> 淘宝用户行为数据集 | 1亿条记录 | Hadoop + Spark 集群 | YARN 调度

---

## 项目简介

在 Hadoop + Spark 分布式集群上，对淘宝用户行为日志（pv/buy/cart/fav）进行端到端分析，覆盖流量概览、漏斗转化、商品销售、购买行为、时间模式五大维度，最终输出可视化图表与运营策略建议。

## 技术栈

| 层级 | 技术 |
|------|------|
| 分布式存储 | HDFS |
| 资源调度 | YARN |
| 计算引擎 | Spark (PySpark) |
| 语言 | Python 3 |
| 可视化 | Matplotlib |

## 集群架构

```
HDFS (NameNode + 3 DataNode)
  └── YARN (ResourceManager + 3 NodeManager)
        └── Spark (Master + 3 Worker)
              └── PySpark 分析任务
```

## 项目结构

```
spark-taobao-analysis/
├── README.md
├── requirements.txt
├── submit.sh                  # spark-submit 一键提交脚本
├── src/
│   ├── config.py              # SparkSession + HDFS 路径
│   ├── data_preprocess.py     # 数据加载、清洗、特征工程
│   ├── user_behavior.py       # 用户行为分析（漏斗/活跃度）
│   ├── product_analysis.py    # 商品分析（热销/复购/长尾）
│   ├── order_analysis.py      # 购买分析（时段/转化/频次）
│   ├── visualization.py       # 图表生成
│   └── main.py                # 主入口
└── output/                    # 分析图表
```

## 分析模块

| 模块 | 分析内容 | 核心指标 |
|------|------|------|
| 用户行为分析 | 流量概览、行为分布、漏斗转化、日活趋势 | PV/UV、漏斗转化率、DAU |
| 商品分析 | 热销排行、品类渗透、复购率、长尾效应 | Top20、复购率、品类渗透率 |
| 购买分析 | 时段分布、转化率趋势、周内模式、用户频次 | 转化率、购买时段、复购频次 |

## 快速开始

### 1. 环境要求

- Hadoop 集群已启动（`start-all.sh`）
- Spark 集群已启动（`$SPARK_HOME/sbin/start-all.sh`）
- 数据已上传 HDFS：`hdfs dfs -put UserBehavior.csv /data/taobao/input/`

### 2. 提交任务

```bash
# 上传代码到 Master 节点
scp -r src/ root@<master-ip>:/root/spark-taobao/

# 执行
bash /root/spark-taobao/submit.sh
```

### 3. 查看结果

```bash
# YARN UI
http://<master-ip>:8088

# Spark UI
http://<master-ip>:8080

# 图表输出
ls /root/spark-taobao/output/
```

## 数据字段

| 字段 | 说明 |
|------|------|
| user_id | 用户ID |
| item_id | 商品ID |
| category_id | 品类ID |
| behavior_type | pv(浏览) / buy(购买) / cart(加购) / fav(收藏) |
| timestamp | Unix 时间戳 |


