#!/bin/bash
# 一键提交到 YARN 集群（低内存配置）

unset PYSPARK_DRIVER_PYTHON
export PYSPARK_PYTHON=python3

spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 1g \
  --num-executors 2 \
  --executor-cores 1 \
  --driver-memory 1g \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.eventLog.enabled=false \
  --conf spark.rpc.askTimeout=600s \
  --conf spark.network.timeout=600s \
  --conf spark.sql.shuffle.partitions=24 \
  /root/spark-taobao/src/main.py
