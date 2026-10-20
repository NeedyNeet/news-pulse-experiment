"""Spark 环境自检与修复（Windows 特有两坑）。

1. SPARK_HOME 残留：系统级环境变量可能指向失效目录（如 C:\\spark），
   PySpark 会优先采信它而找不到 spark-submit。
2. winutils.exe：Spark 在 Windows 上做文件系统操作（写 Parquet、
   checkpoint、权限检查）需要 Hadoop 原生二进制，否则报
   HADOOP_HOME unset 或 UnsatisfiedLinkError。

所有需要启动 Spark 的模块（streaming / batch / ai）都应在
创建 SparkSession 之前调用 fix_spark_env()。
"""

from __future__ import annotations

import os
from pathlib import Path

# 项目根目录：storage/ 的上一级
ROOT = Path(__file__).resolve().parent.parent


def fix_spark_env() -> None:
    """在当前进程设置好 SPARK_HOME / HADOOP_HOME / PATH。"""
    import pyspark

    # 1) SPARK_HOME 指向 venv 内的 pyspark
    spark_home = os.path.dirname(os.path.abspath(pyspark.__file__))
    if os.environ.get("SPARK_HOME") != spark_home:
        os.environ["SPARK_HOME"] = spark_home

    # 2) winutils 放在 infra/hadoop
    hadoop_home = ROOT / "infra" / "hadoop"
    winutils = hadoop_home / "bin" / "winutils.exe"
    if winutils.exists():
        hadoop_home_str = str(hadoop_home)
        if os.environ.get("HADOOP_HOME") != hadoop_home_str:
            os.environ["HADOOP_HOME"] = hadoop_home_str
            os.environ["hadoop.home.dir"] = hadoop_home_str
        bin_dir = str(hadoop_home / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = os.environ.get("PATH", "") + ";" + bin_dir


def spark_base_config(cfg: dict, app_name: str):
    """构造 SparkSession builder 的公共部分。"""
    fix_spark_env()
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", cfg["spark"]["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(cfg["spark"]["shuffle_partitions"]))
        .config("spark.ui.enabled", "false")
        # 临时目录固定到项目内，避免系统 temp 下的 jar 被占用时退出报
        # "Exception while deleting Spark temp dir"（无害但看着像错误）
        .config("spark.local.dir", str(ROOT / "data" / "spark-tmp"))
        # connector 版本必须与 Spark 完全一致，否则 NoSuchMethodError
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
        # Spark 4.x + JDK 17：关闭向量化读与 incubator 模块依赖，
        # 否则 local 模式下报 BlockManagerId.executorId() NPE
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        # 关掉 ANSI 严格模式：日期解析失败返回 NULL 而非抛异常。
        # 真实 RSS 的时间格式很杂，不能让一条脏数据挂掉整条流。
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    )
