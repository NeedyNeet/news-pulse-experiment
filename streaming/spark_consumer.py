"""Spark Structured Streaming consumer。

五段流水线：
    1. 从 Kafka 读 JSON
    2. 清洗（去 HTML、时间解析、字段规范）
    3. 去重（URL 归一化 + watermark 窗口）
    4. 实时聚合（每分钟新闻量 / 来源分布 / 关键词频率）
    5. 输出（原始 → Parquet，聚合 → console）

设计要点：采集层容错、URL 归一化去重。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import (  # noqa: E402
    coalesce,
    col,
    explode,
    from_json,
    length,
    lower,
    regexp_replace,
    split,
    to_timestamp,
    trim,
    window,
)
from pyspark.sql.types import (  # noqa: E402
    StringType,
    StructField,
    StructType,
)
from producer.stopwords import STOPWORDS  # noqa: E402
from storage.spark_env import spark_base_config  # noqa: E402

log = logging.getLogger("newspulse.streaming")

# ── 消息 schema（与 producer/schemas.py 对齐）──
NEWS_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("title", StringType()),
        StructField("content", StringType()),
        StructField("publish_time", StringType()),
        StructField("source", StringType()),
        StructField("url", StringType()),
        StructField("lang", StringType()),
        StructField("ingested_at", StringType()),
    ]
)


def build_spark(cfg: dict) -> SparkSession:
    return spark_base_config(cfg, cfg["spark"]["app_name"]).getOrCreate()


def read_from_kafka(spark: SparkSession, cfg: dict):
    kafka_cfg = cfg["kafka"]
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg["bootstrap_servers"])
        .option("subscribe", kafka_cfg["topic"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")  # demo 数据丢了不要崩
        .load()
    )


def clean_and_dedup(raw_df):
    """阶段 2+3：解析 JSON → 清洗 → 去重。"""
    parsed = raw_df.select(
        from_json(col("value").cast("string"), NEWS_SCHEMA).alias("d")
    ).select("d.*")

    cleaned = (
        parsed.filter(col("title").isNotNull() & col("url").isNotNull())
        .withColumn("title", trim(regexp_replace(col("title"), r"<[^>]+>", "")))
        .withColumn("content", trim(regexp_replace(col("content"), r"<[^>]+>", "")))
        .withColumn("source", trim(col("source")))
        .withColumn("lang", lower(trim(col("lang"))))
        # URL 归一化：去尾斜杠，作为去重键
        .withColumn("url_key", lower(regexp_replace(col("url"), r"/+$", "")))
        # 时间解析：RSS 用 RFC-2822，producer 用 ISO8601。
        # Spark datetime pattern 不支持星期名（EEE），先剥掉 "Mon, " 前缀。
        # 全部用 try_to_timestamp / try_cast 语义：解析失败返回 NULL 而非报错，
        # 因为脏时间戳不应让整条流挂掉。
        .withColumn("_pt_nodow", regexp_replace(col("publish_time"), r"^[A-Za-z]{3},\s*", ""))
        .withColumn("_ia_nodow", regexp_replace(col("ingested_at"), r"^[A-Za-z]{3},\s*", ""))
        .withColumn(
            "publish_ts",
            coalesce(
                to_timestamp(col("_pt_nodow"), "dd MMM yyyy HH:mm:ss Z"),
                to_timestamp(col("_pt_nodow"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
                to_timestamp(col("_pt_nodow")),
            ),
        )
        .withColumn(
            "ingested_ts",
            coalesce(
                to_timestamp(col("_ia_nodow"), "dd MMM yyyy HH:mm:ss Z"),
                to_timestamp(col("_ia_nodow"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
                to_timestamp(col("_ia_nodow")),
            ),
        )
        .drop("_pt_nodow", "_ia_nodow")
    )

    # watermark + dropDuplicates：同一 URL 在 10 分钟窗口内只保留一次
    # 思路：跨源同一 URL 视为重复，保留信息最全的一条，
    # 但流式场景下用 URL 精确去重比 AI 语义去重更可行
    deduped = (
        cleaned.withWatermark("ingested_ts", "10 minutes")
        .dropDuplicates(["url_key"])
    )
    return deduped


def aggregate(df):
    """阶段 4：三个实时聚合指标（作业点名要求）。"""
    per_minute = df.groupBy(window(col("ingested_ts"), "1 minute").alias("w")).count()

    per_source = df.groupBy(col("source")).count()

    # 关键词频率：标题小写切词，用 length() 过滤短词（不用正则量词，
    # 避免 Python 正则转 Java 正则时的解析差异）
    words = df.select(
        explode(
            split(lower(regexp_replace(col("title"), r"[^\w\s\u4e00-\u9fff]", " ")), r"\s+")
        ).alias("word")
    ).filter((col("word") != "") & (col("word").isNotNull()) & (length(col("word")) >= 2))

    # 过滤停用词：says / are / in / for 等虚词没有舆情价值。
    # 用 isin 把停用词表下推到 Spark（列名取反），避免在流里跑 Python UDF。
    stop = sorted(STOPWORDS)
    if stop:
        words = words.filter(~col("word").isin(stop))

    keywords = words.groupBy(col("word")).count()
    return per_minute, per_source, keywords


def write_parquet(df, path: str, checkpoint: str, mode: str = "append"):
    """写 Parquet 流。

    Parquet sink 只支持 append 模式，聚合结果（per_minute 等）无法直接
    用 complete/update 落盘。聚合的落盘由 write_agg_batch() 负责，
    这里只处理 append 场景（原始新闻）。
    """
    return (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode(mode)
        .trigger(processingTime="5 seconds")
        .start()
    )


def write_agg_batch(df, epoch_id: int, path: str) -> None:
    """foreachBatch 回调：把每个 micro-batch 的聚合结果静态写入 Parquet。

    这是流式聚合落盘的通行做法 —— 在批内转成普通 DataFrame，
    绕开 Parquet sink 不支持 complete/update 的限制。
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    df.write.mode("append").parquet(str(path_obj))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    import yaml

    with (ROOT / "config.yaml").open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    raw = read_from_kafka(spark, cfg)
    cleaned = clean_and_dedup(raw)
    per_minute, per_source, keywords = aggregate(cleaned)

    lake = ROOT / cfg["storage"]["lake_dir"]
    ckpt = ROOT / cfg["spark"]["checkpoint_dir"]
    (lake / "news_raw").mkdir(parents=True, exist_ok=True)
    (lake / "agg_minute").mkdir(parents=True, exist_ok=True)
    (lake / "agg_source").mkdir(parents=True, exist_ok=True)
    (lake / "agg_keyword").mkdir(parents=True, exist_ok=True)

    # 注意：Parquet sink 只支持 append 模式，聚合结果用 foreachBatch 落盘。
    # complete 只用于 console sink（终端观察）。
    queries = [
        write_parquet(cleaned.drop("url_key"), str(lake / "news_raw"), str(ckpt / "raw")),
        (per_minute.writeStream.outputMode("update")
            .foreachBatch(lambda df, eid: write_agg_batch(df, eid, str(lake / "agg_minute")))
            .option("checkpointLocation", str(ckpt / "minute"))
            .trigger(processingTime="5 seconds").start()),
        (per_source.writeStream.outputMode("update")
            .foreachBatch(lambda df, eid: write_agg_batch(df, eid, str(lake / "agg_source")))
            .option("checkpointLocation", str(ckpt / "source"))
            .trigger(processingTime="5 seconds").start()),
        (keywords.writeStream.outputMode("update")
            .foreachBatch(lambda df, eid: write_agg_batch(df, eid, str(lake / "agg_keyword")))
            .option("checkpointLocation", str(ckpt / "keyword"))
            .trigger(processingTime="5 seconds").start()),
    ]

    # console sink：便于终端观察实时聚合
    console_q = (
        per_minute.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")     # 布尔值，true 表示截断长字段
        .option("numRows", "10")
        .trigger(processingTime="5 seconds")
        .start()
    )
    queries.append(console_q)

    log.info("🚀 流处理已启动，%d 个查询", len(queries))
    log.info("   Parquet → %s", lake)
    log.info("   Ctrl+C 停止")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("⏹ 收到中断，停止流")
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:  # noqa: BLE001
                pass
        spark.stop()


if __name__ == "__main__":
    main()
