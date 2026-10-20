"""批处理分析：Spark SQL 热点识别 + 趋势统计。

与 streaming/ 的区别：这是**批式**读 Parquet，跑完即退，
对应作业"定期（每小时/每天）对存储数据做批量分析"。

可挂 cron / Windows 任务计划实现定期执行。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import col, count, desc, explode, length, lower, regexp_replace, split  # noqa: E402

from producer.stopwords import STOPWORDS  # noqa: E402
from storage.spark_env import spark_base_config  # noqa: E402

log = logging.getLogger("newspulse.batch")


def build_spark(cfg: dict) -> SparkSession:
    return spark_base_config(cfg, "NewsPulse-Batch").getOrCreate()


def run_hotspot_job(cfg: dict, top_n: int = 20) -> dict:
    """热点新闻识别 + 关键词趋势，结果写 SQLite。"""
    from storage.sink import SqliteSink

    lake_dir = ROOT / cfg["storage"]["lake_dir"]

    # 优先用 AI 富化后的数据（带摘要），否则退回原始湖
    enriched = lake_dir / "ai_enriched"
    raw = lake_dir / "news_raw"
    target = enriched if enriched.exists() else raw
    if not target.exists():
        log.error("数据湖为空：%s", lake_dir)
        return {"hotspots": [], "trends": []}

    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(str(target))
    has_sentiment = "sentiment" in df.columns
    df.createOrReplaceTempView("news")

    # 热点：同一标题在多源重复出现 = 关注度高（跨源重复度作为热度信号）
    # sentiment 列只在 AI 富化后才存在，用 COALESCE 兜底
    if has_sentiment:
        score_expr = (
            "MAX(COALESCE("
            "CASE WHEN sentiment = 'positive' THEN 1.0 "
            "WHEN sentiment = 'negative' THEN 0.5 "
            "ELSE 0.7 END, 0.7))"
        )
    else:
        score_expr = "MAX(0.7)"

    hotspots_sql = f"""
        SELECT
            ANY_VALUE(url)      AS url,
            title,
            COUNT(*)            AS mentions,
            {score_expr}        AS score
        FROM news
        GROUP BY title
        ORDER BY mentions DESC, score DESC
        LIMIT {top_n}
    """
    hotspots = [r.asDict() for r in spark.sql(hotspots_sql).collect()]

    # 关键词趋势：标题切词计数
    # 注意：filter 的长度判断用 length() 而非正则量词，避免 Java 正则解析问题
    words = df.select(
        explode(
            split(lower(regexp_replace(col("title"), r"[^\w\s\u4e00-\u9fff]", " ")), r"\s+")
        ).alias("word")
    ).filter((col("word") != "") & (col("word").isNotNull()) & (length(col("word")) >= 2))

    # 过滤停用词，与 streaming 保持一致
    if STOPWORDS:
        words = words.filter(~col("word").isin(sorted(STOPWORDS)))

    # score 列用 Python 侧转 float，mentions 转 int，避免 Decimal 进 SQLite
    for h in hotspots:
        if h.get("mentions") is not None:
            h["mentions"] = int(h["mentions"])
        if h.get("score") is not None:
            h["score"] = float(h["score"])
        h.setdefault("window_start", "")

    # 字段名对齐 SQLite：word → keyword，count → cnt
    trends = []
    for r in words.groupBy("word").count().orderBy(desc("count")).limit(30).collect():
        d = r.asDict()
        trends.append({
            "keyword": d.get("word"),
            "cnt": int(d.get("count") or 0),
            "window_start": "",
        })

    sink = SqliteSink(ROOT / cfg["storage"]["sqlite_path"])
    sink.insert_hotspots(hotspots)
    sink.insert_trends(trends)

    spark.stop()
    log.info("✅ 批处理完成：热点 %d 条，趋势词 %d 个", len(hotspots), len(trends))
    return {"hotspots": hotspots, "trends": trends}


def main() -> None:
    # 日志走 stdout（默认是 stderr），这样 stderr 只剩 Spark 的 JVM 噪音，
    # run.ps1 可以安全地用 2>$null 抑制，不影响进度输出的可见性
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    import argparse

    import yaml

    parser = argparse.ArgumentParser(description="NewsPulse batch hotspot job")
    parser.add_argument("--top-n", type=int, default=20)
    args = parser.parse_args()

    with (ROOT / "config.yaml").open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    run_hotspot_job(cfg, top_n=args.top_n)


if __name__ == "__main__":
    main()
