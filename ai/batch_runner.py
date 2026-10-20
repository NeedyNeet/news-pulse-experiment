"""批量分析运行器。

- 读 Parquet 里未分析的新闻
- 逐条跑三任务（带缓存，二次运行不重复请求）
- 结果写回 Parquet

注意：这里逐条而非一次带 N 条。常见做法是"一次 40 条"
是因为它只做摘要且追求成本；本作业要求摘要+情感+实体三类，
单条失败不应影响同批其他条，逐条更稳。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ai.cache import ResponseCache  # noqa: E402
from ai.client import call_with_retry, create_client  # noqa: E402
from ai.tasks import analyze_one  # noqa: E402

log = logging.getLogger("newspulse.ai.batch")


def _fix_spark_home() -> None:
    """兼容旧调用，实际逻辑已收到 storage.spark_env.fix_spark_env()。"""
    from storage.spark_env import fix_spark_env

    fix_spark_env()


def run_analysis(cfg: dict, limit: int = 50) -> int:
    """对数据湖里未分析的新闻做 AI 富化。返回处理条数。"""
    from storage.spark_env import spark_base_config

    from storage.sink import write_parquet_df

    _fix_spark_home()
    ai_cfg = cfg["ai"]
    client = create_client(ai_cfg)
    cache = ResponseCache(ai_cfg["cache_dir"])
    log.info("💾 缓存命中基数：%d 条", len(cache))

    lake = ROOT / cfg["storage"]["lake_dir"] / "news_raw"
    if not lake.exists():
        log.error("数据湖为空：%s，请先跑 streaming", lake)
        return 0

    spark = spark_base_config(cfg, "NewsPulse-AI").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(str(lake))
    # 只取还没分析过的
    if "analysis_status" in df.columns:
        df = df.filter((df.analysis_status.isNull()) | (df.analysis_status == ""))
    df = df.orderBy(df.ingested_ts.desc()).limit(limit)

    rows = df.select(
        "event_id", "title", "content", "source", "url", "lang", "publish_time"
    ).collect()
    log.info("🤖 待分析 %d 条", len(rows))
    if not rows:
        spark.stop()
        return 0

    enriched = []
    ok = failed = 0
    for i, r in enumerate(rows, 1):
        res = analyze_one(
            client, ai_cfg, cache, call_with_retry, r.title or "", r.content or ""
        )
        if res.status == "ok":
            ok += 1
        else:
            failed += 1
        enriched.append(
            {
                "event_id": r.event_id,
                "title": r.title,
                "content": r.content,
                "source": r.source,
                "url": r.url,
                "lang": r.lang,
                "publish_time": r.publish_time,
                **res.to_dict(),
            }
        )
        if i % 5 == 0 or i == len(rows):
            log.info("   进度 %d/%d（ok=%d failed=%d）", i, len(rows), ok, failed)

    # 写回独立目录，不覆盖原始湖（保留分层）
    out_dir = ROOT / cfg["storage"]["lake_dir"] / "ai_enriched"
    write_parquet_df(spark, enriched, out_dir)

    spark.stop()
    log.info("✅ 分析完成：ok=%d failed=%d → %s", ok, failed, out_dir)
    return len(enriched)


def main() -> None:
    # 日志走 stdout（默认是 stderr），这样 stderr 只剩 Spark 的 JVM 噪音，
    # run.ps1 可以安全地用 2>$null 抑制，不影响进度输出的可见性
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # httpx 每次 API 调用都会打一行 INFO，淹没进度信息，降到 WARNING
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    import argparse

    import yaml

    parser = argparse.ArgumentParser(description="NewsPulse AI batch")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    with (ROOT / "config.yaml").open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    run_analysis(cfg, limit=args.limit)


if __name__ == "__main__":
    main()
