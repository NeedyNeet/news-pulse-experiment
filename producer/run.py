"""producer 入口：RSS 采集 / 数据集重放 → Kafka。

用法：
    python -m producer.run --source rss
    python -m producer.run --source replay --rate 5
    python -m producer.run --gen-sample 60      # 只生成模拟数据
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from producer.kafka_producer import NewsKafkaProducer  # noqa: E402
from producer.replay import load_jsonl, make_sample, replay  # noqa: E402
from producer.rss_fetcher import fetch_all  # noqa: E402


def load_config() -> dict:
    with (ROOT / "config.yaml").open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def main() -> None:
    setup_logging()
    log = logging.getLogger("newspulse.run")

    parser = argparse.ArgumentParser(description="NewsPulse producer")
    parser.add_argument("--source", choices=["rss", "replay"], default="replay")
    parser.add_argument("--rate", type=float, default=None, help="重放速率（条/秒）")
    parser.add_argument("--gen-sample", type=int, default=0, help="生成 N 条模拟数据后退出")
    args = parser.parse_args()

    cfg = load_config()
    replay_cfg = cfg["replay"]

    if args.gen_sample:
        make_sample(ROOT / replay_cfg["data_file"], args.gen_sample)
        return

    # 数据集不存在时自动生成，保证 demo 可跑
    sample_path = ROOT / replay_cfg["data_file"]
    if args.source == "replay" and not sample_path.exists():
        log.warning("数据集不存在，自动生成 60 条模拟数据")
        make_sample(sample_path, 60)

    kafka_cfg = cfg["kafka"]
    producer = NewsKafkaProducer(kafka_cfg["bootstrap_servers"], kafka_cfg["topic"])
    try:
        if args.source == "rss":
            src_cfg = cfg["sources"]["rss"]
            events = fetch_all(
                src_cfg["feeds"],
                max_entries=src_cfg["max_entries"],
                workers=src_cfg["workers"],
                per_host=src_cfg["per_host"],
            )
            if not events:
                log.error("未采集到任何新闻，退出")
                sys.exit(1)
            sent = sum(1 for ev in events if producer.send(ev))
        else:
            rate = args.rate if args.rate is not None else replay_cfg["rate_per_sec"]
            events = load_jsonl(sample_path)
            log.info("▶️ 开始重放 %d 条（速率 %.1f 条/秒）", len(events), rate)
            sent = replay(events, producer.send, rate_per_sec=rate)

        producer.flush_and_close()
        log.info("✅ 发送完成：%d/%d 条 → topic %s", sent, len(events), kafka_cfg["topic"])
    finally:
        try:
            producer.flush_and_close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
